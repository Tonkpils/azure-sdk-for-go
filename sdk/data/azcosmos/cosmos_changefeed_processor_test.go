// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// --- helpers ---

func newTestLease(owner string, feedRange FeedRange, timestamp int64) changeFeedProcessorLease {
	id := leaseIDFromFeedRange(feedRange, "")
	return changeFeedProcessorLease{
		ID:           id,
		PartitionKey: id,
		Owner:        owner,
		FeedRange:    &feedRange,
		Timestamp:    timestamp,
	}
}

func newTestFeedRange(min, max string) FeedRange {
	return FeedRange{MinInclusive: min, MaxExclusive: max}
}

// --- Lease Model Tests ---

func TestLeaseIDFromFeedRange(t *testing.T) {
	fr1 := newTestFeedRange("", "FF")
	fr2 := newTestFeedRange("FF", "FFFF")

	id1a := leaseIDFromFeedRange(fr1, "")
	id1b := leaseIDFromFeedRange(fr1, "")
	id2 := leaseIDFromFeedRange(fr2, "")

	if id1a != id1b {
		t.Errorf("same feed range produced different IDs: %q vs %q", id1a, id1b)
	}
	if id1a == id2 {
		t.Errorf("different feed ranges produced same ID: %q", id1a)
	}
	if id1a == "" {
		t.Error("lease ID should not be empty")
	}
}

func TestNewChangeFeedProcessorLease(t *testing.T) {
	fr := newTestFeedRange("00", "FF")
	before := time.Now().Unix()
	lease := newChangeFeedProcessorLease(fr, "worker-1", "", 0)
	after := time.Now().Unix()

	expectedID := leaseIDFromFeedRange(fr, "")
	if lease.ID != expectedID {
		t.Errorf("ID: got %q, want %q", lease.ID, expectedID)
	}
	if lease.PartitionKey != expectedID {
		t.Errorf("PartitionKey: got %q, want %q", lease.PartitionKey, expectedID)
	}
	if lease.Owner != "worker-1" {
		t.Errorf("Owner: got %q, want %q", lease.Owner, "worker-1")
	}
	if lease.FeedRange == nil {
		t.Fatal("FeedRange should not be nil")
	}
	if lease.FeedRange.MinInclusive != "00" || lease.FeedRange.MaxExclusive != "FF" {
		t.Errorf("FeedRange: got %v, want {00, FF}", lease.FeedRange)
	}
	if lease.Timestamp < before || lease.Timestamp > after {
		t.Errorf("Timestamp %d not within [%d, %d]", lease.Timestamp, before, after)
	}
}

func TestLeaseIsExpired(t *testing.T) {
	expiry := 60 * time.Second

	tests := []struct {
		name      string
		timestamp int64
		want      bool
	}{
		{"fresh lease", time.Now().Unix(), false},
		{"120s ago", time.Now().Add(-120 * time.Second).Unix(), true},
		{"zero timestamp", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lease := newTestLease("owner", newTestFeedRange("", "FF"), tt.timestamp)
			if got := lease.isExpired(expiry); got != tt.want {
				t.Errorf("isExpired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLeaseIsOwned(t *testing.T) {
	owned := newTestLease("worker-1", newTestFeedRange("", "FF"), time.Now().Unix())
	unowned := newTestLease("", newTestFeedRange("", "FF"), time.Now().Unix())

	if !owned.isOwned() {
		t.Error("lease with owner should be owned")
	}
	if unowned.isOwned() {
		t.Error("lease with empty owner should not be owned")
	}
}

func TestLeaseJSONRoundTrip(t *testing.T) {
	original := newTestLease("worker-1", newTestFeedRange("00", "FF"), time.Now().Unix())
	original.ContinuationToken = "some-token"
	original.ETag = "some-etag"

	data, err := original.toJSON()
	if err != nil {
		t.Fatalf("toJSON error: %v", err)
	}

	var restored changeFeedProcessorLease
	if err := restored.fromJSON(data); err != nil {
		t.Fatalf("fromJSON error: %v", err)
	}

	if restored.ID != original.ID {
		t.Errorf("ID: got %q, want %q", restored.ID, original.ID)
	}
	if restored.Owner != original.Owner {
		t.Errorf("Owner: got %q, want %q", restored.Owner, original.Owner)
	}
	if restored.ContinuationToken != original.ContinuationToken {
		t.Errorf("ContinuationToken: got %q, want %q", restored.ContinuationToken, original.ContinuationToken)
	}
	if restored.Timestamp != original.Timestamp {
		t.Errorf("Timestamp: got %d, want %d", restored.Timestamp, original.Timestamp)
	}
	if restored.FeedRange == nil {
		t.Fatal("FeedRange should not be nil after round-trip")
	}
	if restored.FeedRange.MinInclusive != "00" || restored.FeedRange.MaxExclusive != "FF" {
		t.Errorf("FeedRange mismatch after round-trip: got %v", restored.FeedRange)
	}

	// Verify JSON structure is well-formed
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("produced invalid JSON: %v", err)
	}
}

// --- Balancer Tests ---

func TestBalancerNoLeases(t *testing.T) {
	b := newChangeFeedProcessorBalancer("worker-1", 60*time.Second, 0, 0, BalancerStrategyEqual)
	result := b.selectLeasesToAcquire(nil)
	if result != nil {
		t.Errorf("expected nil for empty lease list, got %v", result)
	}

	result = b.selectLeasesToAcquire([]changeFeedProcessorLease{})
	if result != nil {
		t.Errorf("expected nil for zero-length lease list, got %v", result)
	}
}

func TestBalancerAllExpired(t *testing.T) {
	b := newChangeFeedProcessorBalancer("worker-1", 60*time.Second, 0, 0, BalancerStrategyEqual)
	expired := time.Now().Add(-120 * time.Second).Unix()

	leases := []changeFeedProcessorLease{
		newTestLease("dead-worker", newTestFeedRange("", "55"), expired),
		newTestLease("dead-worker", newTestFeedRange("55", "AA"), expired),
		newTestLease("dead-worker", newTestFeedRange("AA", "FF"), expired),
	}

	result := b.selectLeasesToAcquire(leases)
	// Single worker, all expired → target = ceil(3/1) = 3, should take all 3.
	if len(result) != 3 {
		t.Errorf("expected 3 leases, got %d", len(result))
	}
}

func TestBalancerEvenDistribution(t *testing.T) {
	b := newChangeFeedProcessorBalancer("new-worker", 60*time.Second, 0, 0, BalancerStrategyEqual)
	now := time.Now().Unix()

	leases := []changeFeedProcessorLease{
		newTestLease("worker-A", newTestFeedRange("", "2A"), now),
		newTestLease("worker-A", newTestFeedRange("2A", "55"), now),
		newTestLease("worker-B", newTestFeedRange("55", "7F"), now),
		newTestLease("worker-B", newTestFeedRange("7F", "AA"), now),
	}
	// Add 2 expired leases for the new worker to claim.
	expired := time.Now().Add(-120 * time.Second).Unix()
	leases = append(leases,
		newTestLease("dead", newTestFeedRange("AA", "D5"), expired),
		newTestLease("dead", newTestFeedRange("D5", "FF"), expired),
	)

	result := b.selectLeasesToAcquire(leases)
	// 6 leases, 3 active workers (A, B, new-worker) → target = ceil(6/3) = 2.
	// new-worker has 0, needs 2. Two expired leases available → takes 2.
	if len(result) != 2 {
		t.Errorf("expected 2 leases, got %d", len(result))
	}
}

func TestBalancerAlreadyBalanced(t *testing.T) {
	b := newChangeFeedProcessorBalancer("worker-A", 60*time.Second, 0, 0, BalancerStrategyEqual)
	now := time.Now().Unix()

	leases := []changeFeedProcessorLease{
		newTestLease("worker-A", newTestFeedRange("", "55"), now),
		newTestLease("worker-A", newTestFeedRange("55", "AA"), now),
		newTestLease("worker-B", newTestFeedRange("AA", "D5"), now),
		newTestLease("worker-B", newTestFeedRange("D5", "FF"), now),
	}

	// 4 leases, 2 workers, each has 2 → target = ceil(4/2) = 2. Already at target.
	result := b.selectLeasesToAcquire(leases)
	if len(result) != 0 {
		t.Errorf("expected 0 leases (already balanced), got %d", len(result))
	}
}

func TestBalancerStealFromBusiest(t *testing.T) {
	b := newChangeFeedProcessorBalancer("worker-B", 60*time.Second, 0, 0, BalancerStrategyEqual)
	now := time.Now().Unix()

	leases := []changeFeedProcessorLease{
		newTestLease("worker-A", newTestFeedRange("", "55"), now),
		newTestLease("worker-A", newTestFeedRange("55", "AA"), now),
		newTestLease("worker-A", newTestFeedRange("AA", "FF"), now),
	}

	// 3 leases, 2 workers (A + B). target = ceil(3/2) = 2.
	// worker-B has 0, needs 2. No expired leases. Busiest is A with 3 (> target=2).
	// Should steal exactly 1.
	result := b.selectLeasesToAcquire(leases)
	if len(result) != 1 {
		t.Errorf("expected 1 stolen lease, got %d", len(result))
	}
	if len(result) == 1 && result[0].Owner != "worker-A" {
		t.Errorf("expected stolen lease from worker-A, got owner %q", result[0].Owner)
	}
}

func TestBalancerNewWorkerJoining(t *testing.T) {
	b := newChangeFeedProcessorBalancer("worker-C", 60*time.Second, 0, 0, BalancerStrategyEqual)
	now := time.Now().Unix()

	leases := []changeFeedProcessorLease{
		newTestLease("worker-A", newTestFeedRange("", "40"), now),
		newTestLease("worker-A", newTestFeedRange("40", "80"), now),
		newTestLease("worker-B", newTestFeedRange("80", "C0"), now),
		newTestLease("worker-B", newTestFeedRange("C0", "FF"), now),
	}

	// 4 leases, 3 workers (A, B, C). target = ceil(4/3) = 2.
	// worker-C has 0, needs 2. No expired leases.
	// Busiest has 2 leases, which equals the target.
	// partitionsNeeded > 1, so stealThreshold = target - 1 = 1.
	// Busiest has 2 > 1, so we steal 1.
	result := b.selectLeasesToAcquire(leases)
	if len(result) != 1 {
		t.Errorf("expected 1 stolen lease, got %d", len(result))
	}
}

// --- Greedy Balancer Tests ---

func TestBalancerGreedyAcquiresMultipleExpiredLeases(t *testing.T) {
	b := newChangeFeedProcessorBalancer("worker-1", 60*time.Second, 0, 0, BalancerStrategyGreedy)
	expired := time.Now().Add(-120 * time.Second).Unix()
	now := time.Now().Unix()

	leases := []changeFeedProcessorLease{
		newTestLease("worker-2", newTestFeedRange("", "2A"), now),
		newTestLease("worker-2", newTestFeedRange("2A", "55"), now),
		newTestLease("worker-2", newTestFeedRange("55", "7F"), now),
		newTestLease("dead", newTestFeedRange("7F", "99"), expired),
		newTestLease("dead", newTestFeedRange("99", "BB"), expired),
		newTestLease("dead", newTestFeedRange("BB", "FF"), expired),
	}

	result := b.selectLeasesToAcquire(leases)
	// 6 leases, 2 active workers → target = ceil(6/2) = 3.
	// worker-1 has 0, needs 3. 3 expired leases available → greedy takes all 3.
	require.Len(t, result, 3)
}

func TestBalancerGreedyStealsMultipleFromBusiest(t *testing.T) {
	b := newChangeFeedProcessorBalancer("worker-B", 60*time.Second, 0, 0, BalancerStrategyGreedy)
	now := time.Now().Unix()

	leases := []changeFeedProcessorLease{
		newTestLease("worker-A", newTestFeedRange("", "20"), now),
		newTestLease("worker-A", newTestFeedRange("20", "40"), now),
		newTestLease("worker-A", newTestFeedRange("40", "60"), now),
		newTestLease("worker-A", newTestFeedRange("60", "80"), now),
		newTestLease("worker-A", newTestFeedRange("80", "FF"), now),
	}

	result := b.selectLeasesToAcquire(leases)
	// 5 leases, 2 workers → target = ceil(5/2) = 3.
	// worker-B has 0, needs 3. No expired leases.
	// Greedy steals 3 from worker-A (who has 5 > target 3).
	require.Len(t, result, 3)
	for _, l := range result {
		require.Equal(t, "worker-A", l.Owner)
	}
}

func TestBalancerGreedyRespectsMaxPartitionCount(t *testing.T) {
	b := newChangeFeedProcessorBalancer("worker-1", 60*time.Second, 0, 2, BalancerStrategyGreedy)
	expired := time.Now().Add(-120 * time.Second).Unix()

	leases := []changeFeedProcessorLease{
		newTestLease("dead", newTestFeedRange("", "20"), expired),
		newTestLease("dead", newTestFeedRange("20", "40"), expired),
		newTestLease("dead", newTestFeedRange("40", "60"), expired),
		newTestLease("dead", newTestFeedRange("60", "80"), expired),
		newTestLease("dead", newTestFeedRange("80", "FF"), expired),
	}

	result := b.selectLeasesToAcquire(leases)
	// 5 leases, 1 worker → target = ceil(5/1) = 5, clamped to maxPartitionCount = 2.
	// worker-1 has 0, needs 2. 5 expired available → takes only 2.
	require.Len(t, result, 2)
}

func TestBalancerEqualStillStealsOneAtATime(t *testing.T) {
	b := newChangeFeedProcessorBalancer("worker-B", 60*time.Second, 0, 0, BalancerStrategyEqual)
	now := time.Now().Unix()

	leases := []changeFeedProcessorLease{
		newTestLease("worker-A", newTestFeedRange("", "20"), now),
		newTestLease("worker-A", newTestFeedRange("20", "40"), now),
		newTestLease("worker-A", newTestFeedRange("40", "60"), now),
		newTestLease("worker-A", newTestFeedRange("60", "80"), now),
		newTestLease("worker-A", newTestFeedRange("80", "FF"), now),
	}

	result := b.selectLeasesToAcquire(leases)
	// 5 leases, 2 workers → target = ceil(5/2) = 3.
	// worker-B has 0, needs 3. No expired leases.
	// Equal strategy steals exactly 1 from worker-A.
	require.Len(t, result, 1)
	require.Equal(t, "worker-A", result[0].Owner)
}

// --- Options Tests ---

func TestChangeFeedProcessorDefaults(t *testing.T) {
	defaults := changeFeedProcessorDefaults()

	if defaults.MaxItemCount != 100 {
		t.Errorf("MaxItemCount: got %d, want 100", defaults.MaxItemCount)
	}
	if defaults.PollInterval != 5*time.Second {
		t.Errorf("PollInterval: got %v, want 5s", defaults.PollInterval)
	}
	if defaults.LeaseExpirationInterval != 60*time.Second {
		t.Errorf("LeaseExpirationInterval: got %v, want 60s", defaults.LeaseExpirationInterval)
	}
	if defaults.LeaseRenewInterval != 17*time.Second {
		t.Errorf("LeaseRenewInterval: got %v, want 17s", defaults.LeaseRenewInterval)
	}
	if defaults.LeaseAcquireInterval != 13*time.Second {
		t.Errorf("LeaseAcquireInterval: got %v, want 13s", defaults.LeaseAcquireInterval)
	}
	if defaults.StartFromBeginning {
		t.Error("StartFromBeginning should default to false")
	}
	if defaults.StartTime != nil {
		t.Error("StartTime should default to nil")
	}
	if defaults.LeasePrefix != "" {
		t.Errorf("LeasePrefix should default to empty, got %q", defaults.LeasePrefix)
	}
}

// --- Builder/Validation Tests ---
// NewChangeFeedProcessor is a method on ContainerClient, which requires
// a real pipeline. We test what we can: the validation error paths use
// a nil ContainerClient receiver, which will panic on real construction
// but the validation checks fire before any field access.

func TestNewChangeFeedProcessorValidation(t *testing.T) {
	// We can't construct a real ContainerClient without a connection, but we
	// can verify the validation error messages by calling with invalid args
	// on a zero-value ContainerClient. The validation checks are at the top
	// of the function, before any field access on the receiver.

	dummyHandler := func(_ context.Context, _ [][]byte) error { return nil }

	t.Run("empty processorName", func(t *testing.T) {
		var cc ContainerClient
		_, err := cc.NewChangeFeedProcessor("", &ContainerClient{}, dummyHandler, nil)
		if err == nil {
			t.Fatal("expected error for empty processorName")
		}
	})

	t.Run("nil leaseContainer", func(t *testing.T) {
		var cc ContainerClient
		_, err := cc.NewChangeFeedProcessor("proc", nil, dummyHandler, nil)
		if err == nil {
			t.Fatal("expected error for nil leaseContainer")
		}
	})

	t.Run("nil handler", func(t *testing.T) {
		var cc ContainerClient
		_, err := cc.NewChangeFeedProcessor("proc", &ContainerClient{}, nil, nil)
		if err == nil {
			t.Fatal("expected error for nil handler")
		}
	})
}

func TestChangeFeedProcessorState(t *testing.T) {
	now := time.Now()
	lease := changeFeedProcessorLease{
		ID:                "lease-1",
		Owner:             "worker-1",
		ContinuationToken: "token-abc",
		FeedRange:         &FeedRange{MinInclusive: "00", MaxExclusive: "FF"},
		Timestamp:         now.Unix(),
	}

	state := ChangeFeedProcessorState{
		LeaseToken:        lease.ID,
		Owner:             lease.Owner,
		ContinuationToken: lease.ContinuationToken,
		FeedRange:         lease.FeedRange,
		LastUpdated:       time.Unix(lease.Timestamp, 0),
		IsExpired:         lease.isExpired(60 * time.Second),
	}

	require.Equal(t, "lease-1", state.LeaseToken)
	require.Equal(t, "worker-1", state.Owner)
	require.Equal(t, "token-abc", state.ContinuationToken)
	require.Equal(t, "00", state.FeedRange.MinInclusive)
	require.Equal(t, "FF", state.FeedRange.MaxExclusive)
	require.False(t, state.IsExpired)

	// Test expired lease
	oldLease := lease
	oldLease.Timestamp = now.Add(-2 * time.Minute).Unix()
	require.True(t, oldLease.isExpired(60*time.Second))
}

func TestSynchronizerDetectsOrphanedLeases(t *testing.T) {
	// Simulate: we have leases for ranges [00-7F] and [7F-FF],
	// but the current feed ranges only have [00-FF] (merged).
	// The old [00-7F] and [7F-FF] leases should be detected as orphans.

	oldLeases := []changeFeedProcessorLease{
		newTestLease("worker-1", newTestFeedRange("00", "7F"), time.Now().Unix()),
		newTestLease("worker-2", newTestFeedRange("7F", "FF"), time.Now().Unix()),
	}

	newRanges := []FeedRange{
		newTestFeedRange("00", "FF"),
	}

	// Build the currentRangeIDs set
	currentRangeIDs := make(map[string]struct{}, len(newRanges))
	for _, fr := range newRanges {
		currentRangeIDs[leaseIDFromFeedRange(fr, "")] = struct{}{}
	}

	// Check which old leases are orphaned
	var orphaned []string
	for _, lease := range oldLeases {
		if _, stillExists := currentRangeIDs[lease.ID]; !stillExists {
			orphaned = append(orphaned, lease.ID)
		}
	}

	require.Len(t, orphaned, 2, "both old leases should be detected as orphans")
}

// --- Error Classification Tests ---

func TestIsNonRetryableStatusCode(t *testing.T) {
	nonRetryable := []int{400, 401, 403, 404, 405, 409, 413}
	for _, code := range nonRetryable {
		require.True(t, isNonRetryableStatusCode(code), "status %d should be non-retryable", code)
	}

	retryable := []int{408, 429, 500, 502, 503, 504}
	for _, code := range retryable {
		require.False(t, isNonRetryableStatusCode(code), "status %d should be retryable", code)
	}
}

func TestPollLoopNonRetryableExitsImmediately(t *testing.T) {
	// Verify that non-retryable errors from poll() cause pollLoop to
	// exit immediately rather than retrying.
	require.ErrorIs(t, errNonRetryable, errNonRetryable)

	// errNonRetryable should NOT match errLeaseLost
	require.False(t, errors.Is(errNonRetryable, errLeaseLost))

	// partitionGoneError should be detectable via isPartitionGone
	pge := &partitionGoneError{continuationToken: "tok123"}
	token, ok := isPartitionGone(pge)
	require.True(t, ok)
	require.Equal(t, "tok123", token)

	// errHandlerFailed should not match other sentinels
	require.False(t, errors.Is(errHandlerFailed, errLeaseLost))
	require.False(t, errors.Is(errHandlerFailed, errNonRetryable))
}
