// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"encoding/json"
	"testing"
	"time"
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
	b := newChangeFeedProcessorBalancer("worker-1", 60*time.Second)
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
	b := newChangeFeedProcessorBalancer("worker-1", 60*time.Second)
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
	b := newChangeFeedProcessorBalancer("new-worker", 60*time.Second)
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
	b := newChangeFeedProcessorBalancer("worker-A", 60*time.Second)
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
	b := newChangeFeedProcessorBalancer("worker-B", 60*time.Second)
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
	b := newChangeFeedProcessorBalancer("worker-C", 60*time.Second)
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
