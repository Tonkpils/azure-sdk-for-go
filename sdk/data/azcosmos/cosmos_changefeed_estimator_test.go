// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChangeFeedEstimationStruct(t *testing.T) {
	fr := FeedRange{MinInclusive: "00", MaxExclusive: "FF"}
	est := ChangeFeedEstimation{
		LeaseToken:   "lease-1",
		Owner:        "worker-1",
		EstimatedLag: 42,
		FeedRange:    &fr,
	}

	require.Equal(t, "lease-1", est.LeaseToken)
	require.Equal(t, "worker-1", est.Owner)
	require.Equal(t, 42, est.EstimatedLag)
	require.Equal(t, "00", est.FeedRange.MinInclusive)
}

func TestChangeFeedEstimatorDefaults(t *testing.T) {
	defaults := changeFeedEstimatorDefaults()
	require.Equal(t, 5*time.Second, defaults.PollInterval)
	require.Equal(t, int32(100), defaults.MaxItemCount)
	require.Empty(t, defaults.LeasePrefix)
}

func TestChangeFeedEstimatorOptionsOverride(t *testing.T) {
	opts := &ChangeFeedEstimatorOptions{
		PollInterval: 10 * time.Second,
		MaxItemCount: 50,
		LeasePrefix:  "mygroup",
	}

	defaults := changeFeedEstimatorDefaults()
	if opts.PollInterval > 0 {
		defaults.PollInterval = opts.PollInterval
	}
	if opts.MaxItemCount > 0 {
		defaults.MaxItemCount = opts.MaxItemCount
	}
	defaults.LeasePrefix = opts.LeasePrefix

	require.Equal(t, 10*time.Second, defaults.PollInterval)
	require.Equal(t, int32(50), defaults.MaxItemCount)
	require.Equal(t, "mygroup", defaults.LeasePrefix)
}

func TestNewChangeFeedEstimatorValidation(t *testing.T) {
	_, err := NewChangeFeedEstimator(nil, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "monitoredContainer")
}

func TestExtractLSNFromSessionToken(t *testing.T) {
	// Standard format: pkRangeId:version#globalLsn
	require.Equal(t, int64(5678), extractLSNFromSessionToken("0:1234#5678"))

	// Multiple segments
	require.Equal(t, int64(9999), extractLSNFromSessionToken("42:100#9999"))

	// No pkRangeId prefix
	require.Equal(t, int64(5678), extractLSNFromSessionToken("1234#5678"))

	// Single number (no # separator)
	require.Equal(t, int64(1234), extractLSNFromSessionToken("0:1234"))

	// Empty
	require.Equal(t, int64(0), extractLSNFromSessionToken(""))

	// Just a number
	require.Equal(t, int64(42), extractLSNFromSessionToken("42"))
}

func TestExtractLSNFromDocument(t *testing.T) {
	// Numeric _lsn
	require.Equal(t, int64(12345), extractLSNFromDocument([]byte(`{"id":"doc1","_lsn":12345}`)))

	// String _lsn
	require.Equal(t, int64(67890), extractLSNFromDocument([]byte(`{"id":"doc1","_lsn":"67890"}`)))

	// Missing _lsn
	require.Equal(t, int64(0), extractLSNFromDocument([]byte(`{"id":"doc1"}`)))

	// Invalid JSON
	require.Equal(t, int64(0), extractLSNFromDocument([]byte(`not json`)))
}
