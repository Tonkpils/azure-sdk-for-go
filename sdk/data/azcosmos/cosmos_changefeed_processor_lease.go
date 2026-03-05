// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"
)

// changeFeedProcessorLease represents a lease document stored in the lease container.
// Each lease corresponds to a feed range (partition key range) and tracks:
//   - Which processor instance owns it
//   - The continuation token (progress checkpoint)
//   - When it was last renewed (for expiry detection)
type changeFeedProcessorLease struct {
	// ID is the unique identifier for the lease, derived from the feed range.
	ID string `json:"id"`
	// PartitionKey for the lease document (same as ID for simplicity).
	PartitionKey string `json:"partitionKey"`
	// Owner is the instance name that currently holds this lease.
	Owner string `json:"owner,omitempty"`
	// ContinuationToken is the change feed checkpoint for resuming.
	ContinuationToken string `json:"continuationToken,omitempty"`
	// FeedRange is the partition key range this lease covers.
	FeedRange *FeedRange `json:"feedRange,omitempty"`
	// Timestamp is when this lease was last updated (Unix epoch seconds).
	Timestamp int64 `json:"timestamp,omitempty"`
	// ETag is the Cosmos DB ETag for optimistic concurrency control.
	ETag string `json:"_etag,omitempty"`
	// ResourceID from Cosmos DB.
	ResourceID string `json:"_rid,omitempty"`
	// Self link from Cosmos DB.
	Self string `json:"_self,omitempty"`
	// TTL is the time-to-live in seconds for the lease document.
	TTL int32 `json:"ttl,omitempty"`
}

// newChangeFeedProcessorLease creates a new lease for the given feed range and owner.
func newChangeFeedProcessorLease(feedRange FeedRange, owner string) changeFeedProcessorLease {
	id := leaseIDFromFeedRange(feedRange)
	return changeFeedProcessorLease{
		ID:           id,
		PartitionKey: id,
		Owner:        owner,
		FeedRange:    &feedRange,
		Timestamp:    time.Now().Unix(),
	}
}

// isExpired reports whether the lease has not been renewed within the given interval.
func (l *changeFeedProcessorLease) isExpired(expirationInterval time.Duration) bool {
	if l.Timestamp == 0 {
		return true
	}
	lastUpdated := time.Unix(l.Timestamp, 0)
	return time.Since(lastUpdated) > expirationInterval
}

// isOwned reports whether the lease currently has an owner.
func (l *changeFeedProcessorLease) isOwned() bool {
	return l.Owner != ""
}

// toJSON serializes the lease to JSON for storage in Cosmos DB.
func (l *changeFeedProcessorLease) toJSON() ([]byte, error) {
	return json.Marshal(l)
}

// fromJSON deserializes a lease from a Cosmos DB response body.
func (l *changeFeedProcessorLease) fromJSON(data []byte) error {
	return json.Unmarshal(data, l)
}

// leaseIDFromFeedRange generates a deterministic lease ID from a feed range.
// The ID is a truncated SHA-256 hex digest of the range boundaries, keeping it
// short while avoiding collisions across realistic partition key ranges.
func leaseIDFromFeedRange(feedRange FeedRange) string {
	hash := sha256.Sum256([]byte(feedRange.MinInclusive + "-" + feedRange.MaxExclusive))
	return fmt.Sprintf("%x", hash[:16])
}
