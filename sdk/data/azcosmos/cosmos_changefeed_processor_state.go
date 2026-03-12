// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import "time"

// ChangeFeedProcessorState describes the current state of a single partition
// lease as seen by the change feed processor. Use [ChangeFeedProcessor.GetCurrentState]
// to retrieve the state of all leases.
type ChangeFeedProcessorState struct {
	// LeaseToken is the unique identifier for the lease.
	LeaseToken string

	// Owner is the instance name that currently holds this lease.
	// Empty if the lease is unowned.
	Owner string

	// ContinuationToken is the change feed checkpoint for this partition.
	ContinuationToken string

	// FeedRange is the partition key range this lease covers.
	FeedRange *FeedRange

	// LastUpdated is when the lease was last renewed or checkpointed.
	LastUpdated time.Time

	// IsExpired indicates whether the lease has expired (not renewed within
	// the configured expiration interval).
	IsExpired bool
}
