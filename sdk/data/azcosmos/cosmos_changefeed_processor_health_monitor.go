// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"fmt"
)

// ChangeFeedProcessorHealthMonitor provides hooks into the processor lifecycle
// for observability and error reporting. All methods are optional — implement
// only the ones you need. Nil implementations are safe and ignored.
//
// Example:
//
//	monitor := &azcosmos.ChangeFeedProcessorHealthMonitor{
//	    OnLeaseAcquired: func(ctx context.Context, leaseID string) {
//	        metrics.Inc("changefeed.lease.acquired")
//	    },
//	    OnError: func(ctx context.Context, leaseID string, err error) {
//	        logger.Error("changefeed error", "lease", leaseID, "err", err)
//	    },
//	}
type ChangeFeedProcessorHealthMonitor struct {
	// OnLeaseAcquired is called when this instance successfully acquires a lease.
	OnLeaseAcquired func(ctx context.Context, leaseID string)

	// OnLeaseReleased is called when this instance releases a lease (shutdown or rebalance).
	OnLeaseReleased func(ctx context.Context, leaseID string)

	// OnError is called for all errors (catch-all). Always called regardless of
	// whether a more specific callback (OnLeaseContention, OnProcessingError) is set.
	// The leaseID may be empty for errors not tied to a specific lease.
	OnError func(ctx context.Context, leaseID string, err error)

	// OnLeaseContention is called when a lease operation fails due to contention
	// (412 Precondition Failed / ETag mismatch). This is expected during rebalancing
	// and does not indicate a problem. Use this to distinguish contention from real
	// failures instead of string-matching in OnError.
	OnLeaseContention func(ctx context.Context, leaseID string)

	// OnProcessingError is called when an error occurs during change feed reading,
	// handler execution, or checkpointing — i.e., real processing failures as opposed
	// to lease contention.
	OnProcessingError func(ctx context.Context, leaseID string, err error)

	// OnSyncComplete is called after each lease synchronization cycle with the
	// total number of feed ranges (physical partitions) in the monitored container.
	// Use this to metric partition count without polling GetCurrentState.
	OnSyncComplete func(ctx context.Context, totalRanges int)
}

// notifyLeaseAcquired calls OnLeaseAcquired if the monitor and callback are non-nil.
func (m *ChangeFeedProcessorHealthMonitor) notifyLeaseAcquired(ctx context.Context, leaseID string) {
	if m != nil && m.OnLeaseAcquired != nil {
		m.OnLeaseAcquired(ctx, leaseID)
	}
}

// notifyLeaseReleased calls OnLeaseReleased if the monitor and callback are non-nil.
func (m *ChangeFeedProcessorHealthMonitor) notifyLeaseReleased(ctx context.Context, leaseID string) {
	if m != nil && m.OnLeaseReleased != nil {
		m.OnLeaseReleased(ctx, leaseID)
	}
}

// notifyError calls OnError if the monitor and callback are non-nil.
func (m *ChangeFeedProcessorHealthMonitor) notifyError(ctx context.Context, leaseID string, err error) {
	if m != nil && m.OnError != nil {
		m.OnError(ctx, leaseID, err)
	}
}

// notifyLeaseContention calls OnLeaseContention for 412/ETag mismatch errors.
// Also calls OnError as the catch-all.
func (m *ChangeFeedProcessorHealthMonitor) notifyLeaseContention(ctx context.Context, leaseID string) {
	if m == nil {
		return
	}
	if m.OnLeaseContention != nil {
		m.OnLeaseContention(ctx, leaseID)
	}
	if m.OnError != nil {
		m.OnError(ctx, leaseID, fmt.Errorf("lease contention (412) on lease %s", leaseID))
	}
}

// notifyProcessingError calls OnProcessingError for real processing failures.
// Also calls OnError as the catch-all.
func (m *ChangeFeedProcessorHealthMonitor) notifyProcessingError(ctx context.Context, leaseID string, err error) {
	if m == nil {
		return
	}
	if m.OnProcessingError != nil {
		m.OnProcessingError(ctx, leaseID, err)
	}
	if m.OnError != nil {
		m.OnError(ctx, leaseID, err)
	}
}

// notifySyncComplete calls OnSyncComplete if the monitor and callback are non-nil.
func (m *ChangeFeedProcessorHealthMonitor) notifySyncComplete(ctx context.Context, totalRanges int) {
	if m != nil && m.OnSyncComplete != nil {
		m.OnSyncComplete(ctx, totalRanges)
	}
}
