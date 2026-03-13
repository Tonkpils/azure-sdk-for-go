// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import "context"

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

	// OnError is called when an error occurs during processing, renewal, or checkpointing.
	// The leaseID may be empty for errors not tied to a specific lease.
	OnError func(ctx context.Context, leaseID string, err error)

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

// notifySyncComplete calls OnSyncComplete if the monitor and callback are non-nil.
func (m *ChangeFeedProcessorHealthMonitor) notifySyncComplete(ctx context.Context, totalRanges int) {
	if m != nil && m.OnSyncComplete != nil {
		m.OnSyncComplete(ctx, totalRanges)
	}
}
