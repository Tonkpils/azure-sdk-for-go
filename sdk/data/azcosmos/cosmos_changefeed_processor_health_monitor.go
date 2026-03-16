// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"fmt"
	"time"
)

// LeaseCloseReason describes why a lease was released.
// Matches .NET SDK's ChangeFeedObserverCloseReason enum.
type LeaseCloseReason int

const (
	// LeaseCloseReasonShutdown indicates the processor is shutting down gracefully.
	LeaseCloseReasonShutdown LeaseCloseReason = iota
	// LeaseCloseReasonLeaseLost indicates another instance took ownership of the lease.
	LeaseCloseReasonLeaseLost
	// LeaseCloseReasonPartitionGone indicates the partition was split or merged.
	LeaseCloseReasonPartitionGone
	// LeaseCloseReasonObserverError indicates the user's handler returned an error.
	LeaseCloseReasonObserverError
	// LeaseCloseReasonNonRetryableError indicates a non-retryable HTTP error (401, 403, etc.).
	LeaseCloseReasonNonRetryableError
	// LeaseCloseReasonUnknown indicates an unexpected error.
	LeaseCloseReasonUnknown
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

	// OnLeaseClosed is called when a supervisor stops processing a lease, with a
	// typed reason. This matches .NET's observer.CloseAsync(leaseToken, closeReason)
	// pattern and fires on every exit path (shutdown, error, partition gone, etc.).
	// If both OnLeaseClosed and OnLeaseReleased are set, both are called.
	OnLeaseClosed func(ctx context.Context, leaseID string, reason LeaseCloseReason)

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

	// OnPollComplete is called after every GetChangeFeed poll with timing and outcome.
	// Use for poll duration histograms and outcome breakdowns per lease.
	OnPollComplete func(ctx context.Context, leaseID string, duration time.Duration, itemCount int, err error)

	// OnSupervisorStart is called when a supervisor goroutine begins running.
	OnSupervisorStart func(ctx context.Context, leaseID string)

	// OnSupervisorStop is called when a supervisor goroutine exits (for any reason).
	OnSupervisorStop func(ctx context.Context, leaseID string)
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

// notifyLeaseClosed calls OnLeaseClosed with the given reason, then calls
// OnLeaseReleased for backward compatibility.
func (m *ChangeFeedProcessorHealthMonitor) notifyLeaseClosed(ctx context.Context, leaseID string, reason LeaseCloseReason) {
	if m == nil {
		return
	}
	if m.OnLeaseClosed != nil {
		m.OnLeaseClosed(ctx, leaseID, reason)
	}
	if m.OnLeaseReleased != nil {
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

func (m *ChangeFeedProcessorHealthMonitor) notifyPollComplete(ctx context.Context, leaseID string, duration time.Duration, itemCount int, err error) {
	if m != nil && m.OnPollComplete != nil {
		m.OnPollComplete(ctx, leaseID, duration, itemCount, err)
	}
}

func (m *ChangeFeedProcessorHealthMonitor) notifySupervisorStart(ctx context.Context, leaseID string) {
	if m != nil && m.OnSupervisorStart != nil {
		m.OnSupervisorStart(ctx, leaseID)
	}
}

func (m *ChangeFeedProcessorHealthMonitor) notifySupervisorStop(ctx context.Context, leaseID string) {
	if m != nil && m.OnSupervisorStop != nil {
		m.OnSupervisorStop(ctx, leaseID)
	}
}
