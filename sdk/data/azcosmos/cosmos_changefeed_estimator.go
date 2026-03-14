// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ChangeFeedEstimation describes the estimated lag for a single partition.
type ChangeFeedEstimation struct {
	// LeaseToken is the unique identifier for the lease.
	LeaseToken string
	// Owner is the instance name currently processing this partition.
	Owner string
	// EstimatedLag is the approximate number of unprocessed items.
	EstimatedLag int
	// FeedRange is the partition key range this estimation covers.
	FeedRange *FeedRange
}

// ChangeFeedEstimatorHandler is called periodically with lag estimations.
type ChangeFeedEstimatorHandler func(ctx context.Context, estimations []ChangeFeedEstimation)

// ChangeFeedEstimatorOptions configures a ChangeFeedEstimator.
type ChangeFeedEstimatorOptions struct {
	// PollInterval is how often the estimator checks for lag in push mode.
	// Default: 5 seconds
	PollInterval time.Duration

	// MaxItemCount is the maximum items per change feed probe.
	// A smaller value makes estimation faster but less accurate.
	// Default: 100
	MaxItemCount int32

	// LeasePrefix filters leases to only those matching the processor group.
	// Must match the prefix used by the ChangeFeedProcessor.
	LeasePrefix string
}

func changeFeedEstimatorDefaults() ChangeFeedEstimatorOptions {
	return ChangeFeedEstimatorOptions{
		PollInterval: 5 * time.Second,
		MaxItemCount: 100,
	}
}

// ChangeFeedEstimator monitors change feed processing lag without consuming
// any documents. It reads lease continuation tokens from the lease container
// and probes the change feed to estimate how many items remain unprocessed.
//
// Use [ChangeFeedEstimator.GetEstimatedLag] for on-demand (pull) estimation,
// or [ChangeFeedEstimator.Start] for periodic (push) monitoring.
type ChangeFeedEstimator struct {
	monitoredContainer *ContainerClient
	leaseStore         *changeFeedProcessorLeaseStore
	options            ChangeFeedEstimatorOptions

	cancelFunc context.CancelFunc
	done       chan struct{}
	mu         sync.Mutex
}

// NewChangeFeedEstimator creates an estimator for the given monitored and lease containers.
// The leasePrefix should match the LeasePrefix used by the ChangeFeedProcessor being monitored.
func NewChangeFeedEstimator(
	monitoredContainer *ContainerClient,
	leaseContainer *ContainerClient,
	options *ChangeFeedEstimatorOptions,
) (*ChangeFeedEstimator, error) {
	if monitoredContainer == nil {
		return nil, fmt.Errorf("azcosmos: monitoredContainer must not be nil")
	}
	if leaseContainer == nil {
		return nil, fmt.Errorf("azcosmos: leaseContainer must not be nil")
	}

	opts := changeFeedEstimatorDefaults()
	if options != nil {
		if options.PollInterval > 0 {
			opts.PollInterval = options.PollInterval
		}
		if options.MaxItemCount > 0 {
			opts.MaxItemCount = options.MaxItemCount
		}
		opts.LeasePrefix = options.LeasePrefix
	}

	leaseStore := newChangeFeedProcessorLeaseStore(leaseContainer, opts.LeasePrefix, 50)

	return &ChangeFeedEstimator{
		monitoredContainer: monitoredContainer,
		leaseStore:         leaseStore,
		options:            opts,
	}, nil
}

// GetEstimatedLag returns the current estimated lag for all partitions.
// This is a one-shot pull operation — call it whenever you need a snapshot.
func (e *ChangeFeedEstimator) GetEstimatedLag(ctx context.Context) ([]ChangeFeedEstimation, error) {
	leases, err := e.leaseStore.getAllLeases(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read leases: %w", err)
	}

	estimations := make([]ChangeFeedEstimation, 0, len(leases))
	for _, lease := range leases {
		lag, err := e.estimatePartitionLag(ctx, &lease)
		if err != nil {
			// Skip partitions we can't estimate rather than failing entirely.
			continue
		}
		estimations = append(estimations, ChangeFeedEstimation{
			LeaseToken:   lease.ID,
			Owner:        lease.Owner,
			EstimatedLag: lag,
			FeedRange:    lease.FeedRange,
		})
	}

	return estimations, nil
}

// Start begins periodic lag estimation, calling handler at each interval.
// It blocks until ctx is cancelled or Stop is called.
func (e *ChangeFeedEstimator) Start(ctx context.Context, handler ChangeFeedEstimatorHandler) error {
	if handler == nil {
		return fmt.Errorf("azcosmos: handler must not be nil")
	}

	e.mu.Lock()
	if e.cancelFunc != nil {
		e.mu.Unlock()
		return fmt.Errorf("azcosmos: estimator is already running")
	}
	ctx, cancel := context.WithCancel(ctx)
	e.cancelFunc = cancel
	e.done = make(chan struct{})
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		e.cancelFunc = nil
		close(e.done)
		e.mu.Unlock()
	}()

	ticker := time.NewTicker(e.options.PollInterval)
	defer ticker.Stop()

	for {
		estimations, err := e.GetEstimatedLag(ctx)
		if err == nil {
			handler(ctx, estimations)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Stop gracefully shuts down a running estimator started with Start.
func (e *ChangeFeedEstimator) Stop() {
	e.mu.Lock()
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
	done := e.done
	e.mu.Unlock()

	if done != nil {
		<-done
	}
}

// estimatePartitionLag probes the change feed for a single partition to count
// how many items are available from the lease's continuation token.
func (e *ChangeFeedEstimator) estimatePartitionLag(ctx context.Context, lease *changeFeedProcessorLease) (int, error) {
	if lease.FeedRange == nil {
		return 0, fmt.Errorf("lease %s has no feed range", lease.ID)
	}

	opts := ChangeFeedOptions{
		FeedRange:    lease.FeedRange,
		MaxItemCount: e.options.MaxItemCount,
	}

	if lease.ContinuationToken != "" {
		opts.Continuation = &lease.ContinuationToken
	}

	totalCount := 0
	// Read up to a few pages to estimate lag. We cap at 10 pages to avoid
	// reading the entire change feed for very-behind partitions.
	maxPages := 10
	for page := 0; page < maxPages; page++ {
		resp, err := e.monitoredContainer.GetChangeFeed(ctx, &opts)
		if err != nil {
			if page == 0 {
				return 0, err
			}
			break
		}

		// 304 Not Modified = fully caught up
		if resp.RawResponse != nil && resp.RawResponse.StatusCode == 304 {
			break
		}

		totalCount += resp.Count

		if resp.ContinuationToken == "" {
			break
		}
		opts.Continuation = &resp.ContinuationToken
	}

	return totalCount, nil
}
