// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/internal/uuid"
)

// ChangeFeedProcessorHandler is the callback function invoked for each batch of changes.
// The handler receives the context and a slice of raw JSON documents.
// Return an error to signal processing failure (the batch will be retried).
type ChangeFeedProcessorHandler func(ctx context.Context, changes [][]byte) error

// ChangeFeedProcessor reads the Cosmos DB change feed and distributes work
// across multiple instances using a lease-based coordination protocol.
//
// Usage:
//
//	processor, err := container.NewChangeFeedProcessor(
//	    "processorName",
//	    leaseContainer,
//	    handler,
//	    &ChangeFeedProcessorOptions{...},
//	)
//	err = processor.Start(ctx)
//	// ... later ...
//	err = processor.Stop()
type ChangeFeedProcessor struct {
	processorName      string
	instanceName       string
	monitoredContainer *ContainerClient
	handler            ChangeFeedProcessorHandler
	options            ChangeFeedProcessorOptions
	monitor            *ChangeFeedProcessorHealthMonitor
	throttle           *changeFeedProcessorThrottle

	leaseStore   *changeFeedProcessorLeaseStore
	leaseManager *changeFeedProcessorLeaseManager
	synchronizer *changeFeedProcessorSynchronizer
	balancer     *changeFeedProcessorBalancer
	supervisors  map[string]context.CancelFunc // leaseID → cancel function
	mu           sync.Mutex                    // protects supervisors map
	wg           sync.WaitGroup                // tracks running supervisor goroutines

	cancelFunc context.CancelFunc
	done       chan struct{}
}

// NewChangeFeedProcessor creates a new ChangeFeedProcessor for this container.
// processorName identifies this processor group (used as lease prefix).
// leaseContainer is the container used for distributed lease coordination.
// handler is called for each batch of changes.
// options may be nil, in which case sensible defaults are applied.
func (c *ContainerClient) NewChangeFeedProcessor(
	processorName string,
	leaseContainer *ContainerClient,
	handler ChangeFeedProcessorHandler,
	options *ChangeFeedProcessorOptions,
) (*ChangeFeedProcessor, error) {
	if processorName == "" {
		return nil, errors.New("azcosmos: processorName must not be empty")
	}
	if leaseContainer == nil {
		return nil, errors.New("azcosmos: leaseContainer must not be nil")
	}
	if handler == nil {
		return nil, errors.New("azcosmos: handler must not be nil")
	}

	opts := changeFeedProcessorDefaults()
	if options != nil {
		if options.MaxItemCount > 0 {
			opts.MaxItemCount = options.MaxItemCount
		}
		if options.PollInterval > 0 {
			opts.PollInterval = options.PollInterval
		}
		if options.LeaseExpirationInterval > 0 {
			opts.LeaseExpirationInterval = options.LeaseExpirationInterval
		}
		if options.LeaseRenewInterval > 0 {
			opts.LeaseRenewInterval = options.LeaseRenewInterval
		}
		if options.LeaseAcquireInterval > 0 {
			opts.LeaseAcquireInterval = options.LeaseAcquireInterval
		}
		if options.RequestTimeout > 0 {
			opts.RequestTimeout = options.RequestTimeout
		}
		opts.StartFromBeginning = options.StartFromBeginning
		opts.StartTime = options.StartTime
		opts.LeasePrefix = options.LeasePrefix
		opts.Mode = options.Mode
		opts.MinPartitionCount = options.MinPartitionCount
		opts.MaxPartitionCount = options.MaxPartitionCount
		opts.BalancerStrategy = options.BalancerStrategy
		opts.HealthMonitor = options.HealthMonitor
		opts.MaxRUPerSecond = options.MaxRUPerSecond
	}

	instanceName, err := generateInstanceName()
	if err != nil {
		return nil, fmt.Errorf("azcosmos: failed to generate instance name: %w", err)
	}

	leaseStore := newChangeFeedProcessorLeaseStore(leaseContainer, opts.LeasePrefix)
	leaseManager := newChangeFeedProcessorLeaseManager(leaseStore, instanceName, opts)
	synchronizer := newChangeFeedProcessorSynchronizer(c, leaseStore, opts.LeasePrefix, opts.Mode, opts.HealthMonitor)
	balancer := newChangeFeedProcessorBalancer(instanceName, opts.LeaseExpirationInterval, opts.MinPartitionCount, opts.MaxPartitionCount, opts.BalancerStrategy)
	throttle := newChangeFeedProcessorThrottle(opts.MaxRUPerSecond)

	return &ChangeFeedProcessor{
		processorName:      processorName,
		instanceName:       instanceName,
		monitoredContainer: c,
		handler:            handler,
		options:            opts,
		monitor:            opts.HealthMonitor,
		throttle:           throttle,
		leaseStore:         leaseStore,
		leaseManager:       leaseManager,
		synchronizer:       synchronizer,
		balancer:           balancer,
		supervisors:        make(map[string]context.CancelFunc),
	}, nil
}

// Start begins processing the change feed. It blocks until ctx is cancelled
// or Stop is called. Use a goroutine for background processing:
//
//	go processor.Start(ctx)
func (p *ChangeFeedProcessor) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	p.cancelFunc = cancel
	p.done = make(chan struct{})
	defer close(p.done)

	// 1. Synchronize leases (create missing ones for all feed ranges)
	if err := p.synchronizer.synchronizeLeases(ctx); err != nil {
		return fmt.Errorf("failed to synchronize leases: %w", err)
	}

	// 2. Validate mode consistency — leases created with a different mode
	// cannot be reused because the change feed wire format differs between modes.
	leases, err := p.leaseStore.getAllLeases(ctx)
	if err != nil {
		return fmt.Errorf("failed to read leases for mode validation: %w", err)
	}
	for _, lease := range leases {
		if lease.Mode != 0 && lease.Mode != p.options.Mode {
			return fmt.Errorf("azcosmos: lease %s was created with mode %d but processor is configured for mode %d; delete leases to switch modes", lease.ID, lease.Mode, p.options.Mode)
		}
	}

	// 3. Start lease renewal goroutine
	go p.renewLoop(ctx)

	// 4. Eagerly start supervisors for leases we already own (e.g. after a
	// restart). Matches .NET's LoadLeasesAsync which starts processing
	// owned leases immediately rather than waiting for the first acquire cycle.
	p.loadOwnedLeases(ctx)

	// 5. Main acquisition loop — uses jittered interval to reduce 412
	// collisions when multiple instances start with the same configuration.
	for {
		// Re-synchronize on every cycle to pick up partition splits/merges.
		if err := p.synchronizer.synchronizeLeases(ctx); err != nil {
			p.monitor.notifyError(ctx, "", fmt.Errorf("lease sync failed: %w", err))
		}

		p.acquireLeases(ctx)

		jittered := jitteredInterval(p.options.LeaseAcquireInterval)
		select {
		case <-ctx.Done():
			p.releaseAllLeases()
			return nil
		case <-time.After(jittered):
		}
	}
}

// Stop gracefully shuts down the processor, releasing all leases.
func (p *ChangeFeedProcessor) Stop() error {
	if p.cancelFunc != nil {
		p.cancelFunc()
	}

	if p.done != nil {
		select {
		case <-p.done:
		case <-time.After(30 * time.Second):
			return errors.New("azcosmos: processor shutdown timed out")
		}
	}

	return nil
}

// GetCurrentState returns the current state of all leases in the processor group.
// This provides operational visibility into which instances own which partitions,
// their checkpoint positions, and whether any leases have expired.
//
// This method can be called whether or not the processor is running.
func (p *ChangeFeedProcessor) GetCurrentState(ctx context.Context) ([]ChangeFeedProcessorState, error) {
	leases, err := p.leaseStore.getAllLeases(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read leases: %w", err)
	}

	states := make([]ChangeFeedProcessorState, len(leases))
	for i, lease := range leases {
		states[i] = ChangeFeedProcessorState{
			LeaseToken:        lease.ID,
			Owner:             lease.Owner,
			ContinuationToken: lease.ContinuationToken,
			FeedRange:         lease.FeedRange,
			LastUpdated:       time.Unix(lease.Timestamp, 0),
			IsExpired:         lease.isExpired(p.options.LeaseExpirationInterval),
		}
	}

	return states, nil
}

// acquireLeases runs one load-balancing cycle: reads all leases, asks the
// balancer which ones to take, then acquires and starts supervisors for them.
// It also detects leases we own but have no active supervisor for (e.g., after
// a supervisor crash) and restarts them.
func (p *ChangeFeedProcessor) acquireLeases(ctx context.Context) {
	allLeases, err := p.leaseStore.getAllLeases(ctx)
	if err != nil {
		p.monitor.notifyError(ctx, "", fmt.Errorf("failed to list leases: %w", err))
		return
	}

	// Restart supervisors for leases we own but lost due to supervisor crash.
	p.startSupervisorsForOwnedLeases(ctx, allLeases)

	leasesToTake := p.balancer.selectLeasesToAcquire(allLeases)
	for i := range leasesToTake {
		lease := &leasesToTake[i]
		if err := p.leaseManager.acquireLease(ctx, lease); err != nil {
			if isPreconditionFailed(err) {
				p.monitor.notifyLeaseContention(ctx, lease.ID)
			} else {
				p.monitor.notifyError(ctx, lease.ID, err)
			}
			continue
		}
		p.monitor.notifyLeaseAcquired(ctx, lease.ID)
		p.startSupervisor(ctx, lease)
	}
}

// loadOwnedLeases reads all leases and immediately starts supervisors for
// leases this instance already owns. Called once on startup before the main
// acquire loop so processing resumes immediately after a restart.
func (p *ChangeFeedProcessor) loadOwnedLeases(ctx context.Context) {
	allLeases, err := p.leaseStore.getAllLeases(ctx)
	if err != nil {
		p.monitor.notifyError(ctx, "", fmt.Errorf("failed to load owned leases on startup: %w", err))
		return
	}
	p.startSupervisorsForOwnedLeases(ctx, allLeases)
}

// startSupervisorsForOwnedLeases starts supervisors for any leases this
// instance owns but doesn't have a running supervisor for.
func (p *ChangeFeedProcessor) startSupervisorsForOwnedLeases(ctx context.Context, allLeases []changeFeedProcessorLease) {
	p.mu.Lock()
	for i := range allLeases {
		lease := &allLeases[i]
		if lease.Owner == p.instanceName && !lease.isExpired(p.options.LeaseExpirationInterval) {
			if _, running := p.supervisors[lease.ID]; !running {
				p.mu.Unlock()
				p.monitor.notifyLeaseAcquired(ctx, lease.ID)
				p.startSupervisor(ctx, lease)
				p.mu.Lock()
			}
		}
	}
	p.mu.Unlock()
}

// startSupervisor launches a goroutine that polls the change feed for a
// newly acquired lease. No-op if a supervisor is already running for it.
func (p *ChangeFeedProcessor) startSupervisor(ctx context.Context, lease *changeFeedProcessorLease) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.supervisors[lease.ID]; exists {
		return
	}
	supervisorCtx, supervisorCancel := context.WithCancel(ctx)
	p.supervisors[lease.ID] = supervisorCancel

	supervisor := newChangeFeedProcessorSupervisor(
		lease, p.monitoredContainer, p.leaseStore, p.handler, p.options, p.monitor, p.throttle,
	)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() {
			p.mu.Lock()
			delete(p.supervisors, lease.ID)
			p.mu.Unlock()
		}()

		if err := supervisor.run(supervisorCtx); err != nil {
			// Determine close reason from the error type.
			reason := LeaseCloseReasonUnknown
			if _, ok := isPartitionGone(err); ok {
				reason = LeaseCloseReasonPartitionGone
				// Persist the last continuation token so the synchronizer
				// can pass it to child leases during splits.
				if token, _ := isPartitionGone(err); token != "" {
					lease.ContinuationToken = token
					_ = p.leaseStore.updateLease(ctx, lease)
				}
			} else if errors.Is(err, errLeaseLost) {
				reason = LeaseCloseReasonLeaseLost
			} else if errors.Is(err, errHandlerFailed) {
				reason = LeaseCloseReasonObserverError
			} else if errors.Is(err, errNonRetryable) {
				reason = LeaseCloseReasonNonRetryableError
			} else if errors.Is(err, context.Canceled) {
				reason = LeaseCloseReasonShutdown
			}

			if reason != LeaseCloseReasonShutdown && reason != LeaseCloseReasonLeaseLost {
				p.monitor.notifyError(ctx, lease.ID, fmt.Errorf("supervisor exited: %w", err))
			}
			if reason == LeaseCloseReasonLeaseLost {
				// Expected rebalancing — use contention notification, not error.
				// Matches .NET: bare LeaseLostException is not reported to the monitor.
				p.monitor.notifyLeaseContention(ctx, lease.ID)
			}
			// Release the lease so the balancer can re-acquire it on the
			// next cycle instead of thinking we still own it.
			p.leaseManager.releaseLease(ctx, lease)
			p.monitor.notifyLeaseClosed(ctx, lease.ID, reason)
		}
	}()
}

// renewLoop periodically renews all leases owned by this instance.
func (p *ChangeFeedProcessor) renewLoop(ctx context.Context) {
	ticker := time.NewTicker(p.options.LeaseRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.renewOwnedLeases(ctx)
		}
	}
}

// renewOwnedLeases renews every lease this instance currently owns.
func (p *ChangeFeedProcessor) renewOwnedLeases(ctx context.Context) {
	allLeases, err := p.leaseStore.getAllLeases(ctx)
	if err != nil {
		p.monitor.notifyError(ctx, "", fmt.Errorf("failed to list leases for renewal: %w", err))
		return
	}
	for i := range allLeases {
		if allLeases[i].Owner == p.instanceName {
			if err := p.leaseManager.renewLease(ctx, &allLeases[i]); err != nil {
				p.monitor.notifyError(ctx, allLeases[i].ID, fmt.Errorf("failed to renew lease: %w", err))
			}
		}
	}
}

// releaseAllLeases cancels all supervisors and releases owned leases during shutdown.
// This deliberately queries Cosmos for ALL leases matching our instance name rather
// than relying on the in-memory supervisors map. This handles the case where a
// supervisor exited early (crash, errNonRetryable, etc.) and removed itself from
// the map — the lease in Cosmos still shows our instance as owner and must be released
// so other instances can acquire it immediately instead of waiting for expiration.
func (p *ChangeFeedProcessor) releaseAllLeases() {
	p.mu.Lock()
	for _, cancel := range p.supervisors {
		cancel()
	}
	p.mu.Unlock()

	// Wait for all supervisor goroutines to finish before releasing leases.
	// This matches .NET's Task.WhenAll pattern in ShutdownAsync.
	p.wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	allLeases, err := p.leaseStore.getAllLeases(ctx)
	if err != nil {
		return
	}
	for i := range allLeases {
		if allLeases[i].Owner == p.instanceName {
			_ = p.leaseManager.releaseLease(ctx, &allLeases[i])
			p.monitor.notifyLeaseClosed(ctx, allLeases[i].ID, LeaseCloseReasonShutdown)
		}
	}
}

// generateInstanceName builds a unique identifier for this processor instance
// by combining the hostname with a UUID.
func generateInstanceName() (string, error) {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	id, err := uuid.New()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%s", host, id), nil
}

// jitteredInterval returns the given interval with ±30% random jitter applied.
// This spreads out acquire cycles across instances that started with the same
// configuration, reducing 412 contention on lease writes.
func jitteredInterval(base time.Duration) time.Duration {
	// jitter in range [-0.3, +0.3]
	jitter := (rand.Float64()*0.6 - 0.3) * float64(base)
	return base + time.Duration(jitter)
}
