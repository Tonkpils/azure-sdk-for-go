// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"errors"
	"fmt"
	"log"
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

	leaseStore   *changeFeedProcessorLeaseStore
	leaseManager *changeFeedProcessorLeaseManager
	synchronizer *changeFeedProcessorSynchronizer
	balancer     *changeFeedProcessorBalancer
	supervisors  map[string]context.CancelFunc // leaseID → cancel function
	mu           sync.Mutex                    // protects supervisors map

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
		opts.StartFromBeginning = options.StartFromBeginning
		opts.StartTime = options.StartTime
		opts.LeasePrefix = options.LeasePrefix
		opts.Mode = options.Mode
	}

	instanceName, err := generateInstanceName()
	if err != nil {
		return nil, fmt.Errorf("azcosmos: failed to generate instance name: %w", err)
	}

	leaseStore := newChangeFeedProcessorLeaseStore(leaseContainer)
	leaseManager := newChangeFeedProcessorLeaseManager(leaseStore, instanceName, opts)
	synchronizer := newChangeFeedProcessorSynchronizer(c, leaseStore)
	balancer := newChangeFeedProcessorBalancer(instanceName, opts.LeaseExpirationInterval)

	return &ChangeFeedProcessor{
		processorName:      processorName,
		instanceName:       instanceName,
		monitoredContainer: c,
		handler:            handler,
		options:            opts,
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

	// 2. Start lease renewal goroutine
	go p.renewLoop(ctx)

	// 3. Main acquisition loop
	acquireTicker := time.NewTicker(p.options.LeaseAcquireInterval)
	defer acquireTicker.Stop()

	for {
		p.acquireLeases(ctx)

		select {
		case <-ctx.Done():
			p.releaseAllLeases()
			return nil
		case <-acquireTicker.C:
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

// acquireLeases runs one load-balancing cycle: reads all leases, asks the
// balancer which ones to take, then acquires and starts supervisors for them.
func (p *ChangeFeedProcessor) acquireLeases(ctx context.Context) {
	allLeases, err := p.leaseStore.getAllLeases(ctx)
	if err != nil {
		log.Printf("changefeed processor: failed to list leases: %v", err)
		return
	}

	leasesToTake := p.balancer.selectLeasesToAcquire(allLeases)
	for i := range leasesToTake {
		lease := &leasesToTake[i]
		if err := p.leaseManager.acquireLease(ctx, lease); err != nil {
			continue // another instance got it — expected
		}
		p.startSupervisor(ctx, lease)
	}
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
		lease, p.monitoredContainer, p.leaseStore, p.handler, p.options,
	)
	go func() {
		defer func() {
			p.mu.Lock()
			delete(p.supervisors, lease.ID)
			p.mu.Unlock()
		}()
		supervisor.run(supervisorCtx)
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
		log.Printf("changefeed processor: failed to list leases for renewal: %v", err)
		return
	}
	for i := range allLeases {
		if allLeases[i].Owner == p.instanceName {
			if err := p.leaseManager.renewLease(ctx, &allLeases[i]); err != nil {
				log.Printf("changefeed processor: failed to renew lease %s: %v", allLeases[i].ID, err)
			}
		}
	}
}

// releaseAllLeases cancels all supervisors and releases owned leases during shutdown.
func (p *ChangeFeedProcessor) releaseAllLeases() {
	p.mu.Lock()
	for _, cancel := range p.supervisors {
		cancel()
	}
	p.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	allLeases, err := p.leaseStore.getAllLeases(ctx)
	if err != nil {
		return
	}
	for i := range allLeases {
		if allLeases[i].Owner == p.instanceName {
			_ = p.leaseManager.releaseLease(ctx, &allLeases[i])
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
