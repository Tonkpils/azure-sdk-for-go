// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import "time"

// ChangeFeedProcessorOptions configures a ChangeFeedProcessor.
type ChangeFeedProcessorOptions struct {
	// MaxItemCount is the maximum number of items per change feed page.
	// Default: 100
	MaxItemCount int32

	// PollInterval is the delay between polls when no changes are available.
	// Default: 5 seconds
	PollInterval time.Duration

	// LeaseExpirationInterval is how long before an unrenewed lease is considered expired.
	// Default: 60 seconds
	LeaseExpirationInterval time.Duration

	// LeaseRenewInterval is how often to renew owned leases.
	// Default: 17 seconds (matching .NET SDK default)
	LeaseRenewInterval time.Duration

	// LeaseAcquireInterval is how often to check for available leases.
	// Default: 13 seconds (matching .NET SDK default)
	LeaseAcquireInterval time.Duration

	// StartFromBeginning starts processing from the beginning of the change feed.
	// Default: false (starts from current point)
	//
	// When StartFromBeginning is true and no StartTime is set, the supervisor
	// sets StartFrom to the Unix epoch (time.Unix(0, 0)) so the change feed
	// returns all available history. If StartTime is also set, StartTime takes
	// precedence.
	StartFromBeginning bool

	// StartTime starts processing from a specific point in time.
	// Takes precedence over StartFromBeginning if both are set.
	StartTime *time.Time

	// LeasePrefix is prepended to lease IDs for namespace isolation.
	// Useful when multiple processors share a lease container.
	LeasePrefix string

	// Mode specifies the change feed mode.
	// Default: ChangeFeedModeLatestVersion (incremental, creates and updates only).
	// Set to ChangeFeedModeAllVersionsAndDeletes for full fidelity mode
	// (includes deletes and all intermediate versions). Requires the container
	// to have a ChangeFeedPolicy with a retention window configured.
	Mode ChangeFeedMode

	// MinPartitionCount is the minimum number of partitions this instance should own.
	// Default: 0 (no minimum, let the balancer decide).
	MinPartitionCount int

	// MaxPartitionCount is the maximum number of partitions this instance should own.
	// Default: 0 (no maximum, take as many as the balancer assigns).
	MaxPartitionCount int

	// RequestTimeout is the maximum duration for a single change feed request.
	// If a request takes longer, it is cancelled and retried.
	// Default: 30 seconds.
	RequestTimeout time.Duration

	// BalancerStrategy controls lease acquisition behavior during rebalancing.
	// BalancerStrategyEqual (default) acquires at most one lease per cycle for stability.
	// BalancerStrategyGreedy acquires up to the target count in a single cycle for faster convergence.
	BalancerStrategy BalancerStrategy

	// HealthMonitor provides optional callbacks for lease lifecycle events and errors.
	// If nil, events are logged to the standard logger.
	HealthMonitor *ChangeFeedProcessorHealthMonitor

	// MaxRUPerSecond limits the total request units consumed per second across
	// all partition supervisors. When set, supervisors will pause between polls
	// to stay within the RU budget. 0 means unlimited (default).
	MaxRUPerSecond float64

	// MaxLeasesPerAcquireCycle limits how many leases are acquired in a single
	// balancer cycle. Default: 0 (unlimited — the balancer decides based on
	// target distribution, and the MaxConcurrentOperations semaphore prevents
	// HTTP/2 overload). Set a positive value to limit goroutine creation rate
	// if memory is a concern at very high partition counts.
	MaxLeasesPerAcquireCycle int
}

// changeFeedProcessorDefaults returns options with sensible defaults.
func changeFeedProcessorDefaults() ChangeFeedProcessorOptions {
	return ChangeFeedProcessorOptions{
		MaxItemCount:            100,
		PollInterval:            5 * time.Second,
		LeaseExpirationInterval: 60 * time.Second,
		LeaseRenewInterval:      17 * time.Second,
		LeaseAcquireInterval:    13 * time.Second,
		RequestTimeout:          30 * time.Second,
		MaxLeasesPerAcquireCycle: 0, // unlimited — HTTP/2 transport manages concurrency
	}
}
