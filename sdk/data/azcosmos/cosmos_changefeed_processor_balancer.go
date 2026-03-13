// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"math"
	"math/rand"
	"sort"
	"time"
)

// BalancerStrategy controls how the processor acquires leases during rebalancing.
type BalancerStrategy int

const (
	// BalancerStrategyEqual acquires at most one lease per cycle (conservative, less churn).
	BalancerStrategyEqual BalancerStrategy = iota
	// BalancerStrategyGreedy acquires up to the target count in a single cycle (faster convergence).
	BalancerStrategyGreedy
)

// changeFeedProcessorBalancer implements an equal-partition load balancing strategy.
// It distributes leases evenly across processor instances by:
//  1. First claiming any expired/unowned leases
//  2. If needed, stealing from the instance with the most leases
type changeFeedProcessorBalancer struct {
	instanceName       string
	expirationInterval time.Duration
	minPartitionCount  int
	maxPartitionCount  int
	strategy           BalancerStrategy
}

// newChangeFeedProcessorBalancer creates a balancer for the given processor instance.
func newChangeFeedProcessorBalancer(instanceName string, expirationInterval time.Duration, minPartitionCount, maxPartitionCount int, strategy BalancerStrategy) *changeFeedProcessorBalancer {
	return &changeFeedProcessorBalancer{
		instanceName:       instanceName,
		expirationInterval: expirationInterval,
		minPartitionCount:  minPartitionCount,
		maxPartitionCount:  maxPartitionCount,
		strategy:           strategy,
	}
}

// selectLeasesToAcquire returns the leases this instance should try to acquire.
// It follows the equal-partition balancing strategy from the .NET SDK:
//  1. Categorize leases into owned vs expired
//  2. Calculate target partition count per worker
//  3. If we need more leases:
//     a. Take from expired leases first
//     b. If none expired, steal one from the busiest worker
func (b *changeFeedProcessorBalancer) selectLeasesToAcquire(allLeases []changeFeedProcessorLease) []changeFeedProcessorLease {
	if len(allLeases) == 0 {
		return nil
	}

	// Step 1: Categorize all leases.
	workerToPartitionCount := map[string]int{}
	var expiredLeases []changeFeedProcessorLease

	for i := range allLeases {
		lease := &allLeases[i]
		if !lease.isOwned() || lease.isExpired(b.expirationInterval) {
			expiredLeases = append(expiredLeases, *lease)
		} else {
			workerToPartitionCount[lease.Owner]++
		}
	}

	// Ensure this instance is in the map even if it owns nothing yet.
	if _, ok := workerToPartitionCount[b.instanceName]; !ok {
		workerToPartitionCount[b.instanceName] = 0
	}

	totalPartitions := len(allLeases)
	totalWorkers := len(workerToPartitionCount)

	// Step 2: Calculate target partitions per worker = ceil(total / workers).
	target := int(math.Ceil(float64(totalPartitions) / float64(totalWorkers)))
	if b.maxPartitionCount > 0 && target > b.maxPartitionCount {
		target = b.maxPartitionCount
	}
	if b.minPartitionCount > 0 && target < b.minPartitionCount {
		target = b.minPartitionCount
	}

	// Step 3: How many more partitions does this instance need?
	myCurrentCount := workerToPartitionCount[b.instanceName]
	partitionsNeeded := target - myCurrentCount
	if partitionsNeeded <= 0 {
		return nil
	}

	// Step 4: Take from expired leases first.
	// Shuffle to reduce collisions when multiple instances target the same
	// expired leases simultaneously (same approach as Java SDK).
	if len(expiredLeases) > 0 {
		rand.Shuffle(len(expiredLeases), func(i, j int) {
			expiredLeases[i], expiredLeases[j] = expiredLeases[j], expiredLeases[i]
		})
		if partitionsNeeded > len(expiredLeases) {
			partitionsNeeded = len(expiredLeases)
		}
		return expiredLeases[:partitionsNeeded]
	}

	// Step 5: Steal leases from busiest workers.
	if b.strategy == BalancerStrategyGreedy {
		// Greedy — steal multiple leases from busiest workers.
		type workerCount struct {
			name  string
			count int
		}
		var workers []workerCount
		for worker, count := range workerToPartitionCount {
			if worker != b.instanceName {
				workers = append(workers, workerCount{worker, count})
			}
		}
		sort.Slice(workers, func(i, j int) bool { return workers[i].count > workers[j].count })

		var stolen []changeFeedProcessorLease
		remaining := partitionsNeeded
		for _, w := range workers {
			if remaining <= 0 {
				break
			}
			if w.count <= target {
				break
			}
			for i := range allLeases {
				if remaining <= 0 {
					break
				}
				if allLeases[i].Owner == w.name {
					stolen = append(stolen, allLeases[i])
					remaining--
				}
			}
		}
		return stolen
	}

	// Equal — steal at most one lease from the busiest worker.
	busiestWorker := ""
	busiestCount := 0
	for worker, count := range workerToPartitionCount {
		if worker == b.instanceName {
			continue
		}
		if count > busiestCount {
			busiestWorker = worker
			busiestCount = count
		}
	}

	stealThreshold := target
	if partitionsNeeded > 1 {
		stealThreshold = target - 1
	}
	if busiestWorker == "" || busiestCount <= stealThreshold {
		return nil
	}

	for i := range allLeases {
		if allLeases[i].Owner == busiestWorker {
			return []changeFeedProcessorLease{allLeases[i]}
		}
	}

	return nil
}
