// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"math"
	"time"
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
}

// newChangeFeedProcessorBalancer creates a balancer for the given processor instance.
func newChangeFeedProcessorBalancer(instanceName string, expirationInterval time.Duration, minPartitionCount, maxPartitionCount int) *changeFeedProcessorBalancer {
	return &changeFeedProcessorBalancer{
		instanceName:       instanceName,
		expirationInterval: expirationInterval,
		minPartitionCount:  minPartitionCount,
		maxPartitionCount:  maxPartitionCount,
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
	if len(expiredLeases) > 0 {
		if partitionsNeeded > len(expiredLeases) {
			partitionsNeeded = len(expiredLeases)
		}
		return expiredLeases[:partitionsNeeded]
	}

	// Step 5: No expired leases — try to steal ONE from the busiest worker.
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

	// Only steal if the busiest worker has more leases than the threshold.
	// Threshold: target-1 when we need multiple, target when we only need one.
	stealThreshold := target
	if partitionsNeeded > 1 {
		stealThreshold = target - 1
	}
	if busiestWorker == "" || busiestCount <= stealThreshold {
		return nil
	}

	// Find one lease owned by the busiest worker.
	for i := range allLeases {
		if allLeases[i].Owner == busiestWorker {
			return []changeFeedProcessorLease{allLeases[i]}
		}
	}

	return nil
}
