// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"fmt"
)

// changeFeedProcessorSynchronizer detects partition splits/merges and ensures
// that a lease exists for every current feed range in the monitored container.
type changeFeedProcessorSynchronizer struct {
	container *ContainerClient
	store     *changeFeedProcessorLeaseStore
}

// newChangeFeedProcessorSynchronizer creates a synchronizer for the given container and lease store.
func newChangeFeedProcessorSynchronizer(
	container *ContainerClient,
	store *changeFeedProcessorLeaseStore,
) *changeFeedProcessorSynchronizer {
	return &changeFeedProcessorSynchronizer{
		container: container,
		store:     store,
	}
}

// synchronizeLeases compares the container's current feed ranges against existing
// leases and creates new leases for any ranges that are missing. This handles
// initial bootstrap (no leases exist), partition splits, and scale-up scenarios.
func (s *changeFeedProcessorSynchronizer) synchronizeLeases(ctx context.Context) error {
	feedRanges, err := s.container.GetFeedRanges(ctx)
	if err != nil {
		return fmt.Errorf("failed to get feed ranges: %w", err)
	}

	leases, err := s.store.getAllLeases(ctx)
	if err != nil {
		return fmt.Errorf("failed to get existing leases: %w", err)
	}

	existing := make(map[string]struct{}, len(leases))
	for _, lease := range leases {
		existing[lease.ID] = struct{}{}
	}

	for _, fr := range feedRanges {
		id := leaseIDFromFeedRange(fr)
		if _, ok := existing[id]; ok {
			continue
		}

		lease := newChangeFeedProcessorLease(fr, "")
		if _, err := s.store.createLeaseIfNotExists(ctx, &lease); err != nil {
			return fmt.Errorf("failed to create lease for range %s-%s: %w", fr.MinInclusive, fr.MaxExclusive, err)
		}
	}

	return nil
}
