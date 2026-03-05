// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"fmt"
	"log"
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
// leases and creates new leases for any ranges that are missing. When a new range
// falls within an existing lease's range (partition split), the child lease inherits
// the parent's continuation token so processing resumes from where it left off.
// Stale parent leases are removed after their children are created.
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

	// Track parent leases whose ranges have been split into children.
	staleParents := make(map[string]struct{})

	for _, fr := range feedRanges {
		id := leaseIDFromFeedRange(fr)
		if _, ok := existing[id]; ok {
			continue
		}

		// New range — look for a parent lease whose range contains this one.
		lease := newChangeFeedProcessorLease(fr, "")
		if parent := findParentLease(fr, leases); parent != nil {
			lease.ContinuationToken = parent.ContinuationToken
			staleParents[parent.ID] = struct{}{}
		}

		if _, err := s.store.createLeaseIfNotExists(ctx, &lease); err != nil {
			return fmt.Errorf("failed to create lease for range %s-%s: %w", fr.MinInclusive, fr.MaxExclusive, err)
		}
	}

	// Clean up parent leases that were split — their children have taken over.
	for parentID := range staleParents {
		if err := s.store.deleteLease(ctx, parentID); err != nil {
			log.Printf("changefeed synchronizer: failed to delete stale parent lease %s: %v", parentID, err)
		}
	}

	return nil
}

// findParentLease returns the lease whose feed range fully contains the given
// child range, or nil if no parent exists. This detects partition splits where
// one range has been divided into smaller ranges.
func findParentLease(child FeedRange, leases []changeFeedProcessorLease) *changeFeedProcessorLease {
	for i := range leases {
		l := &leases[i]
		if l.FeedRange == nil {
			continue
		}
		if l.FeedRange.MinInclusive <= child.MinInclusive && l.FeedRange.MaxExclusive >= child.MaxExclusive {
			// Same exact range — not a parent, it's the same lease.
			if l.FeedRange.MinInclusive == child.MinInclusive && l.FeedRange.MaxExclusive == child.MaxExclusive {
				continue
			}
			return l
		}
	}
	return nil
}
