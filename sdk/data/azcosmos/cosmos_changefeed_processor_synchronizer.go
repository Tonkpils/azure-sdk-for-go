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
	prefix    string
	mode      ChangeFeedMode
	monitor   *ChangeFeedProcessorHealthMonitor
}

// newChangeFeedProcessorSynchronizer creates a synchronizer for the given container and lease store.
func newChangeFeedProcessorSynchronizer(
	container *ContainerClient,
	store *changeFeedProcessorLeaseStore,
	prefix string,
	mode ChangeFeedMode,
	monitor *ChangeFeedProcessorHealthMonitor,
) *changeFeedProcessorSynchronizer {
	return &changeFeedProcessorSynchronizer{
		container: container,
		store:     store,
		prefix:    prefix,
		mode:      mode,
		monitor:   monitor,
	}
}

// synchronizeLeases compares the container's current feed ranges against existing
// leases and creates new leases for any ranges that are missing. It handles both
// partition splits (one parent → multiple children) and partition merges (multiple
// children → one parent). When a split is detected the child inherits the parent's
// continuation token. When a merge is detected the new lease inherits the
// continuation token with the latest timestamp among the merged children.
// Stale leases are removed after their replacements are created.
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

	staleLeases := make(map[string]struct{})

	for _, fr := range feedRanges {
		id := leaseIDFromFeedRange(fr, s.prefix)
		if _, ok := existing[id]; ok {
			continue
		}

		lease := newChangeFeedProcessorLease(fr, "", s.prefix, s.mode)

		// Phase 1: Check for split (one parent contains this child)
		if parent := findParentLease(fr, leases); parent != nil {
			lease.ContinuationToken = parent.ContinuationToken
			staleLeases[parent.ID] = struct{}{}
		} else {
			// Phase 2: Check for merge (this range covers multiple children)
			children := findChildLeases(fr, leases)
			if len(children) > 0 {
				best := children[0]
				for _, child := range children[1:] {
					if child.Timestamp > best.Timestamp {
						best = child
					}
				}
				lease.ContinuationToken = best.ContinuationToken
				for _, child := range children {
					staleLeases[child.ID] = struct{}{}
				}
			}
		}

		if _, err := s.store.createLeaseIfNotExists(ctx, &lease); err != nil {
			return fmt.Errorf("failed to create lease for range %s-%s: %w", fr.MinInclusive, fr.MaxExclusive, err)
		}
	}

	// Detect leases whose feed ranges no longer exist (fully merged away).
	currentRangeIDs := make(map[string]struct{}, len(feedRanges))
	for _, fr := range feedRanges {
		currentRangeIDs[leaseIDFromFeedRange(fr, s.prefix)] = struct{}{}
	}
	for _, lease := range leases {
		if _, stillExists := currentRangeIDs[lease.ID]; !stillExists {
			staleLeases[lease.ID] = struct{}{}
		}
	}

	for leaseID := range staleLeases {
		s.monitor.notifyError(ctx, leaseID, fmt.Errorf("orphaned lease detected, cleaning up"))
		if err := s.store.deleteLease(ctx, leaseID); err != nil {
			s.monitor.notifyError(ctx, leaseID, fmt.Errorf("failed to delete orphaned lease: %w", err))
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

// findChildLeases returns all leases whose feed ranges are fully contained
// within the given parent range. This detects partition merges where multiple
// smaller ranges have been combined into one larger range.
func findChildLeases(parent FeedRange, leases []changeFeedProcessorLease) []*changeFeedProcessorLease {
	var children []*changeFeedProcessorLease
	for i := range leases {
		l := &leases[i]
		if l.FeedRange == nil {
			continue
		}
		// Child must be strictly contained (not equal)
		if parent.MinInclusive <= l.FeedRange.MinInclusive && parent.MaxExclusive >= l.FeedRange.MaxExclusive {
			if parent.MinInclusive == l.FeedRange.MinInclusive && parent.MaxExclusive == l.FeedRange.MaxExclusive {
				continue
			}
			children = append(children, l)
		}
	}
	return children
}
