// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

// changeFeedProcessorLeaseManager manages lease lifecycle operations.
// All write operations use ETag optimistic concurrency to prevent conflicts.
type changeFeedProcessorLeaseManager struct {
	store        *changeFeedProcessorLeaseStore
	instanceName string
	options      ChangeFeedProcessorOptions
}

// newChangeFeedProcessorLeaseManager creates a new lease manager.
func newChangeFeedProcessorLeaseManager(store *changeFeedProcessorLeaseStore, instanceName string, options ChangeFeedProcessorOptions) *changeFeedProcessorLeaseManager {
	return &changeFeedProcessorLeaseManager{
		store:        store,
		instanceName: instanceName,
		options:      options,
	}
}

// acquireLease attempts to take ownership of a lease by setting the Owner field
// and updating the Timestamp. Uses ETag concurrency — returns an error if another
// instance modified the lease since it was last read. Callers should treat
// 412 Precondition Failed as a normal contention signal, not a fatal error.
// On success the lease's ETag is updated in place.
func (m *changeFeedProcessorLeaseManager) acquireLease(ctx context.Context, lease *changeFeedProcessorLease) error {
	lease.Owner = m.instanceName
	lease.Timestamp = time.Now().Unix()

	if err := m.store.updateLease(ctx, lease); err != nil {
		if isPreconditionFailed(err) {
			return fmt.Errorf("lease %s was acquired by another instance: %w", lease.ID, err)
		}
		return fmt.Errorf("failed to acquire lease %s: %w", lease.ID, err)
	}

	return nil
}

// renewLease renews an owned lease by updating its Timestamp.
// Only succeeds if the caller is still the owner (enforced via ETag).
// On success the lease's ETag and Timestamp are updated in place.
func (m *changeFeedProcessorLeaseManager) renewLease(ctx context.Context, lease *changeFeedProcessorLease) error {
	lease.Timestamp = time.Now().Unix()

	if err := m.store.updateLease(ctx, lease); err != nil {
		if isPreconditionFailed(err) {
			return fmt.Errorf("lease %s was modified by another instance during renewal: %w", lease.ID, err)
		}
		return fmt.Errorf("failed to renew lease %s: %w", lease.ID, err)
	}

	return nil
}

// releaseLease releases a lease by clearing the Owner field.
// Reads the current lease first to get the latest ETag, then only clears
// ownership if we are still the owner. Fails silently if the lease was
// already taken by another instance.
func (m *changeFeedProcessorLeaseManager) releaseLease(ctx context.Context, lease *changeFeedProcessorLease) error {
	currentLease, err := m.store.getLease(ctx, lease.ID)
	if err != nil {
		// 404 — lease was already deleted (by synchronizer during split/merge).
		// This is harmless; the lease is already gone.
		var responseErr *azcore.ResponseError
		if errors.As(err, &responseErr) && responseErr.StatusCode == http.StatusNotFound {
			return nil
		}
		return fmt.Errorf("failed to read lease %s for release: %w", lease.ID, err)
	}

	if currentLease.Owner != m.instanceName {
		return nil
	}

	currentLease.Owner = ""
	currentLease.Timestamp = time.Now().Unix()

	if err := m.store.updateLease(ctx, currentLease); err != nil {
		if isPreconditionFailed(err) {
			return nil
		}
		// 404 on write — lease was deleted between our read and write.
		var responseErr *azcore.ResponseError
		if errors.As(err, &responseErr) && responseErr.StatusCode == http.StatusNotFound {
			return nil
		}
		return fmt.Errorf("failed to release lease %s: %w", lease.ID, err)
	}

	lease.Owner = currentLease.Owner
	lease.ETag = currentLease.ETag
	lease.Timestamp = currentLease.Timestamp
	return nil
}

// acquireExpiredLeases scans the provided leases and acquires any that are
// expired according to options.LeaseExpirationInterval. Returns only the
// leases that were successfully acquired.
func (m *changeFeedProcessorLeaseManager) acquireExpiredLeases(ctx context.Context, allLeases []changeFeedProcessorLease) ([]changeFeedProcessorLease, error) {
	var acquired []changeFeedProcessorLease

	for i := range allLeases {
		lease := &allLeases[i]
		if !lease.isExpired(m.options.LeaseExpirationInterval) {
			continue
		}

		if err := m.acquireLease(ctx, lease); err != nil {
			if isPreconditionFailed(err) {
				// Another instance beat us — expected in distributed operation.
				continue
			}
			return acquired, fmt.Errorf("error acquiring expired lease %s: %w", lease.ID, err)
		}

		acquired = append(acquired, *lease)
	}

	return acquired, nil
}

// initializeLeases creates lease documents for all feed ranges.
// Uses createLeaseIfNotExists so the operation is idempotent and safe
// to call on every processor startup.
func (m *changeFeedProcessorLeaseManager) initializeLeases(ctx context.Context, feedRanges []FeedRange) error {
	for _, fr := range feedRanges {
		lease := newChangeFeedProcessorLease(fr, "", m.options.LeasePrefix, m.options.Mode)
		if _, err := m.store.createLeaseIfNotExists(ctx, &lease); err != nil {
			return fmt.Errorf("failed to initialize lease for range %s-%s: %w", fr.MinInclusive, fr.MaxExclusive, err)
		}
	}
	return nil
}

// isPreconditionFailed reports whether err is a Cosmos DB 412 Precondition Failed
// response, which indicates an ETag mismatch during an optimistic-concurrency write.
func isPreconditionFailed(err error) bool {
	var responseErr *azcore.ResponseError
	return errors.As(err, &responseErr) && responseErr.StatusCode == http.StatusPreconditionFailed
}
