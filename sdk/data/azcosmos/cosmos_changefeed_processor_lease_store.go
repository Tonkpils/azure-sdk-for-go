// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

// changeFeedProcessorLeaseStore handles CRUD operations for lease documents
// in the lease container. Uses ETag-based optimistic concurrency for all writes.
// A channel-based semaphore limits concurrent Cosmos requests to prevent HTTP/2
// MAX_CONCURRENT_STREAMS exhaustion in gateway mode.
type changeFeedProcessorLeaseStore struct {
	container *ContainerClient
	prefix    string
	sem       chan struct{} // concurrency limiter
}

// newChangeFeedProcessorLeaseStore creates a new lease store backed by the given container.
// maxConcurrent controls the maximum number of simultaneous Cosmos operations.
func newChangeFeedProcessorLeaseStore(container *ContainerClient, prefix string, maxConcurrent int) *changeFeedProcessorLeaseStore {
	if maxConcurrent <= 0 {
		maxConcurrent = 50
	}
	return &changeFeedProcessorLeaseStore{
		container: container,
		prefix:    prefix,
		sem:       make(chan struct{}, maxConcurrent),
	}
}

func (s *changeFeedProcessorLeaseStore) acquire() { s.sem <- struct{}{} }
func (s *changeFeedProcessorLeaseStore) release() { <-s.sem }

// getAllLeases reads all lease documents from the lease container.
// Uses a cross-partition query to retrieve every lease across all partitions.
func (s *changeFeedProcessorLeaseStore) getAllLeases(ctx context.Context) ([]changeFeedProcessorLease, error) {
	query := "SELECT * FROM c"
	if s.prefix != "" {
		query = fmt.Sprintf("SELECT * FROM c WHERE STARTSWITH(c.id, '%s')", s.prefix+"..")
	}
	pk := NewPartitionKey()
	pager := s.container.NewQueryItemsPager(query, pk, nil)

	var leases []changeFeedProcessorLease
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to query leases: %w", err)
		}

		for _, item := range page.Items {
			var lease changeFeedProcessorLease
			if err := json.Unmarshal(item, &lease); err != nil {
				return nil, fmt.Errorf("failed to unmarshal lease: %w", err)
			}
			leases = append(leases, lease)
		}
	}

	return leases, nil
}

// getLease reads a specific lease document by ID.
func (s *changeFeedProcessorLeaseStore) getLease(ctx context.Context, leaseID string) (*changeFeedProcessorLease, error) {
	s.acquire()
	defer s.release()

	pk := NewPartitionKeyString(leaseID)
	resp, err := s.container.ReadItem(ctx, pk, leaseID, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read lease %s: %w", leaseID, err)
	}

	var lease changeFeedProcessorLease
	if err := json.Unmarshal(resp.Value, &lease); err != nil {
		return nil, fmt.Errorf("failed to unmarshal lease %s: %w", leaseID, err)
	}

	lease.ETag = string(resp.ETag)
	return &lease, nil
}

// createLease creates a new lease document. Returns an error if it already exists (409 Conflict).
func (s *changeFeedProcessorLeaseStore) createLease(ctx context.Context, lease *changeFeedProcessorLease) error {
	s.acquire()
	defer s.release()

	data, err := json.Marshal(lease)
	if err != nil {
		return fmt.Errorf("failed to marshal lease: %w", err)
	}

	pk := NewPartitionKeyString(lease.ID)
	resp, err := s.container.CreateItem(ctx, pk, data, nil)
	if err != nil {
		return fmt.Errorf("failed to create lease %s: %w", lease.ID, err)
	}

	lease.ETag = string(resp.ETag)
	return nil
}

// updateLease updates an existing lease with ETag optimistic concurrency.
// The lease's ETag must match the current document — if another instance modified
// the lease since it was last read, the update fails with 412 Precondition Failed.
func (s *changeFeedProcessorLeaseStore) updateLease(ctx context.Context, lease *changeFeedProcessorLease) error {
	s.acquire()
	defer s.release()

	data, err := json.Marshal(lease)
	if err != nil {
		return fmt.Errorf("failed to marshal lease: %w", err)
	}

	etag := azcore.ETag(lease.ETag)
	opts := &ItemOptions{
		IfMatchEtag: &etag,
	}

	pk := NewPartitionKeyString(lease.ID)
	resp, err := s.container.ReplaceItem(ctx, pk, lease.ID, data, opts)
	if err != nil {
		return fmt.Errorf("failed to update lease %s: %w", lease.ID, err)
	}

	lease.ETag = string(resp.ETag)
	return nil
}

// deleteLease removes a lease document from the container.
func (s *changeFeedProcessorLeaseStore) deleteLease(ctx context.Context, leaseID string) error {
	s.acquire()
	defer s.release()

	pk := NewPartitionKeyString(leaseID)
	_, err := s.container.DeleteItem(ctx, pk, leaseID, nil)
	if err != nil {
		return fmt.Errorf("failed to delete lease %s: %w", leaseID, err)
	}
	return nil
}

// createLeaseIfNotExists creates a lease only if it doesn't already exist.
// If the lease already exists (409 Conflict), the existing lease is returned.
// This makes the operation idempotent for safe concurrent initialization.
func (s *changeFeedProcessorLeaseStore) createLeaseIfNotExists(ctx context.Context, lease *changeFeedProcessorLease) (*changeFeedProcessorLease, error) {
	s.acquire()
	defer s.release()

	data, err := json.Marshal(lease)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal lease: %w", err)
	}

	pk := NewPartitionKeyString(lease.ID)
	resp, err := s.container.CreateItem(ctx, pk, data, nil)
	if err != nil {
		var responseErr *azcore.ResponseError
		if errors.As(err, &responseErr) && responseErr.StatusCode == http.StatusConflict {
			existing, readErr := s.getLease(ctx, lease.ID)
			if readErr != nil {
				return nil, fmt.Errorf("lease %s exists but failed to read it: %w", lease.ID, readErr)
			}
			return existing, nil
		}
		return nil, fmt.Errorf("failed to create lease %s: %w", lease.ID, err)
	}

	lease.ETag = string(resp.ETag)
	return lease, nil
}
