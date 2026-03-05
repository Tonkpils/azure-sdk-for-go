// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

// changeFeedProcessorSupervisor manages a single partition's change feed polling loop.
// It reads changes from the monitored container, invokes the user handler, and
// checkpoints progress by updating the lease's continuation token.
//
// The supervisor runs two goroutines linked by a shared context:
//   - Poller: reads the change feed, calls the handler, and checkpoints
//   - Renewer: periodically renews the lease
//
// If either goroutine fails, both stop.
type changeFeedProcessorSupervisor struct {
	lease     *changeFeedProcessorLease
	container *ContainerClient
	store     *changeFeedProcessorLeaseStore
	handler   ChangeFeedProcessorHandler
	options   ChangeFeedProcessorOptions
	monitor   *ChangeFeedProcessorHealthMonitor
}

// newChangeFeedProcessorSupervisor creates a supervisor for the given lease.
func newChangeFeedProcessorSupervisor(
	lease *changeFeedProcessorLease,
	container *ContainerClient,
	store *changeFeedProcessorLeaseStore,
	handler ChangeFeedProcessorHandler,
	options ChangeFeedProcessorOptions,
	monitor *ChangeFeedProcessorHealthMonitor,
) *changeFeedProcessorSupervisor {
	return &changeFeedProcessorSupervisor{
		lease:     lease,
		container: container,
		store:     store,
		handler:   handler,
		options:   options,
		monitor:   monitor,
	}
}

var (
	// errPartitionGone signals that the partition range has split or merged.
	// The supervisor should stop and let the orchestrator re-sync leases.
	errPartitionGone = errors.New("partition gone (split or merge detected)")

	// errLeaseLost signals that another instance took ownership of the lease.
	// The supervisor must stop immediately.
	errLeaseLost = errors.New("lease lost (another instance took ownership)")

	// errMaxRetriesExceeded signals that the handler failed too many times
	// in a row, so the orchestrator should release the lease.
	errMaxRetriesExceeded = errors.New("max handler retries exceeded")
)

// run starts the poller and renewer goroutines. The first error from either
// goroutine cancels both and is returned to the caller.
func (s *changeFeedProcessorSupervisor) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)

	go func() {
		errCh <- s.renewLoop(ctx)
	}()

	go func() {
		errCh <- s.pollLoop(ctx)
	}()

	err := <-errCh
	cancel()
	return err
}

// renewLoop periodically renews the lease. If renewal gets a 412
// (ETag mismatch), the lease was stolen and the supervisor must exit.
func (s *changeFeedProcessorSupervisor) renewLoop(ctx context.Context) error {
	ticker := time.NewTicker(s.options.LeaseRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.lease.Timestamp = time.Now().Unix()
			if err := s.store.updateLease(ctx, s.lease); err != nil {
				if isPreconditionFailed(err) {
					return errLeaseLost
				}
				s.monitor.notifyError(ctx, s.lease.ID, fmt.Errorf("renew failed: %w", err))
			}
		}
	}
}

// pollLoop reads the change feed in a loop with exponential backoff on
// handler errors and Retry-After awareness for 429 responses.
func (s *changeFeedProcessorSupervisor) pollLoop(ctx context.Context) error {
	consecutiveFailures := 0
	maxRetries := 10

	for {
		delay, err := s.poll(ctx)
		if err != nil {
			if errors.Is(err, errPartitionGone) || errors.Is(err, errLeaseLost) {
				return err
			}
			consecutiveFailures++
			if consecutiveFailures >= maxRetries {
				return fmt.Errorf("%w: lease %s failed %d times", errMaxRetriesExceeded, s.lease.ID, consecutiveFailures)
			}
			backoff := s.options.PollInterval * time.Duration(1<<uint(consecutiveFailures))
			if backoff > 60*time.Second {
				backoff = 60 * time.Second
			}
			delay = backoff
		} else {
			consecutiveFailures = 0
		}

		if delay <= 0 {
			delay = s.options.PollInterval
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// poll executes a single change feed read, dispatches documents to the handler,
// and checkpoints the lease on success. It returns a suggested delay (non-zero
// only for 429 throttling) and an error for fatal conditions.
func (s *changeFeedProcessorSupervisor) poll(ctx context.Context) (time.Duration, error) {
	opts := s.buildChangeFeedOptions()

	// Apply per-request timeout
	reqCtx := ctx
	if s.options.RequestTimeout > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, s.options.RequestTimeout)
		defer cancel()
	}

	resp, err := s.container.GetChangeFeed(reqCtx, &opts)
	if err != nil {
		var responseErr *azcore.ResponseError
		if errors.As(err, &responseErr) {
			if responseErr.StatusCode == http.StatusGone {
				return 0, errPartitionGone
			}
			if responseErr.StatusCode == http.StatusTooManyRequests {
				retryAfter := 5 * time.Second
				if responseErr.RawResponse != nil {
					if ra := responseErr.RawResponse.Header.Get("Retry-After"); ra != "" {
						if secs, parseErr := strconv.Atoi(ra); parseErr == nil {
							retryAfter = time.Duration(secs) * time.Second
						}
					}
				}
				return retryAfter, nil
			}
		}
		cfErr := fmt.Errorf("change feed read error: %w", err)
		s.monitor.notifyError(ctx, s.lease.ID, cfErr)
		return 0, cfErr
	}

	if resp.RawResponse != nil && resp.RawResponse.StatusCode == http.StatusNotModified {
		return 0, nil
	}
	if resp.Count == 0 {
		return 0, nil
	}

	documents := make([][]byte, len(resp.Documents))
	for i, doc := range resp.Documents {
		documents[i] = []byte(doc)
	}

	if err := s.handler(ctx, documents); err != nil {
		return 0, fmt.Errorf("handler error: %w", err)
	}

	if err := s.checkpoint(ctx, &resp); err != nil {
		return 0, err
	}

	return 0, nil
}

// buildChangeFeedOptions constructs the options for a GetChangeFeed call based
// on the current lease state and processor options.
func (s *changeFeedProcessorSupervisor) buildChangeFeedOptions() ChangeFeedOptions {
	opts := ChangeFeedOptions{
		FeedRange:    s.lease.FeedRange,
		MaxItemCount: s.options.MaxItemCount,
		Mode:         s.options.Mode,
	}

	if s.lease.ContinuationToken != "" {
		opts.Continuation = &s.lease.ContinuationToken
	} else if s.options.StartTime != nil {
		opts.StartFrom = s.options.StartTime
	}

	return opts
}

// checkpoint persists the continuation token from the response into the lease.
// Returns errLeaseLost if the lease was taken by another instance (412).
func (s *changeFeedProcessorSupervisor) checkpoint(ctx context.Context, resp *ChangeFeedResponse) error {
	resp.PopulateCompositeContinuationToken()
	if resp.ContinuationToken != "" {
		s.lease.ContinuationToken = resp.ContinuationToken
	}
	s.lease.Timestamp = time.Now().Unix()

	if err := s.store.updateLease(ctx, s.lease); err != nil {
		if isPreconditionFailed(err) {
			return errLeaseLost
		}
		return fmt.Errorf("checkpoint failed: %w", err)
	}
	return nil
}
