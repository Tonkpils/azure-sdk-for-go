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
	throttle  *changeFeedProcessorThrottle
}

// newChangeFeedProcessorSupervisor creates a supervisor for the given lease.
func newChangeFeedProcessorSupervisor(
	lease *changeFeedProcessorLease,
	container *ContainerClient,
	store *changeFeedProcessorLeaseStore,
	handler ChangeFeedProcessorHandler,
	options ChangeFeedProcessorOptions,
	monitor *ChangeFeedProcessorHealthMonitor,
	throttle *changeFeedProcessorThrottle,
) *changeFeedProcessorSupervisor {
	return &changeFeedProcessorSupervisor{
		lease:     lease,
		container: container,
		store:     store,
		handler:   handler,
		options:   options,
		monitor:   monitor,
		throttle:  throttle,
	}
}

var (
	// errLeaseLost signals that another instance took ownership of the lease.
	// The supervisor must stop immediately.
	errLeaseLost = errors.New("lease lost (another instance took ownership)")

	// errNonRetryable signals a permanent error that should not be retried
	// (e.g. 401 Unauthorized, 400 Bad Request, 404 Not Found, 403 Forbidden).
	errNonRetryable = errors.New("non-retryable error")

	// errHandlerFailed signals that the user's handler returned an error.
	// The supervisor exits so the lease can be released and re-acquired,
	// matching .NET's ObserverError close reason.
	errHandlerFailed = errors.New("handler error")
)

// partitionGoneError carries the last continuation token when a 410 Gone
// response indicates a partition split or merge. The orchestrator uses this
// token to create child leases that resume from the correct position.
type partitionGoneError struct {
	continuationToken string
}

func (e *partitionGoneError) Error() string {
	return "partition gone (split or merge detected)"
}

// isPartitionGone checks if an error is a partition gone error and returns the
// continuation token if so.
func isPartitionGone(err error) (string, bool) {
	var pge *partitionGoneError
	if errors.As(err, &pge) {
		return pge.continuationToken, true
	}
	return "", false
}

// isNonRetryableStatusCode returns true for HTTP status codes that indicate
// a permanent error. Aligned with .NET and Java SDK behavior.
func isNonRetryableStatusCode(statusCode int) bool {
	switch statusCode {
	case http.StatusUnauthorized,       // 401 - auth failure
		http.StatusBadRequest,          // 400 - client error
		http.StatusNotFound,            // 404 - resource not found
		http.StatusForbidden,           // 403 - permission denied
		http.StatusMethodNotAllowed,    // 405
		http.StatusConflict,            // 409
		http.StatusRequestEntityTooLarge: // 413
		return true
	default:
		return false
	}
}

// run starts the poller and renewer goroutines. The first error from either
// goroutine cancels both and is returned to the caller.
func (s *changeFeedProcessorSupervisor) run(ctx context.Context) error {
	s.monitor.notifySupervisorStart(ctx, s.lease.ID)
	defer s.monitor.notifySupervisorStop(ctx, s.lease.ID)

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
				s.monitor.notifyProcessingError(ctx, s.lease.ID, fmt.Errorf("renew failed: %w", err))
			}
		}
	}
}

// pollLoop reads the change feed in a loop with exponential backoff on
// transient errors and Retry-After awareness for 429 responses.
//
// Matching .NET SDK behavior: the loop runs indefinitely until cancelled or a
// fatal error occurs. Non-retryable errors (401, 403, 404) and structural
// errors (partition gone, lease lost) exit immediately. Transient errors
// (503, timeouts, network blips) retry with capped exponential backoff.
// The backoff resets on any successful poll.
func (s *changeFeedProcessorSupervisor) pollLoop(ctx context.Context) error {
	consecutiveFailures := 0

	for {
		delay, err := s.poll(ctx)
		if err != nil {
			// Fatal errors — exit immediately, no retry.
			var pge *partitionGoneError
			if errors.As(err, &pge) || errors.Is(err, errLeaseLost) || errors.Is(err, errNonRetryable) || errors.Is(err, errHandlerFailed) {
				return err
			}
			consecutiveFailures++
			backoff := s.options.PollInterval * time.Duration(1<<uint(min(consecutiveFailures, 6)))
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
//
// Error classification (aligned with .NET/Java SDKs):
//   - 410 Gone: partition split/merge → partitionGoneError (fatal, orchestrator re-syncs)
//   - 429 Too Many Requests: throttled → returns Retry-After delay, nil error (retryable)
//   - 401, 400, 403, 404, etc.: non-retryable → errNonRetryable (fatal, no retry)
//   - 503, 408, 500, network errors: transient → returns error (pollLoop retries with backoff)
func (s *changeFeedProcessorSupervisor) poll(ctx context.Context) (time.Duration, error) {
	opts := s.buildChangeFeedOptions()

	// Apply per-request timeout
	reqCtx := ctx
	if s.options.RequestTimeout > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, s.options.RequestTimeout)
		defer cancel()
	}

	pollStart := time.Now()
	resp, err := s.container.GetChangeFeed(reqCtx, &opts)
	pollDuration := time.Since(pollStart)

	if err != nil {
		s.monitor.notifyPollComplete(ctx, s.lease.ID, pollDuration, 0, err)

		var responseErr *azcore.ResponseError
		if errors.As(err, &responseErr) {
			// 410 Gone — partition split or merge. Stop and let orchestrator re-sync.
			// Carry the current continuation token so child leases can resume.
			if responseErr.StatusCode == http.StatusGone {
				return 0, &partitionGoneError{continuationToken: s.lease.ContinuationToken}
			}

			// 429 Too Many Requests — respect Retry-After, not counted as failure.
			if responseErr.StatusCode == http.StatusTooManyRequests {
				retryAfter := 5 * time.Second
				if responseErr.RawResponse != nil {
					if ra := responseErr.RawResponse.Header.Get("Retry-After"); ra != "" {
						if secs, parseErr := strconv.Atoi(ra); parseErr == nil {
							retryAfter = time.Duration(secs) * time.Second
						}
					}
				}
				// Cap at 30s per .NET/Java SDK defaults.
				if retryAfter > 30*time.Second {
					retryAfter = 30 * time.Second
				}
				return retryAfter, nil
			}

			// Non-retryable errors — fail fast, don't waste retry budget.
			if isNonRetryableStatusCode(responseErr.StatusCode) {
				s.monitor.notifyProcessingError(ctx, s.lease.ID, fmt.Errorf("non-retryable error (HTTP %d): %w", responseErr.StatusCode, err))
				return 0, fmt.Errorf("%w: HTTP %d: %s", errNonRetryable, responseErr.StatusCode, responseErr.ErrorCode)
			}
		}

		// Transient / unknown errors — let pollLoop retry with backoff.
		cfErr := fmt.Errorf("change feed read error: %w", err)
		s.monitor.notifyProcessingError(ctx, s.lease.ID, cfErr)
		return 0, cfErr
	}

	itemCount := int(resp.Count)
	s.monitor.notifyPollComplete(ctx, s.lease.ID, pollDuration, itemCount, nil)

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
		s.monitor.notifyProcessingError(ctx, s.lease.ID, fmt.Errorf("handler error: %w", err))
		return 0, fmt.Errorf("%w: %v", errHandlerFailed, err)
	}

	if err := s.checkpoint(ctx, &resp); err != nil {
		return 0, err
	}

	// Throttle based on RU consumption if configured.
	if resp.RequestCharge > 0 {
		if err := s.throttle.Wait(ctx, float64(resp.RequestCharge)); err != nil {
			return 0, err
		}
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
	// Build the composite continuation token from the response's feed range
	// and ETag. In beta.6+ the queue-driven GetChangeFeed populates
	// ContinuationToken internally, but we still need the composite form for
	// our lease store so we can resume with the correct feed range context.
	if token, err := resp.GetCompositeContinuationToken(); err == nil && token != "" {
		resp.ContinuationToken = token
	}
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
