// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"log"
	"net/http"
	"time"
)

// changeFeedProcessorSupervisor manages a single partition's change feed polling loop.
// It reads changes from the monitored container, invokes the user handler, and
// checkpoints progress by updating the lease's continuation token.
type changeFeedProcessorSupervisor struct {
	lease     *changeFeedProcessorLease
	container *ContainerClient
	store     *changeFeedProcessorLeaseStore
	handler   ChangeFeedProcessorHandler
	options   ChangeFeedProcessorOptions
}

// newChangeFeedProcessorSupervisor creates a supervisor for the given lease.
func newChangeFeedProcessorSupervisor(
	lease *changeFeedProcessorLease,
	container *ContainerClient,
	store *changeFeedProcessorLeaseStore,
	handler ChangeFeedProcessorHandler,
	options ChangeFeedProcessorOptions,
) *changeFeedProcessorSupervisor {
	return &changeFeedProcessorSupervisor{
		lease:     lease,
		container: container,
		store:     store,
		handler:   handler,
		options:   options,
	}
}

// run is the main polling loop for this partition. It runs until ctx is cancelled.
func (s *changeFeedProcessorSupervisor) run(ctx context.Context) error {
	ticker := time.NewTicker(s.options.PollInterval)
	defer ticker.Stop()

	for {
		s.poll(ctx)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// poll executes a single change feed read, dispatches documents to the handler,
// and checkpoints the lease on success.
func (s *changeFeedProcessorSupervisor) poll(ctx context.Context) {
	opts := s.buildChangeFeedOptions()

	resp, err := s.container.GetChangeFeed(ctx, &opts)
	if err != nil {
		log.Printf("changefeed supervisor: error reading feed range %s: %v", s.lease.ID, err)
		return
	}

	if resp.RawResponse != nil && resp.RawResponse.StatusCode == http.StatusNotModified {
		return
	}

	if resp.Count == 0 {
		return
	}

	documents := make([][]byte, len(resp.Documents))
	for i, doc := range resp.Documents {
		documents[i] = []byte(doc)
	}

	if err := s.handler(ctx, documents); err != nil {
		log.Printf("changefeed supervisor: handler error for lease %s: %v", s.lease.ID, err)
		return
	}

	s.checkpoint(ctx, &resp)
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
func (s *changeFeedProcessorSupervisor) checkpoint(ctx context.Context, resp *ChangeFeedResponse) {
	resp.PopulateCompositeContinuationToken()
	if resp.ContinuationToken != "" {
		s.lease.ContinuationToken = resp.ContinuationToken
	}
	s.lease.Timestamp = time.Now().Unix()

	if err := s.store.updateLease(ctx, s.lease); err != nil {
		log.Printf("changefeed supervisor: checkpoint failed for lease %s: %v", s.lease.ID, err)
	}
}
