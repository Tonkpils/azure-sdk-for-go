// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"sync"
	"time"
)

// changeFeedProcessorThrottle is a simple token bucket rate limiter for RU consumption.
// It refills at MaxRUPerSecond and blocks when the bucket is empty.
type changeFeedProcessorThrottle struct {
	maxRUPerSecond float64
	available      float64
	lastRefill     time.Time
	mu             sync.Mutex
}

// newChangeFeedProcessorThrottle creates a throttle with the given RU/s limit.
// If maxRUPerSecond is 0 or negative, returns nil (no throttling).
func newChangeFeedProcessorThrottle(maxRUPerSecond float64) *changeFeedProcessorThrottle {
	if maxRUPerSecond <= 0 {
		return nil
	}
	return &changeFeedProcessorThrottle{
		maxRUPerSecond: maxRUPerSecond,
		available:      maxRUPerSecond, // Start with a full bucket
		lastRefill:     time.Now(),
	}
}

// Wait blocks until enough RU tokens are available to proceed after consuming
// the given amount of RUs. Returns immediately if the throttle is nil or
// the context is cancelled.
func (t *changeFeedProcessorThrottle) Wait(ctx context.Context, ruConsumed float64) error {
	if t == nil || ruConsumed <= 0 {
		return nil
	}

	for {
		t.mu.Lock()
		t.refill()
		if t.available >= ruConsumed {
			t.available -= ruConsumed
			t.mu.Unlock()
			return nil
		}
		// Calculate how long until enough tokens are available.
		deficit := ruConsumed - t.available
		waitDuration := time.Duration(float64(time.Second) * deficit / t.maxRUPerSecond)
		t.mu.Unlock()

		// Wait, but respect context cancellation.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitDuration):
		}
	}
}

// refill adds tokens based on elapsed time since last refill.
// Must be called with mu held.
func (t *changeFeedProcessorThrottle) refill() {
	now := time.Now()
	elapsed := now.Sub(t.lastRefill).Seconds()
	t.available += elapsed * t.maxRUPerSecond
	if t.available > t.maxRUPerSecond {
		t.available = t.maxRUPerSecond // Cap at max burst
	}
	t.lastRefill = now
}
