// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestThrottleNilIsNoOp(t *testing.T) {
	var throttle *changeFeedProcessorThrottle
	err := throttle.Wait(context.Background(), 100)
	require.NoError(t, err)
}

func TestThrottleZeroRUReturnsNil(t *testing.T) {
	throttle := newChangeFeedProcessorThrottle(0)
	require.Nil(t, throttle)
}

func TestThrottleNegativeRUReturnsNil(t *testing.T) {
	throttle := newChangeFeedProcessorThrottle(-10)
	require.Nil(t, throttle)
}

func TestThrottleImmediateWhenBudgetAvailable(t *testing.T) {
	throttle := newChangeFeedProcessorThrottle(1000)

	start := time.Now()
	err := throttle.Wait(context.Background(), 100)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Less(t, elapsed, 100*time.Millisecond, "should not wait when budget is available")
}

func TestThrottleBlocksWhenExhausted(t *testing.T) {
	throttle := newChangeFeedProcessorThrottle(100) // 100 RU/s

	// Consume the entire budget
	err := throttle.Wait(context.Background(), 100)
	require.NoError(t, err)

	// Next request should block
	start := time.Now()
	err = throttle.Wait(context.Background(), 10)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Greater(t, elapsed, 50*time.Millisecond, "should wait when budget is exhausted")
}

func TestThrottleRespectsContextCancellation(t *testing.T) {
	throttle := newChangeFeedProcessorThrottle(10) // Very low budget

	// Exhaust the budget
	_ = throttle.Wait(context.Background(), 10)

	// Cancel immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := throttle.Wait(ctx, 100)
	require.ErrorIs(t, err, context.Canceled)
}

func TestThrottleRefillsOverTime(t *testing.T) {
	throttle := newChangeFeedProcessorThrottle(1000) // 1000 RU/s

	// Exhaust the budget
	_ = throttle.Wait(context.Background(), 1000)

	// Wait 100ms — should refill ~100 RUs
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	err := throttle.Wait(context.Background(), 50)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Less(t, elapsed, 50*time.Millisecond, "should have enough tokens after refill")
}
