//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package rntbd

import (
	"context"
	"fmt"
	"sync"
)

var errEndpointManagerClosed = fmt.Errorf("rntbd endpoint manager closed")

// EndpointManager maps replica authorities to connection pools.
type EndpointManager struct {
	pools  map[string]*Pool // authority (host:port) -> pool
	mu     sync.RWMutex
	config PoolConfig
	closed bool
}

// EndpointStats contains aggregate endpoint state.
type EndpointStats struct {
	Endpoints   int
	Connections int
	PendingReqs int
}

// NewEndpointManager creates an endpoint manager with shared pool configuration.
func NewEndpointManager(config PoolConfig) *EndpointManager {
	return &EndpointManager{
		pools:  map[string]*Pool{},
		config: normalizePoolConfig(config),
	}
}

// Send routes a request to the correct pool based on the target address.
func (em *EndpointManager) Send(ctx context.Context, addr string, req *Request) (*Response, error) {
	if em == nil {
		return nil, fmt.Errorf("rntbd endpoint manager send: manager is nil")
	}
	if addr == "" {
		return nil, fmt.Errorf("rntbd endpoint manager send: address is empty")
	}

	em.mu.RLock()
	pool := em.pools[addr]
	closed := em.closed
	em.mu.RUnlock()
	if closed {
		return nil, errEndpointManagerClosed
	}
	if pool != nil {
		return pool.Send(ctx, req)
	}

	em.mu.Lock()
	if em.closed {
		em.mu.Unlock()
		return nil, errEndpointManagerClosed
	}
	pool = em.pools[addr]
	if pool == nil {
		pool = NewPool(addr, em.config)
		em.pools[addr] = pool
	}
	em.mu.Unlock()

	return pool.Send(ctx, req)
}

// Close closes all pools and connections.
func (em *EndpointManager) Close() error {
	if em == nil {
		return nil
	}

	em.mu.Lock()
	if em.closed {
		em.mu.Unlock()
		return nil
	}
	em.closed = true
	pools := make([]*Pool, 0, len(em.pools))
	for _, pool := range em.pools {
		pools = append(pools, pool)
	}
	em.pools = map[string]*Pool{}
	em.mu.Unlock()

	var closeErr error
	for _, pool := range pools {
		if err := pool.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

// Stats returns endpoint/connection/request counts for monitoring.
func (em *EndpointManager) Stats() EndpointStats {
	if em == nil {
		return EndpointStats{}
	}

	em.mu.RLock()
	pools := make([]*Pool, 0, len(em.pools))
	for _, pool := range em.pools {
		pools = append(pools, pool)
	}
	em.mu.RUnlock()

	stats := EndpointStats{Endpoints: len(pools)}
	for _, pool := range pools {
		stats.Connections += pool.ActiveConnections()
		stats.PendingReqs += pool.PendingRequests()
	}
	return stats
}
