//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package rntbd

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"
)

const (
	defaultMaxConnectionsPerEndpoint = 10
	defaultMaxRequestsPerConnection  = 30
	defaultConnectTimeout            = 5 * time.Second
	defaultIdleTimeout               = 60 * time.Second
)

var errPoolClosed = fmt.Errorf("rntbd pool closed")

// PoolConfig controls connection pool behavior.
type PoolConfig struct {
	MaxConnectionsPerEndpoint int           // max TCP connections per replica (default 10)
	MaxRequestsPerConnection  int           // max concurrent requests per connection (default 30)
	ConnectTimeout            time.Duration // dial timeout (default 5s)
	IdleTimeout               time.Duration // close idle connections after (default 60s)
	TLSConfig                 *tls.Config   // TLS configuration
}

// Pool manages a set of RNTBD connections to a single endpoint (host:port).
type Pool struct {
	addr         string
	config       PoolConfig
	connections  []*Connection
	reservations map[*Connection]int
	pendingDials int
	mu           sync.Mutex
	closed       bool
}

// NewPool creates a connection pool for a single RNTBD endpoint.
func NewPool(addr string, config PoolConfig) *Pool {
	return &Pool{
		addr:         addr,
		config:       normalizePoolConfig(config),
		reservations: map[*Connection]int{},
	}
}

// Send picks an available connection (or creates one) and sends the request.
func (p *Pool) Send(ctx context.Context, req *Request) (*Response, error) {
	if p == nil {
		return nil, fmt.Errorf("rntbd pool send: pool is nil")
	}

	conn, err := p.acquireConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer p.releaseReservation(conn)

	return conn.Send(ctx, req)
}

// Close closes all connections in the pool.
func (p *Pool) Close() error {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	connections := append([]*Connection(nil), p.connections...)
	p.connections = nil
	p.reservations = map[*Connection]int{}
	p.mu.Unlock()

	var closeErr error
	for _, conn := range connections {
		if err := conn.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

// ActiveConnections returns the number of open connections.
func (p *Pool) ActiveConnections() int {
	if p == nil {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.reapClosedLocked()
	return len(p.connections)
}

// PendingRequests returns total pending requests across all connections.
func (p *Pool) PendingRequests() int {
	if p == nil {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.reapClosedLocked()

	total := 0
	for _, conn := range p.connections {
		total += conn.PendingRequests()
	}
	return total
}

func (p *Pool) acquireConnection(ctx context.Context) (*Connection, error) {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, errPoolClosed
		}

		p.reapClosedLocked()
		conn, pending := p.leastLoadedLocked()
		canCreate := len(p.connections)+p.pendingDials < p.config.MaxConnectionsPerEndpoint

		switch {
		case conn == nil && canCreate:
			p.pendingDials++
			p.mu.Unlock()
			return p.dialAndTrack(ctx)
		case conn == nil:
			p.mu.Unlock()
			time.Sleep(time.Millisecond)
			continue
		case pending < p.config.MaxRequestsPerConnection:
			p.reservations[conn]++
			p.mu.Unlock()
			return conn, nil
		case canCreate:
			p.pendingDials++
			p.mu.Unlock()
			return p.dialAndTrack(ctx)
		default:
			p.reservations[conn]++
			p.mu.Unlock()
			return conn, nil
		}
	}
}

func (p *Pool) dialAndTrack(ctx context.Context) (*Connection, error) {
	conn, err := p.dialConnection(ctx)

	p.mu.Lock()
	defer p.mu.Unlock()
	p.pendingDials--
	if err != nil {
		return nil, err
	}
	if p.closed {
		_ = conn.Close()
		return nil, errPoolClosed
	}

	p.reapClosedLocked()
	if len(p.connections) >= p.config.MaxConnectionsPerEndpoint {
		existing, _ := p.leastLoadedLocked()
		if existing != nil {
			p.reservations[existing]++
			_ = conn.Close()
			return existing, nil
		}
	}

	p.connections = append(p.connections, conn)
	p.reservations[conn]++
	return conn, nil
}

func (p *Pool) dialConnection(ctx context.Context) (*Connection, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	dialCtx := ctx
	var cancel context.CancelFunc
	if p.config.ConnectTimeout > 0 {
		dialCtx, cancel = context.WithTimeout(ctx, p.config.ConnectTimeout)
		defer cancel()
	}

	conn, err := Dial(dialCtx, p.addr, p.config.TLSConfig)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (p *Pool) releaseReservation(conn *Connection) {
	if p == nil || conn == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if pending := p.reservations[conn]; pending <= 1 {
		delete(p.reservations, conn)
	} else {
		p.reservations[conn] = pending - 1
	}
}

func (p *Pool) leastLoadedLocked() (*Connection, int) {
	var selected *Connection
	selectedPending := 0
	for _, conn := range p.connections {
		pending := conn.PendingRequests() + p.reservations[conn]
		if selected == nil || pending < selectedPending {
			selected = conn
			selectedPending = pending
		}
	}
	return selected, selectedPending
}

func (p *Pool) reapClosedLocked() {
	connections := p.connections[:0]
	for _, conn := range p.connections {
		if conn != nil && !conn.IsClosed() {
			connections = append(connections, conn)
			continue
		}
		delete(p.reservations, conn)
	}
	p.connections = connections
}

func normalizePoolConfig(config PoolConfig) PoolConfig {
	if config.MaxConnectionsPerEndpoint <= 0 {
		config.MaxConnectionsPerEndpoint = defaultMaxConnectionsPerEndpoint
	}
	if config.MaxRequestsPerConnection <= 0 {
		config.MaxRequestsPerConnection = defaultMaxRequestsPerConnection
	}
	if config.ConnectTimeout <= 0 {
		config.ConnectTimeout = defaultConnectTimeout
	}
	if config.IdleTimeout <= 0 {
		config.IdleTimeout = defaultIdleTimeout
	}
	if config.TLSConfig != nil {
		config.TLSConfig = config.TLSConfig.Clone()
	}
	return config
}
