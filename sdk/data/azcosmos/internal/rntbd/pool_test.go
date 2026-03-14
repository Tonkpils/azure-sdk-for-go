//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package rntbd

import (
	"context"
	"crypto/tls"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRntbdPoolSingleConnection(t *testing.T) {
	addr, cleanup := startMockRntbdServer(t)
	defer cleanup()

	pool := NewPool(addr, PoolConfig{TLSConfig: &tls.Config{InsecureSkipVerify: true}})
	defer func() { require.NoError(t, pool.Close()) }()

	payload := []byte(`{"id":"doc-1"}`)
	response, err := pool.Send(context.TODO(), newTestDocumentRequest(t, payload))
	require.NoError(t, err)
	require.Equal(t, int32(200), response.StatusCode())
	require.Equal(t, payload, response.Payload)
	require.Equal(t, 1, pool.ActiveConnections())
	require.Zero(t, pool.PendingRequests())
}

func TestRntbdPoolCreatesNewConnection(t *testing.T) {
	addr, cleanup := startMockRntbdServer(t)
	defer cleanup()

	pool := NewPool(addr, PoolConfig{
		MaxConnectionsPerEndpoint: 4,
		MaxRequestsPerConnection:  1,
		TLSConfig:                 &tls.Config{InsecureSkipVerify: true},
	})
	defer func() { require.NoError(t, pool.Close()) }()

	requests := []*Request{
		newTestDocumentRequest(t, []byte("delay=250|request-0")),
		newTestDocumentRequest(t, []byte("delay=250|request-1")),
	}

	responses := make([]*Response, len(requests))
	errors := make([]error, len(requests))
	start := make(chan struct{})

	var wg sync.WaitGroup
	for i, req := range requests {
		wg.Add(1)
		go func(i int, req *Request) {
			defer wg.Done()
			<-start
			responses[i], errors[i] = pool.Send(context.TODO(), req)
		}(i, req)
	}
	close(start)

	require.Eventually(t, func() bool {
		return pool.ActiveConnections() == 2
	}, time.Second, 10*time.Millisecond)

	wg.Wait()
	for i, response := range responses {
		require.NoError(t, errors[i])
		require.Equal(t, []byte("delay=250|request-"+string(rune('0'+i))), response.Payload)
	}
	require.Equal(t, 2, pool.ActiveConnections())
}

func TestRntbdPoolMaxConnections(t *testing.T) {
	addr, cleanup := startMockRntbdServer(t)
	defer cleanup()

	pool := NewPool(addr, PoolConfig{
		MaxConnectionsPerEndpoint: 2,
		MaxRequestsPerConnection:  1,
		TLSConfig:                 &tls.Config{InsecureSkipVerify: true},
	})
	defer func() { require.NoError(t, pool.Close()) }()

	requests := []*Request{
		newTestDocumentRequest(t, []byte("delay=250|max-0")),
		newTestDocumentRequest(t, []byte("delay=250|max-1")),
		newTestDocumentRequest(t, []byte("delay=250|max-2")),
		newTestDocumentRequest(t, []byte("delay=250|max-3")),
	}

	errors := make([]error, len(requests))
	start := make(chan struct{})

	var wg sync.WaitGroup
	for i, req := range requests {
		wg.Add(1)
		go func(i int, req *Request) {
			defer wg.Done()
			<-start
			_, errors[i] = pool.Send(context.TODO(), req)
		}(i, req)
	}
	close(start)

	require.Eventually(t, func() bool {
		return pool.ActiveConnections() == 2
	}, time.Second, 10*time.Millisecond)

	wg.Wait()
	for _, err := range errors {
		require.NoError(t, err)
	}
	require.Equal(t, 2, pool.ActiveConnections())
}

func TestRntbdEndpointManagerRouting(t *testing.T) {
	addr1, cleanup1 := startMockRntbdServer(t)
	defer cleanup1()
	addr2, cleanup2 := startMockRntbdServer(t)
	defer cleanup2()

	manager := NewEndpointManager(PoolConfig{TLSConfig: &tls.Config{InsecureSkipVerify: true}})
	defer func() { require.NoError(t, manager.Close()) }()

	response1, err := manager.Send(context.TODO(), addr1, newTestDocumentRequest(t, []byte("route-1")))
	require.NoError(t, err)
	require.Equal(t, []byte("route-1"), response1.Payload)

	response2, err := manager.Send(context.TODO(), addr2, newTestDocumentRequest(t, []byte("route-2")))
	require.NoError(t, err)
	require.Equal(t, []byte("route-2"), response2.Payload)

	stats := manager.Stats()
	require.Equal(t, 2, stats.Endpoints)
	require.Equal(t, 2, stats.Connections)
	require.Zero(t, stats.PendingReqs)
	require.Len(t, manager.pools, 2)
	require.Equal(t, 1, manager.pools[addr1].ActiveConnections())
	require.Equal(t, 1, manager.pools[addr2].ActiveConnections())
}
