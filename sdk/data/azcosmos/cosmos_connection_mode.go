//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import "time"

// ConnectionMode defines how the client connects to the Cosmos DB service.
type ConnectionMode int

const (
	// ConnectionModeGateway routes all requests through the Cosmos DB gateway
	// using HTTPS. This is the default and works in all network environments.
	ConnectionModeGateway ConnectionMode = iota

	// ConnectionModeDirect connects directly to Cosmos DB backend replicas
	// using the RNTBD binary protocol over TCP/TLS. Lower latency and higher
	// throughput than gateway mode, but requires TCP port access (10000-20000).
	ConnectionModeDirect
)

// DirectModeOptions configures the direct mode TCP transport.
type DirectModeOptions struct {
	// MaxConnectionsPerEndpoint is the max TCP connections per replica.
	// Default: 10.
	MaxConnectionsPerEndpoint int

	// MaxRequestsPerConnection is the max concurrent requests per TCP connection.
	// Default: 30.
	MaxRequestsPerConnection int

	// ConnectTimeout for establishing new TCP connections.
	// Default: 5s.
	ConnectTimeout time.Duration

	// IdleTimeout closes connections idle longer than this duration.
	// Default: 60s.
	IdleTimeout time.Duration
}
