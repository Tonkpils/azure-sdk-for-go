// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"golang.org/x/net/http2"
)

// ClientOptions defines the options for the Cosmos client.
type ClientOptions struct {
	azcore.ClientOptions
	// When EnableContentResponseOnWrite is false will cause the response to have a null resource. This reduces networking and CPU load by not sending the resource back over the network and serializing it on the client.
	// The default is false.
	EnableContentResponseOnWrite bool
	// PreferredRegions is a list of regions to be used when initializing the client in case the default region fails.
	PreferredRegions []string
	// PriorityLevel defines the default priority level for all requests made by this client.
	// This feature is currently in preview. For more information, see https://aka.ms/CosmosDB/PriorityBasedExecution
	// Valid values are PriorityLevelHigh and PriorityLevelLow.
	// Can be overridden per-request via the operation options.
	PriorityLevel *PriorityLevel
	// ThroughputBucket defines the default throughput bucket for all requests made by this client.
	// This feature is currently in preview. For more information, see https://aka.ms/CosmosDB/ThroughputBuckets
	// The valid range is 1 to 5 (inclusive).
	// Can be overridden per-request via the operation options.
	ThroughputBucket *int32
	// GatewayMaxConnections controls the maximum number of TCP connections
	// the HTTP transport maintains to the Cosmos gateway. Setting this
	// creates a custom transport with MaxConnsPerHost, which stabilizes
	// connection management under high concurrency. This was found to
	// resolve context deadline exceeded errors when running thousands of
	// concurrent operations (e.g., ChangeFeed Processor with 9,200 partitions).
	// The .NET SDK defaults GatewayModeMaxConnectionLimit to 50.
	// Set to 0 for the Go default behavior.
	GatewayMaxConnections int
}

// newHighConcurrencyTransport creates an *http.Client with MaxConnsPerHost
// set to stabilize connection management under high concurrency workloads.
func newHighConcurrencyTransport(maxConnsPerHost int) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          maxConnsPerHost * 10,
		MaxIdleConnsPerHost:   maxConnsPerHost,
		MaxConnsPerHost:       maxConnsPerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			MinVersion:    tls.VersionTLS12,
			Renegotiation: tls.RenegotiateFreelyAsClient,
		},
	}
	if h2Transport, err := http2.ConfigureTransports(transport); err == nil {
		h2Transport.ReadIdleTimeout = 10 * time.Second
		h2Transport.PingTimeout = 5 * time.Second
	}
	return &http.Client{Transport: transport}
}

// applyGatewayConnectionLimit sets a high-concurrency transport on the
// ClientOptions if GatewayMaxConnections is configured and no custom
// transport was already provided.
func applyGatewayConnectionLimit(o *ClientOptions) {
	if o.GatewayMaxConnections > 0 && o.Transport == nil {
		o.Transport = newHighConcurrencyTransport(o.GatewayMaxConnections)
	}
}
