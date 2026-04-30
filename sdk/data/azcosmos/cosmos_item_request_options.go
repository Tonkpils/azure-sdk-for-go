// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

// ItemOptions includes options for operations on items.
type ItemOptions struct {
	// Triggers to be invoked before the operation.
	PreTriggers []string
	// Triggers to be invoked after the operation.
	PostTriggers []string
	// SessionToken to be used when using Session consistency on the account.
	// When working with Session consistency, each new write request to Azure Cosmos DB is assigned a new SessionToken.
	// The client instance will use this token internally with each read/query request to ensure that the set consistency level is maintained.
	// In some scenarios you need to manage this Session yourself: Consider a web application with multiple nodes, each node will have its own client instance.
	// If you wanted these nodes to participate in the same session (to be able read your own writes consistently across web tiers),
	// you would have to send the SessionToken from the response of the write action on one node to the client tier, using a cookie or some other mechanism, and have that token flow back to the web tier for subsequent reads.
	// If you are using a round-robin load balancer which does not maintain session affinity between requests, such as the Azure Load Balancer,the read could potentially land on a different node to the write request, where the session was created.
	SessionToken *string
	// ConsistencyLevel overrides the account defined consistency level for this operation.
	// Consistency can only be relaxed.
	ConsistencyLevel *ConsistencyLevel
	// Indexing directive to be applied to the operation.
	IndexingDirective *IndexingDirective
	// When EnableContentResponseOnWrite is false will cause the response on write operations to have a null resource. This reduces networking and CPU load by not sending the resource back over the network and serializing it on the client.
	// The default is false.
	EnableContentResponseOnWrite bool
	// IfMatchEtag is used to ensure optimistic concurrency control.
	// https://docs.microsoft.com/azure/cosmos-db/sql/database-transactions-optimistic-concurrency#optimistic-concurrency-control
	IfMatchEtag *azcore.ETag
	// Options for operations in the dedicated gateway.
	DedicatedGatewayRequestOptions *DedicatedGatewayRequestOptions
	// ExcludeRegions allows the caller to skip listed regions for this single
	// operation. Region names are case-sensitive and must match values in the
	// account's preferred-region list (e.g. "East US 2").
	//
	// Behavior mirrors the .NET SDK's RequestOptions.ExcludeRegions:
	//   - Reads: excluded regions are skipped, and the SDK routes to the next
	//     non-excluded preferred region.
	//   - Document writes on multi-write accounts: excluded regions are
	//     skipped.
	//   - Document writes on single-master accounts and metadata writes: the
	//     filter is silently ignored because routing is fixed to the write
	//     region. This is parity with the .NET SDK.
	//   - If every preferred region is excluded, the SDK falls back to the
	//     account's default endpoint.
	//
	// ExcludeRegions does not bypass the SDK's cross-region failover retries;
	// retries continue to respect the filter.
	ExcludeRegions []string
}

func (options *ItemOptions) getExcludeRegions() []string {
	if options == nil {
		return nil
	}
	return options.ExcludeRegions
}

func (options *ItemOptions) toHeaders() *map[string]string {
	headers := make(map[string]string)

	if len(options.PreTriggers) > 0 {
		headers[cosmosHeaderPreTriggerInclude] = strings.Join(options.PreTriggers, ",")
	}

	if len(options.PostTriggers) > 0 {
		headers[cosmosHeaderPostTriggerInclude] = strings.Join(options.PostTriggers, ",")
	}

	if options.ConsistencyLevel != nil {
		headers[cosmosHeaderConsistencyLevel] = string(*options.ConsistencyLevel)
	}

	if options.IndexingDirective != nil {
		headers[cosmosHeaderIndexingDirective] = string(*options.IndexingDirective)
	}

	if options.SessionToken != nil {
		headers[cosmosHeaderSessionToken] = *options.SessionToken
	}

	if options.IfMatchEtag != nil {
		headers[headerIfMatch] = string(*options.IfMatchEtag)
	}

	if options.DedicatedGatewayRequestOptions != nil {
		dedicatedGatewayRequestOptions := options.DedicatedGatewayRequestOptions

		if dedicatedGatewayRequestOptions.MaxIntegratedCacheStaleness != nil {
			milliseconds := dedicatedGatewayRequestOptions.MaxIntegratedCacheStaleness.Milliseconds()
			headers[headerDedicatedGatewayMaxAge] = strconv.FormatInt(milliseconds, 10)
		}

		if dedicatedGatewayRequestOptions.BypassIntegratedCache {
			headers[headerDedicatedGatewayBypassCache] = "true"
		}
	}

	return &headers
}
