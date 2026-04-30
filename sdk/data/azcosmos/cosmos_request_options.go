// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

type cosmosRequestOptions interface {
	toHeaders() *map[string]string
}

// excludeRegionsProvider is an optional interface implemented by per-operation
// options structs that support excluding regions from per-request endpoint
// resolution. When the request options passed to [Client.createRequest]
// implement this interface, the returned region list flows into
// [pipelineRequestOptions.excludeRegions] and is consulted by the
// [globalEndpointManager] when picking an endpoint for each attempt.
//
// Treating this as an optional interface keeps the feature additive: options
// types that don't expose [ExcludeRegions] (for example operations that always
// route to the write region) simply don't implement it.
type excludeRegionsProvider interface {
	getExcludeRegions() []string
}
