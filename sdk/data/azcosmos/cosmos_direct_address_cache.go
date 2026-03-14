// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//go:build go1.18

package azcosmos

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	azruntime "github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
)

const defaultDirectAddressCacheTTL = 5 * time.Minute

// AddressInformation represents a single replica's physical address.
type AddressInformation struct {
	PhysicalURI string
	IsPrimary   bool
	Protocol    string
}

// directAddressCache resolves and caches physical replica addresses for
// partition key ranges. Uses the gateway's /addresses endpoint.
type directAddressCache struct {
	client      *Client
	cache       map[string][]AddressInformation
	mu          sync.RWMutex
	cacheTTL    time.Duration
	cacheExpiry map[string]time.Time
}

type gatewayAddressResponse struct {
	Addresses []gatewayPartitionAddress `json:"Addresss"`
}

type gatewayPartitionAddress struct {
	PartitionKeyRangeID string           `json:"partitionKeyRangeId"`
	Addresses           []gatewayAddress `json:"addresses"`
}

type gatewayAddress struct {
	PhysicalURI    string `json:"physcialUri"`
	AltPhysicalURI string `json:"physicalUri"`
	IsPrimary      bool   `json:"isPrimary"`
	Protocol       string `json:"protocol"`
}

func newDirectAddressCache(client *Client) *directAddressCache {
	return &directAddressCache{
		client:      client,
		cache:       map[string][]AddressInformation{},
		cacheTTL:    defaultDirectAddressCacheTTL,
		cacheExpiry: map[string]time.Time{},
	}
}

// getAddresses returns the physical addresses for a partition key range.
// Makes a gateway HTTP call if not cached or expired.
func (c *directAddressCache) getAddresses(
	ctx context.Context,
	resourcePath string,
	partitionKeyRangeIDs []string,
	forceRefresh bool,
) (map[string][]AddressInformation, error) {
	addressesByRange := make(map[string][]AddressInformation, len(partitionKeyRangeIDs))
	if len(partitionKeyRangeIDs) == 0 {
		return addressesByRange, nil
	}

	missingRangeIDs := partitionKeyRangeIDs
	if !forceRefresh {
		now := time.Now()
		missingRangeIDs = make([]string, 0, len(partitionKeyRangeIDs))

		c.mu.RLock()
		for _, partitionKeyRangeID := range partitionKeyRangeIDs {
			cachedAddresses, hasCache := c.cache[partitionKeyRangeID]
			expiry, hasExpiry := c.cacheExpiry[partitionKeyRangeID]
			if hasCache && hasExpiry && now.Before(expiry) {
				addressesByRange[partitionKeyRangeID] = cloneAddressInformation(cachedAddresses)
				continue
			}

			missingRangeIDs = append(missingRangeIDs, partitionKeyRangeID)
		}
		c.mu.RUnlock()
	}

	if len(missingRangeIDs) == 0 {
		return addressesByRange, nil
	}

	refreshedAddresses, err := c.fetchAddresses(ctx, resourcePath, missingRangeIDs)
	if err != nil {
		return nil, err
	}

	expiresAt := time.Now().Add(c.cacheTTL)
	c.mu.Lock()
	for partitionKeyRangeID, resolvedAddresses := range refreshedAddresses {
		clonedAddresses := cloneAddressInformation(resolvedAddresses)
		c.cache[partitionKeyRangeID] = clonedAddresses
		c.cacheExpiry[partitionKeyRangeID] = expiresAt
	}
	c.mu.Unlock()

	for _, partitionKeyRangeID := range missingRangeIDs {
		if resolvedAddresses, ok := refreshedAddresses[partitionKeyRangeID]; ok {
			addressesByRange[partitionKeyRangeID] = cloneAddressInformation(resolvedAddresses)
		}
	}

	return addressesByRange, nil
}

func (c *directAddressCache) fetchAddresses(
	ctx context.Context,
	resourcePath string,
	partitionKeyRangeIDs []string,
) (map[string][]AddressInformation, error) {
	requestURL, err := c.buildAddressRequestURL(resourcePath, partitionKeyRangeIDs)
	if err != nil {
		return nil, err
	}

	request, err := azruntime.NewRequest(ctx, http.MethodGet, requestURL)
	if err != nil {
		return nil, err
	}

	addDefaultHeaders(request)
	request.SetOperationValue(pipelineRequestOptions{
		resourceType:    resourceTypeDocument,
		resourceAddress: resourcePath,
	})

	response, err := c.client.internal.Pipeline().Do(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, azruntime.NewResponseErrorWithErrorCode(response, response.Status)
	}

	return parseDirectAddressResponse(response)
}

func (c *directAddressCache) buildAddressRequestURL(resourcePath string, partitionKeyRangeIDs []string) (string, error) {
	var endpointURL url.URL
	if c.client.endpointUrl != nil {
		endpointURL = *c.client.endpointUrl
	} else {
		parsedURL, err := url.Parse(c.client.endpoint)
		if err != nil {
			return "", err
		}
		endpointURL = *parsedURL
	}

	endpointURL.Path = strings.TrimRight(endpointURL.Path, "/") + "/addresses/"
	query := endpointURL.Query()
	query.Set("url", resourcePath)
	query.Set("filter", "protocol eq tcp")
	query.Set("partitionKeyRangeIds", strings.Join(partitionKeyRangeIDs, ","))
	endpointURL.RawQuery = query.Encode()

	return endpointURL.String(), nil
}

func parseDirectAddressResponse(resp *http.Response) (map[string][]AddressInformation, error) {
	defer func() { _ = resp.Body.Close() }()

	body, err := azruntime.Payload(resp)
	if err != nil {
		return nil, err
	}

	parsedResponse := gatewayAddressResponse{}
	if err := json.Unmarshal(body, &parsedResponse); err != nil {
		return nil, err
	}

	addressesByRange := make(map[string][]AddressInformation, len(parsedResponse.Addresses))
	for _, partitionRange := range parsedResponse.Addresses {
		addresses := make([]AddressInformation, len(partitionRange.Addresses))
		for i, address := range partitionRange.Addresses {
			physicalURI := address.PhysicalURI
			if physicalURI == "" {
				physicalURI = address.AltPhysicalURI
			}

			addresses[i] = AddressInformation{
				PhysicalURI: physicalURI,
				IsPrimary:   address.IsPrimary,
				Protocol:    address.Protocol,
			}
		}

		addressesByRange[partitionRange.PartitionKeyRangeID] = addresses
	}

	return addressesByRange, nil
}

func cloneAddressInformation(addresses []AddressInformation) []AddressInformation {
	clonedAddresses := make([]AddressInformation, len(addresses))
	copy(clonedAddresses, addresses)
	return clonedAddresses
}
