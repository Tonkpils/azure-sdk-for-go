//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos/internal/rntbd"
	"github.com/Azure/azure-sdk-for-go/sdk/internal/uuid"
)

// directTransport handles data-plane operations via RNTBD TCP connections.
// Control-plane operations (account metadata, database CRUD) still go through
// the gateway HTTP pipeline.
type directTransport struct {
	endpointManager *rntbd.EndpointManager
	addressCache    *directAddressCache
	client          *Client // for auth token generation and gateway fallback
}

func newDirectTransport(client *Client, opts DirectModeOptions) *directTransport {
	poolConfig := rntbd.PoolConfig{
		MaxConnectionsPerEndpoint: opts.MaxConnectionsPerEndpoint,
		MaxRequestsPerConnection:  opts.MaxRequestsPerConnection,
		ConnectTimeout:            opts.ConnectTimeout,
		IdleTimeout:               opts.IdleTimeout,
		TLSConfig:                 &tls.Config{},
	}

	var addressCache *directAddressCache
	if client != nil {
		addressCache = newDirectAddressCache(client)
	}

	return &directTransport{
		endpointManager: rntbd.NewEndpointManager(poolConfig),
		addressCache:    addressCache,
		client:          client,
	}
}

// sendRequest routes a data-plane request through RNTBD.
func (dt *directTransport) sendRequest(
	ctx context.Context,
	operationType rntbd.RntbdOperationType,
	resourceType rntbd.RntbdResourceType,
	resourcePath string,
	partitionKeyRangeID string,
	authToken string,
	payload []byte,
) (*http.Response, error) {
	if dt == nil {
		return nil, fmt.Errorf("direct transport is nil")
	}
	if dt.endpointManager == nil {
		return nil, fmt.Errorf("direct transport endpoint manager is nil")
	}
	if dt.addressCache == nil {
		return nil, fmt.Errorf("direct transport address cache is nil")
	}
	if resourcePath == "" {
		return nil, fmt.Errorf("resource path is empty")
	}
	if partitionKeyRangeID == "" {
		return nil, fmt.Errorf("partition key range ID is empty")
	}

	addressesByRange, err := dt.addressCache.getAddresses(ctx, resourcePath, []string{partitionKeyRangeID}, false)
	if err != nil {
		return nil, err
	}

	addresses, ok := addressesByRange[partitionKeyRangeID]
	if !ok || len(addresses) == 0 {
		return nil, fmt.Errorf("no replica addresses for partition key range ID %q", partitionKeyRangeID)
	}

	primary, err := primaryReplicaAddress(addresses)
	if err != nil {
		return nil, err
	}

	addr, replicaPath, err := parseRntbdURI(primary.PhysicalURI)
	if err != nil {
		return nil, err
	}

	activityID, err := uuid.New()
	if err != nil {
		return nil, fmt.Errorf("creating activity ID: %w", err)
	}

	req := rntbd.NewDocumentRequest(operationType, resourceType, activityID, authToken, replicaPath, nil, 0, payload)
	req.Headers.Set(uint16(rntbd.RequestHeaderPartitionKeyRangeID), rntbd.TokenTypeString, partitionKeyRangeID)

	resp, err := dt.endpointManager.Send(ctx, addr, req)
	if err != nil {
		return nil, err
	}

	return rntbdResponseToHTTP(resp), nil
}

// Close shuts down all RNTBD connections.
func (dt *directTransport) Close() error {
	if dt == nil || dt.endpointManager == nil {
		return nil
	}
	return dt.endpointManager.Close()
}

func primaryReplicaAddress(addresses []AddressInformation) (AddressInformation, error) {
	for _, address := range addresses {
		if address.IsPrimary {
			return address, nil
		}
	}
	return AddressInformation{}, fmt.Errorf("no primary replica address found")
}

func parseRntbdURI(rawURI string) (string, string, error) {
	if rawURI == "" {
		return "", "", fmt.Errorf("rntbd URI is empty")
	}

	parsedURI, err := url.Parse(rawURI)
	if err != nil {
		return "", "", fmt.Errorf("parse rntbd URI: %w", err)
	}
	if !strings.EqualFold(parsedURI.Scheme, "rntbd") {
		return "", "", fmt.Errorf("invalid rntbd URI scheme %q", parsedURI.Scheme)
	}
	if parsedURI.Host == "" {
		return "", "", fmt.Errorf("rntbd URI host is empty")
	}
	if _, _, err := net.SplitHostPort(parsedURI.Host); err != nil {
		return "", "", fmt.Errorf("invalid rntbd URI host %q: %w", parsedURI.Host, err)
	}
	if parsedURI.Path == "" {
		return "", "", fmt.Errorf("rntbd URI replica path is empty")
	}

	return parsedURI.Host, parsedURI.Path, nil
}

func rntbdResponseToHTTP(resp *rntbd.Response) *http.Response {
	httpResp := &http.Response{
		Header:     make(http.Header),
		Body:       http.NoBody,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
	}
	if resp == nil {
		return httpResp
	}

	statusCode := int(resp.StatusCode())
	httpResp.StatusCode = statusCode
	statusText := http.StatusText(statusCode)
	if statusText == "" {
		httpResp.Status = strconv.Itoa(statusCode)
	} else {
		httpResp.Status = fmt.Sprintf("%d %s", statusCode, statusText)
	}

	if ct := resp.ContinuationToken(); ct != "" {
		httpResp.Header.Set(cosmosHeaderContinuationToken, ct)
	}
	if etag := resp.ETag(); etag != "" {
		httpResp.Header.Set(cosmosHeaderEtag, etag)
	}
	if rc := resp.RequestCharge(); rc > 0 {
		httpResp.Header.Set(cosmosHeaderRequestCharge, fmt.Sprintf("%g", rc))
	}
	if st := resp.SessionToken(); st != "" {
		httpResp.Header.Set(cosmosHeaderSessionToken, st)
	}
	if subStatus := resp.SubStatus(); subStatus > 0 {
		httpResp.Header.Set(cosmosHeaderSubstatus, strconv.FormatUint(uint64(subStatus), 10))
	}
	if transportRequestID, ok := resp.TransportRequestID(); ok {
		httpResp.Header.Set(headerXmsTransportRequestId, strconv.FormatUint(uint64(transportRequestID), 10))
	}
	if lsn, ok := resp.Headers.Get(uint16(rntbd.ResponseHeaderLSN)); ok {
		switch value := lsn.(type) {
		case int64:
			httpResp.Header.Set(headerLsn, strconv.FormatInt(value, 10))
		case uint64:
			httpResp.Header.Set(headerLsn, strconv.FormatUint(value, 10))
		}
	}
	if partitionKeyRangeID, ok := resp.Headers.Get(uint16(rntbd.ResponseHeaderPartitionKeyRangeID)); ok {
		if value, ok := partitionKeyRangeID.(string); ok && value != "" {
			httpResp.Header.Set(headerXmsDocumentDbPartitionKeyRangeId, value)
		}
	}
	httpResp.Header.Set(cosmosHeaderActivityId, resp.ActivityID().String())

	if resp.IsPayloadPresent() && len(resp.Payload) > 0 {
		httpResp.Body = io.NopCloser(bytes.NewReader(resp.Payload))
		httpResp.ContentLength = int64(len(resp.Payload))
	}

	return httpResp
}
