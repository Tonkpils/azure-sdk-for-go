// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//go:build go1.18

package azcosmos

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	azruntime "github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/internal/mock"
	"github.com/stretchr/testify/require"
)

func TestDirectAddressParsesResponse(t *testing.T) {
	jsonResponse := []byte(`{
		"Addresss": [
			{
				"partitionKeyRangeId": "0",
				"addresses": [
					{"physcialUri": "rntbd://cdb-1.documents.azure.com:14001/apps/primary", "isPrimary": true, "protocol": "tcp"},
					{"physcialUri": "rntbd://cdb-1.documents.azure.com:14002/apps/secondary", "isPrimary": false, "protocol": "tcp"}
				]
			}
		]
	}`)

	srv, close := mock.NewTLSServer()
	defer close()
	srv.SetResponse(mock.WithBody(jsonResponse), mock.WithStatusCode(http.StatusOK))

	capture := &directAddressCapturePolicy{}
	cache := newDirectAddressCache(newDirectAddressTestClient(t, srv, capture))

	addressesByRange, err := cache.getAddresses(context.Background(), "dbs/testdb/colls/testcoll", []string{"0"}, false)
	require.NoError(t, err)
	require.Equal(t, 1, srv.Requests())
	require.Len(t, addressesByRange, 1)
	require.Contains(t, addressesByRange, "0")
	require.Len(t, addressesByRange["0"], 2)
	require.Equal(t, "rntbd://cdb-1.documents.azure.com:14001/apps/primary", addressesByRange["0"][0].PhysicalURI)
	require.True(t, addressesByRange["0"][0].IsPrimary)
	require.Equal(t, "tcp", addressesByRange["0"][0].Protocol)
	require.Equal(t, "rntbd://cdb-1.documents.azure.com:14002/apps/secondary", addressesByRange["0"][1].PhysicalURI)
	require.False(t, addressesByRange["0"][1].IsPrimary)

	require.Equal(t, http.MethodGet, capture.lastMethod())
	require.Equal(t, "/addresses/", capture.lastURL().Path)
	require.Equal(t, "dbs/testdb/colls/testcoll", capture.lastURL().Query().Get("url"))
	require.Equal(t, "protocol eq tcp", capture.lastURL().Query().Get("filter"))
	require.Equal(t, "0", capture.lastURL().Query().Get("partitionKeyRangeIds"))
	require.NotEmpty(t, capture.lastHeader().Get(headerAuthorization))
	require.NotEmpty(t, capture.lastHeader().Get(headerXmsDate))
	require.Equal(t, apiVersion, capture.lastHeader().Get(headerXmsVersion))
}

func TestDirectAddressCacheHitMissBehavior(t *testing.T) {
	jsonResponse := []byte(`{
		"Addresss": [
			{
				"partitionKeyRangeId": "0",
				"addresses": [
					{"physcialUri": "rntbd://cdb-1.documents.azure.com:14001/apps/primary", "isPrimary": true, "protocol": "tcp"}
				]
			}
		]
	}`)

	srv, close := mock.NewTLSServer()
	defer close()
	srv.SetResponse(mock.WithBody(jsonResponse), mock.WithStatusCode(http.StatusOK))

	cache := newDirectAddressCache(newDirectAddressTestClient(t, srv, nil))

	firstAddresses, err := cache.getAddresses(context.Background(), "dbs/testdb/colls/testcoll", []string{"0"}, false)
	require.NoError(t, err)
	require.Equal(t, 1, srv.Requests())

	secondAddresses, err := cache.getAddresses(context.Background(), "dbs/testdb/colls/testcoll", []string{"0"}, false)
	require.NoError(t, err)
	require.Equal(t, 1, srv.Requests())
	require.Equal(t, firstAddresses, secondAddresses)

	cache.mu.Lock()
	cache.cacheExpiry["0"] = time.Now().Add(-time.Minute)
	cache.mu.Unlock()

	thirdAddresses, err := cache.getAddresses(context.Background(), "dbs/testdb/colls/testcoll", []string{"0"}, false)
	require.NoError(t, err)
	require.Equal(t, 2, srv.Requests())
	require.Equal(t, firstAddresses, thirdAddresses)
}

func TestDirectAddressForceRefreshBypassesCache(t *testing.T) {
	jsonResponse := []byte(`{
		"Addresss": [
			{
				"partitionKeyRangeId": "0",
				"addresses": [
					{"physcialUri": "rntbd://cdb-1.documents.azure.com:14001/apps/primary", "isPrimary": true, "protocol": "tcp"}
				]
			}
		]
	}`)

	srv, close := mock.NewTLSServer()
	defer close()
	srv.SetResponse(mock.WithBody(jsonResponse), mock.WithStatusCode(http.StatusOK))

	cache := newDirectAddressCache(newDirectAddressTestClient(t, srv, nil))

	_, err := cache.getAddresses(context.Background(), "dbs/testdb/colls/testcoll", []string{"0"}, false)
	require.NoError(t, err)
	require.Equal(t, 1, srv.Requests())

	_, err = cache.getAddresses(context.Background(), "dbs/testdb/colls/testcoll", []string{"0"}, true)
	require.NoError(t, err)
	require.Equal(t, 2, srv.Requests())
}

func TestDirectAddressMultiplePartitionKeyRanges(t *testing.T) {
	jsonResponse := []byte(`{
		"Addresss": [
			{
				"partitionKeyRangeId": "0",
				"addresses": [
					{"physcialUri": "rntbd://cdb-1.documents.azure.com:14001/apps/primary", "isPrimary": true, "protocol": "tcp"}
				]
			},
			{
				"partitionKeyRangeId": "1",
				"addresses": [
					{"physcialUri": "rntbd://cdb-1.documents.azure.com:14003/apps/secondary", "isPrimary": false, "protocol": "tcp"}
				]
			}
		]
	}`)

	srv, close := mock.NewTLSServer()
	defer close()
	srv.SetResponse(mock.WithBody(jsonResponse), mock.WithStatusCode(http.StatusOK))

	capture := &directAddressCapturePolicy{}
	cache := newDirectAddressCache(newDirectAddressTestClient(t, srv, capture))

	addressesByRange, err := cache.getAddresses(context.Background(), "dbs/testdb/colls/testcoll", []string{"0", "1"}, false)
	require.NoError(t, err)
	require.Equal(t, 1, srv.Requests())
	require.Len(t, addressesByRange, 2)
	require.Equal(t, "rntbd://cdb-1.documents.azure.com:14001/apps/primary", addressesByRange["0"][0].PhysicalURI)
	require.Equal(t, "rntbd://cdb-1.documents.azure.com:14003/apps/secondary", addressesByRange["1"][0].PhysicalURI)
	require.Equal(t, "0,1", capture.lastURL().Query().Get("partitionKeyRangeIds"))
}

type directAddressCapturePolicy struct {
	mu      sync.Mutex
	method  string
	url     *url.URL
	headers http.Header
}

func (p *directAddressCapturePolicy) Do(req *policy.Request) (*http.Response, error) {
	p.mu.Lock()
	p.method = req.Raw().Method
	copiedURL := *req.Raw().URL
	p.url = &copiedURL
	p.headers = req.Raw().Header.Clone()
	p.mu.Unlock()

	return req.Next()
}

func (p *directAddressCapturePolicy) lastMethod() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.method
}

func (p *directAddressCapturePolicy) lastURL() *url.URL {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.url == nil {
		return nil
	}
	copiedURL := *p.url
	return &copiedURL
}

func (p *directAddressCapturePolicy) lastHeader() http.Header {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.headers.Clone()
}

func newDirectAddressTestClient(t *testing.T, srv *mock.Server, capture policy.Policy) *Client {
	t.Helper()

	cred, err := NewKeyCredential("dG9fYmFzZV82NA==")
	require.NoError(t, err)

	endpointURL, err := url.Parse(srv.URL())
	require.NoError(t, err)

	perRetryPolicies := []policy.Policy{newSharedKeyCredPolicy(cred)}
	if capture != nil {
		perRetryPolicies = append(perRetryPolicies, capture)
	}

	internalClient, err := azcore.NewClient("azcosmostest", "v1.0.0", azruntime.PipelineOptions{PerRetry: perRetryPolicies}, &policy.ClientOptions{Transport: srv})
	require.NoError(t, err)

	return &Client{
		endpoint:    srv.URL(),
		endpointUrl: endpointURL,
		internal:    internalClient,
		gem:         &globalEndpointManager{preferredLocations: []string{}},
	}
}
