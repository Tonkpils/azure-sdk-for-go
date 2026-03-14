//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"io"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos/internal/rntbd"
	"github.com/Azure/azure-sdk-for-go/sdk/internal/uuid"
	"github.com/stretchr/testify/require"
)

func TestDirectTransportRntbdResponseToHTTP(t *testing.T) {
	activityID, err := uuid.Parse("11111111-2222-3333-4444-555555555555")
	require.NoError(t, err)

	headers := &rntbd.TokenSet{}
	headers.Set(uint16(rntbd.ResponseHeaderPayloadPresent), rntbd.TokenTypeByte, byte(1))
	headers.Set(uint16(rntbd.ResponseHeaderContinuationToken), rntbd.TokenTypeString, "continuation-token")
	headers.Set(uint16(rntbd.ResponseHeaderETag), rntbd.TokenTypeString, "etag-value")
	headers.Set(uint16(rntbd.ResponseHeaderRequestCharge), rntbd.TokenTypeDouble, 3.5)
	headers.Set(uint16(rntbd.ResponseHeaderSessionToken), rntbd.TokenTypeString, "session-token")
	headers.Set(uint16(rntbd.ResponseHeaderSubStatus), rntbd.TokenTypeULong, uint32(1002))
	headers.Set(uint16(rntbd.ResponseHeaderTransportRequestID), rntbd.TokenTypeULong, uint32(42))
	headers.Set(uint16(rntbd.ResponseHeaderLSN), rntbd.TokenTypeLongLong, int64(99))
	headers.Set(uint16(rntbd.ResponseHeaderPartitionKeyRangeID), rntbd.TokenTypeString, "3")

	resp := &rntbd.Response{
		Frame: rntbd.ResponseFrame{
			StatusCode: http.StatusAccepted,
			ActivityID: activityID,
		},
		Headers: headers,
		Payload: []byte(`{"ok":true}`),
	}

	httpResp := rntbdResponseToHTTP(resp)
	require.Equal(t, http.StatusAccepted, httpResp.StatusCode)
	require.Equal(t, "202 Accepted", httpResp.Status)
	require.Equal(t, "continuation-token", httpResp.Header.Get(cosmosHeaderContinuationToken))
	require.Equal(t, "etag-value", httpResp.Header.Get(cosmosHeaderEtag))
	require.Equal(t, "3.5", httpResp.Header.Get(cosmosHeaderRequestCharge))
	require.Equal(t, "session-token", httpResp.Header.Get(cosmosHeaderSessionToken))
	require.Equal(t, "1002", httpResp.Header.Get(cosmosHeaderSubstatus))
	require.Equal(t, "42", httpResp.Header.Get(headerXmsTransportRequestId))
	require.Equal(t, "99", httpResp.Header.Get(headerLsn))
	require.Equal(t, "3", httpResp.Header.Get(headerXmsDocumentDbPartitionKeyRangeId))
	require.Equal(t, activityID.String(), httpResp.Header.Get(cosmosHeaderActivityId))
	require.Equal(t, int64(len(resp.Payload)), httpResp.ContentLength)

	body, err := io.ReadAll(httpResp.Body)
	require.NoError(t, err)
	require.Equal(t, resp.Payload, body)
}

func TestParseRntbdURI(t *testing.T) {
	addr, replicaPath, err := parseRntbdURI("rntbd://cdb-1.documents.azure.com:14001/apps/primary")
	require.NoError(t, err)
	require.Equal(t, "cdb-1.documents.azure.com:14001", addr)
	require.Equal(t, "/apps/primary", replicaPath)

	_, _, err = parseRntbdURI("https://cdb-1.documents.azure.com:14001/apps/primary")
	require.Error(t, err)

	_, _, err = parseRntbdURI("rntbd://cdb-1.documents.azure.com/apps/primary")
	require.Error(t, err)

	_, _, err = parseRntbdURI("rntbd://cdb-1.documents.azure.com:14001")
	require.Error(t, err)
}
