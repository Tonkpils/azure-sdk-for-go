//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package rntbd

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	azuuid "github.com/Azure/azure-sdk-for-go/sdk/internal/uuid"
)

func TestRntbdTokenSetEncodeDecodeRoundTrip(t *testing.T) {
	activityID := mustParseUUID(t, "00112233-4455-6677-8899-aabbccddeeff")

	tokens := &TokenSet{}
	tokens.Set(uint16(ResponseHeaderPayloadPresent), TokenTypeByte, byte(1))
	tokens.Set(uint16(ResponseHeaderTransportRequestID), TokenTypeULong, uint32(77))
	tokens.Set(uint16(ResponseHeaderETag), TokenTypeString, "etag-value")
	tokens.Set(uint16(ResponseHeaderRequestCharge), TokenTypeDouble, 3.14)
	tokens.Set(uint16(ResponseHeaderSessionToken), TokenTypeString, "session-token")
	tokens.Set(0x0FFF, TokenTypeGuid, activityID)

	buf := make([]byte, tokens.EncodedLength())
	written := tokens.Encode(buf)
	require.Equal(t, len(buf), written)

	decoded := &TokenSet{}
	require.NoError(t, decoded.Decode(buf))
	require.Len(t, decoded.tokens, len(tokens.tokens))

	value, ok := decoded.Get(uint16(ResponseHeaderTransportRequestID))
	require.True(t, ok)
	require.Equal(t, uint32(77), value)

	etag, ok := decoded.Get(uint16(ResponseHeaderETag))
	require.True(t, ok)
	require.Equal(t, "etag-value", etag)

	charge, ok := decoded.Get(uint16(ResponseHeaderRequestCharge))
	require.True(t, ok)
	require.InDelta(t, 3.14, charge.(float64), 0.0001)

	ownerID, ok := decoded.Get(0x0FFF)
	require.True(t, ok)
	require.Equal(t, activityID, ownerID)
}

func TestRntbdContextRequestRoundTrip(t *testing.T) {
	request := NewContextRequest("azcosmos/1.0.0", "azure-sdk-for-go")

	frame, headers, payload := decodeRequestWire(t, request.Encode())
	require.Equal(t, ResourceTypeConnection, frame.ResourceType)
	require.Equal(t, OperationTypeConnection, frame.OperationType)
	require.Nil(t, payload)
	require.Len(t, headers.tokens, 3)

	protocolVersion, ok := headers.Get(uint16(ContextRequestHeaderProtocolVersion))
	require.True(t, ok)
	require.Equal(t, uint32(CurrentProtocolVersion), protocolVersion)

	clientVersion, ok := headers.Get(uint16(ContextRequestHeaderClientVersion))
	require.True(t, ok)
	require.Equal(t, "azcosmos/1.0.0", clientVersion)

	userAgent, ok := headers.Get(uint16(ContextRequestHeaderUserAgent))
	require.True(t, ok)
	require.Equal(t, "azure-sdk-for-go", userAgent)
}

func TestRntbdDocumentReadRequestRoundTrip(t *testing.T) {
	activityID := mustParseUUID(t, "11111111-2222-3333-4444-555555555555")
	resourceID := []byte{0x01, 0x02, 0x03, 0x04}

	request := NewDocumentRequest(
		OperationTypeRead,
		ResourceTypeDocument,
		activityID,
		"type=master&ver=1.0&sig=read",
		"/apps/replicas/0/",
		resourceID,
		99,
		nil,
	)

	frame, headers, payload := decodeRequestWire(t, request.Encode())
	require.Equal(t, ResourceTypeDocument, frame.ResourceType)
	require.Equal(t, OperationTypeRead, frame.OperationType)
	require.Equal(t, activityID, frame.ActivityID)
	require.Nil(t, payload)

	payloadPresent, ok := headers.Get(uint16(RequestHeaderPayloadPresent))
	require.True(t, ok)
	require.Equal(t, byte(0), payloadPresent)

	authToken, ok := headers.Get(uint16(RequestHeaderAuthorizationToken))
	require.True(t, ok)
	require.Equal(t, "type=master&ver=1.0&sig=read", authToken)

	replicaPath, ok := headers.Get(uint16(RequestHeaderReplicaPath))
	require.True(t, ok)
	require.Equal(t, "/apps/replicas/0/", replicaPath)

	decodedResourceID, ok := headers.Get(uint16(RequestHeaderResourceID))
	require.True(t, ok)
	require.Equal(t, resourceID, decodedResourceID)

	transportRequestID, ok := headers.Get(uint16(RequestHeaderTransportRequestID))
	require.True(t, ok)
	require.Equal(t, uint32(99), transportRequestID)
}

func TestRntbdDocumentCreateRequestRoundTrip(t *testing.T) {
	activityID := mustParseUUID(t, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	payload := []byte(`{"id":"doc-1"}`)

	request := NewDocumentRequest(
		OperationTypeCreate,
		ResourceTypeDocument,
		activityID,
		"type=master&ver=1.0&sig=create",
		"/apps/replicas/1/",
		[]byte("resource-id"),
		100,
		payload,
	)

	frame, headers, decodedPayload := decodeRequestWire(t, request.Encode())
	require.Equal(t, ResourceTypeDocument, frame.ResourceType)
	require.Equal(t, OperationTypeCreate, frame.OperationType)
	require.Equal(t, activityID, frame.ActivityID)
	require.Equal(t, payload, decodedPayload)

	payloadPresent, ok := headers.Get(uint16(RequestHeaderPayloadPresent))
	require.True(t, ok)
	require.Equal(t, byte(1), payloadPresent)
}

func TestRntbdResponseRoundTripWithoutPayload(t *testing.T) {
	activityID := mustParseUUID(t, "12345678-1234-5678-90ab-cdef12345678")
	headers := &TokenSet{}
	headers.Set(uint16(ResponseHeaderPayloadPresent), TokenTypeByte, byte(0))
	headers.Set(uint16(ResponseHeaderContinuationToken), TokenTypeString, "token")
	headers.Set(uint16(ResponseHeaderETag), TokenTypeString, "etag")
	headers.Set(uint16(ResponseHeaderRequestCharge), TokenTypeDouble, 7.5)
	headers.Set(uint16(ResponseHeaderSessionToken), TokenTypeString, "session")
	headers.Set(uint16(ResponseHeaderSubStatus), TokenTypeULong, uint32(1002))
	headers.Set(uint16(ResponseHeaderRetryAfterMilliseconds), TokenTypeULong, uint32(250))
	headers.Set(uint16(ResponseHeaderTransportRequestID), TokenTypeULong, uint32(11))

	wire := encodeResponseWire(activityID, 200, headers, nil)
	response, consumed, err := DecodeResponse(wire)
	require.NoError(t, err)
	require.Equal(t, len(wire), consumed)
	require.Equal(t, int32(200), response.StatusCode())
	require.Equal(t, activityID, response.ActivityID())
	require.False(t, response.IsPayloadPresent())
	require.Nil(t, response.Payload)
	require.Equal(t, "token", response.ContinuationToken())
	require.Equal(t, "etag", response.ETag())
	require.InDelta(t, 7.5, response.RequestCharge(), 0.0001)
	require.Equal(t, "session", response.SessionToken())
	require.Equal(t, uint32(1002), response.SubStatus())
	require.Equal(t, uint32(250), response.RetryAfterMs())
	transportRequestID, ok := response.TransportRequestID()
	require.True(t, ok)
	require.Equal(t, uint32(11), transportRequestID)
}

func TestRntbdResponseRoundTripWithPayload(t *testing.T) {
	activityID := mustParseUUID(t, "87654321-4321-8765-ba09-fedcba654321")
	headers := &TokenSet{}
	headers.Set(uint16(ResponseHeaderPayloadPresent), TokenTypeByte, byte(1))
	headers.Set(uint16(ResponseHeaderTransportRequestID), TokenTypeULong, uint32(12))
	payload := []byte(`{"Documents":[{"id":"doc-1"}]}`)

	wire := encodeResponseWire(activityID, 200, headers, payload)
	response, consumed, err := DecodeResponse(wire)
	require.NoError(t, err)
	require.Equal(t, len(wire), consumed)
	require.True(t, response.IsPayloadPresent())
	require.Equal(t, payload, response.Payload)
	transportRequestID, ok := response.TransportRequestID()
	require.True(t, ok)
	require.Equal(t, uint32(12), transportRequestID)
}

func TestRntbdContextResponseDecode(t *testing.T) {
	activityID := mustParseUUID(t, "99999999-8888-7777-6666-555555555555")
	headers := &TokenSet{}
	headers.Set(uint16(ContextResponseHeaderProtocolVersion), TokenTypeULong, uint32(CurrentProtocolVersion))
	headers.Set(uint16(ContextResponseHeaderServerAgent), TokenTypeSmallString, "gateway")
	headers.Set(uint16(ContextResponseHeaderServerVersion), TokenTypeSmallString, "1.2.3")
	headers.Set(uint16(ContextResponseHeaderIdleTimeoutInSeconds), TokenTypeULong, uint32(120))

	wire := encodeResponseWire(activityID, 200, headers, nil)
	response, consumed, err := DecodeResponse(wire)
	require.NoError(t, err)
	require.Equal(t, len(wire), consumed)

	contextResponse, err := DecodeContextResponse(response)
	require.NoError(t, err)
	require.Equal(t, "gateway", contextResponse.ServerAgent)
	require.Equal(t, "1.2.3", contextResponse.ServerVersion)
	require.Equal(t, uint32(CurrentProtocolVersion), contextResponse.ProtocolVersion)
	require.Equal(t, uint32(120), contextResponse.IdleTimeout)
}

func decodeRequestWire(t *testing.T, wire []byte) (RequestFrame, *TokenSet, []byte) {
	t.Helper()
	require.GreaterOrEqual(t, len(wire), RequestFrameLength)

	frame := DecodeRequestFrame(wire[:RequestFrameLength])
	require.GreaterOrEqual(t, int(frame.MetadataLength), RequestFrameLength)
	require.LessOrEqual(t, int(frame.MetadataLength), len(wire))

	headers := &TokenSet{}
	require.NoError(t, headers.Decode(wire[RequestFrameLength:frame.MetadataLength]))

	if int(frame.MetadataLength) == len(wire) {
		return frame, headers, nil
	}

	offset := int(frame.MetadataLength)
	require.GreaterOrEqual(t, len(wire[offset:]), 4)
	payloadLength := int(binary.LittleEndian.Uint32(wire[offset:]))
	payloadStart := offset + 4
	payloadEnd := payloadStart + payloadLength
	require.LessOrEqual(t, payloadEnd, len(wire))
	return frame, headers, append([]byte(nil), wire[payloadStart:payloadEnd]...)
}

func encodeResponseWire(activityID azuuid.UUID, statusCode int32, headers *TokenSet, payload []byte) []byte {
	if headers == nil {
		headers = &TokenSet{}
	}

	metadataLength := ResponseFrameLength + headers.EncodedLength()
	totalLength := metadataLength
	if payload != nil {
		totalLength += 4 + len(payload)
	}

	buf := make([]byte, totalLength)
	binary.LittleEndian.PutUint32(buf[0:], uint32(metadataLength))
	binary.LittleEndian.PutUint32(buf[4:], uint32(statusCode))
	EncodeGUID(buf[8:], activityID)
	headers.Encode(buf[ResponseFrameLength:metadataLength])
	if payload != nil {
		binary.LittleEndian.PutUint32(buf[metadataLength:], uint32(len(payload)))
		copy(buf[metadataLength+4:], payload)
	}
	return buf
}

func mustParseUUID(t *testing.T, value string) azuuid.UUID {
	t.Helper()
	id, err := azuuid.Parse(value)
	require.NoError(t, err)
	return id
}
