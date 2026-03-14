//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package rntbd

import (
	"encoding/binary"

	azuuid "github.com/Azure/azure-sdk-for-go/sdk/internal/uuid"
)

// Request represents a full RNTBD request (frame + headers + optional payload).
type Request struct {
	Frame   RequestFrame
	Headers *TokenSet
	Payload []byte
}

// Encode serializes the request to wire format.
func (r *Request) Encode() []byte {
	headersLen := 0
	if r.Headers != nil {
		headersLen = r.Headers.EncodedLength()
	}

	r.Frame.MetadataLength = uint32(RequestFrameLength + headersLen)
	totalLength := int(r.Frame.MetadataLength)
	if r.Payload != nil {
		totalLength += 4 + len(r.Payload)
	}

	buf := make([]byte, totalLength)
	r.Frame.Encode(buf[:RequestFrameLength])
	if r.Headers != nil {
		r.Headers.Encode(buf[RequestFrameLength:r.Frame.MetadataLength])
	}

	if r.Payload != nil {
		offset := int(r.Frame.MetadataLength)
		binary.LittleEndian.PutUint32(buf[offset:], uint32(len(r.Payload)))
		copy(buf[offset+4:], r.Payload)
	}

	return buf
}

// NewContextRequest creates the initial handshake request.
func NewContextRequest(clientVersion, userAgent string) *Request {
	headers := &TokenSet{}
	headers.Set(uint16(ContextRequestHeaderProtocolVersion), TokenTypeULong, uint32(CurrentProtocolVersion))
	headers.Set(uint16(ContextRequestHeaderClientVersion), TokenTypeSmallString, clientVersion)
	headers.Set(uint16(ContextRequestHeaderUserAgent), TokenTypeSmallString, userAgent)

	return &Request{
		Frame: RequestFrame{
			ResourceType:  ResourceTypeConnection,
			OperationType: OperationTypeConnection,
			ActivityID:    newActivityID(),
		},
		Headers: headers,
	}
}

// NewDocumentRequest creates a request for a document operation.
func NewDocumentRequest(
	opType RntbdOperationType,
	resType RntbdResourceType,
	activityID azuuid.UUID,
	authToken string,
	replicaPath string,
	resourceID []byte,
	transportRequestID uint32,
	payload []byte,
) *Request {
	headers := &TokenSet{}
	headers.Set(uint16(RequestHeaderResourceID), TokenTypeBytes, resourceID)
	headers.Set(uint16(RequestHeaderAuthorizationToken), TokenTypeString, authToken)
	headers.Set(uint16(RequestHeaderPayloadPresent), TokenTypeByte, payloadPresentValue(payload))
	headers.Set(uint16(RequestHeaderReplicaPath), TokenTypeString, replicaPath)
	headers.Set(uint16(RequestHeaderTransportRequestID), TokenTypeULong, transportRequestID)

	return &Request{
		Frame: RequestFrame{
			ResourceType:   resType,
			OperationType:  opType,
			ActivityID:     activityID,
			MetadataLength: uint32(RequestFrameLength + headers.EncodedLength()),
		},
		Headers: headers,
		Payload: clonePayload(payload),
	}
}

func newActivityID() azuuid.UUID {
	activityID, err := azuuid.New()
	if err != nil {
		return azuuid.UUID{}
	}
	return activityID
}

func payloadPresentValue(payload []byte) byte {
	if payload != nil {
		return 1
	}
	return 0
}

func clonePayload(payload []byte) []byte {
	if payload == nil {
		return nil
	}
	return append([]byte(nil), payload...)
}
