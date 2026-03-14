//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package rntbd

import (
	"encoding/binary"
	"fmt"

	azuuid "github.com/Azure/azure-sdk-for-go/sdk/internal/uuid"
)

// Response represents a decoded RNTBD response.
type Response struct {
	Frame   ResponseFrame
	Headers *TokenSet
	Payload []byte
}

// DecodeResponse reads a complete response from buf.
// Returns the response, total bytes consumed, and any error.
func DecodeResponse(buf []byte) (*Response, int, error) {
	if len(buf) < ResponseFrameLength {
		return nil, 0, fmt.Errorf("decode response frame: need %d bytes, have %d", ResponseFrameLength, len(buf))
	}

	frame := DecodeResponseFrame(buf[:ResponseFrameLength])
	if frame.MetadataLength < ResponseFrameLength {
		return nil, 0, fmt.Errorf("decode response frame: metadata length %d is smaller than frame length %d", frame.MetadataLength, ResponseFrameLength)
	}
	if int(frame.MetadataLength) > len(buf) {
		return nil, 0, fmt.Errorf("decode response frame: metadata length %d exceeds buffer length %d", frame.MetadataLength, len(buf))
	}

	headers := &TokenSet{}
	if err := headers.Decode(buf[ResponseFrameLength:frame.MetadataLength]); err != nil {
		return nil, 0, err
	}

	resp := &Response{
		Frame:   frame,
		Headers: headers,
	}
	consumed := int(frame.MetadataLength)
	if consumed == len(buf) {
		return resp, consumed, nil
	}
	if len(buf[consumed:]) < 4 {
		return nil, consumed, fmt.Errorf("decode response payload: need 4 bytes for payload length, have %d", len(buf[consumed:]))
	}

	payloadLength := int(binary.LittleEndian.Uint32(buf[consumed:]))
	consumed += 4
	if len(buf[consumed:]) < payloadLength {
		return nil, consumed, fmt.Errorf("decode response payload: need %d bytes, have %d", payloadLength, len(buf[consumed:]))
	}

	resp.Payload = append([]byte(nil), buf[consumed:consumed+payloadLength]...)
	consumed += payloadLength
	return resp, consumed, nil
}

func (r *Response) StatusCode() int32 {
	return r.Frame.StatusCode
}

func (r *Response) ActivityID() azuuid.UUID {
	return r.Frame.ActivityID
}

func (r *Response) TransportRequestID() (uint32, bool) {
	return tokenUint32(r.Headers, uint16(ResponseHeaderTransportRequestID))
}

func (r *Response) ContinuationToken() string {
	value, _ := tokenString(r.Headers, uint16(ResponseHeaderContinuationToken))
	return value
}

func (r *Response) ETag() string {
	value, _ := tokenString(r.Headers, uint16(ResponseHeaderETag))
	return value
}

func (r *Response) RequestCharge() float64 {
	value, _ := tokenFloat64(r.Headers, uint16(ResponseHeaderRequestCharge))
	return value
}

func (r *Response) SessionToken() string {
	value, _ := tokenString(r.Headers, uint16(ResponseHeaderSessionToken))
	return value
}

func (r *Response) SubStatus() uint32 {
	value, _ := tokenUint32(r.Headers, uint16(ResponseHeaderSubStatus))
	return value
}

func (r *Response) RetryAfterMs() uint32 {
	value, _ := tokenUint32(r.Headers, uint16(ResponseHeaderRetryAfterMilliseconds))
	return value
}

func (r *Response) IsPayloadPresent() bool {
	value, ok := tokenByte(r.Headers, uint16(ResponseHeaderPayloadPresent))
	if ok {
		return value != 0
	}
	return r.Payload != nil
}

func tokenByte(headers *TokenSet, id uint16) (byte, bool) {
	value, ok := headers.Get(id)
	if !ok {
		return 0, false
	}

	switch v := value.(type) {
	case byte:
		return v, true
	default:
		return 0, false
	}
}

func tokenUint32(headers *TokenSet, id uint16) (uint32, bool) {
	value, ok := headers.Get(id)
	if !ok {
		return 0, false
	}

	v, ok := value.(uint32)
	return v, ok
}

func tokenString(headers *TokenSet, id uint16) (string, bool) {
	value, ok := headers.Get(id)
	if !ok {
		return "", false
	}

	v, ok := value.(string)
	return v, ok
}

func tokenFloat64(headers *TokenSet, id uint16) (float64, bool) {
	value, ok := headers.Get(id)
	if !ok {
		return 0, false
	}

	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	default:
		return 0, false
	}
}
