//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package rntbd

import (
	"encoding/json"
	"fmt"
)

// ContextResponse holds the parsed context negotiation result.
type ContextResponse struct {
	ServerAgent     string
	ServerVersion   string
	IdleTimeout     uint32 // seconds
	ProtocolVersion uint32
}

// DecodeContextResponse parses a context response.
func DecodeContextResponse(resp *Response) (*ContextResponse, error) {
	if resp == nil {
		return nil, fmt.Errorf("context response is nil")
	}
	if resp.StatusCode() != 200 {
		return nil, contextResponseError(resp.StatusCode(), resp.Payload)
	}

	serverAgent, ok := tokenString(resp.Headers, uint16(ContextResponseHeaderServerAgent))
	if !ok {
		return nil, fmt.Errorf("context response missing server agent")
	}
	serverVersion, ok := tokenString(resp.Headers, uint16(ContextResponseHeaderServerVersion))
	if !ok {
		return nil, fmt.Errorf("context response missing server version")
	}

	contextResp := &ContextResponse{
		ServerAgent:   serverAgent,
		ServerVersion: serverVersion,
	}
	if protocolVersion, ok := tokenUint32(resp.Headers, uint16(ContextResponseHeaderProtocolVersion)); ok {
		contextResp.ProtocolVersion = protocolVersion
	}
	if idleTimeout, ok := tokenUint32(resp.Headers, uint16(ContextResponseHeaderIdleTimeoutInSeconds)); ok {
		contextResp.IdleTimeout = idleTimeout
	}

	return contextResp, nil
}

func contextResponseError(statusCode int32, payload []byte) error {
	if len(payload) == 0 {
		return fmt.Errorf("context response failed with status %d", statusCode)
	}

	var decoded interface{}
	if err := json.Unmarshal(payload, &decoded); err == nil {
		if compact, err := json.Marshal(decoded); err == nil {
			return fmt.Errorf("context response failed with status %d: %s", statusCode, compact)
		}
	}

	return fmt.Errorf("context response failed with status %d: %s", statusCode, string(payload))
}
