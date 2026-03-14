//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package rntbd

import (
	"encoding/binary"
	"fmt"
)

const tokenHeaderLength = 3

// Token represents a single RNTBD header token (TLV).
type Token struct {
	ID       uint16
	Type     RntbdTokenType
	Value    interface{}
	Required bool
	Present  bool
}

// TokenSet is an ordered collection of tokens for encoding/decoding.
type TokenSet struct {
	tokens []Token
}

// Set adds or replaces a token in the set while preserving order.
func (ts *TokenSet) Set(id uint16, tokenType RntbdTokenType, value interface{}) {
	for i := range ts.tokens {
		if ts.tokens[i].ID == id {
			ts.tokens[i].Type = tokenType
			ts.tokens[i].Value = cloneTokenValue(value)
			ts.tokens[i].Present = true
			return
		}
	}

	ts.tokens = append(ts.tokens, Token{
		ID:      id,
		Type:    tokenType,
		Value:   cloneTokenValue(value),
		Present: true,
	})
}

// Get returns the decoded value for a token if present.
func (ts *TokenSet) Get(id uint16) (interface{}, bool) {
	if ts == nil {
		return nil, false
	}

	for _, token := range ts.tokens {
		if token.ID == id && token.Present {
			return cloneTokenValue(token.Value), true
		}
	}
	return nil, false
}

// EncodedLength returns the total byte length of all present tokens.
func (ts *TokenSet) EncodedLength() int {
	if ts == nil {
		return 0
	}

	total := 0
	for _, token := range ts.tokens {
		if !token.Present {
			continue
		}
		total += tokenHeaderLength + token.Type.ValueLength(token.Value)
	}
	return total
}

// Encode writes all present tokens to buf. Returns bytes written.
func (ts *TokenSet) Encode(buf []byte) int {
	if ts == nil {
		return 0
	}

	offset := 0
	for _, token := range ts.tokens {
		if !token.Present {
			continue
		}

		binary.LittleEndian.PutUint16(buf[offset:], token.ID)
		buf[offset+2] = byte(token.Type)
		offset += tokenHeaderLength
		offset += token.Type.Encode(buf[offset:], token.Value)
	}
	return offset
}

// Decode reads tokens from buf until all bytes are consumed.
func (ts *TokenSet) Decode(buf []byte) error {
	ts.tokens = ts.tokens[:0]

	offset := 0
	for offset < len(buf) {
		if len(buf[offset:]) < tokenHeaderLength {
			return fmt.Errorf("decode token header: need %d bytes, have %d", tokenHeaderLength, len(buf[offset:]))
		}

		id := binary.LittleEndian.Uint16(buf[offset:])
		tokenType := RntbdTokenType(buf[offset+2])
		offset += tokenHeaderLength

		value, n, err := tokenType.Decode(buf[offset:])
		if err != nil {
			return fmt.Errorf("decode token 0x%04X: %w", id, err)
		}
		offset += n

		ts.tokens = append(ts.tokens, Token{
			ID:      id,
			Type:    tokenType,
			Value:   value,
			Present: true,
		})
	}

	return nil
}

func cloneTokenValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case []byte:
		return append([]byte(nil), v...)
	default:
		return v
	}
}
