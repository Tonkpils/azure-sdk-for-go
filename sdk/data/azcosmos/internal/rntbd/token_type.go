//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package rntbd

import (
	"encoding/binary"
	"fmt"
	"math"

	azuuid "github.com/Azure/azure-sdk-for-go/sdk/internal/uuid"
)

// RntbdTokenType identifies the wire encoding for a token value.
type RntbdTokenType uint8

const (
	TokenTypeByte        RntbdTokenType = 0x00
	TokenTypeUShort      RntbdTokenType = 0x01
	TokenTypeULong       RntbdTokenType = 0x02
	TokenTypeLong        RntbdTokenType = 0x03
	TokenTypeULongLong   RntbdTokenType = 0x04
	TokenTypeLongLong    RntbdTokenType = 0x05
	TokenTypeGuid        RntbdTokenType = 0x06
	TokenTypeSmallString RntbdTokenType = 0x07
	TokenTypeString      RntbdTokenType = 0x08
	TokenTypeULongString RntbdTokenType = 0x09
	TokenTypeSmallBytes  RntbdTokenType = 0x0A
	TokenTypeBytes       RntbdTokenType = 0x0B
	TokenTypeULongBytes  RntbdTokenType = 0x0C
	TokenTypeFloat       RntbdTokenType = 0x0D
	TokenTypeDouble      RntbdTokenType = 0x0E
)

// ValueLength returns the encoded byte length for a given token type and value.
func (t RntbdTokenType) ValueLength(value interface{}) int {
	switch t {
	case TokenTypeByte:
		return 1
	case TokenTypeUShort:
		return 2
	case TokenTypeULong, TokenTypeLong, TokenTypeFloat:
		return 4
	case TokenTypeULongLong, TokenTypeLongLong, TokenTypeDouble:
		return 8
	case TokenTypeGuid:
		return 16
	case TokenTypeSmallString:
		return 1 + smallLength(len(mustString(value)), "small string")
	case TokenTypeString:
		return 2 + shortLength(len(mustString(value)), "string")
	case TokenTypeULongString:
		return 4 + ulongLength(len(mustString(value)), "ulong string")
	case TokenTypeSmallBytes:
		return 1 + smallLength(len(mustBytes(value)), "small bytes")
	case TokenTypeBytes:
		return 2 + shortLength(len(mustBytes(value)), "bytes")
	case TokenTypeULongBytes:
		return 4 + ulongLength(len(mustBytes(value)), "ulong bytes")
	default:
		panic(fmt.Sprintf("unsupported RNTBD token type %d", t))
	}
}

// Encode writes a token value to a byte buffer in little-endian format.
func (t RntbdTokenType) Encode(buf []byte, value interface{}) int {
	switch t {
	case TokenTypeByte:
		buf[0] = mustByte(value)
		return 1
	case TokenTypeUShort:
		binary.LittleEndian.PutUint16(buf, mustUShort(value))
		return 2
	case TokenTypeULong:
		binary.LittleEndian.PutUint32(buf, mustULong(value))
		return 4
	case TokenTypeLong:
		binary.LittleEndian.PutUint32(buf, uint32(mustLong(value)))
		return 4
	case TokenTypeULongLong:
		binary.LittleEndian.PutUint64(buf, mustULongLong(value))
		return 8
	case TokenTypeLongLong:
		binary.LittleEndian.PutUint64(buf, uint64(mustLongLong(value)))
		return 8
	case TokenTypeGuid:
		EncodeGUID(buf, mustGUID(value))
		return 16
	case TokenTypeSmallString:
		return encodeSmallString(buf, mustString(value))
	case TokenTypeString:
		return encodeString(buf, mustString(value))
	case TokenTypeULongString:
		return encodeULongString(buf, mustString(value))
	case TokenTypeSmallBytes:
		return encodeSmallBytes(buf, mustBytes(value))
	case TokenTypeBytes:
		return encodeBytes(buf, mustBytes(value))
	case TokenTypeULongBytes:
		return encodeULongBytes(buf, mustBytes(value))
	case TokenTypeFloat:
		binary.LittleEndian.PutUint32(buf, math.Float32bits(mustFloat(value)))
		return 4
	case TokenTypeDouble:
		binary.LittleEndian.PutUint64(buf, math.Float64bits(mustDouble(value)))
		return 8
	default:
		panic(fmt.Sprintf("unsupported RNTBD token type %d", t))
	}
}

// Decode reads a token value from a byte buffer.
func (t RntbdTokenType) Decode(buf []byte) (interface{}, int, error) {
	switch t {
	case TokenTypeByte:
		if len(buf) < 1 {
			return nil, 0, fmt.Errorf("decode %d: need 1 byte, have %d", t, len(buf))
		}
		return buf[0], 1, nil
	case TokenTypeUShort:
		if len(buf) < 2 {
			return nil, 0, fmt.Errorf("decode %d: need 2 bytes, have %d", t, len(buf))
		}
		return binary.LittleEndian.Uint16(buf), 2, nil
	case TokenTypeULong:
		if len(buf) < 4 {
			return nil, 0, fmt.Errorf("decode %d: need 4 bytes, have %d", t, len(buf))
		}
		return binary.LittleEndian.Uint32(buf), 4, nil
	case TokenTypeLong:
		if len(buf) < 4 {
			return nil, 0, fmt.Errorf("decode %d: need 4 bytes, have %d", t, len(buf))
		}
		return int32(binary.LittleEndian.Uint32(buf)), 4, nil
	case TokenTypeULongLong:
		if len(buf) < 8 {
			return nil, 0, fmt.Errorf("decode %d: need 8 bytes, have %d", t, len(buf))
		}
		return binary.LittleEndian.Uint64(buf), 8, nil
	case TokenTypeLongLong:
		if len(buf) < 8 {
			return nil, 0, fmt.Errorf("decode %d: need 8 bytes, have %d", t, len(buf))
		}
		return int64(binary.LittleEndian.Uint64(buf)), 8, nil
	case TokenTypeGuid:
		if len(buf) < 16 {
			return nil, 0, fmt.Errorf("decode %d: need 16 bytes, have %d", t, len(buf))
		}
		return DecodeGUID(buf), 16, nil
	case TokenTypeSmallString:
		return decodeSmallString(buf)
	case TokenTypeString:
		return decodeString(buf)
	case TokenTypeULongString:
		return decodeULongString(buf)
	case TokenTypeSmallBytes:
		return decodeSmallBytes(buf)
	case TokenTypeBytes:
		return decodeBytes(buf)
	case TokenTypeULongBytes:
		return decodeULongBytes(buf)
	case TokenTypeFloat:
		if len(buf) < 4 {
			return nil, 0, fmt.Errorf("decode %d: need 4 bytes, have %d", t, len(buf))
		}
		return math.Float32frombits(binary.LittleEndian.Uint32(buf)), 4, nil
	case TokenTypeDouble:
		if len(buf) < 8 {
			return nil, 0, fmt.Errorf("decode %d: need 8 bytes, have %d", t, len(buf))
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(buf)), 8, nil
	default:
		return nil, 0, fmt.Errorf("unsupported RNTBD token type %d", t)
	}
}

func encodeSmallString(buf []byte, value string) int {
	length := smallLength(len(value), "small string")
	buf[0] = byte(length)
	return 1 + copy(buf[1:], value)
}

func encodeString(buf []byte, value string) int {
	length := shortLength(len(value), "string")
	binary.LittleEndian.PutUint16(buf, uint16(length))
	return 2 + copy(buf[2:], value)
}

func encodeULongString(buf []byte, value string) int {
	length := ulongLength(len(value), "ulong string")
	binary.LittleEndian.PutUint32(buf, uint32(length))
	return 4 + copy(buf[4:], value)
}

func encodeSmallBytes(buf []byte, value []byte) int {
	length := smallLength(len(value), "small bytes")
	buf[0] = byte(length)
	return 1 + copy(buf[1:], value)
}

func encodeBytes(buf []byte, value []byte) int {
	length := shortLength(len(value), "bytes")
	binary.LittleEndian.PutUint16(buf, uint16(length))
	return 2 + copy(buf[2:], value)
}

func encodeULongBytes(buf []byte, value []byte) int {
	length := ulongLength(len(value), "ulong bytes")
	binary.LittleEndian.PutUint32(buf, uint32(length))
	return 4 + copy(buf[4:], value)
}

func decodeSmallString(buf []byte) (string, int, error) {
	if len(buf) < 1 {
		return "", 0, fmt.Errorf("decode small string: need length prefix, have %d", len(buf))
	}
	length := int(buf[0])
	if len(buf) < 1+length {
		return "", 0, fmt.Errorf("decode small string: need %d bytes, have %d", 1+length, len(buf))
	}
	return string(buf[1 : 1+length]), 1 + length, nil
}

func decodeString(buf []byte) (string, int, error) {
	if len(buf) < 2 {
		return "", 0, fmt.Errorf("decode string: need length prefix, have %d", len(buf))
	}
	length := int(binary.LittleEndian.Uint16(buf))
	if len(buf) < 2+length {
		return "", 0, fmt.Errorf("decode string: need %d bytes, have %d", 2+length, len(buf))
	}
	return string(buf[2 : 2+length]), 2 + length, nil
}

func decodeULongString(buf []byte) (string, int, error) {
	if len(buf) < 4 {
		return "", 0, fmt.Errorf("decode ulong string: need length prefix, have %d", len(buf))
	}
	length := binary.LittleEndian.Uint32(buf)
	if uint32(len(buf)-4) < length {
		return "", 0, fmt.Errorf("decode ulong string: need %d bytes, have %d", 4+length, len(buf))
	}
	end := 4 + int(length)
	return string(buf[4:end]), end, nil
}

func decodeSmallBytes(buf []byte) ([]byte, int, error) {
	if len(buf) < 1 {
		return nil, 0, fmt.Errorf("decode small bytes: need length prefix, have %d", len(buf))
	}
	length := int(buf[0])
	if len(buf) < 1+length {
		return nil, 0, fmt.Errorf("decode small bytes: need %d bytes, have %d", 1+length, len(buf))
	}
	return append([]byte(nil), buf[1:1+length]...), 1 + length, nil
}

func decodeBytes(buf []byte) ([]byte, int, error) {
	if len(buf) < 2 {
		return nil, 0, fmt.Errorf("decode bytes: need length prefix, have %d", len(buf))
	}
	length := int(binary.LittleEndian.Uint16(buf))
	if len(buf) < 2+length {
		return nil, 0, fmt.Errorf("decode bytes: need %d bytes, have %d", 2+length, len(buf))
	}
	return append([]byte(nil), buf[2:2+length]...), 2 + length, nil
}

func decodeULongBytes(buf []byte) ([]byte, int, error) {
	if len(buf) < 4 {
		return nil, 0, fmt.Errorf("decode ulong bytes: need length prefix, have %d", len(buf))
	}
	length := binary.LittleEndian.Uint32(buf)
	if uint32(len(buf)-4) < length {
		return nil, 0, fmt.Errorf("decode ulong bytes: need %d bytes, have %d", 4+length, len(buf))
	}
	end := 4 + int(length)
	return append([]byte(nil), buf[4:end]...), end, nil
}

func smallLength(length int, kind string) int {
	if length > math.MaxUint8 {
		panic(fmt.Sprintf("%s length %d exceeds %d", kind, length, math.MaxUint8))
	}
	return length
}

func shortLength(length int, kind string) int {
	if length > math.MaxUint16 {
		panic(fmt.Sprintf("%s length %d exceeds %d", kind, length, math.MaxUint16))
	}
	return length
}

func ulongLength(length int, kind string) int {
	if uint64(length) > math.MaxUint32 {
		panic(fmt.Sprintf("%s length %d exceeds %d", kind, length, uint64(math.MaxUint32)))
	}
	return length
}

func mustByte(value interface{}) byte {
	switch v := value.(type) {
	case byte:
		return v
	case uint16:
		return byte(v)
	case uint32:
		return byte(v)
	case uint64:
		return byte(v)
	case uint:
		return byte(v)
	case int8:
		return byte(v)
	case int16:
		return byte(v)
	case int32:
		return byte(v)
	case int64:
		return byte(v)
	case int:
		return byte(v)
	default:
		panic(fmt.Sprintf("expected byte-compatible value, got %T", value))
	}
}

func mustUShort(value interface{}) uint16 {
	switch v := value.(type) {
	case byte:
		return uint16(v)
	case uint16:
		return v
	case uint32:
		return uint16(v)
	case uint64:
		return uint16(v)
	case uint:
		return uint16(v)
	case int8:
		return uint16(v)
	case int16:
		return uint16(v)
	case int32:
		return uint16(v)
	case int64:
		return uint16(v)
	case int:
		return uint16(v)
	default:
		panic(fmt.Sprintf("expected uint16-compatible value, got %T", value))
	}
}

func mustULong(value interface{}) uint32 {
	switch v := value.(type) {
	case byte:
		return uint32(v)
	case uint16:
		return uint32(v)
	case uint32:
		return v
	case uint64:
		return uint32(v)
	case uint:
		return uint32(v)
	case int8:
		return uint32(v)
	case int16:
		return uint32(v)
	case int32:
		return uint32(v)
	case int64:
		return uint32(v)
	case int:
		return uint32(v)
	default:
		panic(fmt.Sprintf("expected uint32-compatible value, got %T", value))
	}
}

func mustLong(value interface{}) int32 {
	switch v := value.(type) {
	case byte:
		return int32(v)
	case uint16:
		return int32(v)
	case uint32:
		return int32(v)
	case uint64:
		return int32(v)
	case uint:
		return int32(v)
	case int8:
		return int32(v)
	case int16:
		return int32(v)
	case int32:
		return v
	case int64:
		return int32(v)
	case int:
		return int32(v)
	default:
		panic(fmt.Sprintf("expected int32-compatible value, got %T", value))
	}
}

func mustULongLong(value interface{}) uint64 {
	switch v := value.(type) {
	case byte:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return v
	case uint:
		return uint64(v)
	case int8:
		return uint64(v)
	case int16:
		return uint64(v)
	case int32:
		return uint64(v)
	case int64:
		return uint64(v)
	case int:
		return uint64(v)
	default:
		panic(fmt.Sprintf("expected uint64-compatible value, got %T", value))
	}
}

func mustLongLong(value interface{}) int64 {
	switch v := value.(type) {
	case byte:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return int64(v)
	case uint:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	default:
		panic(fmt.Sprintf("expected int64-compatible value, got %T", value))
	}
}

func mustGUID(value interface{}) azuuid.UUID {
	switch v := value.(type) {
	case azuuid.UUID:
		return v
	default:
		panic(fmt.Sprintf("expected uuid.UUID value, got %T", value))
	}
}

func mustString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	default:
		panic(fmt.Sprintf("expected string value, got %T", value))
	}
}

func mustBytes(value interface{}) []byte {
	switch v := value.(type) {
	case []byte:
		return v
	default:
		panic(fmt.Sprintf("expected []byte value, got %T", value))
	}
}

func mustFloat(value interface{}) float32 {
	switch v := value.(type) {
	case float32:
		return v
	case float64:
		return float32(v)
	default:
		panic(fmt.Sprintf("expected float32-compatible value, got %T", value))
	}
}

func mustDouble(value interface{}) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	default:
		panic(fmt.Sprintf("expected float64-compatible value, got %T", value))
	}
}
