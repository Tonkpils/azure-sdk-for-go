//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package rntbd

import (
	"encoding/binary"

	azuuid "github.com/Azure/azure-sdk-for-go/sdk/internal/uuid"
)

const RequestFrameLength = 24  // u32 + u16 + u16 + guid16
const ResponseFrameLength = 24 // u32 + i32 + guid16

type RequestFrame struct {
	MetadataLength uint32
	ResourceType   RntbdResourceType
	OperationType  RntbdOperationType
	ActivityID     azuuid.UUID
}

func (f *RequestFrame) Encode(buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:], f.MetadataLength)
	binary.LittleEndian.PutUint16(buf[4:], uint16(f.ResourceType))
	binary.LittleEndian.PutUint16(buf[6:], uint16(f.OperationType))
	EncodeGUID(buf[8:], f.ActivityID)
}

func DecodeRequestFrame(buf []byte) RequestFrame {
	return RequestFrame{
		MetadataLength: binary.LittleEndian.Uint32(buf[0:]),
		ResourceType:   RntbdResourceType(binary.LittleEndian.Uint16(buf[4:])),
		OperationType:  RntbdOperationType(binary.LittleEndian.Uint16(buf[6:])),
		ActivityID:     DecodeGUID(buf[8:24]),
	}
}

type ResponseFrame struct {
	MetadataLength uint32
	StatusCode     int32
	ActivityID     azuuid.UUID
}

func DecodeResponseFrame(buf []byte) ResponseFrame {
	return ResponseFrame{
		MetadataLength: binary.LittleEndian.Uint32(buf[0:]),
		StatusCode:     int32(binary.LittleEndian.Uint32(buf[4:])),
		ActivityID:     DecodeGUID(buf[8:24]),
	}
}
