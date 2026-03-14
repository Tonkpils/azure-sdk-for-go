//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package rntbd

import azuuid "github.com/Azure/azure-sdk-for-go/sdk/internal/uuid"

// EncodeGUID writes a UUID in Microsoft GUID byte order to buf (16 bytes).
func EncodeGUID(buf []byte, id azuuid.UUID) {
	buf[0] = id[3]
	buf[1] = id[2]
	buf[2] = id[1]
	buf[3] = id[0]
	buf[4] = id[5]
	buf[5] = id[4]
	buf[6] = id[7]
	buf[7] = id[6]
	buf[8] = id[8]
	buf[9] = id[9]
	buf[10] = id[10]
	buf[11] = id[11]
	buf[12] = id[12]
	buf[13] = id[13]
	buf[14] = id[14]
	buf[15] = id[15]
}

// DecodeGUID reads a UUID from Microsoft GUID byte order in buf (16 bytes).
func DecodeGUID(buf []byte) azuuid.UUID {
	return azuuid.UUID{
		buf[3],
		buf[2],
		buf[1],
		buf[0],
		buf[5],
		buf[4],
		buf[7],
		buf[6],
		buf[8],
		buf[9],
		buf[10],
		buf[11],
		buf[12],
		buf[13],
		buf[14],
		buf[15],
	}
}
