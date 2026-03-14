//go:build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package rntbd

const CurrentProtocolVersion = 1

// RntbdOperationType maps SDK operation types to wire values.
type RntbdOperationType uint16

const (
	OperationTypeConnection RntbdOperationType = 0x0000
	OperationTypeCreate     RntbdOperationType = 0x0001
	OperationTypeRead       RntbdOperationType = 0x0003
	OperationTypeReplace    RntbdOperationType = 0x0004
	OperationTypeDelete     RntbdOperationType = 0x0005
	OperationTypeQuery      RntbdOperationType = 0x000E
	OperationTypeHead       RntbdOperationType = 0x000C
	OperationTypeReadFeed   RntbdOperationType = 0x000F
	OperationTypePatch      RntbdOperationType = 0x0019
	OperationTypeBatch      RntbdOperationType = 0x0016
	OperationTypeUpsert     RntbdOperationType = 0x0008
)

type RntbdResourceType uint16

const (
	ResourceTypeConnection        RntbdResourceType = 0x0000
	ResourceTypeDatabase          RntbdResourceType = 0x0001
	ResourceTypeCollection        RntbdResourceType = 0x0002
	ResourceTypeDocument          RntbdResourceType = 0x0003
	ResourceTypeStoredProcedure   RntbdResourceType = 0x0008
	ResourceTypePartitionKeyRange RntbdResourceType = 0x0017
)

// RntbdContextRequestHeader identifies headers in an RNTBD context request.
type RntbdContextRequestHeader uint16

const (
	ContextRequestHeaderProtocolVersion RntbdContextRequestHeader = 0x0000 // TokenTypeULong, required.
	ContextRequestHeaderClientVersion   RntbdContextRequestHeader = 0x0001 // TokenTypeSmallString, required.
	ContextRequestHeaderUserAgent       RntbdContextRequestHeader = 0x0002 // TokenTypeSmallString, required.
)

// RntbdContextResponseHeader identifies headers in an RNTBD context response.
type RntbdContextResponseHeader uint16

const (
	ContextResponseHeaderProtocolVersion                 RntbdContextResponseHeader = 0x0000 // TokenTypeULong.
	ContextResponseHeaderClientVersion                   RntbdContextResponseHeader = 0x0001 // TokenTypeSmallString.
	ContextResponseHeaderServerAgent                     RntbdContextResponseHeader = 0x0002 // TokenTypeSmallString, required.
	ContextResponseHeaderServerVersion                   RntbdContextResponseHeader = 0x0003 // TokenTypeSmallString, required.
	ContextResponseHeaderIdleTimeoutInSeconds            RntbdContextResponseHeader = 0x0004 // TokenTypeULong.
	ContextResponseHeaderUnauthenticatedTimeoutInSeconds RntbdContextResponseHeader = 0x0005 // TokenTypeULong.
)

// RntbdRequestHeader identifies headers in an RNTBD request.
type RntbdRequestHeader uint16

const (
	RequestHeaderResourceID                 RntbdRequestHeader = 0x0000 // TokenTypeBytes.
	RequestHeaderAuthorizationToken         RntbdRequestHeader = 0x0001 // TokenTypeString.
	RequestHeaderPayloadPresent             RntbdRequestHeader = 0x0002 // TokenTypeByte, required.
	RequestHeaderDate                       RntbdRequestHeader = 0x0003 // TokenTypeSmallString.
	RequestHeaderSessionToken               RntbdRequestHeader = 0x0005 // TokenTypeString.
	RequestHeaderContinuationToken          RntbdRequestHeader = 0x0006 // TokenTypeString.
	RequestHeaderConsistencyLevel           RntbdRequestHeader = 0x0010 // TokenTypeByte.
	RequestHeaderReplicaPath                RntbdRequestHeader = 0x0013 // TokenTypeString.
	RequestHeaderPartitionKey               RntbdRequestHeader = 0x002B // TokenTypeString.
	RequestHeaderPartitionKeyRangeID        RntbdRequestHeader = 0x002C // TokenTypeString.
	RequestHeaderCollectionRID              RntbdRequestHeader = 0x0035 // TokenTypeString.
	RequestHeaderTransportRequestID         RntbdRequestHeader = 0x004D // TokenTypeULong.
	RequestHeaderEffectivePartitionKey      RntbdRequestHeader = 0x005A // TokenTypeBytes.
	RequestHeaderContentSerializationFormat RntbdRequestHeader = 0x0065 // TokenTypeByte.
)

// RntbdResponseHeader identifies headers in an RNTBD response.
type RntbdResponseHeader uint16

const (
	ResponseHeaderPayloadPresent         RntbdResponseHeader = 0x0000 // TokenTypeByte, required.
	ResponseHeaderContinuationToken      RntbdResponseHeader = 0x0003 // TokenTypeString.
	ResponseHeaderETag                   RntbdResponseHeader = 0x0004 // TokenTypeString.
	ResponseHeaderRetryAfterMilliseconds RntbdResponseHeader = 0x000C // TokenTypeULong.
	ResponseHeaderLSN                    RntbdResponseHeader = 0x0013 // TokenTypeLongLong.
	ResponseHeaderRequestCharge          RntbdResponseHeader = 0x0015 // TokenTypeDouble.
	ResponseHeaderOwnerID                RntbdResponseHeader = 0x0018 // TokenTypeString.
	ResponseHeaderSubStatus              RntbdResponseHeader = 0x001C // TokenTypeULong.
	ResponseHeaderPartitionKeyRangeID    RntbdResponseHeader = 0x0021 // TokenTypeString.
	ResponseHeaderTransportRequestID     RntbdResponseHeader = 0x0035 // TokenTypeULong.
	ResponseHeaderSessionToken           RntbdResponseHeader = 0x003E // TokenTypeString.
)
