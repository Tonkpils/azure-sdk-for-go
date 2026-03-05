// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

// ChangeFeedMode specifies the change feed consumption mode.
type ChangeFeedMode int

const (
	// ChangeFeedModeLatestVersion returns only the latest version of changed items.
	// This is the default mode. Includes creates and updates, but not deletes.
	ChangeFeedModeLatestVersion ChangeFeedMode = iota

	// ChangeFeedModeAllVersionsAndDeletes returns all versions of changed items
	// including intermediate changes and delete notifications.
	// Requires the container to have a ChangeFeedPolicy with a retention window.
	ChangeFeedModeAllVersionsAndDeletes
)

// ChangeFeedOperationType represents the type of operation that caused a change.
type ChangeFeedOperationType string

const (
	// ChangeFeedOperationTypeCreate indicates a document was created.
	ChangeFeedOperationTypeCreate ChangeFeedOperationType = "create"
	// ChangeFeedOperationTypeReplace indicates a document was updated/replaced.
	ChangeFeedOperationTypeReplace ChangeFeedOperationType = "replace"
	// ChangeFeedOperationTypeDelete indicates a document was deleted.
	ChangeFeedOperationTypeDelete ChangeFeedOperationType = "delete"
)

// ChangeFeedMetadata contains metadata about a change feed item in full fidelity mode.
type ChangeFeedMetadata struct {
	// OperationType is the type of operation (create, replace, delete).
	OperationType ChangeFeedOperationType `json:"operationType"`
	// Lsn is the logical sequence number of the change.
	Lsn int64 `json:"lsn"`
	// PreviousImageLSN is the logical sequence number of the previous image.
	PreviousImageLSN int64 `json:"previousImageLSN,omitempty"`
	// ConflictResolutionTimestamp is the conflict resolution timestamp.
	ConflictResolutionTimestamp string `json:"crts,omitempty"`
	// IsTimeToLiveExpired indicates whether the delete was caused by TTL expiration.
	IsTimeToLiveExpired bool `json:"timeToLiveExpired"`
}

// ChangeFeedItem represents a single item from the change feed in full fidelity mode.
// It wraps the current and previous document states along with operation metadata.
type ChangeFeedItem struct {
	// Current is the current state of the document (nil for deletes after TTL).
	Current []byte `json:"current,omitempty"`
	// Previous is the previous state of the document (available for updates and deletes).
	Previous []byte `json:"previous,omitempty"`
	// Metadata contains operation details like type, LSN, and TTL expiration info.
	Metadata ChangeFeedMetadata `json:"metadata"`
}
