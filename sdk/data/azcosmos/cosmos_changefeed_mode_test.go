// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"encoding/json"
	"testing"
)

func TestChangeFeedItemJSONDeserialization(t *testing.T) {
	// Create scenario — current + metadata, no previous
	// []byte fields serialize as base64 in JSON
	createJSON := `{
		"current": "eyJpZCI6ICJkb2MxIiwgIm5hbWUiOiAidGVzdCJ9",
		"metadata": {
			"operationType": "create",
			"lsn": 42,
			"timeToLiveExpired": false
		}
	}`

	var createItem ChangeFeedItem
	if err := json.Unmarshal([]byte(createJSON), &createItem); err != nil {
		t.Fatalf("Failed to unmarshal create item: %v", err)
	}

	if createItem.Current == nil {
		t.Fatal("Expected Current to be non-nil for create")
	}
	if createItem.Previous != nil {
		t.Error("Expected Previous to be nil for create")
	}
	if createItem.Metadata.OperationType != ChangeFeedOperationTypeCreate {
		t.Errorf("Expected operationType 'create', got %v", createItem.Metadata.OperationType)
	}
	if createItem.Metadata.Lsn != 42 {
		t.Errorf("Expected lsn 42, got %v", createItem.Metadata.Lsn)
	}
	if createItem.Metadata.IsTimeToLiveExpired {
		t.Error("Expected timeToLiveExpired to be false")
	}

	// Replace scenario — both current and previous
	replaceJSON := `{
		"current": "eyJpZCI6ICJkb2MxIiwgIm5hbWUiOiAidXBkYXRlZCJ9",
		"previous": "eyJpZCI6ICJkb2MxIiwgIm5hbWUiOiAib3JpZ2luYWwifQ==",
		"metadata": {
			"operationType": "replace",
			"lsn": 43,
			"previousImageLSN": 42,
			"timeToLiveExpired": false
		}
	}`

	var replaceItem ChangeFeedItem
	if err := json.Unmarshal([]byte(replaceJSON), &replaceItem); err != nil {
		t.Fatalf("Failed to unmarshal replace item: %v", err)
	}

	if replaceItem.Current == nil {
		t.Fatal("Expected Current to be non-nil for replace")
	}
	if replaceItem.Previous == nil {
		t.Fatal("Expected Previous to be non-nil for replace")
	}
	if replaceItem.Metadata.OperationType != ChangeFeedOperationTypeReplace {
		t.Errorf("Expected operationType 'replace', got %v", replaceItem.Metadata.OperationType)
	}
	if replaceItem.Metadata.Lsn != 43 {
		t.Errorf("Expected lsn 43, got %v", replaceItem.Metadata.Lsn)
	}
	if replaceItem.Metadata.PreviousImageLSN != 42 {
		t.Errorf("Expected previousImageLSN 42, got %v", replaceItem.Metadata.PreviousImageLSN)
	}

	// Delete scenario — previous only, no current
	deleteJSON := `{
		"previous": "eyJpZCI6ICJkb2MxIiwgIm5hbWUiOiAiZGVsZXRlZCJ9",
		"metadata": {
			"operationType": "delete",
			"lsn": 44,
			"timeToLiveExpired": false
		}
	}`

	var deleteItem ChangeFeedItem
	if err := json.Unmarshal([]byte(deleteJSON), &deleteItem); err != nil {
		t.Fatalf("Failed to unmarshal delete item: %v", err)
	}

	if deleteItem.Current != nil {
		t.Error("Expected Current to be nil for delete")
	}
	if deleteItem.Previous == nil {
		t.Fatal("Expected Previous to be non-nil for delete")
	}
	if deleteItem.Metadata.OperationType != ChangeFeedOperationTypeDelete {
		t.Errorf("Expected operationType 'delete', got %v", deleteItem.Metadata.OperationType)
	}
	if deleteItem.Metadata.Lsn != 44 {
		t.Errorf("Expected lsn 44, got %v", deleteItem.Metadata.Lsn)
	}
	if deleteItem.Metadata.IsTimeToLiveExpired {
		t.Error("Expected timeToLiveExpired to be false for explicit delete")
	}

	// TTL expiration delete
	ttlJSON := `{
		"previous": "eyJpZCI6ICJkb2MxIn0=",
		"metadata": {
			"operationType": "delete",
			"lsn": 45,
			"timeToLiveExpired": true
		}
	}`

	var ttlItem ChangeFeedItem
	if err := json.Unmarshal([]byte(ttlJSON), &ttlItem); err != nil {
		t.Fatalf("Failed to unmarshal TTL delete item: %v", err)
	}

	if ttlItem.Current != nil {
		t.Error("Expected Current to be nil for TTL delete")
	}
	if ttlItem.Previous == nil {
		t.Fatal("Expected Previous to be non-nil for TTL delete")
	}
	if ttlItem.Metadata.OperationType != ChangeFeedOperationTypeDelete {
		t.Errorf("Expected operationType 'delete', got %v", ttlItem.Metadata.OperationType)
	}
	if !ttlItem.Metadata.IsTimeToLiveExpired {
		t.Error("Expected timeToLiveExpired to be true for TTL delete")
	}
}

func TestChangeFeedModeDefaults(t *testing.T) {
	var mode ChangeFeedMode
	if mode != ChangeFeedModeLatestVersion {
		t.Error("zero value of ChangeFeedMode should be ChangeFeedModeLatestVersion")
	}
}

func TestChangeFeedOptionsFullFidelityHeaders(t *testing.T) {
	dummyHead := newChangeFeedRange("", "FF", nil)

	// AllVersionsAndDeletes mode
	options := &ChangeFeedOptions{
		Mode: ChangeFeedModeAllVersionsAndDeletes,
	}
	headers, err := options.buildRequestHeaders(dummyHead, "0")
	if err != nil {
		t.Fatalf("buildRequestHeaders error: %v", err)
	}

	if headers[cosmosHeaderChangeFeed] != cosmosHeaderValuesChangeFeedFullFidelity {
		t.Errorf("Expected A-IM to be %v, got %v", cosmosHeaderValuesChangeFeedFullFidelity, headers[cosmosHeaderChangeFeed])
	}
	if headers[cosmosHeaderChangeFeedWireFormatVersion] != cosmosHeaderValuesChangeFeedWireFormat {
		t.Errorf("Expected wire format version to be %v, got %v", cosmosHeaderValuesChangeFeedWireFormat, headers[cosmosHeaderChangeFeedWireFormatVersion])
	}

	// LatestVersion mode (default)
	latestOptions := &ChangeFeedOptions{
		Mode: ChangeFeedModeLatestVersion,
	}
	latestHeaders, err := latestOptions.buildRequestHeaders(dummyHead, "0")
	if err != nil {
		t.Fatalf("buildRequestHeaders error: %v", err)
	}

	if latestHeaders[cosmosHeaderChangeFeed] != cosmosHeaderValuesChangeFeed {
		t.Errorf("Expected A-IM to be %v, got %v", cosmosHeaderValuesChangeFeed, latestHeaders[cosmosHeaderChangeFeed])
	}
	if _, exists := latestHeaders[cosmosHeaderChangeFeedWireFormatVersion]; exists {
		t.Error("Expected no wire format version header for LatestVersion mode")
	}
}

func TestChangeFeedProcessorOptionsMode(t *testing.T) {
	defaults := changeFeedProcessorDefaults()
	if defaults.Mode != ChangeFeedModeLatestVersion {
		t.Errorf("Expected default mode to be ChangeFeedModeLatestVersion, got %v", defaults.Mode)
	}
}
