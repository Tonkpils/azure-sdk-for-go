// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package azcosmos

import (
	"testing"
)

const (
	readOperation  = false
	writeOperation = true
)

// excludeRegionsLC builds a locationCache populated with three readable and
// three writable regions, with prefLocations set to [loc1, loc2, loc3]. The
// caller controls whether multi-write locations are enabled (which determines
// whether writes go through the bypass branch).
func excludeRegionsLC(t *testing.T, enableMultiWrite bool) *locationCache {
	t.Helper()
	lc := newLocationCache([]string{loc1.Name, loc2.Name, loc3.Name}, *defaultEndpoint, true)
	dbAcct := accountProperties{
		WriteRegions:                 []accountRegion{loc1, loc2, loc3},
		ReadRegions:                  []accountRegion{loc1, loc2, loc3},
		EnableMultipleWriteLocations: enableMultiWrite,
	}
	if err := lc.databaseAccountRead(dbAcct); err != nil {
		t.Fatalf("databaseAccountRead failed: %s", err.Error())
	}
	return lc
}

func TestResolveServiceEndpoint_ExcludeRegions_ReadSkipsExcluded(t *testing.T) {
	lc := excludeRegionsLC(t, false)

	endpoint := lc.resolveServiceEndpoint(0, resourceTypeDocument, readOperation, false, []string{loc1.Name})

	if endpoint != *loc2Endpoint {
		t.Errorf("expected %s, got %s", loc2Endpoint, endpoint.String())
	}
}

func TestResolveServiceEndpoint_ExcludeRegions_ReadMultipleExclusions(t *testing.T) {
	lc := excludeRegionsLC(t, false)

	endpoint := lc.resolveServiceEndpoint(0, resourceTypeDocument, readOperation, false, []string{loc1.Name, loc2.Name})

	if endpoint != *loc3Endpoint {
		t.Errorf("expected %s, got %s", loc3Endpoint, endpoint.String())
	}
}

func TestResolveServiceEndpoint_ExcludeRegions_AllExcludedFallsBackToDefault(t *testing.T) {
	lc := excludeRegionsLC(t, false)

	endpoint := lc.resolveServiceEndpoint(0, resourceTypeDocument, readOperation, false, []string{loc1.Name, loc2.Name, loc3.Name})

	if endpoint != *defaultEndpoint {
		t.Errorf("expected default endpoint %s, got %s", defaultEndpoint, endpoint.String())
	}
}

func TestResolveServiceEndpoint_ExcludeRegions_EmptyListUsesCachedFastPath(t *testing.T) {
	lc := excludeRegionsLC(t, false)

	withNil := lc.resolveServiceEndpoint(0, resourceTypeDocument, readOperation, false, nil)
	withEmpty := lc.resolveServiceEndpoint(0, resourceTypeDocument, readOperation, false, []string{})

	if withNil == (*defaultEndpoint) || withEmpty == (*defaultEndpoint) {
		t.Errorf("empty exclude list should not fall back to default endpoint; got nil=%s empty=%s", withNil.String(), withEmpty.String())
	}
	if withNil != withEmpty {
		t.Errorf("nil and empty exclude lists should resolve to the same endpoint; nil=%s empty=%s", withNil.String(), withEmpty.String())
	}
}

func TestResolveServiceEndpoint_ExcludeRegions_CaseSensitive(t *testing.T) {
	// Region names match .NET behavior: HashSet<string> is case-sensitive by
	// default, so a mismatched case should NOT be treated as excluded.
	lc := excludeRegionsLC(t, false)

	endpoint := lc.resolveServiceEndpoint(0, resourceTypeDocument, readOperation, false, []string{"LOCATION1"})

	if endpoint != *loc1Endpoint {
		t.Errorf("case-mismatched exclusion should not skip loc1; expected %s, got %s", loc1Endpoint, endpoint.String())
	}
}

func TestResolveServiceEndpoint_ExcludeRegions_LocationIndexWrapsAroundApplicable(t *testing.T) {
	lc := excludeRegionsLC(t, false)

	// With loc1 excluded, applicable = [loc2, loc3]. Index 5 % 2 = 1 -> loc3.
	endpoint := lc.resolveServiceEndpoint(5, resourceTypeDocument, readOperation, false, []string{loc1.Name})

	if endpoint != *loc3Endpoint {
		t.Errorf("expected %s, got %s", loc3Endpoint, endpoint.String())
	}
}

func TestResolveServiceEndpoint_ExcludeRegions_SingleMasterWriteIgnored(t *testing.T) {
	// Parity with .NET: single-master document writes go through the bypass
	// branch and silently ignore exclude regions. Routing flips between the
	// first 2 write regions regardless of the exclude list.
	lc := excludeRegionsLC(t, false)

	withExclusion := lc.resolveServiceEndpoint(0, resourceTypeDocument, writeOperation, false, []string{loc1.Name})
	withoutExclusion := lc.resolveServiceEndpoint(0, resourceTypeDocument, writeOperation, false, nil)

	if withExclusion != withoutExclusion {
		t.Errorf("single-master write should ignore ExcludeRegions for parity with .NET; with=%s without=%s", withExclusion.String(), withoutExclusion.String())
	}
	// And the chosen endpoint must still be the excluded region (loc1) because
	// the bypass branch flips between first 2 write regions and we asked for
	// index 0.
	if withExclusion != *loc1Endpoint {
		t.Errorf("expected single-master write to route to loc1 (bypass branch flip-flop), got %s", withExclusion.String())
	}
}

func TestResolveServiceEndpoint_ExcludeRegions_MultiMasterDocumentWriteHonored(t *testing.T) {
	// On multi-write accounts, document writes go through the applicable-
	// endpoints branch and DO consult ExcludeRegions, just like reads.
	lc := excludeRegionsLC(t, true)

	endpoint := lc.resolveServiceEndpoint(0, resourceTypeDocument, writeOperation, false, []string{loc1.Name})

	if endpoint != *loc2Endpoint {
		t.Errorf("expected multi-master document write to skip loc1; got %s", endpoint.String())
	}
}

func TestResolveServiceEndpoint_ExcludeRegions_MetadataWriteIgnored(t *testing.T) {
	// Non-document writes (e.g. container Replace) take the bypass branch even
	// on multi-write accounts because canUseMultipleWriteLocsToRoute requires
	// resourceTypeDocument. They silently ignore ExcludeRegions.
	lc := excludeRegionsLC(t, true)

	withExclusion := lc.resolveServiceEndpoint(0, resourceTypeCollection, writeOperation, false, []string{loc1.Name})
	withoutExclusion := lc.resolveServiceEndpoint(0, resourceTypeCollection, writeOperation, false, nil)

	if withExclusion != withoutExclusion {
		t.Errorf("metadata write should ignore ExcludeRegions; with=%s without=%s", withExclusion.String(), withoutExclusion.String())
	}
}

func TestResolveServiceEndpoint_ExcludeRegions_UnknownRegionStillUsesApplicablePath(t *testing.T) {
	// When ExcludeRegions is non-empty (even if no entries match a real
	// region), routing goes through the applicable-endpoints path which
	// iterates prefLocations directly. This mirrors .NET, which calls
	// GetApplicableEndpoints whenever ExcludeRegions has at least one entry
	// regardless of whether any of them actually match.
	//
	// Note: the cached readEndpoints fast path may have a different first
	// element because getPrefAvailableEndpoints pushes the fallback (write
	// endpoint) to the end for reads. This test pins the parity behavior so
	// callers can reason about it.
	lc := excludeRegionsLC(t, false)

	endpoint := lc.resolveServiceEndpoint(0, resourceTypeDocument, readOperation, false, []string{"location-not-real"})

	// prefLocations is [loc1, loc2, loc3], so applicable[0] = loc1.
	if endpoint != *loc1Endpoint {
		t.Errorf("expected %s (applicable-endpoints path iterates prefLocations), got %s", loc1Endpoint, endpoint.String())
	}
}

func TestItemOptions_GetExcludeRegions(t *testing.T) {
	var nilOpts *ItemOptions
	if got := nilOpts.getExcludeRegions(); got != nil {
		t.Errorf("nil ItemOptions should return nil exclude list, got %v", got)
	}

	emptyOpts := &ItemOptions{}
	if got := emptyOpts.getExcludeRegions(); got != nil {
		t.Errorf("empty ItemOptions should return nil exclude list, got %v", got)
	}

	populated := &ItemOptions{ExcludeRegions: []string{"East US 2", "West US"}}
	got := populated.getExcludeRegions()
	if len(got) != 2 || got[0] != "East US 2" || got[1] != "West US" {
		t.Errorf("ItemOptions exclude list mismatch, got %v", got)
	}
}

func TestQueryOptions_GetExcludeRegions(t *testing.T) {
	var nilOpts *QueryOptions
	if got := nilOpts.getExcludeRegions(); got != nil {
		t.Errorf("nil QueryOptions should return nil exclude list, got %v", got)
	}

	populated := &QueryOptions{ExcludeRegions: []string{"East US 2"}}
	if got := populated.getExcludeRegions(); len(got) != 1 || got[0] != "East US 2" {
		t.Errorf("QueryOptions exclude list mismatch, got %v", got)
	}
}

func TestReadManyOptions_GetExcludeRegions(t *testing.T) {
	var nilOpts *ReadManyOptions
	if got := nilOpts.getExcludeRegions(); got != nil {
		t.Errorf("nil ReadManyOptions should return nil exclude list, got %v", got)
	}

	populated := &ReadManyOptions{ExcludeRegions: []string{"East US 2"}}
	if got := populated.getExcludeRegions(); len(got) != 1 || got[0] != "East US 2" {
		t.Errorf("ReadManyOptions exclude list mismatch, got %v", got)
	}
}

// excludeRegionsProvider is satisfied by the per-operation options structs.
// Verifying that statically guards against accidental signature drift.
var (
	_ excludeRegionsProvider = (*ItemOptions)(nil)
	_ excludeRegionsProvider = (*QueryOptions)(nil)
	_ excludeRegionsProvider = (*ReadManyOptions)(nil)
)
