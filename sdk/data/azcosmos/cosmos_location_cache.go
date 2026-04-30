// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"fmt"
	"net/url"
	"sync"
	"time"
)

const defaultExpirationTime time.Duration = time.Minute * 5

const (
	none requestedOperations = iota
	read
	write
	all
)

type requestedOperations int

type locationUnavailabilityInfo struct {
	lastCheckTime  time.Time
	unavailableOps requestedOperations
}

type databaseAccountLocationsInfo struct {
	prefLocations                 []string
	availWriteLocations           []string
	availReadLocations            []string
	availWriteEndpointsByLocation map[string]url.URL
	availReadEndpointsByLocation  map[string]url.URL
	writeEndpoints                []url.URL
	readEndpoints                 []url.URL
}

type accountRegion struct {
	Name     string `json:"name"`
	Endpoint string `json:"databaseAccountEndpoint"`
}

type userConsistencyPolicy struct {
	DefaultConsistencyLevel string `json:"defaultConsistencyLevel"`
}

type accountProperties struct {
	ReadRegions                  []accountRegion       `json:"readableLocations"`
	WriteRegions                 []accountRegion       `json:"writableLocations"`
	EnableMultipleWriteLocations bool                  `json:"enableMultipleWriteLocations"`
	AccountConsistency           userConsistencyPolicy `json:"userConsistencyPolicy"`
}

func (accountProps accountProperties) String() string {
	return fmt.Sprintf("Read regions: %v\nWrite regions: %v\nMulti-region writes: %v\nAccount consistency level: %v",
		accountProps.ReadRegions, accountProps.WriteRegions, accountProps.EnableMultipleWriteLocations, accountProps.AccountConsistency.DefaultConsistencyLevel)
}

type locationCache struct {
	locationInfo                      databaseAccountLocationsInfo
	defaultEndpoint                   url.URL
	enableCrossRegionRetries          bool
	locationUnavailabilityInfoMap     map[url.URL]locationUnavailabilityInfo
	mapMutex                          sync.RWMutex
	lastUpdateTime                    time.Time
	enableMultipleWriteLocations      bool
	unavailableLocationExpirationTime time.Duration
}

func newLocationCache(prefLocations []string, defaultEndpoint url.URL, enableCrossRegionRetries bool) *locationCache {
	return &locationCache{
		defaultEndpoint:                   defaultEndpoint,
		locationInfo:                      *newDatabaseAccountLocationsInfo(prefLocations, defaultEndpoint),
		locationUnavailabilityInfoMap:     make(map[url.URL]locationUnavailabilityInfo),
		unavailableLocationExpirationTime: defaultExpirationTime,
		enableCrossRegionRetries:          enableCrossRegionRetries,
	}
}

func (lc *locationCache) update(writeLocations []accountRegion, readLocations []accountRegion, prefList []string, enableMultipleWriteLocations *bool) error {
	nextLoc := copyDatabaseAccountLocationsInfo(lc.locationInfo)
	if prefList != nil {
		nextLoc.prefLocations = prefList
	}
	if enableMultipleWriteLocations != nil {
		lc.enableMultipleWriteLocations = *enableMultipleWriteLocations
	}
	lc.refreshStaleEndpoints()
	if readLocations != nil {
		availReadEndpointsByLocation, availReadLocations, err := getEndpointsByLocation(readLocations)
		if err != nil {
			return err
		}
		nextLoc.availReadEndpointsByLocation = availReadEndpointsByLocation
		nextLoc.availReadLocations = availReadLocations
	}

	if writeLocations != nil {
		availWriteEndpointsByLocation, availWriteLocations, err := getEndpointsByLocation(writeLocations)
		if err != nil {
			return err
		}
		nextLoc.availWriteEndpointsByLocation = availWriteEndpointsByLocation
		nextLoc.availWriteLocations = availWriteLocations
	}

	nextLoc.writeEndpoints = lc.getPrefAvailableEndpoints(nextLoc.availWriteEndpointsByLocation, nextLoc.availWriteLocations, write, lc.defaultEndpoint)
	nextLoc.readEndpoints = lc.getPrefAvailableEndpoints(nextLoc.availReadEndpointsByLocation, nextLoc.availReadLocations, read, nextLoc.writeEndpoints[0])
	lc.lastUpdateTime = time.Now()
	lc.locationInfo = nextLoc
	// TODO: log
	return nil
}

// resolveServiceEndpoint picks the endpoint to route a single attempt to.
//
// Behavior matches the .NET SDK's LocationCache.ResolveServiceEndpoint:
//   - The bypass branch (single-master document writes, metadata writes, or
//     any operation routed to the write region) flips between the first two
//     write regions and DOES NOT consult excludeRegions. This mirrors the
//     .NET SDK exactly.
//   - The applicable-endpoints branch (reads, and document writes on
//     multi-write accounts) iterates the preferred-region list, looks up the
//     raw endpoint-by-location map, skips any region in excludeRegions, and
//     indexes into the resulting filtered list. If every preferred region is
//     excluded, falls back to the default endpoint.
//
// Filtering uses the raw availability map keyed by region name (matching
// .NET) rather than the cached readEndpoints/writeEndpoints lists, so the
// applicable list reflects the user's stated preference order without the
// SDK's unavailability reordering. When excludeRegions is empty the function
// uses the cached fast path, so the common case is unchanged.
func (lc *locationCache) resolveServiceEndpoint(locationIndex int, resourceType resourceType, isWriteOperation, useWriteEndpoint bool, excludeRegions []string) url.URL {
	if (isWriteOperation || useWriteEndpoint) && !lc.canUseMultipleWriteLocsToRoute(resourceType) {
		if lc.enableCrossRegionRetries && len(lc.locationInfo.availWriteLocations) > 0 {
			locationIndex = min(locationIndex%2, len(lc.locationInfo.availWriteLocations)-1)
			writeLocation := lc.locationInfo.availWriteLocations[locationIndex]
			return lc.locationInfo.availWriteEndpointsByLocation[writeLocation]
		}
		return lc.defaultEndpoint
	}

	if len(excludeRegions) > 0 {
		return lc.getApplicableEndpoint(locationIndex, isWriteOperation, excludeRegions)
	}

	endpoints := lc.locationInfo.readEndpoints
	if isWriteOperation {
		endpoints = lc.locationInfo.writeEndpoints
	}
	return endpoints[locationIndex%len(endpoints)]
}

// getApplicableEndpoint returns the endpoint to route to for a request that
// supplied an excludeRegions filter. It mirrors the .NET SDK's
// LocationCache.GetApplicableEndpoints by iterating the preferred-region list,
// skipping excluded regions, and indexing the filtered list. If the caller
// excludes every preferred region the default endpoint is used as a fallback,
// matching .NET semantics.
func (lc *locationCache) getApplicableEndpoint(locationIndex int, isWriteOperation bool, excludeRegions []string) url.URL {
	excludeSet := make(map[string]struct{}, len(excludeRegions))
	for _, r := range excludeRegions {
		excludeSet[r] = struct{}{}
	}

	regionByEndpoint := lc.locationInfo.availReadEndpointsByLocation
	if isWriteOperation {
		regionByEndpoint = lc.locationInfo.availWriteEndpointsByLocation
	}

	applicable := make([]url.URL, 0, len(lc.locationInfo.prefLocations))
	for _, region := range lc.locationInfo.prefLocations {
		if _, excluded := excludeSet[region]; excluded {
			continue
		}
		if endpoint, ok := regionByEndpoint[region]; ok {
			applicable = append(applicable, endpoint)
		}
	}

	if len(applicable) == 0 {
		return lc.defaultEndpoint
	}
	return applicable[locationIndex%len(applicable)]
}

func (lc *locationCache) canUseMultipleWriteLocsToRoute(resourceType resourceType) bool {
	return lc.canUseMultipleWriteLocs() && resourceType == resourceTypeDocument
}

func (lc *locationCache) readEndpoints() ([]url.URL, error) {
	lc.mapMutex.RLock()
	defer lc.mapMutex.RUnlock()
	if time.Since(lc.lastUpdateTime) > lc.unavailableLocationExpirationTime && len(lc.locationUnavailabilityInfoMap) > 0 {
		err := lc.update(nil, nil, nil, nil)
		if err != nil {
			return nil, err
		}
	}
	return lc.locationInfo.readEndpoints, nil
}

func (lc *locationCache) writeEndpoints() ([]url.URL, error) {
	lc.mapMutex.RLock()
	defer lc.mapMutex.RUnlock()
	if time.Since(lc.lastUpdateTime) > lc.unavailableLocationExpirationTime && len(lc.locationUnavailabilityInfoMap) > 0 {
		err := lc.update(nil, nil, nil, nil)
		if err != nil {
			return nil, err
		}
	}
	return lc.locationInfo.writeEndpoints, nil
}

func (lc *locationCache) getLocation(endpoint url.URL) string {
	firstLoc := ""
	for location, uri := range lc.locationInfo.availWriteEndpointsByLocation {
		if uri == endpoint {
			return location
		}
		if firstLoc == "" {
			firstLoc = location
		}
	}

	for location, uri := range lc.locationInfo.availReadEndpointsByLocation {
		if uri == endpoint {
			return location
		}
	}

	if endpoint == lc.defaultEndpoint && !lc.canUseMultipleWriteLocs() {
		if len(lc.locationInfo.availWriteEndpointsByLocation) > 0 {
			return firstLoc
		}
	}
	return ""
}

func (lc *locationCache) canUseMultipleWriteLocs() bool {
	return lc.enableMultipleWriteLocations
}

func (lc *locationCache) markEndpointUnavailableForRead(endpoint url.URL) error {
	return lc.markEndpointUnavailable(endpoint, read)
}

func (lc *locationCache) markEndpointUnavailableForWrite(endpoint url.URL) error {
	return lc.markEndpointUnavailable(endpoint, write)
}

func (lc *locationCache) markEndpointUnavailable(endpoint url.URL, op requestedOperations) error {
	now := time.Now()
	lc.mapMutex.Lock()
	if info, ok := lc.locationUnavailabilityInfoMap[endpoint]; ok {
		info.lastCheckTime = now
		info.unavailableOps |= op
		lc.locationUnavailabilityInfoMap[endpoint] = info
	} else {
		info = locationUnavailabilityInfo{
			lastCheckTime:  now,
			unavailableOps: op,
		}
		lc.locationUnavailabilityInfoMap[endpoint] = info
	}
	lc.mapMutex.Unlock()
	err := lc.update(nil, nil, nil, nil)
	return err
}

func (lc *locationCache) databaseAccountRead(dbAcct accountProperties) error {
	return lc.update(dbAcct.WriteRegions, dbAcct.ReadRegions, nil, &dbAcct.EnableMultipleWriteLocations)
}

func (lc *locationCache) refreshStaleEndpoints() {
	lc.mapMutex.Lock()
	defer lc.mapMutex.Unlock()
	for endpoint, info := range lc.locationUnavailabilityInfoMap {
		t := time.Since(info.lastCheckTime)
		if t > lc.unavailableLocationExpirationTime {
			delete(lc.locationUnavailabilityInfoMap, endpoint)
		}
	}
}

func (lc *locationCache) isEndpointUnavailable(endpoint url.URL, ops requestedOperations) bool {
	lc.mapMutex.RLock()
	info, ok := lc.locationUnavailabilityInfoMap[endpoint]
	lc.mapMutex.RUnlock()
	if ops == none || !ok || ops&info.unavailableOps != ops {
		return false
	}
	return time.Since(info.lastCheckTime) < lc.unavailableLocationExpirationTime
}

func (lc *locationCache) getPrefAvailableEndpoints(endpointsByLoc map[string]url.URL, locs []string, availOps requestedOperations, fallbackEndpoint url.URL) []url.URL {
	endpoints := make([]url.URL, 0)
	if lc.enableCrossRegionRetries {
		if lc.canUseMultipleWriteLocs() || availOps&read != 0 {
			unavailEndpoints := make([]url.URL, 0)
			unavailEndpoints = append(unavailEndpoints, fallbackEndpoint)
			for _, loc := range lc.locationInfo.prefLocations {
				if endpoint, ok := endpointsByLoc[loc]; ok && endpoint != fallbackEndpoint {
					if lc.isEndpointUnavailable(endpoint, availOps) {
						unavailEndpoints = append(unavailEndpoints, endpoint)
					} else {
						endpoints = append(endpoints, endpoint)
					}
				}
			}
			endpoints = append(endpoints, unavailEndpoints...)
		} else {
			for _, loc := range locs {
				if endpoint, ok := endpointsByLoc[loc]; ok && loc != "" {
					endpoints = append(endpoints, endpoint)
				}
			}
		}
	}
	if len(endpoints) == 0 {
		endpoints = append(endpoints, fallbackEndpoint)
	}
	return endpoints
}

func getEndpointsByLocation(locs []accountRegion) (map[string]url.URL, []string, error) {
	endpointsByLoc := make(map[string]url.URL)
	parsedLocs := make([]string, 0)
	for _, loc := range locs {
		endpoint, err := url.Parse(loc.Endpoint)
		if err != nil {
			return nil, nil, err
		}
		if loc.Name != "" {
			endpointsByLoc[loc.Name] = *endpoint
			parsedLocs = append(parsedLocs, loc.Name)
		}
		// TODO else: log
	}
	return endpointsByLoc, parsedLocs, nil
}

func newDatabaseAccountLocationsInfo(prefLocations []string, defaultEndpoint url.URL) *databaseAccountLocationsInfo {
	availWriteLocs := make([]string, 0)
	availReadLocs := make([]string, 0)
	availWriteEndpointsByLocation := make(map[string]url.URL)
	availReadEndpointsByLocation := make(map[string]url.URL)
	writeEndpoints := []url.URL{defaultEndpoint}
	readEndpoints := []url.URL{defaultEndpoint}
	return &databaseAccountLocationsInfo{
		prefLocations:                 prefLocations,
		availWriteLocations:           availWriteLocs,
		availReadLocations:            availReadLocs,
		availWriteEndpointsByLocation: availWriteEndpointsByLocation,
		availReadEndpointsByLocation:  availReadEndpointsByLocation,
		writeEndpoints:                writeEndpoints,
		readEndpoints:                 readEndpoints,
	}
}

func copyDatabaseAccountLocationsInfo(other databaseAccountLocationsInfo) databaseAccountLocationsInfo {
	return databaseAccountLocationsInfo{
		prefLocations:                 other.prefLocations,
		availWriteLocations:           other.availWriteLocations,
		availReadLocations:            other.availReadLocations,
		availWriteEndpointsByLocation: other.availWriteEndpointsByLocation,
		availReadEndpointsByLocation:  other.availReadEndpointsByLocation,
		writeEndpoints:                other.writeEndpoints,
		readEndpoints:                 other.readEndpoints,
	}
}
