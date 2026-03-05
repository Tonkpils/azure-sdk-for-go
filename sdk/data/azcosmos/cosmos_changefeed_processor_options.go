// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import "time"

// ChangeFeedProcessorOptions configures a ChangeFeedProcessor.
type ChangeFeedProcessorOptions struct {
	// MaxItemCount is the maximum number of items per change feed page.
	// Default: 100
	MaxItemCount int32

	// PollInterval is the delay between polls when no changes are available.
	// Default: 5 seconds
	PollInterval time.Duration

	// LeaseExpirationInterval is how long before an unrenewed lease is considered expired.
	// Default: 60 seconds
	LeaseExpirationInterval time.Duration

	// LeaseRenewInterval is how often to renew owned leases.
	// Default: 17 seconds (matching .NET SDK default)
	LeaseRenewInterval time.Duration

	// LeaseAcquireInterval is how often to check for available leases.
	// Default: 13 seconds (matching .NET SDK default)
	LeaseAcquireInterval time.Duration

	// StartFromBeginning starts processing from the beginning of the change feed.
	// Default: false (starts from current point)
	StartFromBeginning bool

	// StartTime starts processing from a specific point in time.
	// Takes precedence over StartFromBeginning if both are set.
	StartTime *time.Time

	// LeasePrefix is prepended to lease IDs for namespace isolation.
	// Useful when multiple processors share a lease container.
	LeasePrefix string

	// Mode specifies the change feed mode.
	// Default: ChangeFeedModeLatestVersion (incremental, creates and updates only).
	// Set to ChangeFeedModeAllVersionsAndDeletes for full fidelity mode
	// (includes deletes and all intermediate versions). Requires the container
	// to have a ChangeFeedPolicy with a retention window configured.
	Mode ChangeFeedMode
}

// changeFeedProcessorDefaults returns options with sensible defaults.
func changeFeedProcessorDefaults() ChangeFeedProcessorOptions {
	return ChangeFeedProcessorOptions{
		MaxItemCount:            100,
		PollInterval:            5 * time.Second,
		LeaseExpirationInterval: 60 * time.Second,
		LeaseRenewInterval:      17 * time.Second,
		LeaseAcquireInterval:    13 * time.Second,
	}
}
