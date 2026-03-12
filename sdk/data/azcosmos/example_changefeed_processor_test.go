// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

// ExampleContainerClient_NewChangeFeedProcessor demonstrates the basic usage of
// the ChangeFeedProcessor to continuously process changes from a container.
func ExampleContainerClient_NewChangeFeedProcessor() {
	endpoint, ok := os.LookupEnv("AZURE_COSMOS_ENDPOINT")
	if !ok {
		panic("AZURE_COSMOS_ENDPOINT could not be found")
	}

	key, ok := os.LookupEnv("AZURE_COSMOS_KEY")
	if !ok {
		panic("AZURE_COSMOS_KEY could not be found")
	}

	cred, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		panic(err)
	}

	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	if err != nil {
		panic(err)
	}

	// The container we want to monitor for changes.
	monitoredContainer, err := client.NewContainer("myDatabase", "myContainer")
	if err != nil {
		panic(err)
	}

	// A separate container for lease coordination.
	// Partition key must be /id or /partitionKey.
	leaseContainer, err := client.NewContainer("myDatabase", "myLeases")
	if err != nil {
		panic(err)
	}

	// Handler receives batches of changed documents as raw JSON.
	handler := func(ctx context.Context, changes [][]byte) error {
		for _, change := range changes {
			var doc map[string]interface{}
			if err := json.Unmarshal(change, &doc); err != nil {
				return err
			}
			fmt.Printf("Changed document: %s\n", doc["id"])
		}
		return nil
	}

	processor, err := monitoredContainer.NewChangeFeedProcessor(
		"myProcessor",
		leaseContainer,
		handler,
		nil, // default options
	)
	if err != nil {
		panic(err)
	}

	// Start processing in the background.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go func() {
		if err := processor.Start(ctx); err != nil {
			fmt.Printf("Processor stopped: %v\n", err)
		}
	}()

	// Wait for interrupt signal, then shut down gracefully.
	<-ctx.Done()
	if err := processor.Stop(); err != nil {
		fmt.Printf("Error stopping processor: %v\n", err)
	}
}

// ExampleContainerClient_NewChangeFeedProcessor_withOptions shows how to
// configure the processor with custom timing and start position.
func ExampleContainerClient_NewChangeFeedProcessor_withOptions() {
	endpoint, ok := os.LookupEnv("AZURE_COSMOS_ENDPOINT")
	if !ok {
		panic("AZURE_COSMOS_ENDPOINT could not be found")
	}

	key, ok := os.LookupEnv("AZURE_COSMOS_KEY")
	if !ok {
		panic("AZURE_COSMOS_KEY could not be found")
	}

	cred, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		panic(err)
	}

	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	if err != nil {
		panic(err)
	}

	monitoredContainer, err := client.NewContainer("myDatabase", "myContainer")
	if err != nil {
		panic(err)
	}

	leaseContainer, err := client.NewContainer("myDatabase", "myLeases")
	if err != nil {
		panic(err)
	}

	handler := func(ctx context.Context, changes [][]byte) error {
		fmt.Printf("Received %d changes\n", len(changes))
		return nil
	}

	// Start from 90 days ago, poll every 2 seconds, 200 items per page.
	startTime := time.Now().Add(-90 * 24 * time.Hour)
	processor, err := monitoredContainer.NewChangeFeedProcessor(
		"myProcessor",
		leaseContainer,
		handler,
		&azcosmos.ChangeFeedProcessorOptions{
			MaxItemCount:   200,
			PollInterval:   2 * time.Second,
			StartTime:      &startTime,
			StartFromBeginning: false,
		},
	)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := processor.Start(ctx); err != nil {
		fmt.Printf("Processor stopped: %v\n", err)
	}
}

// ExampleContainerClient_NewChangeFeedProcessor_fullFidelity demonstrates
// using AllVersionsAndDeletes mode to receive delete notifications and
// all intermediate document versions.
func ExampleContainerClient_NewChangeFeedProcessor_fullFidelity() {
	endpoint, ok := os.LookupEnv("AZURE_COSMOS_ENDPOINT")
	if !ok {
		panic("AZURE_COSMOS_ENDPOINT could not be found")
	}

	key, ok := os.LookupEnv("AZURE_COSMOS_KEY")
	if !ok {
		panic("AZURE_COSMOS_KEY could not be found")
	}

	cred, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		panic(err)
	}

	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	if err != nil {
		panic(err)
	}

	// The monitored container must have a ChangeFeedPolicy with a retention
	// window configured for full fidelity mode to work.
	monitoredContainer, err := client.NewContainer("myDatabase", "myContainer")
	if err != nil {
		panic(err)
	}

	leaseContainer, err := client.NewContainer("myDatabase", "myLeases")
	if err != nil {
		panic(err)
	}

	// In full fidelity mode, each raw document is a ChangeFeedItem with
	// current/previous states and metadata about the operation.
	handler := func(ctx context.Context, changes [][]byte) error {
		for _, raw := range changes {
			var item azcosmos.ChangeFeedItem
			if err := json.Unmarshal(raw, &item); err != nil {
				return err
			}

			switch item.Metadata.OperationType {
			case azcosmos.ChangeFeedOperationTypeCreate:
				fmt.Printf("Created: %s\n", string(item.Current))
			case azcosmos.ChangeFeedOperationTypeReplace:
				fmt.Printf("Updated: %s (was: %s)\n", string(item.Current), string(item.Previous))
			case azcosmos.ChangeFeedOperationTypeDelete:
				if item.Metadata.IsTimeToLiveExpired {
					fmt.Printf("TTL expired: %s\n", string(item.Previous))
				} else {
					fmt.Printf("Deleted: %s\n", string(item.Previous))
				}
			}
		}
		return nil
	}

	processor, err := monitoredContainer.NewChangeFeedProcessor(
		"myFullFidelityProcessor",
		leaseContainer,
		handler,
		&azcosmos.ChangeFeedProcessorOptions{
			Mode: azcosmos.ChangeFeedModeAllVersionsAndDeletes,
		},
	)
	if err != nil {
		panic(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go func() {
		if err := processor.Start(ctx); err != nil {
			fmt.Printf("Processor stopped: %v\n", err)
		}
	}()

	<-ctx.Done()
	_ = processor.Stop()
}

// ExampleContainerClient_NewChangeFeedProcessor_multipleInstances shows how
// multiple processor instances coordinate via the shared lease container.
// Each instance gets a unique name and the load balancer distributes
// partitions automatically.
func ExampleContainerClient_NewChangeFeedProcessor_multipleInstances() {
	endpoint, ok := os.LookupEnv("AZURE_COSMOS_ENDPOINT")
	if !ok {
		panic("AZURE_COSMOS_ENDPOINT could not be found")
	}

	key, ok := os.LookupEnv("AZURE_COSMOS_KEY")
	if !ok {
		panic("AZURE_COSMOS_KEY could not be found")
	}

	cred, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		panic(err)
	}

	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	if err != nil {
		panic(err)
	}

	monitoredContainer, err := client.NewContainer("myDatabase", "orders")
	if err != nil {
		panic(err)
	}

	// All instances share the same lease container and processor name.
	// The load balancer distributes partitions evenly across them.
	leaseContainer, err := client.NewContainer("myDatabase", "order-processor-leases")
	if err != nil {
		panic(err)
	}

	handler := func(ctx context.Context, changes [][]byte) error {
		for _, change := range changes {
			// Process each order change — dedup is your responsibility
			// if exactly-once semantics matter.
			fmt.Printf("Processing order change: %d bytes\n", len(change))
		}
		return nil
	}

	// Every replica uses the same processor name ("orderProcessor").
	// Each gets a unique instance name automatically (hostname + UUID).
	// Deploy N replicas and they'll split the partitions among themselves.
	processor, err := monitoredContainer.NewChangeFeedProcessor(
		"orderProcessor",
		leaseContainer,
		handler,
		&azcosmos.ChangeFeedProcessorOptions{
			PollInterval:   1 * time.Second,
			MaxItemCount:   500,
			StartFromBeginning: true,
		},
	)
	if err != nil {
		panic(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go processor.Start(ctx)
	<-ctx.Done()
	_ = processor.Stop()
}

// ExampleContainerClient_NewChangeFeedProcessor_healthMonitor shows how to
// use the health monitor for observability into the processor's lease lifecycle.
func ExampleContainerClient_NewChangeFeedProcessor_healthMonitor() {
	endpoint, ok := os.LookupEnv("AZURE_COSMOS_ENDPOINT")
	if !ok {
		panic("AZURE_COSMOS_ENDPOINT could not be found")
	}

	key, ok := os.LookupEnv("AZURE_COSMOS_KEY")
	if !ok {
		panic("AZURE_COSMOS_KEY could not be found")
	}

	cred, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		panic(err)
	}

	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	if err != nil {
		panic(err)
	}

	monitoredContainer, err := client.NewContainer("myDatabase", "myContainer")
	if err != nil {
		panic(err)
	}

	leaseContainer, err := client.NewContainer("myDatabase", "myLeases")
	if err != nil {
		panic(err)
	}

	handler := func(ctx context.Context, changes [][]byte) error {
		fmt.Printf("Processing %d changes\n", len(changes))
		return nil
	}

	// Wire up observability — hook into lease lifecycle and errors
	// without the processor writing to stdout/stderr on its own.
	monitor := &azcosmos.ChangeFeedProcessorHealthMonitor{
		OnLeaseAcquired: func(ctx context.Context, leaseID string) {
			log.Printf("Acquired lease %s", leaseID)
		},
		OnLeaseReleased: func(ctx context.Context, leaseID string) {
			log.Printf("Released lease %s", leaseID)
		},
		OnError: func(ctx context.Context, leaseID string, err error) {
			log.Printf("Error on lease %s: %v", leaseID, err)
		},
	}

	processor, err := monitoredContainer.NewChangeFeedProcessor(
		"myProcessor",
		leaseContainer,
		handler,
		&azcosmos.ChangeFeedProcessorOptions{
			HealthMonitor: monitor,
		},
	)
	if err != nil {
		panic(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go processor.Start(ctx)
	<-ctx.Done()
	_ = processor.Stop()
}

// ExampleContainerClient_GetChangeFeed_pullModel shows the low-level pull model
// for reading the change feed manually without the processor. Useful for
// one-off reads or when you want full control over iteration.
func ExampleContainerClient_GetChangeFeed_pullModel() {
	endpoint, ok := os.LookupEnv("AZURE_COSMOS_ENDPOINT")
	if !ok {
		panic("AZURE_COSMOS_ENDPOINT could not be found")
	}

	key, ok := os.LookupEnv("AZURE_COSMOS_KEY")
	if !ok {
		panic("AZURE_COSMOS_KEY could not be found")
	}

	cred, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		panic(err)
	}

	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	if err != nil {
		panic(err)
	}

	container, err := client.NewContainer("myDatabase", "myContainer")
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	// Get all feed ranges (partition key ranges) for the container.
	feedRanges, err := container.GetFeedRanges(ctx)
	if err != nil {
		panic(err)
	}

	// Read the change feed for each range.
	for _, fr := range feedRanges {
		feedRange := fr
		resp, err := container.GetChangeFeed(ctx, &azcosmos.ChangeFeedOptions{
			FeedRange:    &feedRange,
			MaxItemCount: 10,
		})
		if err != nil {
			fmt.Printf("Error reading feed range: %v\n", err)
			continue
		}

		fmt.Printf("Feed range [%s, %s): %d documents\n",
			feedRange.MinInclusive, feedRange.MaxExclusive, resp.Count)

		// Save resp.ContinuationToken to resume later.
		if resp.ContinuationToken != "" {
			fmt.Printf("Continuation: %s\n", resp.ContinuationToken[:20]+"...")
		}
	}
}

// This example shows how to use the ChangeFeedEstimator to monitor processing lag.
func ExampleChangeFeedEstimator() {
	// Create clients (see ExampleChangeFeedProcessor for full setup)
	endpoint := "https://example.documents.azure.com:443/"
	cred, err := azcosmos.NewKeyCredential("accountKey")
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}

	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}

	monitoredContainer, err := client.NewContainer("mydb", "monitored")
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}

	leaseContainer, err := client.NewContainer("mydb", "leases")
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}

	// Create an estimator to monitor a processor group
	estimator, err := azcosmos.NewChangeFeedEstimator(
		monitoredContainer,
		leaseContainer,
		&azcosmos.ChangeFeedEstimatorOptions{
			PollInterval: 10 * time.Second,
			LeasePrefix:  "myProcessorGroup",
		},
	)
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}

	// Option 1: Pull model — get lag on demand
	ctx := context.TODO()
	estimations, err := estimator.GetEstimatedLag(ctx)
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}
	for _, est := range estimations {
		fmt.Printf("Partition %s (owner: %s): ~%d items behind\n",
			est.LeaseToken, est.Owner, est.EstimatedLag)
	}

	// Option 2: Push model — periodic monitoring
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Minute)
	defer cancel()

	go estimator.Start(ctx, func(ctx context.Context, estimations []azcosmos.ChangeFeedEstimation) {
		totalLag := 0
		for _, est := range estimations {
			totalLag += est.EstimatedLag
		}
		fmt.Printf("Total estimated lag: %d items\n", totalLag)
	})

	// Stop when done
	estimator.Stop()
}
