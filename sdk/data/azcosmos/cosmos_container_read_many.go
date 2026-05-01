// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos/queryengine"
	"github.com/Azure/azure-sdk-for-go/sdk/internal/log"
)

// executeReadManyWithEngine executes a query using the provided query engine.
func (c *ContainerClient) executeReadManyWithEngine(queryEngine queryengine.QueryEngine, items []ItemIdentity, readManyOptions *ReadManyOptions, operationContext pipelineRequestOptions, ctx context.Context) (ReadManyItemsResponse, error) {
	path, _ := generatePathForNameBased(resourceTypeDocument, operationContext.resourceAddress, true)

	// get the partition key ranges for the container
	rawPartitionKeyRanges, err := c.getPartitionKeyRangesRaw(ctx, operationContext)
	if err != nil {
		// if we can't get the partition key ranges, return empty response
		return ReadManyItemsResponse{}, err
	}

	// get the container properties
	containerRsp, err := c.Read(ctx, nil)
	if err != nil {
		return ReadManyItemsResponse{}, err
	}

	// create the item identities for the query engine with json string
	newItemIdentities := make([]queryengine.ItemIdentity, len(items))
	for i := range items {
		pkStr, err := items[i].PartitionKey.toJsonString()
		if err != nil {
			return ReadManyItemsResponse{}, err
		}
		newItemIdentities[i] = queryengine.ItemIdentity{
			PartitionKeyValue: pkStr,
			ID:                items[i].ID,
		}
	}
	var pkVersion uint8
	pkDefinition := containerRsp.ContainerProperties.PartitionKeyDefinition
	if pkDefinition.Version == 0 {
		pkVersion = uint8(1)
	} else {
		pkVersion = uint8(pkDefinition.Version)
	}

	readManyPipeline, err := queryEngine.CreateReadManyPipeline(newItemIdentities, string(rawPartitionKeyRanges), string(pkDefinition.Kind), pkVersion, pkDefinition.Paths)
	if err != nil {
		return ReadManyItemsResponse{}, err
	}
	log.Writef(EventQueryEngine, "Created readMany pipeline")
	// Initial run to get any requests.
	log.Writef(EventQueryEngine, "Fetching more data from readMany pipeline")
	result, err := readManyPipeline.Run()
	if err != nil {
		readManyPipeline.Close()
		return ReadManyItemsResponse{}, err
	}

	concurrency := determineConcurrency(nil)
	if readManyOptions != nil {
		concurrency = determineConcurrency(readManyOptions.MaxConcurrency)
	}
	totalRequestCharge, err := runEngineRequests(ctx, c, path, readManyPipeline, operationContext, result.Requests, concurrency, func(req queryengine.QueryRequest) (string, []QueryParameter, bool) {
		// ReadMany pipeline requests carry a Query (optional override). No parameters and we always page until continuation exhausted.
		return req.Query, nil, true /* treat like drain for full pagination */
	})
	if err != nil {
		readManyPipeline.Close()
		return ReadManyItemsResponse{}, err
	}

	// Final run to gather merged items.
	result, err = readManyPipeline.Run()
	if err != nil {
		readManyPipeline.Close()
		return ReadManyItemsResponse{}, err
	}

	if readManyPipeline.IsComplete() {
		log.Writef(EventQueryEngine, "ReadMany pipeline is complete")
		readManyPipeline.Close()
		return ReadManyItemsResponse{
			Items:         result.Items,
			RequestCharge: totalRequestCharge,
		}, nil
	} else {
		readManyPipeline.Close()
		return ReadManyItemsResponse{}, errors.New("illegal state readMany pipeline did not complete")
	}
}

func (c *ContainerClient) executeReadManyWithPointReads(items []ItemIdentity, readManyOptions *ReadManyOptions, operationContext pipelineRequestOptions, ctx context.Context) (ReadManyItemsResponse, error) {

	// Determine concurrency: use provided MaxConcurrency or number of CPU cores
	concurrency := determineConcurrency(nil)
	if readManyOptions != nil {
		concurrency = determineConcurrency(readManyOptions.MaxConcurrency)
	}

	// Prepare result slots to preserve input order. Each worker writes to a
	// unique idx, so per-slot writes are race-free without a lock.
	type slot struct {
		value         []byte
		requestCharge float32
	}
	results := make([]slot, len(items))

	// Cancellation pattern: derive a cancellable child context. The first
	// worker to encounter an error (or panic) wins via sync.Once -- it
	// records the error and cancels, signalling all other workers to bail.
	// context.WithCancel's cancel() is itself idempotent, but coalescing
	// through Once also makes the firstErr capture atomic with cancellation.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		errOnce  sync.Once
		firstErr error
	)
	setErr := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	// Worker pool
	var wg sync.WaitGroup
	jobs := make(chan int)

	workerCount := concurrency
	if workerCount > len(items) {
		workerCount = len(items)
	}
	itemOptions := ItemOptions{}
	if readManyOptions != nil {
		itemOptions.ConsistencyLevel = readManyOptions.ConsistencyLevel
		itemOptions.SessionToken = readManyOptions.SessionToken
	}
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Convert any panic in this worker into the first error rather
			// than letting the runtime kill the process. Without this, a
			// panic in the http stack (or any user-supplied retry hook)
			// crashes whatever goroutine ReadManyItems was called from.
			defer func() {
				if r := recover(); r != nil {
					setErr(fmt.Errorf("panic in readMany worker: %v", r))
				}
			}()
			for idx := range jobs {
				if ctx.Err() != nil {
					return
				}
				item := items[idx]

				itemResponse, err := c.ReadItem(ctx, item.PartitionKey, item.ID, &itemOptions)
				if err != nil {
					var azErr *azcore.ResponseError
					// for 404, just continue without error
					if errors.As(err, &azErr) {
						if azErr.StatusCode == 404 {
							continue
						}
					}
					setErr(err)
					return
				}
				results[idx].value = itemResponse.Value
				results[idx].requestCharge = itemResponse.RequestCharge
			}
		}()
	}

	// Feeder: distribute item indices to the worker pool until cancelled.
	go func() {
		defer close(jobs)
		for i := range items {
			select {
			case <-ctx.Done():
				return
			case jobs <- i:
			}
		}
	}()

	wg.Wait()

	if firstErr != nil {
		return ReadManyItemsResponse{}, firstErr
	}

	// Build response in original order. Items that 404'd are simply absent
	// (results[i].value == nil for them) and dropped from the response.
	var readManyResponse ReadManyItemsResponse
	for i := range results {
		if results[i].value != nil {
			readManyResponse.Items = append(readManyResponse.Items, results[i].value)
			readManyResponse.RequestCharge += results[i].requestCharge
		}
	}

	return readManyResponse, nil
}
