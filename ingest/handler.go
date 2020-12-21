package ingest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Ingestor is capable of ingesting items into the database
type Ingestor struct {
	BatchSize     int           // The number of items to batch before inserting
	MaxWait       time.Duration // Max amount of time to wait before inserting
	Dgraph        *dgo.Dgraph   // The DGraph connection to use
	DebugChannel  chan UpsertResult
	IngestRetries int

	itemChannel chan ItemInsertion
	mutex       sync.Mutex
}

// UpsertResult Represents the result of handling an upsert
type UpsertResult struct {
	Context              string
	Type                 string
	UniqueAttributeValue string
	Attributes           string
	Request              *api.Request
	Error                error
}

// UpsertBatch Upserts a set of items into the database
func (i *Ingestor) UpsertBatch(batch []ItemNode) (*api.Response, error) {
	var req *api.Request
	var err error
	var ctx context.Context
	var cancel context.CancelFunc
	var timeout time.Duration
	var queries []string
	var mutations []*api.Mutation
	var upsertTimeout string
	var batchMap map[string]ItemNode

	// Deduplicate the batch
	batchMap = make(map[string]ItemNode)

	for _, in := range batch {
		existingItem, exists := batchMap[in.GloballyUniqueName]

		if exists {
			// Compare timestamps
			existingTime := existingItem.Metadata.GetTimestamp().AsTime()
			newTime := in.Metadata.GetTimestamp().AsTime()

			// If the item is newer then add it to the batch. If it's older then
			// just ignore it
			if newTime.After(existingTime) {
				batchMap[in.GloballyUniqueName] = in
			}
		} else {
			batchMap[in.GloballyUniqueName] = in
		}
	}

	// Replace the batch with the deduplicated batch
	batch = make([]ItemNode, 0)

	for _, v := range batchMap {
		batch = append(batch, v)
	}

	// TODO: Ensure that this is reading from memory so it's fast
	upsertTimeout = viper.GetString("dgraph.upsertTimeout")
	timeout, err = time.ParseDuration(upsertTimeout)

	if err != nil {
		return nil, fmt.Errorf("Could not parse dgraph.upsertTimeout value: %v", upsertTimeout)
	}

	// Get the timeout
	ctx, cancel = context.WithTimeout(
		context.Background(),
		timeout,
	)
	defer cancel()

	req = &api.Request{
		CommitNow: true,
	}

	// Extract the queries and mutations
	for _, itemNode := range batch {
		queries = append(queries, itemNode.Query())
		mutations = append(mutations, itemNode.Mutations()...)
	}

	// Combine Queries into a single valid string
	req.Query = "{" + strings.Join(queries, "\n") + "}"
	req.Mutations = mutations

	// Execute the upsert request
	res, err := i.Dgraph.NewTxn().Do(ctx, req)

	return res, err
}

// AsyncHandle Creates a NATS message handler that upserts items into the given database
func (i *Ingestor) AsyncHandle(msg *nats.Msg) {
	i.EnsureItemChannel()

	itemNode, err := MessageToItemNode(msg)

	if err != nil {
		if i.DebugChannel != nil {
			i.DebugChannel <- UpsertResult{
				Error: err,
			}
		}
		return
	}

	upsertRetries := viper.GetInt("dgraph.upsertRetries")

	i.itemChannel <- ItemInsertion{
		Item: itemNode,
		TTL:  upsertRetries,
	}

	log.WithFields(log.Fields{
		"GloballyUniqueName": itemNode.GloballyUniqueName,
	}).Debug("Queued item")
}

// ProcessBatches will start inserting items into the database in batches.
// This will block forever
func (i *Ingestor) ProcessBatches(ctx context.Context) {
	i.EnsureItemChannel()

	insertions := make([]ItemInsertion, 0)
	var full bool

	for {
		if full {
			// Reset the flag
			full = false

			i.RetryUpsert(insertions)

			// Empty the items variable
			insertions = make([]ItemInsertion, 0)
		}
		// Wait for the following conditions and execute the first one to be met.
		// If multiple are met one will be selected at random
		//
		// * If the batch size has been met: Upsert into database
		// * If the max time has elapsed: Upsert into database
		// * If there is an item waiting to be processed: Add it to the batch
		// * If the context was cancelled: Final upsert and return
		select {
		case <-time.After(i.MaxWait):
			if len(insertions) > 0 {
				i.RetryUpsert(insertions)
			}
		case itemInsertion := <-i.itemChannel:
			insertions = append(insertions, itemInsertion)

			if len(insertions) >= i.BatchSize {
				// If we have reached the batch size then place a bool onto the full
				// channel. This will mean that next time around the upsert will be
				// executed
				full = true
			}
		case <-ctx.Done():
			i.RetryUpsert(insertions)
			return
		}
	}
}

// RetryUpsert Will do something about retrying upserts. Maybe put the back in
// the queue using a TTL, maybe just sleep and retry...
// TODO: Decide on the retry functionality
func (i *Ingestor) RetryUpsert(insertions []ItemInsertion) {
	var items []ItemNode
	var startTime time.Time
	var upsertDuration time.Duration

	// Extract the items
	for _, ii := range insertions {
		items = append(items, ii.Item)
	}

	startTime = time.Now()

	response, err := i.UpsertBatch(items)

	upsertDuration = time.Since(startTime)

	if err != nil {
		var retry []ItemInsertion

		retry = make([]ItemInsertion, 0)

		// Check which should be retried
		for _, in := range insertions {
			if in.TTL == 0 {
				log.WithFields(log.Fields{
					"error":                  err,
					"itemType":               in.Item.Type,
					"itemGloballyUniqueName": in.Item.GloballyUniqueName,
					"attributes":             in.Item.Attributes,
				}).Error("Item exceeded maximum retires, it has been dropped")

				if i.DebugChannel != nil {
					i.DebugChannel <- UpsertResult{
						Context:              in.Item.Context,
						Type:                 in.Item.Type,
						Attributes:           in.Item.Attributes,
						UniqueAttributeValue: in.Item.UniqueAttributeValue,
						Error:                err,
					}
				}
			} else {
				in.TTL--
				retry = append(retry, in)
			}
		}

		if len(retry) > 0 {
			log.WithFields(log.Fields{
				"error":      err,
				"response":   response,
				"numRetried": len(retry),
			}).Error("Database upsert failed, retrying items")

			// Spawn a routine to add these back into the channel so we don't block
			go func(r []ItemInsertion) {
				for _, in := range r {
					i.itemChannel <- in
				}
			}(retry)
		}
	} else {
		log.WithFields(log.Fields{
			"response": response.String(),
			"numItems": len(items),
			"duration": upsertDuration.String(),
		}).Debug("Items upserted successfully")

		for _, it := range items {
			if i.DebugChannel != nil {
				i.DebugChannel <- UpsertResult{
					Context:              it.Context,
					Type:                 it.Type,
					UniqueAttributeValue: it.UniqueAttributeValue,
					Attributes:           it.Attributes,
					Error:                nil,
				}
			}
		}
	}
}

// EnsureItemChannel Ensures that the item channel exists
func (i *Ingestor) EnsureItemChannel() {
	i.mutex.Lock()
	if i.itemChannel == nil {
		i.itemChannel = make(chan ItemInsertion)
	}
	i.mutex.Unlock()

}
