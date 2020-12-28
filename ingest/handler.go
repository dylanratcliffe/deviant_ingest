package ingest

import (
	"context"
	"fmt"
	"regexp"
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

// RetryError A Regex that matches a healthy transaction aborted error in
// dgraph. It is part of normal operation for a transaction to be aborted and
// need to be retried due to the fact that transactions don't hold locks in
// dgraph. If we get an error matching this regex them it means there wasn't
// anything wrong with the request, it just wants us t try again later
var RetryError = regexp.MustCompile(`(?i)Please retry`)

// UpsertBatch Upserts a set of items into the database
func (i *Ingestor) UpsertBatch(batch ItemNodes) (*api.Response, error) {
	var req *api.Request
	var err error
	var ctx context.Context
	var cancel context.CancelFunc
	var timeout time.Duration
	var queries Queries
	var mutations []*api.Mutation
	var upsertTimeout string
	var allItems ItemNodes

	allItems = append(allItems, batch...)
	allItems = append(allItems, batch.LinkedItems()...)
	allItems = allItems.Deduplicate()

	// Extract queries and mutations
	for _, i := range allItems {
		queries = append(queries, i.Queries()...)
		mutations = append(mutations, i.Mutation())
	}

	// Deduplicate
	queries = queries.Deduplicate()

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

	// Combine Queries into a single valid string
	req.Query = queries.String()
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

	log.WithFields(log.Fields{
		"GloballyUniqueName": itemNode.GloballyUniqueName,
	}).Trace("Queueing item")

	i.itemChannel <- ItemInsertion{
		Item: itemNode,
		TTL:  upsertRetries,
	}
}

// ProcessBatches will start inserting items into the database in batches.
// This will block forever
func (i *Ingestor) ProcessBatches(ctx context.Context) {
	i.EnsureItemChannel()

	insertions := make([]ItemInsertion, 0)
	var full bool

	for {
		if full {
			log.WithFields(log.Fields{
				"numItems": len(insertions),
			}).Debug("Batch size reached, running upsert")

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
				log.WithFields(log.Fields{
					"numItems": len(insertions),
					"maxWait":  i.MaxWait.String(),
				}).Debug("Max wait reached, running upsert")

				i.RetryUpsert(insertions)

				// Empty the items variable
				insertions = make([]ItemInsertion, 0)
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
			log.WithFields(log.Fields{
				"numItems": len(insertions),
			}).Debug("Cancelled, running final upsert")

			i.RetryUpsert(insertions)

			// Empty the items variable
			insertions = make([]ItemInsertion, 0)

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
		var reduceTTL bool

		retry = make([]ItemInsertion, 0)

		// Reduce TTL if the error is NOT a simple retry error since these are
		// expected in normal operation
		reduceTTL = !RetryError.MatchString(err.Error())

		for _, in := range insertions {
			if reduceTTL {
				in.TTL--
			}

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
				retry = append(retry, in)
			}
		}

		if len(retry) > 0 {
			if reduceTTL {
				log.WithFields(log.Fields{
					"error":      err,
					"response":   response,
					"numRetried": len(retry),
				}).Error("Database upsert failed, retrying items with reduced TTL")
			} else {
				log.WithFields(log.Fields{
					"error":      err,
					"response":   response,
					"numRetried": len(retry),
				}).Debug("Database upsert failed with expected error, retrying items with no TTL penalty")
			}

			// Spawn a routine to add these back into the channel so we don't block
			go func(r []ItemInsertion) {
				for _, in := range r {
					i.itemChannel <- in
				}
			}(retry)
		}
	} else {
		log.WithFields(log.Fields{
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
