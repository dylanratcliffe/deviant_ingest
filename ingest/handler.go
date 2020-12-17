package ingest

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/dylanratcliffe/sdp/go/sdp"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

// Ingestor is capable of ingesting items into the database
type Ingestor struct {
	BatchSize     int           // The number of items to batch before inserting
	MaxWait       time.Duration // Max amount of time to wait before inserting
	Dgraph        *dgo.Dgraph   // The DGraph connection to use
	DebugChannel  chan UpsertResult
	IngestRetries int

	itemChannel chan ItemInsertion
}

// UpsertResult Represents the result of handling an upsert
type UpsertResult struct {
	Request *api.Request
	Respose *api.Response
	Error   error
}

// TODO: Re-Add debugging

// UpsertBatch Upserts a set of items into the database
func (i *Ingestor) UpsertBatch(batch []ItemNode, debugChannel chan UpsertResult) (*api.Response, error) {
	var req *api.Request
	var err error
	var ctx context.Context
	var cancel context.CancelFunc
	var timeout time.Duration
	var queries []string
	var mutations []*api.Mutation
	var upsertTimeout string

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
	if i.itemChannel == nil {
		i.itemChannel = make(chan ItemInsertion)
	}

	var item *sdp.Item
	var itemNode ItemNode
	var err error

	// TODO: Add timing of these operations and log the timing

	item = &sdp.Item{}

	err = proto.Unmarshal(msg.Data, item)

	if err != nil {
		if i.DebugChannel != nil {
			i.DebugChannel <- UpsertResult{
				Error: err,
			}
		}
		return
	}

	// Convert to a local representation so that we can extract the database
	// queries from it
	itemNode = ItemNode{
		Type:            item.GetType(),
		UniqueAttribute: item.GetUniqueAttribute(),
		Context:         item.GetContext(),
		LinkedItems:     item.GetLinkedItems(),
		item:            item,
	}

	itemNode.Metadata = MetadataNode{
		BackendName:            item.GetMetadata().GetBackendName(),
		RequestMethod:          item.GetMetadata().GetRequestMethod().String(),
		Timestamp:              item.GetMetadata().GetTimestamp().AsTime(),
		BackendPackage:         item.GetMetadata().GetBackendPackage(),
		BackendDuration:        item.GetMetadata().GetBackendDuration().AsDuration(),
		BackendDurationPerItem: item.GetMetadata().GetBackendDurationPerItem().AsDuration(),
		itemNode:               &itemNode,
	}

	itemNode.Attributes = AttributesNode{
		Map:            item.GetAttributes().GetAttrStruct().AsMap(),
		ParentItemNode: &itemNode,
	}

	upsertRetries := viper.GetInt("dgraph.upsertRetries")

	i.itemChannel <- ItemInsertion{
		Item: itemNode,
		TTL:  upsertRetries,
	}

	log.WithFields(log.Fields{
		"GloballyUniqueName": item.GloballyUniqueName(),
	}).Debug("Queued item")
}

// ProcessBatches will start inserting items into the database in batches.
// This will block forever
func (i *Ingestor) ProcessBatches(ctx context.Context) {
	if i.itemChannel == nil {
		i.itemChannel = make(chan ItemInsertion)
	}

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

	response, err := i.UpsertBatch(items, i.DebugChannel)

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
					"itemGloballyUniqueName": in.Item.item.GloballyUniqueName(),
					"attributes":             in.Item.Attributes,
				}).Error("Item exceeded maximum retires, it has been dropped")

				if i.DebugChannel != nil {
					i.DebugChannel <- UpsertResult{
						Respose: response,
						Error:   err,
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

		if i.DebugChannel != nil {
			i.DebugChannel <- UpsertResult{
				Respose: response,
			}
		}
	}
}
