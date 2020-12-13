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
	BatchSize    int           // The number of items to batch before inserting
	MaxWait      time.Duration // Max amount of time to wait before inserting
	Dgraph       *dgo.Dgraph   // The DGraph connection to use
	DebugChannel chan UpsertResult

	itemNodeChannel chan ItemNode
}

// UpsertResult Represents the result of handling an upsert
type UpsertResult struct {
	Request *api.Request
	Respose *api.Response
	Error   error
}

// TODO: Re-Add debugging

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

	// TODO: Ensure that this is reading from memory so it's fast
	viper.SetDefault("dgraph.upsertTimeout", "20s")
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
	return i.Dgraph.NewTxn().Do(ctx, req)
}

// AsyncHandle Creates a NATS message handler that upserts items into the given database
func (i *Ingestor) AsyncHandle(msg *nats.Msg) {
	if i.itemNodeChannel == nil {
		i.itemNodeChannel = make(chan ItemNode)
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

	itemNode.Attrributes = AttributesNode{
		Map:      item.GetAttributes().GetAttrStruct().AsMap(),
		itemNode: &itemNode,
	}

	i.itemNodeChannel <- itemNode

	log.WithFields(log.Fields{
		"GloballyUniqueName": item.GloballyUniqueName(),
	}).Debug("Queued item")
}

// ProcessBatches will start inserting items into the databse in batches.
// This will block forever
func (i *Ingestor) ProcessBatches(ctx context.Context) {
	if i.itemNodeChannel == nil {
		i.itemNodeChannel = make(chan ItemNode)
	}

	items := make([]ItemNode, 0)
	var full bool

	for {
		if full {
			res, err := i.UpsertBatch(items)

			if i.DebugChannel != nil {
				i.DebugChannel <- UpsertResult{
					Respose: res,
					Error:   err,
				}
			}
		}
		// Wait for the following conditions and execte the first one to be met.
		// If multiple are met one will be selected at random
		//
		// * If the batch size has been met: Upsert into databse
		// * If the max time has elapsed: Upsert into database
		// * If there is an item waiting to be processed: Add it to the batch
		// * If the context was cancelled: Final upsert and return
		select {
		case <-time.After(i.MaxWait):
			if len(items) > 0 {
				res, err := i.UpsertBatch(items)

				if i.DebugChannel != nil {
					i.DebugChannel <- UpsertResult{
						Respose: res,
						Error:   err,
					}
				}
			}
		case item := <-i.itemNodeChannel:
			items = append(items, item)

			if len(items) >= i.BatchSize {
				// If we have reached the batch size then place a bool onto the full
				// channel. This will mean that next time around the upsert will be
				// executed
				full = true
			}
		case <-ctx.Done():
			res, err := i.UpsertBatch(items)

			if i.DebugChannel != nil {
				i.DebugChannel <- UpsertResult{
					Respose: res,
					Error:   err,
				}
			}
			return
		}
	}
}
