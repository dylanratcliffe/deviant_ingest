package ingest

import (
	"context"
	"encoding/json"
	"time"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/dylanratcliffe/sdp/go/sdp"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

// NewUpsertHandler CReates a NATS message handler that upserts items into the given database
func NewUpsertHandler(dgraph *dgo.Dgraph) func(*nats.Msg) {
	return func(msg *nats.Msg) {
		var item *sdp.Item
		var itemNode ItemNode
		var itemUpsert Upsert
		var txn *dgo.Txn
		var mu *api.Mutation
		var res *api.Response
		var err error
		var b []byte
		var ctx context.Context
		var cancel context.CancelFunc
		var timeout time.Duration
		var errFields log.Fields

		// TODO: Ensure that this is reading from memory so it's fast
		viper.SetDefault("databaseTimeout", "1s")
		timeout, err = time.ParseDuration(viper.GetString("databaseTimeout"))

		// Get the timeout
		ctx, cancel = context.WithTimeout(
			context.Background(),
			timeout,
		)
		defer cancel()

		// TODO: Add timing of these operations and log the timing

		item = &sdp.Item{}

		err = proto.Unmarshal(msg.Data, item)

		if err != nil {
			return
		}

		log.WithFields(log.Fields{
			"type":                  item.GetType(),
			"numLinkedItemRequests": len(item.GetLinkedItemRequests()),
		}).Debug("Processing item")

		// Convert to a local representation so that we can extract the database
		// queries from it
		itemNode = ItemNode{
			Type:            item.GetType(),
			UniqueAttribute: item.GetUniqueAttribute(),
			Context:         item.GetContext(),
			Metadata: MetadataNode{
				BackendName:            item.GetMetadata().GetBackendName(),
				RequestMethod:          item.GetMetadata().GetRequestMethod().String(),
				Timestamp:              item.GetMetadata().GetTimestamp().AsTime(),
				BackendPackage:         item.GetMetadata().GetBackendPackage(),
				BackendDuration:        item.GetMetadata().GetBackendDuration().AsDuration(),
				BackendDurationPerItem: item.GetMetadata().GetBackendDurationPerItem().AsDuration(),
				item:                   item,
			},
			Attrributes: Attributes{
				Map:  item.GetAttributes().GetAttrStruct().AsMap(),
				item: item,
			},
			item: item,
		}

		// Store logs for later
		errFields = log.Fields{
			"itemGloballyUniqueName": item.GloballyUniqueName(),
			"backendName":            itemNode.Metadata.BackendName,
			"requestMethod":          itemNode.Metadata.RequestMethod,
			"timestamp":              itemNode.Metadata.Timestamp,
			"backendPackage":         itemNode.Metadata.BackendPackage,
			"backendDuration":        itemNode.Metadata.BackendDuration,
			"backendDurationPerItem": itemNode.Metadata.BackendDurationPerItem,
			"attributes":             itemNode.Attrributes,
		}

		// Create a transaction
		txn = dgraph.NewTxn()

		// Defer cancel of the transaction. This means that if there is an
		// unhandled exception it will cleanly cancel the transaction. If the
		// transaction has already been committed then the discard won't do
		// anything
		defer txn.Discard(context.Background())

		// Create the mutation
		itemUpsert = Upsert{
			Query:     itemNode.Query(),
			Mutations: itemNode.Mutations(),
		}

		// Convert to JSON
		b, err = json.Marshal(itemUpsert)

		if err != nil {
			errFields["err"] = err
			log.WithFields(errFields).Error("Item JSON Marshal failed before database insertion")

			return
		}

		mu = &api.Mutation{
			SetJson: b,
		}

		// Execute the mutation
		res, err = txn.Mutate(ctx, mu)

		if err != nil {
			errFields["err"] = err
			log.WithFields(errFields).Error("Error during upsert of item into database")

			return
		}

		log.WithFields(log.Fields{
			"latency": res.GetLatency(),
			"metrics": res.GetMetrics(),
		}).Debug("Upsert complete")

		// Commit the transaction
		err = txn.Commit(ctx)

		if err != nil {
			errFields["err"] = err
			log.WithFields(errFields).Error("Error during database transaction commit")

			return
		}

		log.WithFields(log.Fields{
			"itemGloballyUniqueName": item.GloballyUniqueName(),
		}).Error("Commit complete")
	}
}
