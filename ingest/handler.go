package ingest

import (
	"context"
	"time"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/dylanratcliffe/sdp/go/sdp"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

// UpsertResult Represents the result of handling an upsert
type UpsertResult struct {
	Respose  *api.Response
	Mutation *api.Mutation
	Error    error
}

// NewUpsertHandler CReates a NATS message handler that upserts items into the given database
func NewUpsertHandler(dgraph *dgo.Dgraph, debugChannel chan UpsertResult) func(*nats.Msg) {
	return func(msg *nats.Msg) {
		var item *sdp.Item
		var itemNode ItemNode
		var req *api.Request
		var mu *api.Mutation
		var res *api.Response
		var err error
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
			debugChannel <- UpsertResult{
				Error: err,
			}
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

		req = &api.Request{
			CommitNow: true,
		}

		req.Query = itemNode.Query()
		req.Mutations = itemNode.Mutations()

		// Execute the upsert request
		res, err = dgraph.NewTxn().Do(ctx, req)

		if err != nil {
			errFields["err"] = err

			bytes, _ := req.Marshal()
			errFields["request"] = string(bytes)

			log.WithFields(errFields).Error("Error during database upsert")

			debugChannel <- UpsertResult{
				Respose:  res,
				Mutation: mu,
				Error:    err,
			}

			return
		}

		log.WithFields(log.Fields{
			"latency": res.GetLatency(),
			"metrics": res.GetMetrics(),
		}).Debug("Upsert complete")

		debugChannel <- UpsertResult{
			Respose:  res,
			Mutation: mu,
			Error:    nil,
		}
	}
}
