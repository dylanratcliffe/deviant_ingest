package ingest

import (
	"github.com/dgraph-io/dgo/v200"
	"github.com/dylanratcliffe/sdp/go/sdp"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// NewUpsertHandler CReates a NATS message handler that upserts items into the given database
func NewUpsertHandler(dgraph *dgo.Dgraph) func(*nats.Msg) {
	return func(msg *nats.Msg) {
		item := &sdp.Item{}

		err := proto.Unmarshal(msg.Data, item)

		if err != nil {
			return
		}

		log.WithFields(log.Fields{
			"type":                  item.GetType(),
			"numLinkedItemRequests": len(item.GetLinkedItemRequests()),
		}).Debug("Processing item")
	}
}
