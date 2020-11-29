package ingest

import (
	"github.com/dylanratcliffe/sdp/go/sdp"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// MessageHandler handles NATS messages by resolving their links then posting
// the resolved itm to the OutputSubject
func (c *Connection) MessageHandler(msg *nats.Msg) {
	item := &sdp.Item{}

	proto.Unmarshal(msg.Data, item)

	log.WithFields(log.Fields{
		"type":                  item.GetType(),
		"numLinkedItemRequests": len(item.GetLinkedItemRequests()),
	}).Debug("Processing item")

	// Send back onto the output subject
	c.NC.Publish(
		"TODO subject",
		[]byte{},
	)
}
