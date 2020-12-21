package ingest

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/dylanratcliffe/sdp/go/sdp"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// NewNATSConnection connects to a given NATS URL, it also support retries. Servers should be supplied as a slice of URLs e.g.
//
// link.NewNATSConnection([]string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223"},	5, 5)
//
func NewNATSConnection(urls []string, retries int, sleep int, timeout int) *nats.Conn {
	var tries int
	var servers string
	var hostname string
	var timeoutDuration time.Duration
	var sleepDuration time.Duration

	// Set default values
	if sleep == 0 {
		sleepDuration = (5 * time.Second)
	} else {
		sleepDuration = (time.Duration(sleep) * time.Second)
	}

	if timeout == 0 {
		timeoutDuration = 10 * time.Second
	} else {
		timeoutDuration = time.Duration(timeout) * time.Second
	}

	// Get the hostname to use as the connection name
	hostname, _ = os.Hostname()

	servers = strings.Join(urls, ",")

	// Loop until we have a connection
	for tries <= retries {
		log.WithFields(log.Fields{
			"servers": servers,
		}).Info("Connecting to NATS")

		// TODO: Make these options more configurable
		// https://docs.nats.io/developing-with-nats/connecting/pingpong
		nc, err := nats.Connect(
			servers,                       // The servers to connect to
			nats.Name(hostname),           // The connection name
			nats.Timeout(timeoutDuration), // Connection timeout (per server)
		)

		if err == nil {
			log.WithFields(log.Fields{
				"Addr":     nc.ConnectedAddr(),
				"ServerID": nc.ConnectedServerId(),
				"URL":      nc.ConnectedUrl(),
			}).Info("Connected to server")

			return nc
		}

		// if retries is 0 then we loop forever
		if retries > 0 {
			// Increment tries
			tries++
		}

		log.WithFields(log.Fields{
			"servers": servers,
			"err":     err,
		}).Info("Connection failed")

		time.Sleep(sleepDuration)
	}

	panic("Could not connect to NATS, giving up")
}

// MessageToItem Converts a NATS message to an SDP Item
func MessageToItem(msg *nats.Msg) (*sdp.Item, error) {
	var item *sdp.Item
	var err error

	item = &sdp.Item{}

	err = proto.Unmarshal(msg.Data, item)

	// I have noticed that sometimes you will unmarshal an empty linkeditem.
	// Here we will check for them and remove if required. It will also
	// deduplicate the items
	if len(item.GetLinkedItems()) > 0 {
		var newLinkedItems []*sdp.Reference
		var newLinkedItemsMap map[string]*sdp.Reference

		newLinkedItemsMap = make(map[string]*sdp.Reference)

		// Check that the reference is valid
		for _, li := range item.GetLinkedItems() {
			if li.GetContext() == "" {
				continue
			}
			if li.GetType() == "" {
				continue
			}
			if li.GetUniqueAttributeValue() == "" {
				continue
			}
			newLinkedItemsMap[li.GloballyUniqueName()] = li
		}

		// Not convert back to a map
		for _, li := range newLinkedItemsMap {
			newLinkedItems = append(newLinkedItems, li)
		}

		item.LinkedItems = newLinkedItems
	}

	return item, err
}

// MessageToItemNode Converts a NATS message to a DGraph ItemNode
func MessageToItemNode(msg *nats.Msg) (ItemNode, error) {
	var item *sdp.Item
	var itemNode ItemNode
	var attributesJSON []byte
	var err error

	item, err = MessageToItem(msg)

	if err != nil {
		return itemNode, err
	}

	// Convert to a local representation so that we can extract the database
	// queries from it
	itemNode = ItemNode{
		Type:                 item.GetType(),
		UniqueAttribute:      item.GetUniqueAttribute(),
		Context:              item.GetContext(),
		LinkedItems:          item.GetLinkedItems(),
		UniqueAttributeValue: item.UniqueAttributeValue(),
		GloballyUniqueName:   item.GloballyUniqueName(),
	}

	itemNode.Metadata = item.GetMetadata()

	attributesJSON, err = item.GetAttributes().GetAttrStruct().MarshalJSON()

	compactedBuffer := new(bytes.Buffer)
	err = json.Compact(compactedBuffer, []byte(attributesJSON))

	if err == nil {
		itemNode.Attributes = string(compactedBuffer.Bytes())
	}

	return itemNode, err
}
