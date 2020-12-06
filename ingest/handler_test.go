package ingest

import (
	"testing"
	"time"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dylanratcliffe/sdp/go/sdp"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var goodAttributes, _ = sdp.ToAttributes(map[string]interface{}{
	"type":         "couch",
	"colour":       "black",
	"serialNumber": "982734928347",
})

var goodItem = &sdp.Item{
	Context:         "home",
	Attributes:      goodAttributes,
	Type:            "furniture",
	UniqueAttribute: "serialNumber",
	Metadata: &sdp.Metadata{
		BackendName:            "test",
		BackendDuration:        durationpb.New(time.Millisecond),
		BackendDurationPerItem: durationpb.New(time.Millisecond),
		BackendPackage:         "test-package",
		RequestMethod:          sdp.RequestMethod_FIND,
		Timestamp:              timestamppb.Now(),
	},
	LinkedItems: []*sdp.Reference{
		{
			Type:                 "furniture",
			UniqueAttributeValue: "coffee_table",
			Context:              "house1",
		},
	},
}

var goodData, _ = proto.Marshal(goodItem)

var goodMessage = &nats.Msg{
	Subject: "items.house1",
	Reply:   "replysubject",
	Sub: &nats.Subscription{
		Subject: "items.house1",
		Queue:   "q",
	},
	Data: goodData,
}

// TestNewUpsertHandlerDgraph Runs an acceptance test against a real dgraph
// instance locally
func TestNewUpsertHandlerDgraph(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	var d *dgo.Dgraph
	var err error

	// Connect to local DGraph
	d, err = NewDGraphClient(
		"localhost",
		9080,
		(300 * time.Millisecond),
	)

	if err != nil {
		t.Skip(err)
	}

	t.Run("Handling an item", func(t *testing.T) {
		var debugChannel chan UpsertResult
		var handle nats.MsgHandler
		var result UpsertResult

		debugChannel = make(chan UpsertResult)
		handle = NewUpsertHandler(d, debugChannel)

		go handle(goodMessage)

		// Read the result from the debug channel
		result = <-debugChannel

		if result.Error != nil {
			t.Error(result.Error)
		}
	})
}
