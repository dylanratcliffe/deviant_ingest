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

var couchAttributes, _ = sdp.ToAttributes(map[string]interface{}{
	"type":         "couch",
	"colour":       "black",
	"serialNumber": "98273492834-7",
})

var couch = &sdp.Item{
	Context:         "home",
	Attributes:      couchAttributes,
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
			UniqueAttributeValue: "CTB-54",
			Context:              "house1",
		},
	},
}

var couchData, _ = proto.Marshal(couch)

var coffeeTableAttributes, _ = sdp.ToAttributes(map[string]interface{}{
	"type":         "coffee_table",
	"colour":       "wood",
	"serialNumber": "CTB-54",
})

var coffeeTable = &sdp.Item{
	Context:         "home",
	Attributes:      coffeeTableAttributes,
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
}

var coffeeTableData, _ = proto.Marshal(coffeeTable)

var testMessages = []*nats.Msg{
	{
		Subject: "items.house1",
		Reply:   "replysubject",
		Sub: &nats.Subscription{
			Subject: "items.house1",
			Queue:   "q",
		},
		Data: couchData,
	},
	{
		Subject: "items.house1",
		Reply:   "replysubject",
		Sub: &nats.Subscription{
			Subject: "items.house1",
			Queue:   "q",
		},
		Data: coffeeTableData,
	},
	{
		Subject: "items.house1",
		Reply:   "replysubject",
		Sub: &nats.Subscription{
			Subject: "items.house1",
			Queue:   "q",
		},
		Data: couchData,
	},
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

	for _, message := range testMessages {
		t.Run("Handling an item", func(t *testing.T) {
			var debugChannel chan UpsertResult
			var handle nats.MsgHandler
			var result UpsertResult

			debugChannel = make(chan UpsertResult)
			handle = NewUpsertHandler(d, debugChannel)

			go handle(message)

			// Read the result from the debug channel
			result = <-debugChannel

			if result.Error != nil {
				if result.Request != nil {
					t.Log(string(result.Request.String()))
				}

				if result.Respose != nil {
					t.Log(string(result.Respose.GetJson()))
				}

				t.Error(result.Error)
			}

			t.Log(string(result.Request.String()))
			t.Log(string(result.Respose.Json))
		})
	}
}
