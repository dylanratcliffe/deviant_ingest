package ingest

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
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
		(1000 * time.Millisecond),
	)

	if err != nil {
		t.Skip(err)
	}

	// Create ingestor
	ir := Ingestor{
		BatchSize:    100,
		MaxWait:      (300 * time.Millisecond),
		Dgraph:       d,
		DebugChannel: make(chan UpsertResult, 10000),
	}

	messages, err := LoadTestMessages()

	if err != nil {
		t.Skip(err)
	}

	// Make sure the schema is set up
	SetupSchemas(d)

	// Start handler
	// batchContext, cancelProcessing := context.WithCancel(context.Background())
	// go ir.StartBatchProcessing(batchContext)

	// Register a cleanup function to drop all
	t.Cleanup(func() {
		// cancelProcessing()

		d.Alter(context.Background(), &api.Operation{
			DropAll: true,
		})
	})

	t.Run("Handling items asynchronously", func(t *testing.T) {
		for _, message := range messages {
			// At the moment handlers are async. This means that all handles
			// should return vary quickly even if there is actually a
			// significant queue of stuff to insert into the database. I'm
			// wondering for the sake of testing how I would tell that
			// operations were complete... Maybe I should work out some way of
			// making the handler blocking...
			go ir.AsyncHandle(message)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), (120 * time.Second))

	go ir.ProcessBatches(ctx)
	defer cancel()

	t.Run("Upsert results", func(t *testing.T) {
		for result := range ir.DebugChannel {
			if result.Error != nil {
				t.Log(result.Respose)
				t.Fatal(result.Error)
			}
		}
	})
}

// LoadTestMessages Loads a bunch of test messages from the `testdata` folder.
// These were created using the `save` command on test systems
func LoadTestMessages() ([]*nats.Msg, error) {
	var messages []*nats.Msg
	var content []byte
	var files []os.FileInfo
	var err error

	// Get all files in the testdata directory
	files, err = ioutil.ReadDir("../testdata")

	if err != nil {
		return messages, err
	}

	for _, file := range files {
		if file.IsDir() == false {
			content, err = ioutil.ReadFile(filepath.Join("../testdata", file.Name()))

			if err != nil {
				return messages, err
			}

			messages = append(messages, &nats.Msg{
				Subject: "pugs",
				Reply:   "please",
				Data:    content,
				Sub: &nats.Subscription{
					Subject: "pugs",
					Queue:   "stampede",
				},
			})
		}
	}

	return messages, nil
}
