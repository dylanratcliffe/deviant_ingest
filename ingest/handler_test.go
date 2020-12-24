package ingest

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
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

	// Load default values
	SetConfigDefaults()

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
		BatchSize:    250,
		MaxWait:      (1000 * time.Millisecond),
		Dgraph:       d,
		DebugChannel: make(chan UpsertResult, 10000),
	}

	messages, err := LoadTestMessages()

	if err != nil {
		t.Skip(err)
	}

	// Make sure the schema is set up
	SetupSchemas(d)

	t.Run("Handling items asynchronously", func(t *testing.T) {
		go func() {
			for _, message := range messages {
				// At the moment handlers are async. This means that all handles
				// should return vary quickly even if there is actually a
				// significant queue of stuff to insert into the database. I'm
				// wondering for the sake of testing how I would tell that
				// operations were complete... Maybe I should work out some way of
				// making the handler blocking...
				ir.AsyncHandle(message)
			}
		}()
	})

	ctx, cancel := context.WithTimeout(context.Background(), (120 * time.Second))

	go ir.ProcessBatches(ctx)
	defer cancel()

	t.Run("Upsert results", func(t *testing.T) {
		for i := 0; i < len(messages); i++ {
			result := <-ir.DebugChannel

			if result.Error != nil {
				t.Log("UPSERT FAILURE")
				t.Logf("Context: %v", result.Context)
				t.Logf("Type: %v", result.Type)
				t.Logf("UniqueAttributeValue: %v", result.UniqueAttributeValue)
				t.Logf("Attributes: %v", result.Attributes)
				t.Logf("Error: %v", result.Error)
				t.Fatal(result.Error)
			}
		}
	})

	t.Run("Verify database contents", func(t *testing.T) {
		// Loop over all the messages and make sure that they are in the database
		for _, message := range messages {
			// Extract the itemNode
			in, err := MessageToItemNode(message)

			if err != nil {
				t.Fatal(err)
			}

			databaseNode, err := QueryItem(d, in.GloballyUniqueName)

			if err != nil {
				t.Fatal(err)
			}

			// Check that this item was found in the database
			err = ItemMatchy(databaseNode, in)

			if err == nil {
				continue
			}

			t.Fatal(err)
		}
	})

	// Register a cleanup function to drop all
	// t.Cleanup(func() {
	// 	d.Alter(context.Background(), &api.Operation{
	// 		DropAll: true,
	// 	})
	// })
}

// ItemMatchy Returns true of the items are the same, ot of the item we're comparing is older than the database item
func ItemMatchy(databaseItem ItemNode, otherItem ItemNode) error {
	// If the one we are checking for is an older version, then just ignore it
	if otherItem.Metadata.Timestamp.AsTime().Before(databaseItem.Metadata.Timestamp.AsTime()) {
		return nil
	}

	// Sort linked items so that comparison works
	sort.Slice(otherItem.LinkedItems, func(i, j int) bool {
		return otherItem.LinkedItems[i].GloballyUniqueName < otherItem.LinkedItems[j].GloballyUniqueName
	})

	sort.Slice(databaseItem.LinkedItems, func(i, j int) bool {
		return databaseItem.LinkedItems[i].GloballyUniqueName < databaseItem.LinkedItems[j].GloballyUniqueName
	})

	return ItemsEqual(otherItem, databaseItem)
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

// ItemsEqual This checks equality between two ItemNode Structs. This is
// required because the struct contains a mix of structs and protobuf objects
// like Timestamp. These protobuf messages contain state that should not be
// compared and is flagged as such with the DoNotCompare flag. However this is
// not respected by reflect.DeepEqual
func ItemsEqual(x, y ItemNode) error {
	type Comparison struct {
		Name string
		X    interface{}
		Y    interface{}
	}
	var comparisons []Comparison

	comparisons = []Comparison{
		{
			Name: "Type",
			X:    x.Type,
			Y:    y.Type,
		},
		{
			Name: "UniqueAttribute",
			X:    x.UniqueAttribute,
			Y:    y.UniqueAttribute,
		},
		{
			Name: "Context",
			X:    x.Context,
			Y:    y.Context,
		},
		{
			Name: "Attributes",
			X:    x.Attributes,
			Y:    y.Attributes,
		},
		{
			Name: "UniqueAttributeValue",
			X:    x.UniqueAttributeValue,
			Y:    y.UniqueAttributeValue,
		},
		{
			Name: "GloballyUniqueName",
			X:    x.GloballyUniqueName,
			Y:    y.GloballyUniqueName,
		},
		{
			Name: "Hash",
			X:    x.Hash,
			Y:    y.Hash,
		},
		{
			Name: "Metadata.BackendName",
			X:    x.Metadata.GetBackendName(),
			Y:    y.Metadata.GetBackendName(),
		},
		{
			Name: "Metadata.RequestMethod",
			X:    x.Metadata.GetRequestMethod(),
			Y:    y.Metadata.GetRequestMethod(),
		},
		{
			Name: "Metadata.Timestamp.Seconds",
			X:    x.Metadata.GetTimestamp().GetSeconds(),
			Y:    y.Metadata.GetTimestamp().GetSeconds(),
		},
		{
			Name: "Metadata.Timestamp.Nanos",
			X:    x.Metadata.GetTimestamp().GetNanos(),
			Y:    y.Metadata.GetTimestamp().GetNanos(),
		},
		{
			Name: "Metadata.BackendDuration.Seconds",
			X:    x.Metadata.GetBackendDuration().GetSeconds(),
			Y:    y.Metadata.GetBackendDuration().GetSeconds(),
		},
		{
			Name: "Metadata.BackendDuration.Nanos",
			X:    x.Metadata.GetBackendDuration().GetNanos(),
			Y:    y.Metadata.GetBackendDuration().GetNanos(),
		},
		{
			Name: "Metadata.BackendDurationPerItem.Seconds",
			X:    x.Metadata.GetBackendDurationPerItem().GetSeconds(),
			Y:    y.Metadata.GetBackendDurationPerItem().GetSeconds(),
		},
		{
			Name: "Metadata.BackendDurationPerItem.Nanos",
			X:    x.Metadata.GetBackendDurationPerItem().GetNanos(),
			Y:    y.Metadata.GetBackendDurationPerItem().GetNanos(),
		},
		{
			Name: "Metadata.BackendPackage",
			X:    x.Metadata.GetBackendPackage(),
			Y:    y.Metadata.GetBackendPackage(),
		},
		{
			Name: "len(LinkedItem)",
			X:    len(x.LinkedItems),
			Y:    len(x.LinkedItems),
		},
	}

	for i := range x.LinkedItems {
		comparisons = append(comparisons, []Comparison{
			{
				Name: fmt.Sprintf("LinkedItems.%v.Type", i),
				X:    x.LinkedItems[i].Type,
				Y:    y.LinkedItems[i].Type,
			},
			{
				Name: fmt.Sprintf("LinkedItems.%v.UniqueAttributeValue", i),
				X:    x.LinkedItems[i].UniqueAttributeValue,
				Y:    y.LinkedItems[i].UniqueAttributeValue,
			},
			{
				Name: fmt.Sprintf("LinkedItems.%v.Context", i),
				X:    x.LinkedItems[i].Context,
				Y:    y.LinkedItems[i].Context,
			},
		}...)
	}

	for _, c := range comparisons {
		if c.X != c.Y {
			return fmt.Errorf("%v did not match: %v != %v", c.Name, c.X, c.Y)
		}
	}

	return nil
}
