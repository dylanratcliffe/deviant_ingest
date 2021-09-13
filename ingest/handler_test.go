package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/dylanratcliffe/sdp-go"
	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var couchAttributes, _ = sdp.ToAttributes(map[string]interface{}{
	"type":         "couch",
	"colour":       "black",
	"serialNumber": "98273492834-7",
})

var couch = sdp.Item{
	Context:         "home",
	Attributes:      couchAttributes,
	Type:            "furniture",
	UniqueAttribute: "serialNumber",
	Metadata: &sdp.Metadata{
		BackendName:            "test",
		BackendDuration:        durationpb.New(time.Millisecond),
		BackendDurationPerItem: durationpb.New(time.Millisecond),
		BackendPackage:         "test-package",
		Timestamp:              timestamppb.Now(),
		SourceRequest: &sdp.ItemRequest{
			Type:            "furniture",
			Method:          sdp.RequestMethod_GET,
			Query:           "98273492834-7",
			LinkDepth:       12,
			Context:         "testContext",
			ItemSubject:     "items",
			ResponseSubject: "responses",
		},
	},
	LinkedItems: []*sdp.Reference{
		{
			Type:                 "furniture",
			UniqueAttributeValue: "CTB-54",
			Context:              "house1",
		},
	},
}

var coffeeTableAttributes, _ = sdp.ToAttributes(map[string]interface{}{
	"type":         "coffee_table",
	"colour":       "wood",
	"serialNumber": "CTB-54",
})

var coffeeTable = sdp.Item{
	Context:         "home",
	Attributes:      coffeeTableAttributes,
	Type:            "furniture",
	UniqueAttribute: "serialNumber",
	Metadata: &sdp.Metadata{
		BackendName:            "test",
		BackendDuration:        durationpb.New(time.Millisecond),
		BackendDurationPerItem: durationpb.New(time.Millisecond),
		BackendPackage:         "test-package",
		Timestamp:              timestamppb.Now(),
		SourceRequest: &sdp.ItemRequest{
			Type:            "furniture",
			Method:          sdp.RequestMethod_GET,
			Query:           "CTB-54",
			LinkDepth:       11,
			Context:         "testContext",
			ItemSubject:     "items",
			ResponseSubject: "responses",
		},
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
	InitConfig("")

	// Connect to local DGraph
	d, err = NewDGraphClient(
		viper.GetString("dgraph.host"),
		viper.GetInt("dgraph.port"),
		viper.GetDuration("dgraph.connectTimeout"),
	)

	if err != nil {
		t.Fatal(err)
	}

	// Create ingestor
	ir := Ingestor{
		BatchSize:    250,
		MaxWait:      (1000 * time.Millisecond),
		Dgraph:       d,
		DebugChannel: make(chan UpsertResult, 10000),
	}

	ctx, cancel := context.WithTimeout(context.Background(), (5 * time.Minute))

	go ir.ProcessBatches(ctx)
	defer cancel()

	messages, err := LoadTestMessages(-1)

	if err != nil {
		t.Fatal(err)
	}

	// Make sure the schema is set up
	SetupSchemas(d)

	t.Run("With an empty database", func(t *testing.T) {
		t.Run("Cleaning database", func(t *testing.T) {
			d.Alter(context.Background(), &api.Operation{
				DropOp: api.Operation_DATA,
			})
		})

		RunInsertionTests(
			t,
			messages,
			&ir,
		)
	})

	t.Run("With a populated database", func(t *testing.T) {
		RunInsertionTests(
			t,
			messages,
			&ir,
		)
	})

	// Register a cleanup function to drop all
	t.Cleanup(func() {
		d.Alter(context.Background(), &api.Operation{
			DropAll: true,
		})
	})
}

func TestParallelIngestion(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	var d *dgo.Dgraph
	var err error

	// Load default values
	InitConfig("")

	// Connect to local DGraph
	d, err = NewDGraphClient(
		viper.GetString("dgraph.host"),
		viper.GetInt("dgraph.port"),
		viper.GetDuration("dgraph.connectTimeout"),
	)

	if err != nil {
		t.Fatal(err)
	}

	messages, err := LoadTestMessages(-1)

	if err != nil {
		t.Fatal(err)
	}

	// Create shared debug channel
	debugChannel := make(chan UpsertResult, 10000)

	// Create ingestors
	numIngestors := 4
	irs := make([]*Ingestor, numIngestors)
	ctx, cancel := context.WithTimeout(context.Background(), (5 * time.Minute))
	defer cancel()

	for i := 0; i < numIngestors; i++ {
		// Create ingestor
		irs[i] = &Ingestor{
			BatchSize:    10,
			MaxWait:      (100 * time.Millisecond),
			Dgraph:       d,
			DebugChannel: debugChannel,
		}

		// Start batch processing
		go irs[i].ProcessBatches(ctx)
	}

	// Ensure database is clean
	d.Alter(context.Background(), &api.Operation{
		DropOp: api.Operation_DATA,
	})

	// Generate random data
	randData := make([]int, len(messages))
	for i := range randData {
		randData[i] = rand.Intn(numIngestors - 1)
	}

	// Make sure the schema is set up
	SetupSchemas(d)

	// Register a cleanup function to drop all
	t.Cleanup(func() {
		d.Alter(context.Background(), &api.Operation{
			DropAll: true,
		})
	})

	t.Run("Loading messages into ingestors at random", func(t *testing.T) {
		go func() {
			for i, msg := range messages {
				var randInt int
				var targetIngestor *Ingestor

				// Read in a randomly generated int
				randInt = randData[i]

				// Decide which ingestor will be targeted
				targetIngestor = irs[randInt]

				targetIngestor.AsyncHandle(msg)
			}
		}()
	})

	t.Run("Verify upsert results", func(t *testing.T) {
		// Read the expected number of items from the debug channel and fail if
		// there are any error reported
		for i := 0; i < len(messages); i++ {
			result := <-debugChannel

			if result.Error != nil {
				requestJSON, _ := json.Marshal(result.Request)

				t.Log("UPSERT FAILURE")
				t.Logf("Context: %v", result.Context)
				t.Logf("Type: %v", result.Type)
				t.Logf("UniqueAttributeValue: %v", result.UniqueAttributeValue)
				t.Logf("Attributes: %v", result.Attributes)
				t.Logf("Request: %v", string(requestJSON))
				t.Logf("Error: %v", result.Error)
				t.Fatal(result.Error)
			}
		}

		t.Logf("Successfully handled %v messages", len(messages))
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
		t.Logf("Successfully verified %v messages", len(messages))
	})
}

// TestUpdatedLinkedItems Makes sure that if an item get new linked items this
// is set
func TestUpdatedLinkedItems(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	var d *dgo.Dgraph
	var err error

	// Load default values
	InitConfig("")

	// Connect to local DGraph
	d, err = NewDGraphClient(
		viper.GetString("dgraph.host"),
		viper.GetInt("dgraph.port"),
		viper.GetDuration("dgraph.connectTimeout"),
	)

	if err != nil {
		t.Fatal(err)
	}

	debugChan := make(chan UpsertResult, 10)

	ir := Ingestor{
		BatchSize:    2,
		MaxWait:      (10 * time.Millisecond),
		Dgraph:       d,
		DebugChannel: debugChan,
	}

	// Make sure the schema is set up
	SetupSchemas(d)

	t.Run("With an empty database", func(t *testing.T) {
		t.Run("Cleaning database", func(t *testing.T) {
			d.Alter(context.Background(), &api.Operation{
				DropOp: api.Operation_DATA,
			})
		})

		t.Run("Testing LinkedItem Update", func(t *testing.T) {
			var noLinkCouch *sdp.Item
			var linkCouch *sdp.Item

			noLinkCouch = &sdp.Item{}
			linkCouch = &sdp.Item{}
			cc := &couch

			// Insert the couch with no linked items
			cc.Copy(noLinkCouch)
			noLinkCouch.LinkedItems = make([]*sdp.Reference, 0)
			in, _ := ItemToItemNode(noLinkCouch)

			ir.RetryUpsert([]ItemInsertion{
				{
					Item: in,
					TTL:  1,
				},
			})

			// Insert the same item but with links
			cc.Copy(linkCouch)
			in, _ = ItemToItemNode(linkCouch)

			ir.RetryUpsert([]ItemInsertion{
				{
					Item: in,
					TTL:  1,
				},
			})

			// Wait for the items to be processed
			<-debugChan
			<-debugChan

			// Check to see what the deal is
			databaseNode, err := QueryItem(ir.Dgraph, in.GloballyUniqueName)

			if err != nil {
				t.Fatal(err)
			}

			// Check that this item was found in the database
			err = ItemMatchy(databaseNode, in)

			if err != nil {
				t.Error(err)
			}
		})
	})
	// Register a cleanup function to drop all
	t.Cleanup(func() {
		d.Alter(context.Background(), &api.Operation{
			DropAll: true,
		})
	})
}

// TestNewUpsertHandlerDgraph Runs an acceptance test against a real dgraph
// instance locally
func TestRandomInsertion(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	var d *dgo.Dgraph
	var err error

	// Load default values
	InitConfig("")

	// Connect to local DGraph
	d, err = NewDGraphClient(
		viper.GetString("dgraph.host"),
		viper.GetInt("dgraph.port"),
		viper.GetDuration("dgraph.connectTimeout"),
	)

	if err != nil {
		t.Fatal(err)
	}

	// Create ingestor
	ir := Ingestor{
		BatchSize:    250,
		MaxWait:      (1000 * time.Millisecond),
		Dgraph:       d,
		DebugChannel: make(chan UpsertResult, 10000),
	}

	ctx, cancel := context.WithTimeout(context.Background(), (5 * time.Minute))

	go ir.ProcessBatches(ctx)
	defer cancel()

	messages, err := LoadTestMessages(100)

	if err != nil {
		t.Fatal(err)
	}

	// Make sure the schema is set up
	SetupSchemas(d)

	t.Run("Insert a random 100 messages", func(t *testing.T) {
		t.Run("Cleaning database", func(t *testing.T) {
			d.Alter(context.Background(), &api.Operation{
				DropOp: api.Operation_DATA,
			})
		})

		RunInsertionTests(
			t,
			messages,
			&ir,
		)
	})

	messages, err = LoadTestMessages(-1)

	if err != nil {
		t.Fatal(err)
	}

	t.Run("With a semi populated database", func(t *testing.T) {
		RunInsertionTests(
			t,
			messages,
			&ir,
		)
	})

	// Register a cleanup function to drop all
	t.Cleanup(func() {
		d.Alter(context.Background(), &api.Operation{
			DropAll: true,
		})
	})
}

// RunInsertionTests Runs insertion tests on a set of messages. This involves
// passing the messages to the handler, waiting for handling to complete and
// ensuring that there were no errors, then querying the database to ensure that
// all messages were stored correctly
func RunInsertionTests(t *testing.T, messages []*nats.Msg, ir *Ingestor) {
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

	t.Run("Verify upsert results", func(t *testing.T) {
		// Read the expected number of items from the debug channel and fail if
		// there are any error reported
		for i := 0; i < len(messages); i++ {
			result := <-ir.DebugChannel

			if result.Error != nil {
				requestJSON, _ := json.Marshal(result.Request)

				t.Log("UPSERT FAILURE")
				t.Logf("Context: %v", result.Context)
				t.Logf("Type: %v", result.Type)
				t.Logf("UniqueAttributeValue: %v", result.UniqueAttributeValue)
				t.Logf("Attributes: %v", result.Attributes)
				t.Logf("Request: %v", string(requestJSON))
				t.Logf("Error: %v", result.Error)
				t.Fatal(result.Error)
			}
		}

		t.Logf("Successfully handled %v messages", len(messages))
	})

	t.Run("Verify database contents", func(t *testing.T) {
		// Loop over all the messages and make sure that they are in the database
		for _, message := range messages {
			// Extract the itemNode
			in, err := MessageToItemNode(message)

			if err != nil {
				t.Fatal(err)
			}

			databaseNode, err := QueryItem(ir.Dgraph, in.GloballyUniqueName)

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
		t.Logf("Successfully verified %v messages", len(messages))
	})
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
//
// If num is negative then all messages will be loaded, otherwise it will load a
// random sample of the given number
func LoadTestMessages(num int) ([]*nats.Msg, error) {
	var messages []*nats.Msg
	var content []byte
	var files []os.FileInfo
	var err error
	var loadedContent [][]byte

	// Get all files in the testdata directory
	files, err = ioutil.ReadDir("../testdata")

	if err != nil {
		return messages, err
	}

	if num < 0 {
		for _, file := range files {
			if file.IsDir() == false {
				content, err = ioutil.ReadFile(filepath.Join("../testdata", file.Name()))

				if err != nil {
					return messages, err
				}

				loadedContent = append(loadedContent, content)
			}
		}
	} else {
		for i := 0; i < num; i++ {
			randomIndex := rand.Intn(len(files))
			file := files[randomIndex]

			if file.IsDir() == false {
				content, err = ioutil.ReadFile(filepath.Join("../testdata", file.Name()))

				if err != nil {
					return messages, err
				}

				loadedContent = append(loadedContent, content)
			}

		}
	}

	for _, content := range loadedContent {
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
			Name: "Metadata.SourceRequest.Type",
			X:    x.Metadata.GetSourceRequest().GetType(),
			Y:    x.Metadata.GetSourceRequest().GetType(),
		},
		{
			Name: "Metadata.SourceRequest.Method",
			X:    x.Metadata.GetSourceRequest().GetMethod(),
			Y:    x.Metadata.GetSourceRequest().GetMethod(),
		},
		{
			Name: "Metadata.SourceRequest.Query",
			X:    x.Metadata.GetSourceRequest().GetQuery(),
			Y:    x.Metadata.GetSourceRequest().GetQuery(),
		},
		{
			Name: "Metadata.SourceRequest.LinkDepth",
			X:    x.Metadata.GetSourceRequest().GetLinkDepth(),
			Y:    x.Metadata.GetSourceRequest().GetLinkDepth(),
		},
		{
			Name: "Metadata.SourceRequest.Context",
			X:    x.Metadata.GetSourceRequest().GetContext(),
			Y:    x.Metadata.GetSourceRequest().GetContext(),
		},
		{
			Name: "Metadata.SourceRequest.ItemSubject",
			X:    x.Metadata.GetSourceRequest().GetItemSubject(),
			Y:    x.Metadata.GetSourceRequest().GetItemSubject(),
		},
		{
			Name: "Metadata.SourceRequest.ResponseSubject",
			X:    x.Metadata.GetSourceRequest().GetResponseSubject(),
			Y:    x.Metadata.GetSourceRequest().GetResponseSubject(),
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

	if len(x.LinkedItems) == len(y.LinkedItems) {
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
	} else {
		return fmt.Errorf("%v did not have a matching number of linked items: %v != %v", x.GloballyUniqueName, len(x.LinkedItems), len(y.LinkedItems))
	}

	for _, c := range comparisons {
		if c.X != c.Y {
			return fmt.Errorf("%v did not match: %v != %v", c.Name, c.X, c.Y)
		}
	}

	return nil
}
