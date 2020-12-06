package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dylanratcliffe/sdp/go/sdp"
	log "github.com/sirupsen/logrus"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
)

// DGraph Stores details about the DGraph connection
type DGraph struct {
	Conn *dgo.Dgraph
}

// InsertAllHandler Nats item handler that inserts items into the database
func (d *DGraph) InsertAllHandler(msg *nats.Msg) {

}

// NewDGraphClient Create a dgraph client connection
func NewDGraphClient(hostname string, port int, connectTimeout time.Duration) (*dgo.Dgraph, error) {
	var dc *dgo.Dgraph

	address := fmt.Sprintf("%v:%v", hostname, port)

	log.WithFields(log.Fields{
		"address": address,
	}).Info("Connecting to DGraph")

	// Dial a gRPC connection. The address to dial to can be configured when
	// setting up the dgraph cluster.
	dialOpts := append([]grpc.DialOption{},
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)),
		grpc.WithBlock(),
		grpc.WithTimeout(connectTimeout),
	)
	d, err := grpc.Dial(address, dialOpts...)

	if err != nil {
		return dc, err
	}

	log.WithFields(log.Fields{
		"state": d.GetState().String(),
	}).Info("Connected to DGraph")

	return dgo.NewDgraphClient(
		api.NewDgraphClient(d),
	), nil
}

// Schema Stores the overall schema. I'm sure this is a bad way to do thinks
// from a future compatability perspective...
var Schema = `
type Item {
	Type
	UniqueAttribute
	UniqueAttributeValue
	GloballyUniqueName
	Attributes
	Metadata
}

Type: string @index(hash) .
UniqueAttribute: string @index(hash) .
UniqueAttributeValue: string @index(hash) .
GloballyUniqueName: string @index(hash) .
Attributes: uid .
Metadata: uid .
LinkedItems: [uid] @reverse .

type Metadata {
	BackendName
	RequestMethod
	Timestamp
	BackendDuration
	BackendDurationPerItem
	BackendPackage
}

BackendName: string @index(hash) .
RequestMethod: string .
Timestamp: dateTime @index(hour) .
BackendDuration: int .
BackendDurationPerItem: int .
BackendPackage: string @index(hash) .
`

// SetupSchemas Will create the schemas required for ingest to work. This will
// need to be run before anything can actually be inserted into the database
func SetupSchemas(dg *dgo.Dgraph) error {
	var operation *api.Operation

	operation = &api.Operation{
		Schema: Schema,
	}

	err := dg.Alter(context.Background(), operation)

	return err
}

// ItemNode Represents an item, it also is able to return a full list of
// muatations
type ItemNode struct {
	Type            string       `json:"Type,omitempty"`
	UniqueAttribute string       `json:"UniqueAttribute,omitempty"`
	Context         string       `json:"Context,omitempty"`
	Metadata        MetadataNode `json:"Metadata,omitempty"`
	Attrributes     Attributes   `json:"-"`
	// LinkedItems     []LinkedItem `json:"-"`
	item *sdp.Item `json:"-"`
}

// Mutations Returns a list of mutations that can be
func (i *ItemNode) Mutations() []*api.Mutation {
	itemJSON, _ := json.Marshal(i)
	attributesJSON, _ := json.Marshal(i.Attrributes)
	metadataJSON, _ := json.Marshal(i.Metadata)

	return []*api.Mutation{
		{
			Cond:    "@if(eq(len(item.item), 0))",
			SetJson: itemJSON,
		},
		{
			SetJson: attributesJSON,
		},
		{
			SetJson: metadataJSON,
		},
	}
}

// MarshalJSON Custom marshalling functionality that adds derived fields
// required for DGraph
func (i ItemNode) MarshalJSON() ([]byte, error) {
	type Alias ItemNode
	return json.Marshal(&struct {
		UID                  string `json:"uid"`
		DType                string `json:"dgraph.type,omitempty"`
		UniqueAttributeValue string `json:"UniqueAttributeValue,omitempty"`
		GloballyUniqueName   string `json:"GloballyUniqueName,omitempty"`
		Attributes           string `json:"Attributes,omptempty"`
		Metadata             string `json:"Metadata,omptempty"`
		// LinkedItems          string `json:"LinkedItems"`
		Alias
	}{
		// TODO: Add handling of linked items
		UID:                  "uid(item.item)",
		Attributes:           "uid(item.attributes)",
		Metadata:             "uid(item.metadata)",
		DType:                "Item",
		UniqueAttributeValue: i.item.UniqueAttributeValue(),
		GloballyUniqueName:   i.item.GloballyUniqueName(),
		Alias:                (Alias)(i),
	})
}

// UnmarshalJSON Converts from JSON to ItemNode
func (i *ItemNode) UnmarshalJSON(value []byte) error {
	return json.Unmarshal(value, i)
}

// Query returns a query that should match specifically this item. It will also
// export the following variables:
//
//   * `{GloballyUniqueName}.item`: UID of this item
//   * `{GloballyUniqueName}.attributes`: UID of this item's attributes
//   * `{GloballyUniqueName}.metadata`: UID of this item's metadata
func (i *ItemNode) Query() string {
	var lines []string
	var gun string

	gun = i.item.GloballyUniqueName()

	lines = make([]string, 7)
	lines[0] = "{"
	lines[1] = fmt.Sprintf("  item(func: eq(GloballyUniqueName, \"%v\")) {", gun)
	lines[2] = "    item.item as uid"
	lines[3] = "    item.attributes as Attributes"
	lines[4] = "    item.metadata as Metadata"
	lines[5] = "  }"
	lines[6] = "}"

	return strings.Join(lines, "\n")
}

// MetadataNode Represents metadata as serialised for DGraph
type MetadataNode struct {
	BackendName            string        `json:"BackendName,omitempty"`
	RequestMethod          string        `json:"RequestMethod,omitempty"`
	Timestamp              time.Time     `json:"Timestamp,omitempty"`
	BackendDuration        time.Duration `json:"BackendDuration,omitempty"`
	BackendDurationPerItem time.Duration `json:"BackendDurationPerItem,omitempty"`
	BackendPackage         string        `json:"BackendPackage,omitempty"`
	item                   *sdp.Item
}

// MarshalJSON Custom marshalling functionality that adds derived fields
// required for DGraph
func (i MetadataNode) MarshalJSON() ([]byte, error) {
	type Alias MetadataNode
	return json.Marshal(&struct {
		UID   string `json:"uid"`
		DType string `json:"dgraph.type,omitempty"`
		Alias
	}{
		UID:   "uid(item.metadata)",
		DType: "itemMetadata",
		Alias: (Alias)(i),
	})
}

// UnmarshalJSON Converts from JSON to MetadataNode
func (i *MetadataNode) UnmarshalJSON(value []byte) error {
	return json.Unmarshal(value, i)
}

// TODO: Custom marshalling for MetadataNode: uid and dgraph.type

// Attributes Represents the attributes of a single item
type Attributes struct {
	Map  map[string]interface{}
	item *sdp.Item
}

// MarshalJSON Custom marshalling functionality that adds derived fields
// required for DGraph
func (a Attributes) MarshalJSON() ([]byte, error) {
	// We want to store the attributes in the format {type}.{attribute}. This is
	// so that we don't have a clash of predicates. We wan this becuase we will
	// also be doing dynamic type generation and I think it needs to be unique.
	// though this might be worth looking into...

	// Re-map the map
	newMap := make(map[string]interface{})

	for k, v := range a.Map {
		newMap[fmt.Sprintf("%v.%v", a.item.GetType(), k)] = v
	}

	// Append the dgraph type and UID
	newMap["dgraph.type"] = fmt.Sprintf("%vAttributes", a.item.GetType())
	newMap["uid"] = "uid(item.attributes)"

	return json.Marshal(newMap)
}

// UnmarshalJSON Converts from JSON to Attributes
func (a *Attributes) UnmarshalJSON(value []byte) error {
	dotMap := make(map[string]interface{})
	a.Map = make(map[string]interface{})

	err := json.Unmarshal(value, &dotMap)

	if err != nil {
		return err
	}

	// Remove the {type}.{attribute} formatting
	for k, v := range dotMap {
		// Split on each dot
		sections := strings.Split(k, ".")

		// Select the last element
		a.Map[sections[len(sections)-1]] = v
	}

	return nil
}

// func ToUpsert(items []*sdp.Item) []map[string]interface{} {

// }
