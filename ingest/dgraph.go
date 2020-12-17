package ingest

import (
	"context"
	"crypto/sha1"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dylanratcliffe/sdp/go/sdp"
	log "github.com/sirupsen/logrus"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
)

// DGraph Stores details about the DGraph connection
type DGraph struct {
	Conn *dgo.Dgraph
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
	LinkedItems
}

Type: string @index(hash) .
UniqueAttribute: string @index(hash) .
UniqueAttributeValue: string @index(hash) .
GloballyUniqueName: string @index(hash) .
Attributes: string .
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

// ItemInsertion Represents an item to be inserted, it includes an item and the
// TTL. The TTL will be reduced each time it is retried
type ItemInsertion struct {
	Item ItemNode
	TTL  int
}

// ItemNode Represents an item, it also is able to return a full list of
// mutations
type ItemNode struct {
	Type            string           `json:"Type,omitempty"`
	UniqueAttribute string           `json:"UniqueAttribute,omitempty"`
	Context         string           `json:"Context,omitempty"`
	Metadata        MetadataNode     `json:"Metadata,omitempty"`
	Attributes      string           `json:"Attributes,omitempty"`
	LinkedItems     []*sdp.Reference `json:"-"`
	item            *sdp.Item        `json:"-"`
}

// Mutations Returns a list of mutations that can be
func (i *ItemNode) Mutations() []*api.Mutation {
	var mutations []*api.Mutation

	itemJSON, _ := json.Marshal(i)
	metadataJSON, _ := json.Marshal(i.Metadata)

	// Initial mutations to create the item and its attributes and metadata
	mutations = []*api.Mutation{
		{
			SetJson: itemJSON,
		},
		{
			SetJson: metadataJSON,
		},
	}

	for index, li := range i.LinkedItems {
		liJSON, err := json.Marshal(map[string]string{
			"uid":                fmt.Sprintf("uid(%v.linkedItem%v.item)", i.Hash(), index),
			"GloballyUniqueName": li.GloballyUniqueName(),
		})

		if err == nil {
			// Insert a placeholder for the linked item if it doesn't already exist.
			// This placeholder will be replaced with the actual item once it
			// arrives
			mutations = append(mutations, &api.Mutation{
				Cond:    fmt.Sprintf("@if(eq(len(%v.linkedItem%v.item), 0))", i.Hash(), index),
				SetJson: liJSON,
			})
		}
	}

	return mutations
}

// MarshalJSON Custom marshalling functionality that adds derived fields
// required for DGraph
func (i ItemNode) MarshalJSON() ([]byte, error) {
	var li []string

	// Create the linked items
	for index := range i.LinkedItems {
		// This refers to a variable that was created during the initial query
		li = append(li, fmt.Sprintf("uid(%v.linkedItem%v.item)", i.Hash(), index))
	}

	type Alias ItemNode
	return json.Marshal(&struct {
		UID                  string   `json:"uid"`
		DType                string   `json:"dgraph.type,omitempty"`
		UniqueAttributeValue string   `json:"UniqueAttributeValue,omitempty"`
		GloballyUniqueName   string   `json:"GloballyUniqueName,omitempty"`
		Metadata             string   `json:"Metadata,omitempty"`
		LinkedItems          []string `json:"LinkedItems"`
		Alias
	}{
		UID:                  fmt.Sprintf("uid(%v.item)", i.Hash()),
		Metadata:             fmt.Sprintf("uid(%v.metadata)", i.Hash()),
		DType:                "Item",
		LinkedItems:          li,
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
	var query string

	// Query for the its own UID and the UIDs of the attributes and metadata
	query = fmt.Sprintf(`
		%v(func: eq(GloballyUniqueName, "%v")) {
			%v.item as uid
			%v.metadata as Metadata
		}
	`,
		i.Hash(),
		i.item.GloballyUniqueName(),
		i.Hash(),
		i.Hash(),
	)

	// Add subsequent queries for linked items
	for index, linkedItem := range i.item.LinkedItems {
		q := fmt.Sprintf(`
			%v.linkedItem%v(func: eq(GloballyUniqueName, "%v")) {
				%v.linkedItem%v.item as uid
			}
		`, i.Hash(), index, linkedItem.GloballyUniqueName(), i.Hash(), index)

		query = query + "\n" + q
	}

	return query
}

// Hash Returns a 10 character hash for the item. This is unlikely but not
// guaranteed to be unique
func (i *ItemNode) Hash() string {
	var shaSum [20]byte
	var paddedEncoding *base32.Encoding
	var unpaddedEncoding *base32.Encoding

	shaSum = sha1.Sum([]byte(fmt.Sprint(i.item.GloballyUniqueName(), i.item.GetMetadata().GetTimestamp())))

	// We need to specify a custom encoding here since dGraph has fairly struct
	// requirements aboout what name a variable
	paddedEncoding = base32.NewEncoding("abcdefghijklmnopqrstuvwxyzABCDEF")

	// We also can't have padding since "=" is not allowed in variable names
	unpaddedEncoding = paddedEncoding.WithPadding(base32.NoPadding)

	return unpaddedEncoding.EncodeToString(shaSum[:19])
}

// MetadataNode Represents metadata as serialized for DGraph
type MetadataNode struct {
	BackendName            string        `json:"BackendName,omitempty"`
	RequestMethod          string        `json:"RequestMethod,omitempty"`
	Timestamp              time.Time     `json:"Timestamp,omitempty"`
	BackendDuration        time.Duration `json:"BackendDuration,omitempty"`
	BackendDurationPerItem time.Duration `json:"BackendDurationPerItem,omitempty"`
	BackendPackage         string        `json:"BackendPackage,omitempty"`
	itemNode               *ItemNode
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
		UID:   fmt.Sprintf("uid(%v.metadata)", i.itemNode.Hash()),
		DType: "itemMetadata",
		Alias: (Alias)(i),
	})
}

// UnmarshalJSON Converts from JSON to MetadataNode
func (i *MetadataNode) UnmarshalJSON(value []byte) error {
	return json.Unmarshal(value, i)
}
