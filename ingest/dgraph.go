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
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	Context
	Type
	UniqueAttribute
	UniqueAttributeValue
	GloballyUniqueName
	Attributes
	Hash
	Metadata.BackendName
	Metadata.RequestMethod
	Metadata.Timestamp
	Metadata.BackendDuration
	Metadata.BackendDurationPerItem
	Metadata.BackendPackage
	LinkedItems
}

Context: string @index(fulltext) .
Type: string @index(exact) .
UniqueAttribute: string @index(exact) .
UniqueAttributeValue: string @index(exact) .
GloballyUniqueName: string @index(exact) .
Attributes: string .
Hash: string @index(exact) .
Metadata.BackendName: string @index(exact) .
Metadata.RequestMethod: string .
Metadata.Timestamp: dateTime @index(hour) .
Metadata.BackendDuration: int .
Metadata.BackendDurationPerItem: int .
Metadata.BackendPackage: string @index(exact) .
LinkedItems: [uid] @reverse .
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
	Type                 string        `json:"Type,omitempty"`
	UniqueAttribute      string        `json:"UniqueAttribute,omitempty"`
	Context              string        `json:"Context,omitempty"`
	Attributes           string        `json:"Attributes,omitempty"`
	UniqueAttributeValue string        `json:"UniqueAttributeValue,omitempty"`
	GloballyUniqueName   string        `json:"GloballyUniqueName,omitempty"`
	Hash                 string        `json:"Hash,omitempty"`
	Metadata             *sdp.Metadata `json:"-"`
	LinkedItems          ItemNodes     `json:"-"`
}

// IsPlaceholder Returns true if the item is just a placeholder
func (i ItemNode) IsPlaceholder() bool {
	return (i.Metadata == nil || i.Attributes == "")
}

// TODO: all of these new objects need a Queries() and Mutations() method

// ItemNodes Represents a list of ItemNodes in dgraph
type ItemNodes []ItemNode

// LinkedItems Returns the linked items as ItemNodes
func (i ItemNodes) LinkedItems() ItemNodes {
	in := make(ItemNodes, 0)

	for _, item := range i {
		for _, li := range item.LinkedItems {
			in = append(in, li)
		}
	}

	return in
}

// Deduplicate Removes duplicate items, with clashes being resolved as follows:
//
// * Newer items beat older items
// * Complete items beat items that are only references/placeholders (i.e.
//   those that  do not have attributes and metadata)
//
func (i ItemNodes) Deduplicate() ItemNodes {
	m := make(map[string]ItemNode)

	// Resolve duplicates
	for _, in := range i {
		// Check to see if the item already exists in the map
		existingItem, exists := m[in.Hash]

		if exists == false {
			// If the item does not yet exist then just add it
			m[in.Hash] = in
		} else {
			// If the item is already in the map then we need to apply
			// deduplication logic

			// If the existing item is a placeholder then we will replace it
			// regardless
			if existingItem.IsPlaceholder() {
				m[in.Hash] = in
			} else {
				// Compare timestamps
				existingTime := existingItem.Metadata.GetTimestamp().AsTime()
				newTime := in.Metadata.GetTimestamp().AsTime()

				// If the item is newer then add it to the batch. If it's older then
				// just ignore it
				if newTime.After(existingTime) {
					m[in.Hash] = in
				}
			}
		}
	}

	// Convert back into a slice
	newI := make(ItemNodes, 0)

	for _, in := range m {
		newI = append(newI, in)
	}

	return newI
}

// Queries Is a list of dgraph queries
type Queries []Query

func (q Queries) String() string {
	var queryStrings []string

	for _, qi := range q {
		queryStrings = append(queryStrings, qi.String())
	}

	return "{" + strings.Join(queryStrings, "\n") + "}"
}

// Deduplicate Removes duplicate queries
func (q Queries) Deduplicate() Queries {
	queriesMap := make(map[string]Query)

	for _, qi := range q {
		queriesMap[qi.VariableName] = qi
	}

	newQ := make(Queries, 0)

	for _, qi := range queriesMap {
		newQ = append(newQ, qi)
	}

	return newQ
}

// Query Represents a dgraph query
type Query struct {
	VariableName string
	QueryFunc    string
}

func (q Query) String() string {
	var s string

	if q.VariableName != "" {
		s = fmt.Sprintf(`%v as `, q.VariableName)
	}

	s = s + q.QueryFunc

	return s
}

// MarshalJSON Custom marshalling functionality that adds derived fields
// required for DGraph
func (i ItemNode) MarshalJSON() ([]byte, error) {
	var li []string
	var uid string

	// Create the linked items
	for _, item := range i.LinkedItems {
		// This refers to a variable that was created during the initial query
		li = append(li, fmt.Sprintf("uid(%v.item)", item.Hash))
	}

	uid = fmt.Sprintf("uid(%v.item)", i.Hash)

	type Alias ItemNode
	return json.Marshal(&struct {
		UID                            string        `json:"uid"`
		DType                          string        `json:"dgraph.type,omitempty"`
		LinkedItems                    []string      `json:"LinkedItems"`
		MetadataBackendName            string        `json:"Metadata.BackendName,omitempty"`
		MetadataRequestMethod          string        `json:"Metadata.RequestMethod,omitempty"`
		MetadataTimestamp              string        `json:"Metadata.Timestamp,omitempty"`
		MetadataBackendDuration        time.Duration `json:"Metadata.BackendDuration,omitempty"`
		MetadataBackendDurationPerItem time.Duration `json:"Metadata.BackendDurationPerItem,omitempty"`
		MetadataBackendPackage         string        `json:"Metadata.BackendPackage,omitempty"`
		Alias
	}{
		UID:                            uid,
		DType:                          "Item",
		LinkedItems:                    li,
		MetadataBackendName:            i.Metadata.GetBackendName(),
		MetadataRequestMethod:          i.Metadata.GetRequestMethod().String(),
		MetadataTimestamp:              i.Metadata.GetTimestamp().AsTime().Format(time.RFC3339Nano),
		MetadataBackendDuration:        i.Metadata.GetBackendDuration().AsDuration(),
		MetadataBackendDurationPerItem: i.Metadata.GetBackendDurationPerItem().AsDuration(),
		MetadataBackendPackage:         i.Metadata.GetBackendPackage(),
		Alias:                          (Alias)(i),
	})
}

// UnmarshalJSON Converts from JSON to ItemNode
func (i *ItemNode) UnmarshalJSON(value []byte) error {
	var s struct {
		Attributes         string `json:"Attributes,omitempty"`
		Context            string `json:"Context,omitempty"`
		GloballyUniqueName string `json:"GloballyUniqueName,omitempty"`
		Hash               string `json:"Hash,omitempty"`
		LinkedItems        []struct {
			Context              string `json:"Context,omitempty"`
			Type                 string `json:"Type,omitempty"`
			UniqueAttributeValue string `json:"UniqueAttributeValue,omitempty"`
		} `json:"LinkedItems,omitempty"`
		MetadataBackendName            string        `json:"Metadata.BackendName,omitempty"`
		MetadataRequestMethod          string        `json:"Metadata.RequestMethod,omitempty"`
		MetadataTimestamp              time.Time     `json:"Metadata.Timestamp,omitempty"`
		MetadataBackendDuration        time.Duration `json:"Metadata.BackendDuration,omitempty"`
		MetadataBackendDurationPerItem time.Duration `json:"Metadata.BackendDurationPerItem,omitempty"`
		MetadataBackendPackage         string        `json:"Metadata.BackendPackage,omitempty"`
		Type                           string        `json:"Type,omitempty"`
		UniqueAttribute                string        `json:"UniqueAttribute,omitempty"`
		UniqueAttributeValue           string        `json:"UniqueAttributeValue,omitempty"`
	}

	err := json.Unmarshal(value, &s)

	if err != nil {
		return err
	}

	i.Attributes = s.Attributes
	i.Context = s.Context
	i.GloballyUniqueName = s.GloballyUniqueName
	i.Metadata = &sdp.Metadata{
		BackendName:            s.MetadataBackendName,
		RequestMethod:          sdp.RequestMethod(sdp.RequestMethod_value[s.MetadataRequestMethod]),
		Timestamp:              timestamppb.New(s.MetadataTimestamp),
		BackendDuration:        durationpb.New(s.MetadataBackendDuration),
		BackendDurationPerItem: durationpb.New(s.MetadataBackendDurationPerItem),
		BackendPackage:         s.MetadataBackendPackage,
	}

	i.Type = s.Type
	i.UniqueAttribute = s.UniqueAttribute
	i.UniqueAttributeValue = s.UniqueAttributeValue
	i.Hash = s.Hash

	for _, l := range s.LinkedItems {
		// First create an SDP reference since this is what this was created
		// from. We will use some of the methods from this reference
		r := sdp.Reference{
			Context:              l.Context,
			Type:                 l.Type,
			UniqueAttributeValue: l.UniqueAttributeValue,
		}

		// Convert from a reference to a node since this is what will actually
		// be stopred in the database
		i.LinkedItems = append(i.LinkedItems, ItemNode{
			Context:              r.GetContext(),
			Type:                 r.GetType(),
			UniqueAttributeValue: r.GetUniqueAttributeValue(),
			Hash:                 r.Hash(),
			GloballyUniqueName:   r.GloballyUniqueName(),
		})
	}

	return nil
}

// Queries Returns the queries that should match specifically this item. It will
// also export the following variables:
//
//   * `{GloballyUniqueName}.item`: UID of this item
//   * `{GloballyUniqueName}.attributes`: UID of this item's attributes
//   * `{GloballyUniqueName}.metadata`: UID of this item's metadata
func (i *ItemNode) Queries() Queries {
	q := make(Queries, 0)

	q = append(q, Query{
		VariableName: fmt.Sprintf(`%v.item`, i.Hash),
		QueryFunc:    fmt.Sprintf(`%v(func: eq(Hash, "%v"))`, i.Hash, i.Hash),
	})

	if i.Metadata != nil {
		q = append(q, Query{
			VariableName: fmt.Sprintf(`%v.item.older`, i.Hash),
			QueryFunc: fmt.Sprintf(
				`%v.older(func: uid(%v.item)) @filter(lt(Metadata.Timestamp, "%v") OR NOT has(Metadata.Timestamp))`,
				i.Hash,
				i.Hash,
				i.Metadata.GetTimestamp().AsTime().Format(time.RFC3339Nano),
			),
		})
	}

	return q
}

// Mutation Returns a list of mutations that can be
func (i *ItemNode) Mutation() *api.Mutation {
	var cond string

	itemJSON, _ := json.Marshal(i)

	// Create a condition for the upsert that follows this logic:
	//
	// * If the item doesn't exist create it
	// * If the item does exist, and the timestamp is older than the one that we
	//   have, update it
	// * If our item is older, do nothing
	if i.Metadata == nil {
		cond = fmt.Sprintf(
			"@if(eq(len(%v.item), 0))",
			i.Hash,
		)
	} else {
		cond = fmt.Sprintf(
			"@if(eq(len(%v.item), 0) OR eq(len(%v.item.older), 1))",
			i.Hash,
			i.Hash,
		)
	}

	// Initial mutations to create the item and its attributes and metadata
	return &api.Mutation{
		SetJson: itemJSON,
		Cond:    cond,
	}
}

// QueryItem Queries a single item from the database
func QueryItem(d *dgo.Dgraph, globallyUniqueName string) (ItemNode, error) {
	var res *api.Response
	var err error
	var results map[string][]ItemNode
	var result ItemNode

	// Query to ensure that the items were all inserted okay
	q := fmt.Sprintf(`{
			Items(func: eq(GloballyUniqueName, "%v")) {
				Context
				Type
				UniqueAttribute
				UniqueAttributeValue
				GloballyUniqueName
				Attributes
				Hash
				Metadata.BackendName
				Metadata.RequestMethod
				Metadata.Timestamp
				Metadata.BackendDuration
				Metadata.BackendDurationPerItem
				Metadata.BackendPackage
				LinkedItems {
					Context
					Type
					UniqueAttributeValue
				}
			}
		}`,
		globallyUniqueName,
	)

	res, err = d.NewTxn().Query(context.Background(), q)

	if err != nil {
		return result, err
	}

	// Read the items back into memory
	err = json.Unmarshal(res.GetJson(), &results)

	if len(results["Items"]) > 1 {
		return result, fmt.Errorf("Found >1 item with the GloballyUniqueName: %v JSON Output:\n%v", globallyUniqueName, string(res.GetJson()))
	}

	result = results["Items"][0]

	return result, nil
}
