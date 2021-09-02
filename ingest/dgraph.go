package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/dylanratcliffe/sdp.go"
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

	_, err := net.LookupIP(hostname)

	if err != nil {
		return nil, fmt.Errorf("Host resolution failed for %v. Error: %v", hostname, err)
	}

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

Context: string @index(exact) .
Type: string @index(exact) .
UniqueAttribute: string @index(exact) .
UniqueAttributeValue: string @index(exact) .
GloballyUniqueName: string @index(exact) .
Attributes: string @index(hash) @upsert .
Hash: string @index(exact) .
Metadata.BackendName: string @index(exact) @upsert .
Metadata.RequestMethod: string @index(exact) @upsert .
Metadata.Timestamp: dateTime @index(hour) @upsert .
Metadata.BackendDuration: int @index(int) @upsert .
Metadata.BackendDurationPerItem: int @index(int) @upsert .
Metadata.BackendPackage: string @index(exact) @upsert .
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
//
// ## Attributes Predicate
//
// Currently attributes are stored as a JSON string. This has made the database
// queries very easy but will likely cause performance issues in future. This is
// due to the fact that dgraph does predicate based sharding i.e. data is
// sharded by predicate and not by UID. This means that all values of
// "attributes" (which will represent the vast majority of tha database) will be
// stored in the same shard as it's all in the one predicate. Initially I had
// tried storing each attribute as its won predicate with links between them.
// This would be much better from a sharding perspective, but would make life
// much harder in a number of ways, as detailed below.
//
// ### Predicate Expansion
//
// When querying we would need to expand the predicates which would mean that we
// would either need to know the predicates in advance (difficult due to the
// fact that attributes are arbitrary) or dynamically generate a named type in
// dgraph for each and store this in the schema.
//
// ### Orphaned Nodes
//
// When updating data we could very easily orphan nodes since we are creating
// nodes arbitrarily with arbitrary relationships. Think of a kubernetes pod
// with many statuses, each of these will require a node and once the pod is
// deleted they would need to be deleted too. Also if a nested hash changed it
// would probably need to be re-created as opposed to updated since we don't
// know what makes it unique. This would mean that  he old node would still
// exist but would now be an orphan. There would need to be some regular cleanup
// of these orphaned nodes probably
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
		// TODO: This still technically allows clashing variable names, just not
		// clashing functions
		queriesMap[qi.QueryFunc] = qi
	}

	newQ := make(Queries, 0)

	for _, qi := range queriesMap {
		newQ = append(newQ, qi)
	}

	return newQ
}

// Query Represents a dgraph query
type Query struct {
	QueryFunc string
	Variables []Variable
}

// Variable represents a query variable where Name is the actual name of the
// variable and Source is the thing in the query that populates it, such as
// "count(uid)"
type Variable struct {
	Name   string
	Source string
}

func (q Query) String() string {
	var s string
	var declarations []string

	declarations = make([]string, len(q.Variables))

	// Convert each variable to text format
	for i, v := range q.Variables {
		declarations[i] = v.String()
	}

	s = q.QueryFunc + " { " + strings.Join(declarations, ", ") + " }"

	return s
}

func (v Variable) String() string {
	return fmt.Sprintf(`%v as %v`, v.Name, v.Source)
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
//   * `{Hash}.item`: UID of this item
//   * `{Hash}.item.older`: UID of this item, if it is older than the supplied one
//   * `{Hash}.linkedItemsCount`: count() of the linked items
func (i *ItemNode) Queries() Queries {
	var hashQuery Query
	var metadataQuery Query
	var qs Queries

	// UID of this item if it already exists
	hashQuery = Query{
		QueryFunc: fmt.Sprintf(`%v(func: eq(Hash, "%v"))`, i.Hash, i.Hash),
		Variables: []Variable{
			{
				Name:   fmt.Sprintf(`%v.item`, i.Hash),
				Source: "uid",
			},
		},
	}

	if i.Metadata != nil {
		metadataQuery = Query{
			QueryFunc: fmt.Sprintf(
				`%v.older(func: uid(%v.item)) @filter(lt(Metadata.Timestamp, "%v") OR NOT has(Metadata.Timestamp))`,
				i.Hash,
				i.Hash,
				i.Metadata.GetTimestamp().AsTime().Format(time.RFC3339Nano),
			),
			Variables: []Variable{
				{
					Name:   fmt.Sprintf(`%v.item.older`, i.Hash),
					Source: "uid",
				},
			},
		}

		// We only need to create the linked items variable if the item we're
		// deal with is a full item i.e. has metadata
		hashQuery.Variables = append(hashQuery.Variables, Variable{
			Name:   fmt.Sprintf(`%v.LinkedItems`, i.Hash),
			Source: "LinkedItems",
		})

		// hashQuery.Variables = append(hashQuery.Variables, Variable{
		// 	Name:   fmt.Sprintf(`%v.Metadata.Timestamp`, i.Hash),
		// 	Source: "Metadata.Timestamp",
		// })
	}

	qs = append(qs, hashQuery)

	if i.Metadata != nil {
		qs = append(qs, metadataQuery)
	}

	return qs
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
		// Upsert the item if it doesn't exist. Since the item that we're
		// currently operating on doesn't have metadata we can't determine if
		// it's older or not
		cond = fmt.Sprintf(
			"@if(eq(len(%v.item), 0))",
			i.Hash,
		)
	} else {
		// Upsert the item if (using OR):
		//
		// * The item doesn't exist
		// * The item does exist but the metadata says it's older
		// * The item does exist and it's the same version
		cond = fmt.Sprintf(
			`@if(eq(len(%v.item), 0) OR eq(len(%v.item.older), 1) OR lt(len(%v.LinkedItems), %v))`,
			i.Hash,
			i.Hash,
			i.Hash,
			len(i.LinkedItems),
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
