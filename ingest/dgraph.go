package ingest

import (
	"context"
	"crypto/sha1"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
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
	Attributes      AttributesNode   `json:"-"`
	LinkedItems     []*sdp.Reference `json:"-"`
	item            *sdp.Item        `json:"-"`
}

// Mutations Returns a list of mutations that can be
func (i *ItemNode) Mutations() []*api.Mutation {
	var mutations []*api.Mutation

	itemJSON, _ := json.Marshal(i)
	attributesJSON, _ := json.Marshal(i.Attributes)
	metadataJSON, _ := json.Marshal(i.Metadata)

	// Initial mutations to create the item and its attributes and metadata
	mutations = []*api.Mutation{
		{
			SetJson: itemJSON,
		},
		{
			SetJson: attributesJSON,
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
		Attributes           string   `json:"Attributes,omitempty"`
		Metadata             string   `json:"Metadata,omitempty"`
		LinkedItems          []string `json:"LinkedItems"`
		Alias
	}{
		UID:                  fmt.Sprintf("uid(%v.item)", i.Hash()),
		Attributes:           fmt.Sprintf("uid(%v.attributes)", i.Hash()),
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
			%v.attributes as Attributes
			%v.metadata as Metadata
		}
	`,
		i.Hash(),
		i.item.GloballyUniqueName(),
		i.Hash(),
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

	shaSum = sha1.Sum([]byte(i.item.GloballyUniqueName()))

	// We need to specify a custom encoding here since dGraph has fairly struct
	// requirements aboout what name a variable
	paddedEncoding = base32.NewEncoding("abcdefghijklmnopqrstuvwxyzABCDEF")

	// We also can't have padding since "=" is not allowed in variable names
	unpaddedEncoding = paddedEncoding.WithPadding(base32.NoPadding)

	return unpaddedEncoding.EncodeToString(shaSum[:9])
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

// AttributesNode Represents the attributes of a single item
//
// It can also be used to reference a nested set of attributes. If this is the
// case then you need to create the object with a reference to the parent
// attributes, as well as tell it which attribute specifically is it's parent.
// This allows the object to marshal nicely to JSON with dotted predicates that
// won't clash i.e.
//
//  {type}.{attribute}.{sub-attribute}.{name}
//
type AttributesNode struct {
	Map                  map[string]interface{} // The attributes map
	ParentItemNode       *ItemNode              // A pointer to the item that these attributes are related to
	ParentAttributesNode *AttributesNode        // A pointer to the attributes that these attributes are a child of. This is used for an attribute whose value is a nested hash
	ParentAttributesKey  string                 // The name of the key that these attributes are the value for
}

// Prefix Returns the prefix that should be added to the predicate name to ensure
// that it is unique and doesn't clash with another item's predicates
func (a *AttributesNode) Prefix() (string, error) {
	// If we have parent attributes this means that we are a set of
	// sub-attributes. Therefore the prefix should be whatever the parent's
	// prefix was, plus the key name
	if a.ParentAttributesNode != nil {
		parentPrefix, err := a.ParentAttributesNode.Prefix()

		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%v.%v", parentPrefix, a.ParentAttributesKey), nil
	}

	// Otherwise if we are a direct child of an attribute then the prefix
	// should be just the item type
	if a.ParentItemNode != nil {
		return a.ParentItemNode.item.GetType(), nil
	}

	// If none of the above is true this means that the struct wasn't set up
	// with the correct parent information
	return "", errors.New("Neither ParentItemNode or ParentAttributesNode is set. Cannot calculate prefix")
}

// MarshalJSON Custom marshalling functionality that adds derived fields
// required for DGraph
func (a AttributesNode) MarshalJSON() ([]byte, error) {
	// We want to store the attributes in the format {type}.{attribute}. This is
	// so that we don't have a clash of predicates. We wan this because we will
	// also be doing dynamic type generation and I think it needs to be unique.
	// though this might be worth looking into...

	// Re-map the map
	newMap := make(map[string]interface{})

	// Calculate the prefix that will be used in the type and attributes
	prefix, err := a.Prefix()

	if err != nil {
		return []byte{}, err
	}

	for k, v := range a.Map {
		keyName := fmt.Sprintf("%v.%v", prefix, k)

		newMap[keyName] = ToAttributes(v, &a, k)
	}

	// Append the dgraph type and UID
	newMap["dgraph.type"] = fmt.Sprintf("%v.attributes", prefix)

	// UID is only required to pull from the query if this is a top-level
	// attributes hash. If they are nested then we will just let DGraph generate
	// UIDs for us
	if a.ParentAttributesNode == nil {
		newMap["uid"] = fmt.Sprintf("uid(%v.attributes)", a.ParentItemNode.Hash())
	}

	return json.Marshal(newMap)
}

// ToAttributes Converts values to be compatible with insertion into dgraph.
// This basically involves converting any hash into an AttributesNode
func ToAttributes(value interface{}, parent *AttributesNode, key string) interface{} {
	// Check if the value is a nested hash (Struct or Map)
	vValue := reflect.ValueOf(value)

	switch vValue.Kind() {
	case reflect.Struct, reflect.Map:
		var valueMap map[string]interface{}

		// PERF: This marshals though JSON to convert any nested structs to
		// a map, so that we can then apply our own custom JSON formatting
		// for DGraph. This is probably not the fastest thing in the
		// world...
		b, _ := json.Marshal(value)
		json.Unmarshal(b, &valueMap)

		return AttributesNode{
			Map:                  valueMap,
			ParentItemNode:       parent.ParentItemNode,
			ParentAttributesNode: parent,
			ParentAttributesKey:  key,
		}
	case reflect.Array, reflect.Slice:
		var valueSlice []interface{}

		for i := 0; i < vValue.Len(); i++ {
			valueSlice = append(valueSlice, ToAttributes(vValue.Index(i).Interface(), parent, key))
		}

		return valueSlice
	}

	return value
}

// UnmarshalJSON Converts from JSON to Attributes
func (a *AttributesNode) UnmarshalJSON(value []byte) error {
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
