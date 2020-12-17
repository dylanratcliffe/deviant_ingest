package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// ------------------------------------------------------------------------------------------------------------
// Note that all of the code in this file is currently unused. It is being kept here in case I want it later...
// ------------------------------------------------------------------------------------------------------------

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
