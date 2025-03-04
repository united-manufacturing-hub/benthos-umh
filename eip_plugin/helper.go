package eip_plugin

import (
	"fmt"
	"math"

	"github.com/danomagnum/gologix"
)

func readCIPItem(
	client *gologix.Client,
	item *CIPReadItem,
) (*gologix.CIPItem, error) {

	if item.IsAttribute {
		// Simple case: we already get a CIPItem back
		return client.GetAttrSingle(
			item.CIPClass,
			item.CIPInstance,
			item.CIPAttribute,
		)
	}

	// Otherwise, we’re reading a tag:
	// Suppose we have a convenience method that returns an interface{} typed according
	// to item.CIPDatatype:
	val, err := client.Read_single(item.TagName, item.CIPDatatype, 1)
	if err != nil {
		return nil, err
	}

	// Convert that interface{} to a CIPItem’s Data buffer:
	cipItem := &gologix.CIPItem{
		// Initialize a position if you need it:
		Pos: 0,
		// Data to be filled below
	}

	// Now we must “pack” val into cipItem.Data based on item.CIPDatatype:
	// (Use little-endian or big-endian according to what gologix expects internally.)
	// Typically Rockwell is little-endian, but do check carefully in your library.

	switch typedVal := val.(type) {
	case bool:
		// CIP BOOL is typically 1 byte
		cipItem.Data = []byte{0}
		if typedVal {
			cipItem.Data[0] = 1
		}

	case uint16:
		buf := make([]byte, 2)
		// little-endian:
		buf[0] = byte(typedVal)
		buf[1] = byte(typedVal >> 8)
		cipItem.Data = buf

	case int16:
		buf := make([]byte, 2)
		// ...
		cipItem.Data = buf

	case uint32:
		buf := make([]byte, 4)
		// ...
		cipItem.Data = buf

	case int32:
		buf := make([]byte, 4)
		// ...
		cipItem.Data = buf

	case float32:
		// you can do a math.Float32bits -> uint32 -> bytes
		bits := math.Float32bits(typedVal)
		buf := make([]byte, 4)
		// ...
		cipItem.Data = buf

	case float64:
		buf := make([]byte, 8)
		// ...
		cipItem.Data = buf

	case string:
		// If CIPTypeSTRING, just store it raw:
		cipItem.Data = []byte(typedVal)

	case []byte:
		// e.g. if CIPType is “array of octet”
		cipItem.Data = typedVal

	default:
		return nil, fmt.Errorf("unsupported read type from Read_single: %T", val)
	}

	// Now we have an in-memory CIPItem that your converter can handle:
	return cipItem, nil
}

func readCIPItem(
	client *gologix.Client,
	item *CIPReadItem,
) (*gologix.CIPItem, error) {

	if item.IsAttribute {
		// Simple case: we already get a CIPItem back
		return client.GetAttrSingle(
			item.CIPClass,
			item.CIPInstance,
			item.CIPAttribute,
		)
	}

	// Otherwise, we’re reading a tag:
	// Suppose we have a convenience method that returns an interface{} typed according
	// to item.CIPDatatype:
	val, err := client.Read_single(item.TagName, item.CIPDatatype, 1)
	if err != nil {
		return nil, err
	}

	// Convert that interface{} to a CIPItem’s Data buffer:
	cipItem := &gologix.CIPItem{
		// Initialize a position if you need it:
		Pos: 0,
		// Data to be filled below
	}

	// Now we must “pack” val into cipItem.Data based on item.CIPDatatype:
	// (Use little-endian or big-endian according to what gologix expects internally.)
	// Typically Rockwell is little-endian, but do check carefully in your library.

	switch typedVal := val.(type) {
	case bool:
		// CIP BOOL is typically 1 byte
		cipItem.Data = []byte{0}
		if typedVal {
			cipItem.Data[0] = 1
		}

	case uint16:
		buf := make([]byte, 2)
		// little-endian:
		buf[0] = byte(typedVal)
		buf[1] = byte(typedVal >> 8)
		cipItem.Data = buf

	case int16:
		buf := make([]byte, 2)
		// ...
		cipItem.Data = buf

	case uint32:
		buf := make([]byte, 4)
		// ...
		cipItem.Data = buf

	case int32:
		buf := make([]byte, 4)
		// ...
		cipItem.Data = buf

	case float32:
		// you can do a math.Float32bits -> uint32 -> bytes
		bits := math.Float32bits(typedVal)
		buf := make([]byte, 4)
		// ...
		cipItem.Data = buf

	case float64:
		buf := make([]byte, 8)
		// ...
		cipItem.Data = buf

	case string:
		// If CIPTypeSTRING, just store it raw:
		cipItem.Data = []byte(typedVal)

	case []byte:
		// e.g. if CIPType is “array of octet”
		cipItem.Data = typedVal

	default:
		return nil, fmt.Errorf("unsupported read type from Read_single: %T", val)
	}

	// Now we have an in-memory CIPItem that your converter can handle:
	return cipItem, nil
}
