package eip_plugin_test

import (
	"fmt"

	"github.com/danomagnum/gologix"
)

// MockCIPReader implements CIPReader and stores tagName -> value
type MockCIPReader struct {
	Connected bool
	Tags      map[string]any

	// let's ignore this for now
	Attrs map[[3]uint16]*gologix.CIPItem
}

func (m *MockCIPReader) Connect() error {
	m.Connected = true
	return nil
}
func (m *MockCIPReader) Disconnect() error {
	m.Connected = false
	return nil
}
func (m *MockCIPReader) Read(tag string, data any) error {
	val, ok := m.Tags[tag]
	if !ok {
		return fmt.Errorf("mock CIP: tag '%s' does not exist", tag)
	}

	switch ptr := data.(type) {
	case *bool:
		cast, ok := val.(bool)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not bool", tag)
		}
		*ptr = cast
	case *int8:
		cast, ok := val.(int8)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not bool", tag)
		}
		*ptr = cast
	case *uint8:
		cast, ok := val.(uint8)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not bool", tag)
		}
		*ptr = cast
	case *int16:
		cast, ok := val.(int16)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not int16", tag)
		}
		*ptr = cast
	case *uint16:
		cast, ok := val.(uint16)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not uint16", tag)
		}
		*ptr = cast
	case *int32:
		cast, ok := val.(int32)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not int32", tag)
		}
		*ptr = cast
	case *uint32:
		cast, ok := val.(uint32)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not uint32", tag)
		}
		*ptr = cast
	case *int64:
		cast, ok := val.(int64)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not int64", tag)
		}
		*ptr = cast
	case *uint64:
		cast, ok := val.(uint64)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not uint64", tag)
		}
		*ptr = cast
	case *float32:
		cast, ok := val.(float32)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not float32", tag)
		}
		*ptr = cast
	case *float64:
		cast, ok := val.(float64)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not float64", tag)
		}
		*ptr = cast
	case *string:
		cast, ok := val.(string)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not string", tag)
		}
		*ptr = cast
	case *[]int8:
		sliceVal, ok := val.([]int8)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not []int8", tag)
		}
		// copy sliceVal into *ptr
		if len(*ptr) != len(sliceVal) {
			return fmt.Errorf("mock CIP: slice length mismatch. wanted %d got %d", len(*ptr), len(sliceVal))
		}
		copy(*ptr, sliceVal)
	case *[]uint8:
		sliceVal, ok := val.([]uint8)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not []uint8", tag)
		}
		// copy sliceVal into *ptr
		if len(*ptr) != len(sliceVal) {
			return fmt.Errorf("mock CIP: slice length mismatch. wanted %d got %d", len(*ptr), len(sliceVal))
		}
		copy(*ptr, sliceVal)
	case *[]int16:
		sliceVal, ok := val.([]int16)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not []int16", tag)
		}
		// copy sliceVal into *ptr
		if len(*ptr) != len(sliceVal) {
			return fmt.Errorf("mock CIP: slice length mismatch. wanted %d got %d", len(*ptr), len(sliceVal))
		}
		copy(*ptr, sliceVal)
	case *[]uint16:
		sliceVal, ok := val.([]uint16)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not []uint16", tag)
		}
		// copy sliceVal into *ptr
		if len(*ptr) != len(sliceVal) {
			return fmt.Errorf("mock CIP: slice length mismatch. wanted %d got %d", len(*ptr), len(sliceVal))
		}
		copy(*ptr, sliceVal)
	case *[]int32:
		sliceVal, ok := val.([]int32)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not []int32", tag)
		}
		if len(*ptr) != len(sliceVal) {
			return fmt.Errorf("mock CIP: length mismatch. wanted %d got %d", len(*ptr), len(sliceVal))
		}
		copy(*ptr, sliceVal)
	case *[]uint32:
		sliceVal, ok := val.([]uint32)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not []uint32", tag)
		}
		if len(*ptr) != len(sliceVal) {
			return fmt.Errorf("mock CIP: length mismatch. wanted %d got %d", len(*ptr), len(sliceVal))
		}
		copy(*ptr, sliceVal)
	case *[]int64:
		sliceVal, ok := val.([]int64)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not []int64", tag)
		}
		if len(*ptr) != len(sliceVal) {
			return fmt.Errorf("mock CIP: length mismatch. wanted %d got %d", len(*ptr), len(sliceVal))
		}
		copy(*ptr, sliceVal)
	case *[]uint64:
		sliceVal, ok := val.([]uint64)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not []uint64", tag)
		}
		if len(*ptr) != len(sliceVal) {
			return fmt.Errorf("mock CIP: length mismatch. wanted %d got %d", len(*ptr), len(sliceVal))
		}
		copy(*ptr, sliceVal)
	case *[]float32:
		sliceVal, ok := val.([]float32)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not []float32", tag)
		}
		if len(*ptr) != len(sliceVal) {
			return fmt.Errorf("mock CIP: length mismatch. wanted %d got %d", len(*ptr), len(sliceVal))
		}
		copy(*ptr, sliceVal)
	case *[]float64:
		sliceVal, ok := val.([]float64)
		if !ok {
			return fmt.Errorf("mock CIP: tag '%s' is not []float64", tag)
		}
		if len(*ptr) != len(sliceVal) {
			return fmt.Errorf("mock CIP: length mismatch. wanted %d got %d", len(*ptr), len(sliceVal))
		}
		copy(*ptr, sliceVal)

	default:
		return fmt.Errorf("mock CIP: reading type %T not implemented", ptr)
	}
	return nil
}
func (m *MockCIPReader) GetAttrSingle(cls gologix.CIPClass, inst gologix.CIPInstance, attr gologix.CIPAttribute) (*gologix.CIPItem, error) {
	key := [3]uint16{uint16(cls), uint16(inst), uint16(attr)}

	item, ok := m.Attrs[key]
	if !ok {
		return nil, fmt.Errorf("mock CIP: attribute not found for cls=%d, inst=%d, attr=%d", cls, inst, attr)
	}
	return item, fmt.Errorf("mock CIP: GetAttrSingle not implemented")
}
