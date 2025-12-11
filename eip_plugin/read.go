// Copyright 2025 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eip_plugin

import (
	"fmt"

	"github.com/danomagnum/gologix"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// readSingleTagValue reads the value of a single tag into the specified variable
// of the provided datatype. Therefore it's important that the type is correctly
// set up in the `input.yaml`. Otherwise we will error here.
func (g *EIPInput) readSingleTagValue(item *CIPReadItem) (string, error) {
	var value any
	switch item.CIPDatatype {
	case gologix.CIPTypeBOOL:
		var v bool
		err := g.CIP.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeBYTE:
		var v byte
		err := g.CIP.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeSINT:
		var v int8
		err := g.CIP.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeUSINT:
		var v uint8
		err := g.CIP.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeUINT:
		var v uint16
		err := g.CIP.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeINT:
		var v int16
		err := g.CIP.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeUDINT:
		var v uint32
		err := g.CIP.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeDINT:
		var v int32
		err := g.CIP.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeLWORD, gologix.CIPTypeULINT:
		var v uint64
		err := g.CIP.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeLINT:
		var v int64
		err := g.CIP.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeREAL:
		var v float32
		err := g.CIP.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeLREAL:
		var v float64
		err := g.CIP.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeSTRING:
		var v string
		err := g.CIP.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeStruct:
		err := g.CIP.Read(item.TagName, &value)
		if err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("Failed to read tag for not supported CIPtype: %v", item.CIPDatatype)
	}
	return fmt.Sprintf("%v", value), nil
}

// readArrayTagValue reads the values of a tag which is an array into the specified variable
// of the provided datatype. Therefore it's important that the type is correctly
// set up in the `input.yaml`. Otherwise we will error here.
func (g *EIPInput) readArrayTagValue(item *CIPReadItem) (string, error) {
	switch item.CIPDatatype {
	case gologix.CIPTypeBOOL:
		arr := make([]bool, item.ArrayLength)
		err := g.CIP.Read(item.TagName, arr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", arr), nil
	case gologix.CIPTypeBYTE:
		arr := make([]byte, item.ArrayLength)
		err := g.CIP.Read(item.TagName, arr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", arr), nil
	case gologix.CIPTypeSINT:
		arr := make([]int8, item.ArrayLength)
		err := g.CIP.Read(item.TagName, arr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", arr), nil
	case gologix.CIPTypeUSINT:
		arr := make([]uint8, item.ArrayLength)
		err := g.CIP.Read(item.TagName, arr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", arr), nil
	case gologix.CIPTypeINT:
		arr := make([]int16, item.ArrayLength)
		err := g.CIP.Read(item.TagName, arr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", arr), nil
	case gologix.CIPTypeUINT:
		arr := make([]uint16, item.ArrayLength)
		err := g.CIP.Read(item.TagName, arr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", arr), nil
	case gologix.CIPTypeDINT:
		arr := make([]int32, item.ArrayLength)
		err := g.CIP.Read(item.TagName, arr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", arr), nil
	case gologix.CIPTypeUDINT:
		arr := make([]uint32, item.ArrayLength)
		err := g.CIP.Read(item.TagName, arr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", arr), nil
	case gologix.CIPTypeLINT:
		arr := make([]int64, item.ArrayLength)
		err := g.CIP.Read(item.TagName, arr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", arr), nil
	case gologix.CIPTypeLWORD, gologix.CIPTypeULINT:
		arr := make([]uint64, item.ArrayLength)
		err := g.CIP.Read(item.TagName, arr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", arr), nil
	case gologix.CIPTypeREAL:
		arr := make([]float32, item.ArrayLength)
		err := g.CIP.Read(item.TagName, arr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", arr), nil
	case gologix.CIPTypeLREAL:
		arr := make([]float64, item.ArrayLength)
		err := g.CIP.Read(item.TagName, arr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", arr), nil
	case gologix.CIPTypeSTRING:
		arr := make([]string, item.ArrayLength)
		err := g.CIP.Read(item.TagName, arr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", arr), nil
	default:
		return "", fmt.Errorf("Failed to read array for not supported CIPType: %v", item.CIPDatatype)
	}
}

// createTagVar is prepared so that we can later on store the corresponding variable
// into the tagMap, which is then used to read multiple Tags at once
// It basically creates a variable for the corresponding datatype and returns it's pointer.
func createTagVar(item *CIPReadItem) (any, error) {
	switch item.CIPDatatype {
	case gologix.CIPTypeBOOL:
		var v bool
		return &v, nil
	case gologix.CIPTypeBYTE:
		var v byte
		return &v, nil
	case gologix.CIPTypeSINT:
		var v int8
		return &v, nil
	case gologix.CIPTypeUINT:
		var v uint16
		return &v, nil
	case gologix.CIPTypeUSINT:
		var v uint8
		return &v, nil
	case gologix.CIPTypeINT:
		var v int16
		return &v, nil
	case gologix.CIPTypeUDINT:
		var v uint32
		return &v, nil
	case gologix.CIPTypeULINT:
		var v uint64
		return &v, nil
	case gologix.CIPTypeDINT:
		var v int32
		return &v, nil
	case gologix.CIPTypeLWORD:
		var v uint64
		return &v, nil
	case gologix.CIPTypeLINT:
		var v int64
		return &v, nil
	case gologix.CIPTypeREAL:
		var v float32
		return &v, nil
	case gologix.CIPTypeLREAL:
		var v float64
		return &v, nil
	case gologix.CIPTypeSTRING:
		var v string
		return &v, nil
	case gologix.CIPTypeStruct:
		var v any
		return &v, nil
	default:
		return "", fmt.Errorf("Failed to resolve CIP-Itemtype for variable creation: %v", item.CIPDatatype)
	}
}

// parseTagsIntoMap is prepared to later on read multiple data at once from the
// tagMap, where we store the tagName as a key and the pointer to the variable
// as the value.
func parseTagsIntoMap(items []*CIPReadItem) (map[string]any, error) {
	itemMap := make(map[string]any)
	for _, item := range items {
		v, err := createTagVar(item)
		if err != nil {
			return nil, err
		}

		itemMap[item.TagName] = v
	}

	// should return nil for now, since not implemented
	return nil, nil
}

// CreateMessageFromValue is used to set the metadata for the eip-tags and
// create the message from the rawValue which is read from the tags/attributes
func CreateMessageFromValue(rawValue []byte, item *CIPReadItem) (*service.Message, error) {
	var tagType string
	switch item.CIPDatatype {
	case gologix.CIPTypeBOOL:
		tagType = "bool"
	case gologix.CIPTypeBYTE, gologix.CIPTypeSINT, gologix.CIPTypeUINT, gologix.CIPTypeINT,
		gologix.CIPTypeUDINT, gologix.CIPTypeDINT, gologix.CIPTypeLWORD, gologix.CIPTypeLINT,
		gologix.CIPTypeREAL, gologix.CIPTypeLREAL:
		tagType = "number"
	default:
		tagType = "string"
	}

	msg := service.NewMessage(rawValue)
	msg.MetaSetMut("eip_tag_name", item.TagName) // tag name
	msg.MetaSetMut("eip_tag_type", tagType)      // data type - number, bool, or string
	msg.MetaSetMut("eip_tag_path", item.TagName) // tag path - usually something like `Program:Gologix_Tests.ReadTest`

	if item.IsAttribute {
		msg.MetaSetMut("eip_tag_name", item.AttributeName) // replace tag name by attribute name
	}

	if item.Alias != "" {
		msg.MetaSet("eip_tag_name", item.Alias) // replace tag name by alias if provided
	}

	return msg, nil
}

// readAndConvertAttribute is used to read data from attributes and then use
// the earlier on created ConverterFunc to convert it into the correct datatype.
func (g *EIPInput) readAndConvertAttribute(item *CIPReadItem) (string, error) {
	resp, err := g.CIP.GetAttrSingle(item.CIPClass, item.CIPInstance, item.CIPAttribute)
	if err != nil {
		g.Log.Errorf("failed to get attribute - class %v, instance %v, attribute %v, err: %v",
			item.CIPClass, item.CIPInstance, item.CIPAttribute, err)
		return "", err
	}

	// convert the CIPItem into the corresponding data
	data, err := item.ConverterFunc(resp)
	if err != nil {
		return "", fmt.Errorf("failed to convert CIPItem to corresponding data: %w", err)
	}

	// convert the data into string
	dataAsString := fmt.Sprintf("%v", data)

	return dataAsString, nil
}

// read either Tags or Attributes
//
// can return early here after read - not as in ReadBatch
// TODO: - add multiRead for Attributes
//   - add multiRead for Tags
func (g *EIPInput) readTagsOrAttributes(item *CIPReadItem) (string, error) {
	if item.IsAttribute {
		return g.readAndConvertAttribute(item)
	}

	if item.IsArray {
		return g.readArrayTagValue(item)
	}

	return g.readSingleTagValue(item)
}
