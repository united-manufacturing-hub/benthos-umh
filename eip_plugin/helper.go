package eip_plugin

import (
	"fmt"

	"github.com/danomagnum/gologix"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func (g *EIPInput) readTagValue(item *CIPReadItem) (string, error) {
	var value any
	switch item.CIPDatatype {
	case gologix.CIPTypeBOOL:
		var v bool
		err := g.Client.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeBYTE:
		var v byte
		err := g.Client.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeSINT:
		var v int8
		err := g.Client.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeUINT:
		var v uint16
		err := g.Client.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeINT:
		var v int16
		err := g.Client.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeUDINT:
		var v uint32
		err := g.Client.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeDINT:
		var v int32
		err := g.Client.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeLWORD:
		var v uint64
		err := g.Client.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeLINT:
		var v int64
		err := g.Client.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeREAL:
		var v float32
		err := g.Client.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeLREAL:
		var v float64
		err := g.Client.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeSTRING:
		var v string
		err := g.Client.Read(item.TagName, &v)
		if err != nil {
			return "", err
		}
		value = v
	case gologix.CIPTypeStruct:
		err := g.Client.Read(item.TagName, &value)
		if err != nil {
			return "", err
		}

	default:
		return "", fmt.Errorf("not able to resolve CIP-Itemtype")
	}
	return fmt.Sprintf("%v", value), nil
}

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
	case gologix.CIPTypeINT:
		var v int16
		return &v, nil
	case gologix.CIPTypeUDINT:
		var v uint32
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

	default:
		return "", fmt.Errorf("not able to resolve CIP-Itemtype")
	}

	return nil, nil
}

func parseTagsIntoMap(items []*CIPReadItem) (map[string]any, error) {
	itemMap := make(map[string]any)
	for _, item := range items {
		v, err := createTagVar(item)
		if err != nil {
			return nil, err
		}

		itemMap[item.TagName] = v

	}

	return nil, nil
}

func CreateMessageFromValue(rawValue []byte, item *CIPReadItem) (*service.Message, error) {
	var tagType string
	switch item.CIPDatatype {
	case gologix.CIPTypeBOOL:
		tagType = "bool"
	case gologix.CIPTypeBYTE:
		tagType = "byte"
	case gologix.CIPTypeSINT:
		tagType = "number"
	case gologix.CIPTypeUINT:
		tagType = "number"
	case gologix.CIPTypeINT:
		tagType = "number"
	case gologix.CIPTypeUDINT:
		tagType = "number"
	case gologix.CIPTypeDINT:
		tagType = "number"
	case gologix.CIPTypeLWORD:
		tagType = "number"
	case gologix.CIPTypeLINT:
		tagType = "number"
	case gologix.CIPTypeREAL:
		tagType = "number"
	case gologix.CIPTypeLREAL:
		tagType = "number"
	case gologix.CIPTypeSTRING:
		tagType = "string"
	case gologix.CIPTypeStruct:
		tagType = "string"

	default:

	}

	msg := service.NewMessage(rawValue)
	msg.MetaSetMut("eip_tag_name", item.TagName) // tag name
	msg.MetaSetMut("eip_tag_datatype", tagType)  // data type - number, bool, or string

	if item.IsAttribute {
		msg.MetaSetMut("eip_tag_name", item.AttributeName) // replace tag name by attribute name
	}

	if item.Alias != "" {
		msg.MetaSet("eip_tag_name", item.Alias) // replace tag name by alias if provided
	}

	return msg, nil
}

func (g *EIPInput) readAndConvertAttribute(item *CIPReadItem) (string, error) {
	resp, err := g.Client.GetAttrSingle(item.CIPClass, item.CIPInstance, item.CIPAttribute)
	if err != nil {
		g.Log.Errorf("failed to get attribute - class %v, instance %v, attribute %v, err:",
			item.CIPClass, item.CIPInstance, item.CIPAttribute, err)
		return "", err
	}

	// convert the CIPItem into the corresponding data
	data, err := item.ConverterFunc(resp)
	if err != nil {
		return "", fmt.Errorf("failed to convert CIPItem to corresponding data: %v", err)
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
		dataAsString, err := g.readAndConvertAttribute(item)
		if err != nil {
			return "", err
		}
		return dataAsString, nil
	}

	dataAsString, err := g.readTagValue(item)
	if err != nil {
		g.Log.Errorf("failed to read tag value: %v", err)
		return "", err
	}

	return dataAsString, nil
}
