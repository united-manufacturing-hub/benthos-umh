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
	"bytes"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"

	"github.com/danomagnum/gologix"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// parseController is used to parse the endpoint and the specified path
// into a *gologix.Controller struct, which is needed for connection
// - `endpoint` consists of the ip-address and if provided the port
// otherwise it will just use the default-port 44818
func parseController(endpoint string, pathStr string) (*gologix.Controller, error) {
	host, portStr, err := net.SplitHostPort(endpoint)
	if err != nil {
		host = endpoint
		portStr = "44818"
	}

	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, err
	}

	path, err := buildCIPPath(pathStr)
	if err != nil {
		return nil, err
	}

	controller := &gologix.Controller{
		IpAddress: host,
		Port:      uint(port),
		Path:      path,
	}
	return controller, nil
}

// buildCIPPath is used to parse the CIP Path, which consists of an addressing
// like <rack,slot> and is set by default to `1,0`
func buildCIPPath(pathStr string) (*bytes.Buffer, error) {
	if pathStr == "" {
		return nil, fmt.Errorf("path is empty")
	}

	parts := strings.Split(pathStr, ",")
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty CIP Path from split strings")
	}

	path := new(bytes.Buffer)
	for _, p := range parts {
		val, err := strconv.ParseUint(p, 10, 8)
		if err != nil {
			return nil, err
		}
		path.WriteByte(byte(val))
	}

	return path, nil
}

// parseAttributes parses the attributesConf into a list of CIPReadItems
func parseAttributes(attributesConf []*service.ParsedConfig) ([]*CIPReadItem, error) {
	var items []*CIPReadItem

	for _, attribute := range attributesConf {
		pathStr, err := attribute.FieldString("path")
		if err != nil {
			return nil, err
		}
		datatype, err := attribute.FieldString("type")
		if err != nil {
			return nil, err
		}

		// ignore error because it's optional
		alias, _ := attribute.FieldString("alias")

		// expected format: "class-instance-attribute"
		parts := strings.Split(pathStr, "-")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid attribute path: %s (expected Class-Instance-Attribute)", pathStr)
		}

		parsedClass, err := strconv.ParseUint(parts[0], 0, 16)
		if err != nil {
			return nil, fmt.Errorf("parsing CIPClass from %s: %v", parts[0], err)
		}
		parsedInst, err := strconv.ParseUint(parts[1], 0, 32)
		if err != nil {
			return nil, fmt.Errorf("parsing CIPInstance from %s: %v", parts[1], err)
		}
		parsedAttr, err := strconv.ParseUint(parts[2], 0, 16)
		if err != nil {
			return nil, fmt.Errorf("parsing CIPAttribute from %s: %v", parts[2], err)
		}

		// convert user string "bool", "real", etc. to CIPType
		// not sure if we will need this later on
		cipDatatype, err := parseCIPTypeFromString(datatype)
		if err != nil {
			return nil, err
		}

		converterFn, err := buildConverterFunc(datatype)
		if err != nil {
			return nil, err
		}

		item := &CIPReadItem{
			IsAttribute:   true,
			CIPClass:      gologix.CIPClass(parsedClass),
			CIPInstance:   gologix.CIPInstance(parsedInst),
			CIPAttribute:  gologix.CIPAttribute(parsedAttr),
			CIPDatatype:   cipDatatype,
			Alias:         alias,
			AttributeName: pathStr,
			ConverterFunc: converterFn,
		}
		items = append(items, item)
	}
	return items, nil
}

// parseTags parses the tagsConf into a list of CIPReadItems
func parseTags(tagsConf []*service.ParsedConfig) ([]*CIPReadItem, error) {
	var (
		items []*CIPReadItem
	)

	for _, tag := range tagsConf {
		isArray := false
		arrayLen := 1
		name, err := tag.FieldString("name")
		if err != nil {
			return nil, err
		}
		datatype, err := tag.FieldString("type")
		if err != nil {
			return nil, err
		}
		// ignore error because it's optional
		alias, _ := tag.FieldString("alias")

		datatypeLower := strings.ToLower(datatype)
		if strings.HasPrefix(datatypeLower, "arrayof") {
			isArray = true
			arrayLen, err = tag.FieldInt("length")
			if err != nil {
				return nil, err
			}
			datatypeLower = strings.TrimPrefix(datatypeLower, "arrayof")
		}

		cipDatatype, err := parseCIPTypeFromString(datatypeLower)
		if err != nil {
			return nil, err
		}

		// NOTE: this is currently not needed for reading tags
		//	converterFn, err := buildConverterFunc(datatype)
		//	if err != nil {
		//		return nil, err
		//	}

		item := &CIPReadItem{
			IsAttribute: false,
			IsArray:     isArray,
			TagName:     name,
			ArrayLength: arrayLen,
			CIPDatatype: cipDatatype,
			Alias:       alias,
			//ConverterFunc: converterFn,
		}
		items = append(items, item)
	}
	return items, nil
}

// parseCIPTypeFromString is used to parse the datatype provided in the `input.yaml`
// into the corresponding gologix.CIPType which is later needed for reading data
func parseCIPTypeFromString(datatype string) (gologix.CIPType, error) {
	// put datatype string to lower because some will input "bool" or "BOOL"
	switch datatype {
	case "bool":
		return gologix.CIPTypeBOOL, nil
	case "byte":
		return gologix.CIPTypeBYTE, nil
	case "word":
		return gologix.CIPTypeWORD, nil
	case "dword":
		return gologix.CIPTypeDWORD, nil
	case "uint8":
		return gologix.CIPTypeUSINT, nil
	case "uint16":
		return gologix.CIPTypeUINT, nil
	case "uint32":
		return gologix.CIPTypeUDINT, nil
	case "uint64":
		return gologix.CIPTypeULINT, nil
	case "int8":
		return gologix.CIPTypeSINT, nil
	case "int16":
		return gologix.CIPTypeINT, nil
	case "int32":
		return gologix.CIPTypeDINT, nil
	case "int64":
		return gologix.CIPTypeLINT, nil
	case "real", "float", "float32":
		return gologix.CIPTypeREAL, nil
	case "float64":
		return gologix.CIPTypeLREAL, nil
	case "string":
		return gologix.CIPTypeSTRING, nil
	case "array of octed":
		return gologix.CIPTypeBYTE, nil
	case "struct":
		return gologix.CIPTypeStruct, nil
	default:
		return gologix.CIPTypeUnknown, fmt.Errorf("unsupported CIP data type: %s", datatype)
	}
}

// buildConverterFunc is used to build the function, which is needed to convert
// the values into the correct datatype
// only used for attributes
func buildConverterFunc(datatype string) (func(*gologix.CIPItem) (any, error), error) {
	// to handle "BOOL" as well as "bool" and "bOOl"
	lowercaseDatatype := strings.ToLower(datatype)
	switch lowercaseDatatype {
	case "bool":
		return func(item *gologix.CIPItem) (any, error) {
			bit, err := item.Byte()
			if err != nil {
				return nil, err
			}
			return bit != 0, nil
		}, nil
	case "byte", "uint8":
		return func(item *gologix.CIPItem) (any, error) {
			val, err := item.Byte()
			if err != nil {
				return nil, err
			}
			return val, nil
		}, nil
	case "word":
		return func(item *gologix.CIPItem) (any, error) {
			val, err := item.Bytes()
			if err != nil {
				return nil, err
			}
			return val, nil
		}, nil
	case "dword":
		return func(item *gologix.CIPItem) (any, error) {
			val, err := item.Bytes()
			if err != nil {
				return nil, err
			}
			return val, nil
		}, nil
	case "uint16":
		return func(item *gologix.CIPItem) (any, error) {
			val, err := item.Uint16()
			if err != nil {
				return nil, err
			}
			return val, nil
		}, nil
	case "uint32":
		return func(item *gologix.CIPItem) (any, error) {
			val, err := item.Uint32()
			if err != nil {
				return nil, err
			}
			return val, nil
		}, nil
	case "uint64":
		return func(item *gologix.CIPItem) (any, error) {
			val, err := item.Uint64()
			if err != nil {
				return nil, err
			}
			return val, nil
		}, nil
	case "int16":
		return func(item *gologix.CIPItem) (any, error) {
			val, err := item.Int16()
			if err != nil {
				return nil, err
			}
			return val, nil
		}, nil
	case "int32":
		return func(item *gologix.CIPItem) (any, error) {
			val, err := item.Int32()
			if err != nil {
				return nil, err
			}
			return val, nil
		}, nil
	case "int64":
		return func(item *gologix.CIPItem) (any, error) {
			val, err := item.Int64()
			if err != nil {
				return nil, err
			}
			return val, nil
		}, nil
	case "real", "float", "float32":
		return func(item *gologix.CIPItem) (any, error) {
			bits, err := item.Uint32()
			if err != nil {
				return nil, err
			}
			fl := math.Float32frombits(bits)
			return fl, nil
		}, nil
	case "float64":
		return func(item *gologix.CIPItem) (any, error) {
			fl, err := item.Float64()
			if err != nil {
				return nil, err
			}
			return fl, nil
		}, nil
	case "string":
		return func(item *gologix.CIPItem) (any, error) {
			val, err := item.Bytes()
			if err != nil {
				return nil, err
			}
			return string(val), nil
		}, nil
	case "array of octed":
		return func(item *gologix.CIPItem) (any, error) {
			val, err := item.Bytes()
			if err != nil {
				return nil, err
			}

			return val, nil
		}, nil
	default:
		return nil, fmt.Errorf("Failed to build converterFunc, unsupported datatype: %s", datatype)
	}
}
