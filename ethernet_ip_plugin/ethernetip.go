// Copyright 2024 UMH Systems GmbH
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

package ethernetip_plugin

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/danomagnum/gologix"
	"github.com/redpanda-data/benthos/v4/public/service"
	// gos7 is a Go client library for interacting with Siemens S7 PLCs.
)

var ()

type EthernetIPInput struct {
	Controller   string // IP address of the Ethernet/IP PLC.
	Path         string
	PollRate     time.Duration
	ListAllTags  bool
	UseMultiRead bool

	// addresses for readable data either as an attribute or as a tag
	Attributes []*AttributePathWithConverter
	Tags       []*TagWithConverter

	Client gologix.Client  // Ethernet/IP client for communication.
	Log    *service.Logger // Logger for logging plugin activity.
}

// This struct reflects the path for the attributes we want to read.
// The converterFunc is used to convert it's given datatype to a readable format.
type AttributePathWithConverter struct {
	Class         gologix.CIPClass     // uint16
	Instance      gologix.CIPInstance  // uint32
	Attribute     gologix.CIPAttribute // uint16
	ConverterFunc converterFunc
}

// This struct basically combines the Tag with it's converterFunc, which is needed
// to get the data in a readable format.
type TagWithConverter struct {
	Name          string
	ConverterFunc converterFunc
}

type converterFunc func([]byte) interface{}

var EthernetIPConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that reads data from Ethernet/IP Devices. Created & maintained by the United Manufacturing Hub. About us: www.umh.app").
	Description("This input plugin enables Benthos to read data directly from Ethernet/IP-Devices using the CIP protocol. " +
		"Configure the plugin by specifying the PLC's IP address, path and pollRate, and the data blocks to read.").
	Field(service.NewStringField("controller").Description("IP address of the Ethernet/IP-Device.")).
	Field(service.NewStringField("path").Description("").Default("1,0")).
	Field(service.NewIntField("pollRate").Description("").Default(1)).
	Field(service.NewBoolField("listAllTags").Description("").Default(false)).
	Field(service.NewBoolField("useMultiRead").Description("").Default(true)).
	Field(service.NewObjectListField("attributes",
		service.NewStringField("path").Description(""),
		service.NewStringField("type").Description("")).
		Description("")).
	Field(service.NewObjectListField("tags",
		service.NewStringField("name").Description(""),
		service.NewStringField("type").Description("")).
		Description(""))

// newEthernetIPInput is the constructor function for EthernetIPInput. It parses the plugin configuration,
// establishes a connection with the Ethernet/IP device, and initializes the input plugin instance.
func newEthernetIPInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	controller, err := conf.FieldString("controller")
	if err != nil {
		return nil, err
	}

	path, err := conf.FieldString("path")
	if err != nil {
		return nil, err
	}

	pollRate, err := conf.FieldInt("pollRate")
	if err != nil {
		return nil, err
	}

	listAllTags, err := conf.FieldBool("listAllTags")
	if err != nil {
		return nil, err
	}

	useMultiRead, err := conf.FieldBool("useMultiRead")
	if err != nil {
		return nil, err
	}

	attributesConf, err := conf.FieldObjectList("attributes")
	if err != nil {
		return nil, err
	}

	tagsConf, err := conf.FieldObjectList("tags")
	if err != nil {
		return nil, err
	}

	var attributesList []*AttributePathWithConverter
	for _, attr := range attributesConf {
		path, err := attr.FieldString("path")
		if err != nil {
			return nil, err
		}

		datatype, err := attr.FieldString("type")
		if err != nil {
			return nil, err
		}

		attribute, err := handleFieldAttribute(path, datatype)
		if err != nil {
			return nil, err
		}

		attributesList = append(attributesList, attribute)
	}

	var tagsList []*TagWithConverter
	for _, tag := range tagsConf {
		name, err := tag.FieldString("name")
		if err != nil {
			return nil, err
		}

		datatype, err := tag.FieldString("type")
		if err != nil {
			return nil, err
		}

		tag, err := handleFieldTags(name, datatype)
		if err != nil {
			return nil, err
		}

		tagsList = append(tagsList, tag)
	}

	// if you don't want to read any data lol
	if len(attributesConf) == 0 && len(tagsConf) == 0 && !listAllTags {
		return nil, fmt.Errorf("no attributes or tags to read data from provided")
	}

	m := &EthernetIPInput{
		Controller:   controller,
		Path:         path,
		PollRate:     time.Duration(pollRate) * time.Second,
		ListAllTags:  listAllTags,
		UseMultiRead: useMultiRead,
		Log:          mgr.Logger(),

		// addresses to read data
		Attributes: attributesList,
		Tags:       tagsList,
	}

	return service.AutoRetryNacksBatched(m), nil
}

func init() {
	err := service.RegisterBatchInput(
		"ethernet-ip-cip", EthernetIPConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newEthernetIPInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func (g *EthernetIPInput) Connect(ctx context.Context) error {

	return nil
}

func (g *EthernetIPInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	return nil, nil, nil
}

func (g *EthernetIPInput) Close(ctx context.Context) error {

	return nil
}

func handleFieldAttribute(path string, datatype string) (*AttributePathWithConverter, error) {
	pathSplitted := strings.Split(path, "-")

	class, err := strconv.Atoi(pathSplitted[0])
	if err != nil {
		return nil, err
	}

	instance, err := strconv.Atoi(pathSplitted[1])
	if err != nil {
		return nil, err
	}

	attribute, err := strconv.Atoi(pathSplitted[2])
	if err != nil {
		return nil, err
	}

	a := &AttributePathWithConverter{}
	return a, nil
}

func handleFieldTags(name string, datatype string) (*TagWithConverter, error) {

	return nil, nil
}

func handleConverterFunc(datatype string) (func([]byte) any, error) {
	// to handle "BOOL" as well as "bool" and "bOOl"
	lowercaseDatatype := strings.ToLower(datatype)
	switch lowercaseDatatype {
	case "bool":
		return func([]byte) any {
			return nil
		}, nil
	case "uint16":
		return func([]byte) any {
			return nil
		}, nil
	case "string":
		return func([]byte) any {
			return nil
		}, nil
	case "byte":
		return func([]byte) any {
			return nil
		}, nil
	case "real":
		return func([]byte) any {
			return nil
		}, nil
	case "word":
		return func([]byte) any {
			return nil
		}, nil
	case "dword":
		return func([]byte) any {
			return nil
		}, nil
	default:
		return nil, fmt.Errorf("error handling the converter func, datatype %s not available", lowercaseDatatype)
	}
}
