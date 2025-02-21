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
	"context"
	"fmt"
	"log/slog"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/danomagnum/gologix"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	vendorIdDefault      = 0x9999
	connSizeLargeDefault = 4000
	keepAliveFreq        = time.Second * 30
	//rpiDefault           = time.Millisecond * 2500
	socketTimeoutDefault = time.Second * 10
)

type EIPInput struct {
	// IP address of the EIP plc
	Controller   *gologix.Controller
	Path         string
	PollRate     time.Duration
	ListAllTags  bool
	UseMultiRead bool

	// addresses for readable data either as an attribute or as a tag
	Items []*CIPReadItem

	// EIP client for communicatino
	Client *gologix.Client
	Log    *service.Logger
}

// This struct should unify the Attributes and Tags to have 1 Struct
// which is then used for addressing the data.
type CIPReadItem struct {
	IsAttribute bool

	// attribute addressing
	CIPClass     uint16
	CIPInstance  uint32
	CIPAttribute uint16

	// tag addressing
	TagName string

	// unified fields
	Alias string

	// the datatype string from input
	CIPDatatype string

	// not sure on that
	CIPLibType gologix.CIPType

	// needed to transform data into given type
	ConverterFunc func(*gologix.CIPItem) (any, error)
}

var EthernetIPConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that reads data from Ethernet/IP Devices. Created & maintained by the United Manufacturing Hub. About us: www.umh.app").
	Description("This input plugin enables Benthos to read data directly from Ethernet/IP-Devices using the CIP protocol. " +
		"Configure the plugin by specifying the PLC's IP address, path and pollRate, and the data blocks to read.").
	Field(service.NewStringField("endpoint").Description("IP address of the Ethernet/IP-Device.")).
	Field(service.NewStringField("path").Description("").Default("1,0")).
	Field(service.NewIntField("pollRate").Description("").Default(2500)).
	Field(service.NewBoolField("listAllTags").Description("").Default(false)).
	Field(service.NewBoolField("useMultiRead").Description("").Default(true)).
	Field(service.NewObjectListField("attributes",
		service.NewStringField("path").Description(""),
		service.NewStringField("type").Description(""),
		service.NewStringField("alias").Description("").Optional()).
		Description("")).
	Field(service.NewObjectListField("tags",
		service.NewStringField("name").Description(""),
		service.NewStringField("type").Description(""),
		service.NewStringField("alias").Description("").Optional()).
		Description(""))

// newEthernetIPInput is the constructor function for EthernetIPInput. It parses the plugin configuration,
// establishes a connection with the Ethernet/IP device, and initializes the input plugin instance.
func newEthernetIPInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	endpoint, err := conf.FieldString("endpoint")
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

	attributesItems, err := parseAttributes(attributesConf)
	if err != nil {
		return nil, err
	}

	tagsItems, err := parseTags(tagsConf)
	if err != nil {
		return nil, err
	}

	var allItems []*CIPReadItem
	allItems = append(allItems, attributesItems...)
	allItems = append(allItems, tagsItems...)

	// if you don't want to read any data lol
	if len(allItems) == 0 && !listAllTags {
		return nil, fmt.Errorf("no attributes or tags to read data from provided")
	}

	controller, err := parseController(endpoint, path)
	if err != nil {
		return nil, err
	}

	m := &EIPInput{
		Controller:   controller,
		Path:         path,
		PollRate:     time.Duration(pollRate) * time.Millisecond,
		ListAllTags:  listAllTags,
		UseMultiRead: useMultiRead,
		Log:          mgr.Logger(),

		// addresses to read data
		Items: allItems,
	}

	return service.AutoRetryNacksBatched(m), nil
}

func init() {
	err := service.RegisterBatchInput(
		"ethernetip", EthernetIPConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newEthernetIPInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func (g *EIPInput) Connect(ctx context.Context) error {
	g.Client = &gologix.Client{
		Controller:         *g.Controller,
		VendorId:           vendorIdDefault,
		ConnectionSize:     connSizeLargeDefault,
		AutoConnect:        true,
		KeepAliveAutoStart: false,
		KeepAliveFrequency: keepAliveFreq,
		KeepAliveProps:     []gologix.CIPAttribute{1, 2, 3, 4, 10},
		// this is the Request Packet Interval
		RPI:           g.PollRate,
		SocketTimeout: socketTimeoutDefault,
		KnownTags:     make(map[string]gologix.KnownTag),
		// we only want to use our logs not the gologix-logs here
		//Logger: slog.New(slog.DiscardHandler),
		// but for now we want to see some logs here:
		Logger: slog.Default(),
	}

	err := g.Client.Connect()
	if err != nil {
		g.Log.Errorf("Failed to connect to EIP controller: %v", err)
		return err
	}

	// just an example which works for WAGO maybe reproducable for other devices
	err = g.logDeviceProperties()
	if err != nil {
		g.Log.Warnf("Unable to get device properties: %v", err)
	}

	g.Log.Infof("EIP connection established at %s", g.Controller.IpAddress)

	return nil
}

func (g *EIPInput) logDeviceProperties() error {
	// this should be the device Name Attribute
	deviceNameAttribute, err := g.Client.GetAttrSingle(1, 1, 7)
	if err != nil {
		return err
	}

	data, err := deviceNameAttribute.Bytes()
	if err != nil {
		return err
	}

	deviceName := string(data)
	g.Log.Infof("EIP Device Properties:")
	g.Log.Infof("    Device Name: %s", deviceName)

	return nil
}

func (g *EIPInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	return nil, nil, nil
}

func (g *EIPInput) Close(ctx context.Context) error {
	err := g.Client.Disconnect()
	if err != nil {
		return err
	}

	return nil
}

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

		class, err := strconv.ParseUint(parts[0], 0, 16)
		if err != nil {
			return nil, fmt.Errorf("parsing CIPClass from %s: %v", parts[0], err)
		}
		instance, err := strconv.ParseUint(parts[1], 0, 32)
		if err != nil {
			return nil, fmt.Errorf("parsing CIPInstance from %s: %v", parts[1], err)
		}
		attribute, err := strconv.ParseUint(parts[2], 0, 16)
		if err != nil {
			return nil, fmt.Errorf("parsing CIPAttribute from %s: %v", parts[2], err)
		}

		// convert user string "bool", "real", etc. to CIPType
		// not sure if we will need this later on
		cipLibType, err := parseCIPTypeFromString(datatype)
		if err != nil {
			return nil, err
		}

		item := &CIPReadItem{
			IsAttribute:   true,
			CIPClass:      uint16(class),
			CIPInstance:   uint32(instance),
			CIPAttribute:  uint16(attribute),
			CIPDatatype:   datatype,
			CIPLibType:    cipLibType,
			Alias:         alias,
			ConverterFunc: buildConverterFunc(datatype),
		}
		items = append(items, item)
	}
	return items, nil
}

// parseTags parses the tagsConf into a list of CIPReadItems
func parseTags(tagsConf []*service.ParsedConfig) ([]*CIPReadItem, error) {
	var items []*CIPReadItem

	for _, tag := range tagsConf {
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

		cipLibType, err := parseCIPTypeFromString(datatype)
		if err != nil {
			return nil, err
		}

		item := &CIPReadItem{
			IsAttribute:   false,
			TagName:       name,
			CIPDatatype:   datatype,
			CIPLibType:    cipLibType,
			Alias:         alias,
			ConverterFunc: buildConverterFunc(datatype),
		}
		items = append(items, item)
	}
	return items, nil
}

// not yet sure if this is needed
func parseCIPTypeFromString(datatype string) (gologix.CIPType, error) {
	// put datatype string to lower because some will input "bool" or "BOOL"
	switch strings.ToLower(datatype) {
	case "bool":
		return gologix.CIPTypeBOOL, nil
	case "uint16", "word":
		return gologix.CIPTypeUINT, nil
	case "int16":
		return gologix.CIPTypeINT, nil
	case "real", "float", "float32":
		return gologix.CIPTypeREAL, nil
	case "string":
		return gologix.CIPTypeSTRING, nil
	default:
		return gologix.CIPTypeUnknown, fmt.Errorf("unsupported CIP data type: %s", datatype)
	}
}

func buildConverterFunc(datatype string) func(*gologix.CIPItem) (any, error) {
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
		}
	case "uint16":
		return func(item *gologix.CIPItem) (any, error) {
			val, err := item.Uint16()
			if err != nil {
				return nil, err
			}
			return val, nil
		}
	case "int16":
		return func(item *gologix.CIPItem) (any, error) {
			val, err := item.Int16()
			if err != nil {
				return nil, err
			}
			return val, nil
		}
	case "real", "float", "float32":
		return func(item *gologix.CIPItem) (any, error) {
			bits, err := item.Uint32()
			if err != nil {
				return nil, err
			}
			fl := math.Float32frombits(bits)
			return fl, nil
		}
	case "string":
		return func(item *gologix.CIPItem) (any, error) {
			// not sure yet
			return nil, nil
		}
	default:
		return nil
	}
}
