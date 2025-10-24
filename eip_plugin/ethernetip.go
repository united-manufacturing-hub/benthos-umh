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
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/danomagnum/gologix"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	vendorIdDefault      = 0x9999
	connSizeLargeDefault = 4000
	keepAliveFreq        = time.Second * 30
	// rpiDefault           = time.Millisecond * 2500
	socketTimeoutDefault = time.Second * 10
)

type EIPInput struct {
	// IP address of the EIP plc
	Controller   *gologix.Controller
	Path         string
	PollRate     time.Duration
	ListAllTags  bool
	UseMultiRead bool

	// advanced connection settings
	SocketTimeoutMs int

	// addresses for readable data either as an attribute or as a tag
	Items   []*CIPReadItem
	ItemMap map[string]any

	// EIP client for communicatino
	CIP CIPReader
	//	Client *gologix.Client
	Log *service.Logger
}

// This struct should unify the Attributes and Tags to have 1 Struct
// which is then used for addressing the data.
type CIPReadItem struct {
	IsAttribute bool
	IsArray     bool

	// attribute addressing
	CIPClass     gologix.CIPClass
	CIPInstance  gologix.CIPInstance
	CIPAttribute gologix.CIPAttribute

	// tag addressing
	TagName       string
	AttributeName string

	ArrayLength int

	// unified fields
	Alias string

	// the datatype string from input
	CIPDatatype gologix.CIPType

	// needed to transform data into given type
	ConverterFunc func(*gologix.CIPItem) (any, error)
}

var EthernetIPConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that reads data from Ethernet/IP Devices. Created & maintained by the United Manufacturing Hub. About us: www.umh.app").
	Description("This input plugin enables Benthos to read data directly from Ethernet/IP-Devices using the CIP protocol. " +
		"Configure the plugin by specifying the PLC's IP address, path and pollRate, and the data blocks to read.").
	Field(service.NewStringField("endpoint").Description("IP address of the Ethernet/IP-Device.")).
	Field(service.NewStringField("path").Description("").Default("1,0")).
	Field(service.NewIntField("pollRate").Description("The rate in milliseconds on which we try to read data out of the plc.").Default(1000)).
	Field(service.NewBoolField("listAllTags").Description("You can use this option to list all available Tags, but only specific controllers support this method.").Default(false)).
	Field(service.NewBoolField("useMultiRead").Description("You can use this option to increase the reading time, but be aware that only specific controllers support this method.").Default(true)).
	Field(service.NewIntField("socketTimeoutMs").
		Description("The timeout in milliseconds for socket operations (connection establishment, reads, and writes).").
		Default(10000).
		Examples(5000, 10000, 30000).
		Optional().
		Advanced()).
	Field(service.NewObjectListField("attributes",
		service.NewStringField("path").Description("The Path consists of the following: CIP-Class - CIP-Instance - CIP-Attribute, e.g. 1-1-1. They might vary based on which controller you're using."),
		service.NewStringField("type").Description("The type of the attribute you want to read: e.g. 'bool', 'int16', 'byte'."),
		service.NewStringField("alias").Description("You can set an alias so the data will be stored with this alias set as name via metadata.").Optional()).
		Description("")).
	Field(service.NewObjectListField("tags",
		service.NewStringField("name").Description("The tag name is usually provided by something like this: `Program:Gologix.MyTagSet.TestBool`"),
		service.NewStringField("type").Description("The type of the tag you want to read: e.g. 'bool', 'int16', 'byte'."),
		service.NewIntField("length").Description("The Length of the array, when specified as 'arrayof...'").Default(1),
		service.NewStringField("alias").Description("You can set an alias so the data will be stored with this alias set as name via metadata.").Optional()).
		Description(""))

// NewEthernetIPInput is the constructor function for EthernetIPInput. It parses the plugin configuration,
// establishes a connection with the Ethernet/IP device, and initializes the input plugin instance.
func NewEthernetIPInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
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

	socketTimeoutMs, err := conf.FieldInt("socketTimeoutMs")
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

	itemMap := make(map[string]any)
	if useMultiRead {
		itemMap, err = parseTagsIntoMap(tagsItems)
		if err != nil {
			return nil, err
		}
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

		// advanced connection settings
		SocketTimeoutMs: socketTimeoutMs,

		// addresses to read data
		Items:   allItems,
		ItemMap: itemMap,
	}

	return service.AutoRetryNacksBatched(m), nil
}

func init() {
	err := service.RegisterBatchInput(
		"ethernetip", EthernetIPConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return NewEthernetIPInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func (g *EIPInput) Connect(_ context.Context) error {
	if g.CIP == nil {
		g.CIP = &gologix.Client{
			Controller:         *g.Controller,
			VendorId:           vendorIdDefault,
			ConnectionSize:     connSizeLargeDefault,
			AutoConnect:        true,
			KeepAliveAutoStart: false,
			KeepAliveFrequency: keepAliveFreq,
			KeepAliveProps:     []gologix.CIPAttribute{1, 2, 3, 4, 10},
			// this is the Request Packet Interval
			RPI:           g.PollRate,
			SocketTimeout: time.Duration(g.SocketTimeoutMs) * time.Millisecond,
			KnownTags:     make(map[string]gologix.KnownTag),
			// NOTE:
			// we only want to use our logs not the gologix-logs here
			Logger: slog.New(slog.DiscardHandler),
			// but for now we want to see some logs here:
			// Logger: slog.Default(),
		}
	}

	err := g.CIP.Connect()
	if err != nil {
		g.Log.Errorf("Failed to connect to EIP controller: %v", err)
		return err
	}

	// just an example which works for WAGO maybe reproducible for other devices
	err = g.logDeviceProperties()
	if err != nil {
		g.Log.Warnf("Unable to get device properties: %v", err)
	}

	g.Log.Infof("EIP connection established at %s", g.Controller.IpAddress)

	return nil
}

func (g *EIPInput) logDeviceProperties() error {
	vendorIDAttr, err := g.CIP.GetAttrSingle(1, 1, 1)
	if err != nil {
		return err
	}
	deviceTypeAttr, err := g.CIP.GetAttrSingle(1, 1, 2)
	if err != nil {
		return err
	}
	productCodeAttr, err := g.CIP.GetAttrSingle(1, 1, 3)
	if err != nil {
		return err
	}
	serialAttr, err := g.CIP.GetAttrSingle(1, 1, 6)
	if err != nil {
		return err
	}
	productNameAttr, err := g.CIP.GetAttrSingle(1, 1, 7)
	if err != nil {
		return err
	}

	vendorID, err := vendorIDAttr.Int16()
	if err != nil {
		return err
	}
	deviceType, err := deviceTypeAttr.Int16()
	if err != nil {
		return err
	}
	productCode, err := productCodeAttr.Int16()
	if err != nil {
		return err
	}
	serial, err := serialAttr.Uint32()
	if err != nil {
		return err
	}
	deviceNameBytes, err := productNameAttr.Bytes()
	if err != nil {
		return err
	}

	deviceName := string(deviceNameBytes)

	g.Log.Infof("EIP Device Information:")
	g.Log.Infof("    Device Name: %s", deviceName)
	g.Log.Infof("    Vendor ID: %v", vendorID)
	g.Log.Infof("    Device Type: %v", deviceType)
	g.Log.Infof("    Product Code: %v", productCode)
	g.Log.Infof("    Serial: %v", serial)

	return nil
}

func (g *EIPInput) ReadBatch(_ context.Context) (service.MessageBatch, service.AckFunc, error) {
	var msgs service.MessageBatch

	for _, item := range g.Items {
		// read either tags or attributes
		dataAsString, err := g.readTagsOrAttributes(item)
		if err != nil {
			// service.ErrNotconnected
			return nil, nil, err
		}

		// convert the dataAsString into bytes
		dataAsBytes := []byte(dataAsString)

		msg, err := CreateMessageFromValue(dataAsBytes, item)
		if err != nil {
			// service.ErrNotconnected
			return nil, nil, err
		}

		// append the new message to the msgs slice
		msgs = append(msgs, msg)
	}

	// not sure if we could just set a global "pollRate" for the plc
	time.Sleep(g.PollRate)

	return msgs, func(_ context.Context, _ error) error {
		// for now
		return nil
	}, nil
}

func (g *EIPInput) Close(_ context.Context) error {
	err := g.CIP.Disconnect()
	if err != nil {
		return err
	}

	return nil
}
