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
	"errors"
	"fmt"
	"github.com/robinson/gos7"
	"time"

	"github.com/libplctag/goplctag"
	"github.com/redpanda-data/benthos/v4/public/service"
)

//------------------------------------------------------------------------------

type EthernetIPTag struct {
	Key           string
	Type          string
	Bit           int
	Offset        int
	Path          string
	ConverterFunc converterFunc
}

// EthernetIPInput struct defines the structure for our custom Benthos input plugin.
// It holds the configuration necessary to establish a connection with a Ethernet/IP PLC,
// along with the read requests to fetch data from the PLC.
type EthernetIPInput struct {
	Gateway  string          // IP address of the PLC.
	Protocol string          // Protocol to use for communication. Can be ab_eip, ab_cip
	Path     string          // Routing path for the Tags. (default: 1,0)
	PLCType  string          // Type of the PLC. (default: controllogix). Can be plc,plc5,slc,slc500,micrologix,mlgx,compactlogix,clgx,lgx,controllogix,contrologix,flexlogix,flgx
	Timeout  time.Duration   // Time duration before a connection attempt or read request times out.
	Log      *service.Logger // Logger for logging plugin activity.
	Tags     []EthernetIPTag // List of items to read from the PLC
}

type converterFunc func([]byte) interface{}

// EthernetIPConfigSpec defines the configuration options available for the EthernetIPInput plugin.
// It outlines the required information to establish a connection with the PLC and the data to be read.
var EthernetIPConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that reads data from an Ethernet/IP PLC. Created & maintained by the United Manufacturing Hub. About us: www.umh.app").
	Description("This input plugin enables Benthos to read data directly from SEthernet/IP PLC.").
	Field(service.NewStringField("tcpDevice").Description("IP address of the S7 PLC.")).
	Field(service.NewIntField("rack").Description("Rack number of the PLC. Identifies the physical location of the CPU within the PLC rack.").Default(0)).
	Field(service.NewIntField("slot").Description("Slot number of the PLC. Identifies the CPU slot within the rack.").Default(1)).
	Field(service.NewIntField("batchMaxSize").Description("Maximum count of addresses to be bundled in one batch-request (PDU size).").Default(480)).
	Field(service.NewIntField("timeout").Description("The timeout duration in seconds for connection attempts and read requests.").Default(10)).
	Field(service.NewBoolField("disableCPUInfo").Description("Set this to true to not fetch CPU information from the PLC. Should be used when you get the error 'Failed to get CPU information'").Default(false)).
	Field(service.NewStringListField("addresses").Description("List of S7 addresses to read in the format '<area>.<type><address>[.extra]', e.g., 'DB5.X3.2', 'DB5.B3', or 'DB5.C3'. " +
		"Address formats include direct area access (e.g., DB1 for data block one) and data types (e.g., X for bit, B for byte)."))

// newS7CommInput is the constructor function for S7CommInput. It parses the plugin configuration,
// establishes a connection with the S7 PLC, and initializes the input plugin instance.
func newS7CommInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {

	goplctag.
		tcpDevice, err := conf.FieldString("tcpDevice")
	if err != nil {
		return nil, err
	}

	rack, err := conf.FieldInt("rack")
	if err != nil {
		return nil, err
	}

	slot, err := conf.FieldInt("slot")
	if err != nil {
		return nil, err
	}

	addresses, err := conf.FieldStringList("addresses")
	if err != nil {
		return nil, err
	}

	batchMaxSize, err := conf.FieldInt("batchMaxSize")
	if err != nil {
		return nil, err
	}

	timeoutInt, err := conf.FieldInt("timeout")
	if err != nil {
		return nil, err
	}

	disableCPUInfo, err := conf.FieldBool("disableCPUInfo")
	if err != nil {
		return nil, err
	}

	// Now split the addresses into batches based on the batchMaxSize
	batches, err := ParseAddresses(addresses, batchMaxSize)
	if err != nil {
		return nil, err
	}

	m := &S7CommInput{
		TcpDevice:      tcpDevice,
		Rack:           rack,
		Slot:           slot,
		Log:            mgr.Logger(),
		Batches:        batches,
		BatchMaxSize:   batchMaxSize,
		Timeout:        time.Duration(timeoutInt) * time.Second,
		DisableCPUInfo: disableCPUInfo,
	}

	return service.AutoRetryNacksBatched(m), nil
}

//------------------------------------------------------------------------------

func init() {
	err := service.RegisterBatchInput(
		"s7comm", S7CommConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newS7CommInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func (g *S7CommInput) Connect(ctx context.Context) error {
	g.Handler = gos7.NewTCPClientHandler(g.TcpDevice, g.Rack, g.Slot)
	g.Handler.Timeout = g.Timeout
	g.Handler.IdleTimeout = g.Timeout

	err := g.Handler.Connect()
	if err != nil {
		g.Log.Errorf("Failed to connect to S7 PLC at %s: %v", g.TcpDevice, err)
		return err
	}

	g.Client = gos7.NewClient(g.Handler)
	g.Log.Infof("Successfully connected to S7 PLC at %s", g.TcpDevice)

	// Fetch and show CPU information, but only if the user has not disabled it
	if !g.DisableCPUInfo {
		cpuInfo, err := g.Client.GetCPUInfo()
		if err != nil {
			g.Log.Warnf("Failed to get CPU information: %v", err)
		} else {
			g.Log.Infof("CPU Information: %s", cpuInfo)
		}
	}

	return nil
}

func (g *S7CommInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if g.Client == nil {
		return nil, nil, fmt.Errorf("S7Comm client is not initialized")
	}

	msgs := make(service.MessageBatch, 0)
	for i, b := range g.Batches {

		// Create a new batch to read
		batchToRead := make([]gos7.S7DataItem, len(b))
		for i, item := range b {
			batchToRead[i] = item.Item
		}

		// Read the batch
		g.Log.Debugf("Reading batch %d...", i+1)
		if err := g.Client.AGReadMulti(batchToRead, len(batchToRead)); err != nil {
			// Try to reconnect and skip this gather cycle to avoid hammering
			// the network if the server is down or under load.
			errMsg := fmt.Sprintf("Failed to read batch %d: %v. Reconnecting...", i+1, err)

			// Return the error message so Benthos can handle it appropriately
			return nil, nil, errors.New(errMsg)
		}

		// Read the data from the batch and convert it using the converter function
		buffer := make([]byte, 0)

		for _, item := range b {
			// Execute the converter function to get the converted data
			convertedData := item.ConverterFunc(item.Item.Data)

			// Convert any type of convertedData to a string.
			// The fmt.Sprintf function is used here for its ability to handle various types gracefully.
			dataAsString := fmt.Sprintf("%v", convertedData)

			// Convert the string representation to a []byte
			dataAsBytes := []byte(dataAsString)

			// Append the converted data as bytes to the buffer
			buffer = append(buffer, dataAsBytes...)

			// Create a new message with the current state of the buffer
			// Note: Depending on your requirements, you may want to reset the buffer
			// after creating each message or keep accumulating data in it.
			msg := service.NewMessage(buffer)
			msg.MetaSet("s7_address", item.Address)

			// Append the new message to the msgs slice
			msgs = append(msgs, msg)
		}
	}

	time.Sleep(time.Second)

	return msgs, func(ctx context.Context, err error) error {
		return nil // Acknowledgment handling here if needed
	}, nil
}

func (g *S7CommInput) Close(ctx context.Context) error {
	if g.Handler != nil {
		g.Handler.Close()
		g.Handler = nil
		g.Client = nil
	}

	return nil
}
