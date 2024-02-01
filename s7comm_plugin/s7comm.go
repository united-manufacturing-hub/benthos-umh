// Copyright 2023 UMH Systems GmbH
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

package s7comm_plugin

import (
	"context"
	"fmt"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/robinson/gos7" // gos7 is a Go client library for interacting with Siemens S7 PLCs.
)

//------------------------------------------------------------------------------

// S7CommInput struct defines the structure for our custom Benthos input plugin.
// It holds the configuration necessary to establish a connection with a Siemens S7 PLC,
// along with the read requests to fetch data from the PLC.
type S7CommInput struct {
	tcpDevice    string                 // IP address of the S7 PLC.
	rack         int                    // Rack number where the CPU resides.
	slot         int                    // Slot number where the CPU resides.
	client       gos7.Client            // S7 client for communication.
	handler      *gos7.TCPClientHandler // TCP handler to manage the connection.
	log          *service.Logger        // Logger for logging plugin activity.
	readRequests []ReadRequest          // List of data blocks to read from the PLC.
}

// ReadRequest defines a single read operation from the PLC.
// It specifies which data block to read and the range within it.
type ReadRequest struct {
	DBNumber int // Data Block (DB) number in the PLC to read from.
	Start    int // Starting byte address within the DB.
	Size     int // Number of bytes to read starting from the Start address.
}

// S7CommConfigSpec defines the configuration options available for the S7CommInput plugin.
// It outlines the required information to establish a connection with the PLC and the data to be read.
var S7CommConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that reads data from Siemens S7 PLCs. Created & maintained by the United Manufacturing Hub. About us: www.umh.app").
	Description("This input plugin allows Benthos to read data directly from Siemens S7 PLCs using the S7comm protocol. " +
		"Configure the plugin by specifying the PLC's IP address, rack and slot numbers, and the details of the data blocks to read.").
	Field(service.NewStringField("tcpDevice").Description("IP address of the S7 PLC.")).
	Field(service.NewIntField("rack").Description("Rack number of the PLC. Identifies the physical location of the CPU within the PLC rack.")).
	Field(service.NewIntField("slot").Description("Slot number of the PLC. Identifies the CPU slot within the rack.")).
	Field(service.NewObjectListField("readRequests", service.NewObjectField("readRequest",
		service.NewIntField("DBNumber").Description("The DB number to read from. Specifies the data block within the PLC."),
		service.NewIntField("Start").Description("The start address within the DB. Specifies the starting byte for the read operation."),
		service.NewIntField("Size").Description("The amount of data to read. Specifies the number of bytes to read starting from the 'Start' address."))))

// newS7CommInput is the constructor function for S7CommInput. It parses the plugin configuration,
// establishes a connection with the S7 PLC, and initializes the input plugin instance.
func newS7CommInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
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

	readRequests, err := conf.FieldObjectList("readRequests")
	if err != nil {
		return nil, err
	}
	parsedReadRequests := make([]ReadRequest, len(readRequests))
	for i, req := range readRequests {
		dbNumber, _ := req.FieldInt("DBNumber")
		start, _ := req.FieldInt("Start")
		size, _ := req.FieldInt("Size")
		parsedReadRequests[i] = ReadRequest{
			DBNumber: dbNumber,
			Start:    start,
			Size:     size,
		}
	}

	m := &S7CommInput{
		tcpDevice:    tcpDevice,
		rack:         rack,
		slot:         slot,
		log:          mgr.Logger(),
		readRequests: parsedReadRequests,
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
	g.handler = gos7.NewTCPClientHandler(g.tcpDevice, g.rack, g.slot)
	g.handler.Timeout = 200 * time.Second
	g.handler.IdleTimeout = 200 * time.Second

	err := g.handler.Connect()
	if err != nil {
		g.log.Errorf("Failed to connect to S7 PLC at %s: %v", g.tcpDevice, err)
		return err
	}

	g.client = gos7.NewClient(g.handler)
	g.log.Infof("Successfully connected to S7 PLC at %s", g.tcpDevice)

	cpuInfo, err := g.client.GetCPUInfo()
	if err != nil {
		g.log.Errorf("Failed to get CPU information: %v", err)
	} else {
		g.log.Infof("CPU Information: %s", cpuInfo)
	}

	return nil
}

func (g *S7CommInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if g.client == nil {
		return nil, nil, fmt.Errorf("S7Comm client is not initialized")
	}

	msgs := make(service.MessageBatch, 0, len(g.readRequests))
	for _, req := range g.readRequests {
		buffer := make([]byte, req.Size)
		err := g.client.AGReadDB(req.DBNumber, req.Start, req.Size, buffer)
		if err != nil {
			g.log.Errorf("Failed to read from DB%d: %v", req.DBNumber, err)
			continue
		}

		msg := service.NewMessage(buffer)
		msgs = append(msgs, msg)
	}

	return msgs, func(ctx context.Context, err error) error {
		return nil // Acknowledgment handling here if needed
	}, nil
}

func (g *S7CommInput) Close(ctx context.Context) error {
	if g.handler != nil {
		g.handler.Close()
		g.handler = nil
		g.client = nil
	}

	return nil
}
