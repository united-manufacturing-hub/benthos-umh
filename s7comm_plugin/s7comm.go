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

package s7comm_plugin

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/robinson/gos7" // gos7 is a Go client library for interacting with Siemens S7 PLCs.
)

const addressRegexp = `^(?P<area>[A-Z]+)(?P<no>[0-9]+)\.(?P<type>[A-Z]+)(?P<start>[0-9]+)(?:\.(?P<extra>.*))?$`

var (
	regexAddr = regexp.MustCompile(addressRegexp)
	// Area mapping taken from https://github.com/robinson/gos7/blob/master/client.go
	areaMap = map[string]int{
		"PE": 0x81, // process inputs
		"PA": 0x82, // process outputs
		"MK": 0x83, // Merkers
		"DB": 0x84, // DB
		"C":  0x1C, // counters
		"T":  0x1D, // timers
	}
	// Word-length mapping taken from https://github.com/robinson/gos7/blob/master/client.go
	wordLenMap = map[string]int{
		"X":  0x01, // Bit
		"B":  0x02, // Byte (8 bit)
		"C":  0x03, // Char (8 bit)
		"S":  0x03, // String (8 bit)
		"W":  0x04, // Word (16 bit)
		"I":  0x05, // Integer (16 bit)
		"DW": 0x06, // Double Word (32 bit)
		"DI": 0x07, // Double integer (32 bit)
		"R":  0x08, // IEEE 754 real (32 bit)
		// see https://support.industry.siemens.com/cs/document/36479/date_and_time-format-for-s7-?dti=0&lc=en-DE
		"DT": 0x0F, // Date and time (7 byte)
	}
)

type S7DataItemWithAddressAndConverter struct {
	Address       string
	ConverterFunc converterFunc
	Item          gos7.S7DataItem
}

//------------------------------------------------------------------------------

// S7CommInput struct defines the structure for our custom Benthos input plugin.
// It holds the configuration necessary to establish a connection with a Siemens S7 PLC,
// along with the read requests to fetch data from the PLC.
type S7CommInput struct {
	tcpDevice    string                                // IP address of the S7 PLC.
	rack         int                                   // Rack number where the CPU resides. Identifies the physical location within the PLC rack.
	slot         int                                   // Slot number where the CPU resides. Identifies the CPU slot within the rack.
	batchMaxSize int                                   // Maximum count of addresses to be bundled in one batch-request. Affects PDU size.
	timeout      time.Duration                         // Time duration before a connection attempt or read request times out.
	client       gos7.Client                           // S7 client for communication.
	handler      *gos7.TCPClientHandler                // TCP handler to manage the connection.
	log          *service.Logger                       // Logger for logging plugin activity.
	batches      [][]S7DataItemWithAddressAndConverter // List of items to read from the PLC, grouped into batches with a maximum size.
}

type converterFunc func([]byte) interface{}

// S7CommConfigSpec defines the configuration options available for the S7CommInput plugin.
// It outlines the required information to establish a connection with the PLC and the data to be read.
var S7CommConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that reads data from Siemens S7 PLCs. Created & maintained by the United Manufacturing Hub. About us: www.umh.app").
	Description("This input plugin enables Benthos to read data directly from Siemens S7 PLCs using the S7comm protocol. " +
		"Configure the plugin by specifying the PLC's IP address, rack and slot numbers, and the data blocks to read.").
	Field(service.NewStringField("tcpDevice").Description("IP address of the S7 PLC.")).
	Field(service.NewIntField("rack").Description("Rack number of the PLC. Identifies the physical location of the CPU within the PLC rack.").Default(0)).
	Field(service.NewIntField("slot").Description("Slot number of the PLC. Identifies the CPU slot within the rack.").Default(1)).
	Field(service.NewIntField("batchMaxSize").Description("Maximum count of addresses to be bundled in one batch-request (PDU size).").Default(480)).
	Field(service.NewIntField("timeout").Description("The timeout duration in seconds for connection attempts and read requests.").Default(10)).
	Field(service.NewStringListField("addresses").Description("List of S7 addresses to read in the format '<area>.<type><address>[.extra]', e.g., 'DB5.X3.2', 'DB5.B3', or 'DB5.C3'. " +
		"Address formats include direct area access (e.g., DB1 for data block one) and data types (e.g., X for bit, B for byte)."))

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

	// Now split the addresses into batches based on the batchMaxSize
	batches, err := parseAddresses(addresses, batchMaxSize)
	if err != nil {
		return nil, err
	}

	m := &S7CommInput{
		tcpDevice:    tcpDevice,
		rack:         rack,
		slot:         slot,
		log:          mgr.Logger(),
		batches:      batches,
		batchMaxSize: batchMaxSize,
		timeout:      time.Duration(timeoutInt) * time.Second,
	}

	return service.AutoRetryNacksBatched(m), nil
}

func parseAddresses(addresses []string, batchMaxSize int) ([][]S7DataItemWithAddressAndConverter, error) {
	parsedAddresses := make([]S7DataItemWithAddressAndConverter, 0, len(addresses))

	for _, address := range addresses {
		item, converterFunc, err := handleFieldAddress(address)
		if err != nil {
			return nil, fmt.Errorf("address %q: %w", address, err)
		}

		newS7DataItemWithAddressAndConverter := S7DataItemWithAddressAndConverter{
			Address:       address,
			ConverterFunc: converterFunc,
			Item:          *item,
		}

		parsedAddresses = append(parsedAddresses, newS7DataItemWithAddressAndConverter)
	}

	// check for duplicates

	for i, a := range parsedAddresses {
		for j, b := range parsedAddresses {
			if i == j {
				continue
			}
			if a.Item.Area == b.Item.Area && a.Item.DBNumber == b.Item.DBNumber && a.Item.Start == b.Item.Start {
				return nil, fmt.Errorf("duplicate address %v", a)
			}
		}
	}

	// Now split the addresses into batches based on the batchMaxSize
	batches := make([][]S7DataItemWithAddressAndConverter, 0)
	for i := 0; i < len(parsedAddresses); i += batchMaxSize {
		end := i + batchMaxSize
		if end > len(parsedAddresses) {
			end = len(parsedAddresses)
		}
		batches = append(batches, parsedAddresses[i:end])
	}

	return batches, nil
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
	g.handler.Timeout = g.timeout
	g.handler.IdleTimeout = g.timeout

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

	msgs := make(service.MessageBatch, 0)
	for i, b := range g.batches {

		// Create a new batch to read
		batchToRead := make([]gos7.S7DataItem, len(b))
		for i, item := range b {
			batchToRead[i] = item.Item
		}

		// Read the batch
		g.log.Debugf("Reading batch %d...", i+1)
		if err := g.client.AGReadMulti(batchToRead, len(batchToRead)); err != nil {
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

			// Append the new message to the msgs slice
			msgs = append(msgs, msg)
		}
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

func handleFieldAddress(address string) (*gos7.S7DataItem, converterFunc, error) {
	// Parse the address into the different parts
	if !regexAddr.MatchString(address) {
		return nil, nil, fmt.Errorf("invalid address %q", address)
	}
	names := regexAddr.SubexpNames()[1:]
	parts := regexAddr.FindStringSubmatch(address)[1:]
	if len(names) != len(parts) {
		return nil, nil, fmt.Errorf("names %v do not match parts %v", names, parts)
	}
	groups := make(map[string]string, len(names))
	for i, n := range names {
		groups[n] = parts[i]
	}

	// Check that we do have the required entries in the address
	if _, found := groups["area"]; !found {
		return nil, nil, errors.New("area is missing from address")
	}

	if _, found := groups["no"]; !found {
		return nil, nil, errors.New("area index is missing from address")
	}
	if _, found := groups["type"]; !found {
		return nil, nil, errors.New("type is missing from address")
	}
	if _, found := groups["start"]; !found {
		return nil, nil, errors.New("start address is missing from address")
	}
	dtype := groups["type"]

	// Lookup the item values from names and check the params
	area, found := areaMap[groups["area"]]
	if !found {
		return nil, nil, errors.New("invalid area")
	}
	wordlen, found := wordLenMap[dtype]
	if !found {
		return nil, nil, errors.New("unknown data type")
	}
	areaidx, err := strconv.Atoi(groups["no"])
	if err != nil {
		return nil, nil, fmt.Errorf("invalid area index: %w", err)
	}
	start, err := strconv.Atoi(groups["start"])
	if err != nil {
		return nil, nil, fmt.Errorf("invalid start address: %w", err)
	}

	// Check the amount parameter if any
	var extra, bit int
	switch dtype {
	case "S":
		// We require an extra parameter
		x := groups["extra"]
		if x == "" {
			return nil, nil, errors.New("extra parameter required")
		}

		extra, err = strconv.Atoi(x)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid extra parameter: %w", err)
		}
		if extra < 1 {
			return nil, nil, fmt.Errorf("invalid extra parameter %d", extra)
		}
	case "X":
		// We require an extra parameter
		x := groups["extra"]
		if x == "" {
			return nil, nil, errors.New("extra parameter required")
		}

		bit, err = strconv.Atoi(x)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid extra parameter: %w", err)
		}
		if bit < 0 || bit > 7 {
			// Ensure bit address is valid
			return nil, nil, fmt.Errorf("invalid extra parameter: bit address %d out of range", bit)
		}
	default:
		if groups["extra"] != "" {
			return nil, nil, errors.New("extra parameter specified but not used")
		}
	}

	// Get the required buffer size
	amount := 1
	var buflen int
	switch dtype {
	case "X", "B", "C": // 8-bit types
		buflen = 1
	case "W", "I": // 16-bit types
		buflen = 2
	case "DW", "DI", "R": // 32-bit types
		buflen = 4
	case "DT": // 7-byte
		buflen = 7
	case "S":
		amount = extra
		// Extra bytes as the first byte is the max-length of the string and
		// the second byte is the actual length of the string.
		buflen = extra + 2
	default:
		return nil, nil, errors.New("invalid data type")
	}

	// Setup the data item
	item := &gos7.S7DataItem{
		Area:     area,
		WordLen:  wordlen,
		Bit:      bit,
		DBNumber: areaidx,
		Start:    start,
		Amount:   amount,
		Data:     make([]byte, buflen),
	}

	// Determine the type converter function
	f := determineConversion(dtype)
	return item, f, nil
}
