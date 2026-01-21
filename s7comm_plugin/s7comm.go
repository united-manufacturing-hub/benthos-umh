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
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
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
	TcpDevice      string                                // IP address of the S7 PLC (optionally with port, e.g., "192.168.1.100:102").
	Rack           int                                   // Rack number where the CPU resides. Identifies the physical location within the PLC rack.
	Slot           int                                   // Slot number where the CPU resides. Identifies the CPU slot within the rack.
	BatchMaxSize   int                                   // Maximum count of addresses to be bundled in one batch-request. Affects PDU size.
	Timeout        time.Duration                         // Time duration before a connection attempt or read request times out.
	Client         gos7.Client                           // S7 client for communication.
	Handler        *gos7.TCPClientHandler                // TCP handler to manage the connection.
	Log            *service.Logger                       // Logger for logging plugin activity.
	Batches        [][]S7DataItemWithAddressAndConverter // List of items to read from the PLC, grouped into batches with a maximum size.
	DisableCPUInfo bool                                  // Set this to true to not fetch CPU information from the PLC. Should be used when you get the error "Failed to get CPU information"
}

type converterFunc func([]byte) interface{}

// S7CommConfigSpec defines the configuration options available for the S7CommInput plugin.
// It outlines the required information to establish a connection with the PLC and the data to be read.
var S7CommConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that reads data from Siemens S7 PLCs. Created & maintained by the United Manufacturing Hub. About us: www.umh.app").
	Description("This input plugin enables Benthos to read data directly from Siemens S7 PLCs using the S7comm protocol. " +
		"Configure the plugin by specifying the PLC's IP address, rack and slot numbers, and the data blocks to read.").
	Field(service.NewStringField("tcpDevice").
		Description("IP address or hostname of the S7 PLC, optionally with port. Format: '192.168.1.100' or '192.168.1.100:102'. If no port is specified, the default S7 port 102 is used. Ensure to pick the IP address from your hardware configuration.").
		Examples("192.168.1.100", "192.168.1.100:102", "10.0.0.50:20000", "plc.local")).
	Field(service.NewIntField("rack").
		Description("Rack number from hardware configuration, usually 0.").
		Default(0).
		Examples(0, 1, 2)).
	Field(service.NewIntField("slot").
		Description("Slot number from hardware configuration, usually 1.").
		Default(1).
		Examples(1, 2, 3)).
	Field(service.NewIntField("batchMaxSize").
		Description("Maximum PDU size in bytes. Default (480) works for most PLCs including S7-1500. Reduce for older PLCs like S7-300 (240).").
		Default(480).
		Optional().
		Advanced().
		Examples(480, 240, 960)).
	Field(service.NewIntField("timeout").
		Description("The timeout duration in seconds for connection attempts and read requests.").
		Default(10).
		Optional().
		Advanced().
		Examples(10, 5, 30)).
	Field(service.NewBoolField("disableCPUInfo").
		Description("Set this to true to not fetch CPU information from the PLC. Should be used when you get the error 'Failed to get CPU information'").
		Default(false).
		Optional().
		Advanced().
		Examples(false, true)).
	Field(service.NewStringListField("addresses").
		Description("S7 memory addresses to read. Maximum 20 addresses per connection "+
			"to prevent PLC overload; use multiple S7 inputs for more. "+
			"Format: AREA.TYPE<offset>[.extra]. "+
			"Areas: DB (data block), MK (marker), PE (input), PA (output). "+
			"Types: X (bit), B (byte), W (word), DW (dword), I (int), DI (dint), R (real), S (string). "+
			"For bits (X), add bit number 0-7. For strings (S), add max length.").
		Examples([]string{"DB1.DW20"}, []string{"DB1.X5.2"}, []string{"MK0.W0"}, []string{"PE.W0", "PA.W0"}))

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

func ParseAddresses(addresses []string, batchMaxSize int) ([][]S7DataItemWithAddressAndConverter, error) {
	parsedAddresses := make([]S7DataItemWithAddressAndConverter, 0, len(addresses))

	for _, address := range addresses {
		item, converter, err := handleFieldAddress(address)
		if err != nil {
			return nil, fmt.Errorf("address %q: %w", address, err)
		}

		newS7DataItemWithAddressAndConverter := S7DataItemWithAddressAndConverter{
			Address:       address,
			ConverterFunc: converter,
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
			if reflect.DeepEqual(a.Item, b.Item) {
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

func (g *S7CommInput) Connect(_ context.Context) error {
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

func (g *S7CommInput) ReadBatch(_ context.Context) (service.MessageBatch, service.AckFunc, error) {
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
			g.Log.Errorf("Failed to read batch %d: %v", i+1, err)
			return nil, nil, service.ErrNotConnected
		}

		// Read the data from the batch and convert it using the converter function
		for _, item := range b {
			// Execute the converter function to get the converted data
			convertedData := item.ConverterFunc(item.Item.Data)

			// Convert any type of convertedData to a string.
			// The fmt.Sprintf function is used here for its ability to handle various types gracefully.
			dataAsString := fmt.Sprintf("%v", convertedData)

			// Convert the string representation to a []byte
			dataAsBytes := []byte(dataAsString)

			// Create a new message with the current state of the buffer
			// Note: Depending on your requirements, you may want to reset the buffer
			// after creating each message or keep accumulating data in it.
			msg := service.NewMessage(dataAsBytes)
			msg.MetaSet("s7_address", item.Address)

			// Append the new message to the msgs slice
			msgs = append(msgs, msg)
		}
	}

	time.Sleep(time.Second)

	return msgs, func(_ context.Context, _ error) error {
		return nil // Acknowledgment handling here if needed
	}, nil
}

func (g *S7CommInput) Close(_ context.Context) error {
	if g.Handler != nil {
		g.Handler.Close()
		g.Handler = nil
		g.Client = nil
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
