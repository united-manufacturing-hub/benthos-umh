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

const addressRegexp = `^(?P<area>[A-Z]+)(?P<no>[0-9]*)\.(?P<type>[A-Z]+)(?P<start>[0-9]+)(?:\.(?P<extra>.*))?$`

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
		"DT": 0x0F, // Date and time (8 byte)
	}
)

type S7DataItemWithAddressAndConverter struct {
	Address       string
	ConverterFunc converterFunc
	Item          gos7.S7DataItem
}

//------------------------------------------------------------------------------

// S7CommInput struct defines the structure for our custom Benthos input plugin.
type S7CommInput struct {
	TcpDevice       string
	Rack            int
	Slot            int
	Timeout         time.Duration
	Client          gos7.Client
	Handler         *gos7.TCPClientHandler
	Log             *service.Logger
	ParsedAddresses []S7DataItemWithAddressAndConverter   // All parsed addresses (before batching)
	Batches         [][]S7DataItemWithAddressAndConverter // Batches calculated after connect with actual PDU
	DisableCPUInfo  bool
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
	Field(service.NewIntField("timeout").
		Description("The timeout duration in seconds for connection attempts and read requests.").
		Default(10).
		Optional().
		Advanced().
		Examples(10, 5, 30)).
	Field(service.NewIntField("batchMaxSize").
		Description("DEPRECATED: PDU size is now automatically negotiated with the PLC. This field is ignored.").
		Optional().
		Deprecated()).
	Field(service.NewBoolField("disableCPUInfo").
		Description("Set this to true to not fetch CPU information from the PLC. Should be used when you get the error 'Failed to get CPU information'").
		Default(false).
		Optional().
		Advanced().
		Examples(false, true)).
	Field(service.NewStringListField("addresses").
		Description("S7 memory addresses to read. Addresses are automatically batched to respect "+
			"protocol limits (max 20 per request) and PDU size. "+
			"Format: DB<n>.<type><offset>[.extra] for data blocks, AREA.<type><offset>[.extra] for others. "+
			"Areas: DB (data block, requires block number), MK (marker), PE (input), PA (output), C (counter), T (timer). "+
			"Types: X (bit), B (byte), W (word), DW (dword), I (int), DI (dint), R (real), DT (datetime), C (char), S (string). "+
			"For bits (X), add bit number 0-7. For strings (S), add max length.").
		Examples([]string{"DB1.DW20"}, []string{"DB1.X5.2"}, []string{"MK.W0"}, []string{"PE.W0", "PA.W0"}))

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

	timeoutInt, err := conf.FieldInt("timeout")
	if err != nil {
		return nil, err
	}

	disableCPUInfo, err := conf.FieldBool("disableCPUInfo")
	if err != nil {
		return nil, err
	}

	batchMaxSize, _ := conf.FieldInt("batchMaxSize")
	if batchMaxSize != 0 {
		mgr.Logger().Warn("The 'batchMaxSize' field is deprecated and ignored. PDU size is now automatically negotiated with the PLC.")
	}

	for _, address := range addresses {
		if warning := deprecatedAddressWarning(address); warning != "" {
			mgr.Logger().Warn(warning)
		}
	}

	parsedAddresses, err := ParseAddresses(addresses)
	if err != nil {
		return nil, err
	}

	m := &S7CommInput{
		TcpDevice:       tcpDevice,
		Rack:            rack,
		Slot:            slot,
		Log:             mgr.Logger(),
		ParsedAddresses: parsedAddresses,
		Timeout:         time.Duration(timeoutInt) * time.Second,
		DisableCPUInfo:  disableCPUInfo,
	}

	return service.AutoRetryNacksBatched(m), nil
}

// S7 protocol limits for AGReadMulti
// Values are coming from telegram.go / multi.go from gos7 library
// also they're reproducible via wireshark
const (
	// protocol limit for AGReadMulti of 20 addresses
	maxItemsPerBatch = 20
	// request has a fixed header size and each item address is 12 bytes
	reqHeaderSize = 19
	reqItemSize   = 12
	// response has a fixed header size and each item a 4-byte header
	respHeaderSize     = 21
	respItemHeaderSize = 4
)

// deprecatedAddressWarning returns a warning if the address uses the old format.
func deprecatedAddressWarning(address string) string {
	if !regexAddr.MatchString(address) {
		return ""
	}
	groups := make(map[string]string)
	for i, name := range regexAddr.SubexpNames()[1:] {
		groups[name] = regexAddr.FindStringSubmatch(address)[1:][i]
	}
	if groups["area"] == "DB" || groups["no"] == "" {
		return ""
	}
	suggestion := fmt.Sprintf("%s.%s%s", groups["area"], groups["type"], groups["start"])
	if groups["extra"] != "" {
		suggestion += "." + groups["extra"]
	}
	return fmt.Sprintf("address %q uses a deprecated format, use %q instead — block numbers are ignored for %s and will be rejected in a future version",
		address, suggestion, groups["area"])
}

func ParseAddresses(addresses []string) ([]S7DataItemWithAddressAndConverter, error) {
	parsedAddresses := make([]S7DataItemWithAddressAndConverter, 0, len(addresses))

	for _, address := range addresses {
		item, converter, err := handleFieldAddress(address)
		if err != nil {
			return nil, fmt.Errorf("address %q: %w", address, err)
		}

		parsedAddresses = append(parsedAddresses, S7DataItemWithAddressAndConverter{
			Address:       address,
			ConverterFunc: converter,
			Item:          *item,
		})
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

	return parsedAddresses, nil
}

// BuildBatches splits S7 addresses into batches that respect protocol limits.
func BuildBatches(items []S7DataItemWithAddressAndConverter, pduSize int) ([][]S7DataItemWithAddressAndConverter, error) {
	var batches [][]S7DataItemWithAddressAndConverter
	var batch []S7DataItemWithAddressAndConverter

	reqBytes := reqHeaderSize
	respBytes := respHeaderSize

	for _, item := range items {
		dataSize := len(item.Item.Data)
		if dataSize%2 != 0 {
			dataSize++
		}
		itemReqBytes := reqItemSize
		itemRespBytes := respItemHeaderSize + dataSize

		// Check if single item exceeds PDU size
		singleItemReqSize := reqHeaderSize + itemReqBytes
		singleItemRespSize := respHeaderSize + itemRespBytes
		if singleItemReqSize > pduSize || singleItemRespSize > pduSize {
			return nil, fmt.Errorf("address %q exceeds PDU size %d (request: %d bytes, response: %d bytes)",
				item.Address, pduSize, singleItemReqSize, singleItemRespSize)
		}

		// Check all three constraints
		batchFilled := len(batch) >= maxItemsPerBatch
		reqFilled := reqBytes+itemReqBytes > pduSize
		respFilled := respBytes+itemRespBytes > pduSize

		// Start new batch if any limit is reached
		if len(batch) > 0 && (batchFilled || reqFilled || respFilled) {
			batches = append(batches, batch)
			batch = nil
			reqBytes = reqHeaderSize
			respBytes = respHeaderSize
		}

		batch = append(batch, item)
		reqBytes += itemReqBytes
		respBytes += itemRespBytes
	}

	// Append remaining items
	if len(batch) > 0 {
		batches = append(batches, batch)
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

	// Build batches using the PDU size negotiated with the PLC
	batches, err := BuildBatches(g.ParsedAddresses, g.Handler.PDULength)
	if err != nil {
		return fmt.Errorf("failed to build batches: %w", err)
	}
	g.Batches = batches
	g.Log.Infof("Created %d batches for %d addresses (PDU size: %d)", len(g.Batches), len(g.ParsedAddresses), g.Handler.PDULength)

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
	// Only DB uses block numbers; PE, PA, MK, C, T have a single memory region.
	areaName := groups["area"]
	var (
		areaidx int
		err     error
	)
	switch areaName {
	case "DB":
		if groups["no"] == "" {
			return nil, nil, errors.New("DB requires a block number (e.g., DB1.DW20)")
		}
		areaidx, err = strconv.Atoi(groups["no"])
		if err != nil {
			return nil, nil, fmt.Errorf("invalid DB number: %w", err)
		}
	default:
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
	case "DT": // 8-byte
		buflen = 8
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
