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

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	addressRegexp = `^(?P<` + groupArea + `>[A-Z]+)(?P<` + groupNumber + `>[0-9]*)\.(?P<` + groupType + `>[A-Z]+)(?P<` +
		groupStart + `>[0-9]+)(?:\.(?P<` + groupExtra + `>.*))?$`
	defaultTimeBetweenReads = time.Second
)

var (
	regexAddr = regexp.MustCompile(addressRegexp)
	areaMap   = map[area]int{
		areaProcessInput:  areaCodeProcessInput,
		areaProcessOutput: areaCodeProcessOutput,
		areaMerker:        areaCodeMerker,
		areaDataBlock:     areaCodeDataBlock,
		areaCounter:       areaCodeCounter,
		areaTimer:         areaCodeTimer,
	}
	wordLenMap = map[dataType]int{
		typeBit:      wordLengthBit,
		typeByte:     wordLengthByte,
		typeChar:     wordLengthChar,
		typeString:   wordLengthChar,
		typeWord:     wordLengthWord,
		typeInt:      wordLengthInt,
		typeDWord:    wordLengthDWord,
		typeDInt:     wordLengthDInt,
		typeReal:     wordLengthReal,
		typeDateTime: wordLengthDateTime,
	}
)

type S7DataItemWithAddressAndConverter struct {
	Address       string
	ConverterFunc converterFunc
	Item          Item
}

//------------------------------------------------------------------------------

// S7CommInput struct defines the structure for our custom Benthos input plugin.
type S7CommInput struct {
	TcpDevice        string
	Rack             int
	Slot             int
	TimeBetweenReads time.Duration
	Timeout          time.Duration
	Client           Client
	Log              *service.Logger
	ParsedAddresses  []S7DataItemWithAddressAndConverter   // All parsed addresses (before batching)
	Batches          [][]S7DataItemWithAddressAndConverter // Batches calculated after connect with actual PDU
	DisableCPUInfo   bool

	// newClient opens the connection. It is the gos7 adapter unless a test sets another.
	newClient clientFactory
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
	Field(service.NewDurationField("timeBetweenReads").
		Description("The time we are waiting between read-cycles from the s7 device. Be careful to not overflow the device with very low rates here as this might crash the plc.").
		Default(defaultTimeBetweenReads.String()).
		Advanced().
		Examples("1000ms", "1s", "200ms")).
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
	connection, err := parseConnectionConfig(conf)
	if err != nil {
		return nil, err
	}

	timeBetweenReads, err := conf.FieldDuration("timeBetweenReads")
	if err != nil {
		return nil, err
	}

	if timeBetweenReads <= 0 {
		mgr.Logger().Warnf("invalid timeBetweenReads %v, must be greater than 0. Using default %v.", timeBetweenReads, defaultTimeBetweenReads)
		timeBetweenReads = defaultTimeBetweenReads
	}

	addresses, err := conf.FieldStringList("addresses")
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
		TcpDevice:        connection.tcpDevice,
		Rack:             connection.rack,
		Slot:             connection.slot,
		TimeBetweenReads: timeBetweenReads,
		Log:              mgr.Logger(),
		ParsedAddresses:  parsedAddresses,
		Timeout:          connection.timeout,
		DisableCPUInfo:   disableCPUInfo,
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
// NOTE: when migrating configs the metadata address will change as well, keep that
// in mind when enforcing deprecation.
func deprecatedAddressWarning(address string) string {
	if !regexAddr.MatchString(address) {
		return ""
	}
	groups := make(map[string]string)
	for i, name := range regexAddr.SubexpNames()[1:] {
		groups[name] = regexAddr.FindStringSubmatch(address)[1:][i]
	}
	if groups[groupArea] == string(areaDataBlock) || groups[groupNumber] == "" {
		return ""
	}
	suggestion := fmt.Sprintf("%s.%s%s", groups[groupArea], groups[groupType], groups[groupStart])
	if groups[groupExtra] != "" {
		suggestion += "." + groups[groupExtra]
	}
	return fmt.Sprintf("address %q uses a deprecated format, use %q instead — block numbers are ignored for %s and will be rejected in a future version",
		address, suggestion, groups[groupArea])
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
			if a.Item.sameRead(b.Item) {
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

func (s *S7CommInput) Connect(ctx context.Context) error {
	newClient := s.newClient
	if newClient == nil {
		newClient = newGos7Client
	}

	client, err := newClient(ctx, connectionConfig{tcpDevice: s.TcpDevice, rack: s.Rack, slot: s.Slot, timeout: s.Timeout})
	if err != nil {
		s.Log.Errorf("Failed to connect to S7 PLC at %s: %v", s.TcpDevice, err)
		return err
	}

	s.Client = client
	s.Log.Infof("Successfully connected to S7 PLC at %s", s.TcpDevice)

	// Build batches using the PDU size negotiated with the PLC
	batches, err := BuildBatches(s.ParsedAddresses, client.PDULength())
	if err != nil {
		return fmt.Errorf("failed to build batches: %w", err)
	}
	s.Batches = batches
	s.Log.Infof("Created %d batches for %d addresses (PDU size: %d)", len(s.Batches), len(s.ParsedAddresses), client.PDULength())

	// Fetch and show CPU information, but only if the user has not disabled it
	if !s.DisableCPUInfo {
		cpuInfo, err := client.CPUInfo()
		if err != nil {
			s.Log.Warnf("Failed to get CPU information: %v", err)
		} else {
			s.Log.Infof("CPU Information: %s", cpuInfo)
		}
	}

	return nil
}

func (s *S7CommInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if s.Client == nil {
		return nil, nil, fmt.Errorf("S7Comm client is not initialized")
	}

	// NOTE: this is the wait between reads, not a cycle time: it does not
	// subtract the time the read itself takes.
	err := s.pause(ctx)
	if err != nil {
		return nil, nil, err
	}

	msgs := make(service.MessageBatch, 0)
	for i, b := range s.Batches {
		// Create a new batch to read
		batchToRead := make([]Item, len(b))
		for i, item := range b {
			batchToRead[i] = item.Item
		}

		// Read the batch. A refused item is reported in its Err, which is not
		// looked at: the value is then converted from the zeroed buffer.
		s.Log.Debugf("Reading batch %d...", i+1)
		if err := s.Client.ReadItems(ctx, batchToRead); err != nil {
			// Try to reconnect and skip this gather cycle to avoid hammering
			// the network if the server is down or under load.
			s.Log.Errorf("Failed to read batch %d: %v", i+1, err)
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

	return msgs, func(_ context.Context, _ error) error {
		return nil // Acknowledgment handling here if needed
	}, nil
}

func (s *S7CommInput) pause(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.TimeBetweenReads):
	}
	return nil
}

func (s *S7CommInput) Close(_ context.Context) error {
	if s.Client != nil {
		_ = s.Client.Close()
		s.Client = nil
	}

	return nil
}

func handleFieldAddress(address string) (*Item, converterFunc, error) {
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
	if _, found := groups[groupArea]; !found {
		return nil, nil, errors.New("area is missing from address")
	}

	if _, found := groups[groupType]; !found {
		return nil, nil, errors.New("type is missing from address")
	}
	if _, found := groups[groupStart]; !found {
		return nil, nil, errors.New("start address is missing from address")
	}
	dtype := dataType(groups[groupType])

	// Lookup the item values from names and check the params
	areaCode, found := areaMap[area(groups[groupArea])]
	if !found {
		return nil, nil, errors.New("invalid area")
	}
	wordlen, found := wordLenMap[dtype]
	if !found {
		return nil, nil, errors.New("unknown data type")
	}
	// Only DB uses block numbers; PE, PA, MK, C, T have a single memory region.
	areaName := area(groups[groupArea])
	var (
		areaidx int
		err     error
	)
	switch areaName {
	case areaDataBlock:
		if groups[groupNumber] == "" {
			return nil, nil, errors.New("DB requires a block number (e.g., DB1.DW20)")
		}
		areaidx, err = strconv.Atoi(groups[groupNumber])
		if err != nil {
			return nil, nil, fmt.Errorf("invalid DB number: %w", err)
		}
	default:
	}
	start, err := strconv.Atoi(groups[groupStart])
	if err != nil {
		return nil, nil, fmt.Errorf("invalid start address: %w", err)
	}

	// Check the amount parameter if any
	var extra, bit int
	switch dtype {
	case typeString:
		// We require an extra parameter
		x := groups[groupExtra]
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
	case typeBit:
		// We require an extra parameter
		x := groups[groupExtra]
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
		if groups[groupExtra] != "" {
			return nil, nil, errors.New("extra parameter specified but not used")
		}
	}

	// Get the required buffer size
	amount := 1
	var buflen int
	switch dtype {
	case typeBit, typeByte, typeChar: // 8-bit types
		buflen = 1
	case typeWord, typeInt: // 16-bit types
		buflen = 2
	case typeDWord, typeDInt, typeReal: // 32-bit types
		buflen = 4
	case typeDateTime: // 8-byte
		buflen = 8
	case typeString:
		amount = extra
		// Extra bytes as the first byte is the max-length of the string and
		// the second byte is the actual length of the string.
		buflen = extra + 2
	default:
		return nil, nil, errors.New("invalid data type")
	}

	// Setup the data item
	item := &Item{
		Area:       areaCode,
		WordLength: wordlen,
		Bit:        bit,
		DBNumber:   areaidx,
		ByteOffset: start,
		Amount:     amount,
		Data:       make([]byte, buflen),
	}

	// Determine the type converter function
	f := determineConversion(dtype)
	return item, f, nil
}

// sameRead reports whether two items read the same bytes: the same area, block,
// offset, bit, word length, amount and buffer size.
func (i Item) sameRead(other Item) bool {
	return i.Area == other.Area &&
		i.WordLength == other.WordLength &&
		i.DBNumber == other.DBNumber &&
		i.ByteOffset == other.ByteOffset &&
		i.Bit == other.Bit &&
		i.Amount == other.Amount &&
		len(i.Data) == len(other.Data)
}
