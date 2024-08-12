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

// Implementation:
// Go for the metric implementation of telegraf
// https://github.com/influxdata/telegraf/blob/master/plugins/inputs/modbus/README.md
// TODO: migrate modbus.go

package modbus_plugin

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"time"

	"github.com/grid-x/modbus"
	"github.com/redpanda-data/benthos/v4/public/service"
)

//The plugin supports connections to PLCs via MODBUS/TCP, RTU over TCP, ASCII over TCP

// ModbusDataItemWithAddress struct defines the structure for the data items to be read from the Modbus device.
type ModbusDataItemWithAddress struct {
	Name     string // Field Name
	Register string // Register type. Can be "coil", "discrete", "holding" or "input". Defaults to "holding".
	Address  uint16 // Address of the register to query. For coil and discrete inputs this is the bit address.

	// Type is the type of the modbus field
	// Can be
	// BIT (single bit of a register)
	//	INT8L, INT8H, UINT8L, UINT8H (low and high byte variants)
	//	INT16, UINT16, INT32, UINT32, INT64, UINT64 and
	//	FLOAT16, FLOAT32, FLOAT64 (IEEE 754 binary representation)
	//	STRING (byte-sequence converted to string)
	Type string

	// Length is the number of registers, ONLY valid for STRING type. Defaults to 1.
	Length uint16

	// Bit is the bit of the register, ONLY valid for BIT type. Defaults to 0.
	Bit uint16

	// Scale is the factor to scale the variable with. Defaults to 1.0.
	Scale float64

	// Output is the type of resulting field, can be INT64, UINT64 or FLOAT64. Defaults to FLOAT64 if
	// "scale" is provided and to the input "type" class otherwise (i.e. INT* -> INT64, etc).
	Output string

	ConverterFunc converterFunc
}

// ModbusInput struct defines the structure for our custom Benthos input plugin.
// It holds the configuration necessary to establish a connection with a Modbus PLC,
type ModbusInput struct {

	// Standard
	Controller       string // e.g., "tcp://localhost:502"
	TransmissionMode string // Can be "TCP" (default), "RTUOverTCP", "ASCIIOverTCP"\
	SlaveID          byte
	BusyRetries      int           // Maximum number of retries when the device is busy
	BusyRetriesWait  time.Duration // Time to wait between retries when the device is busy

	// Optimization Request optimization algorithm across metrics
	//  |---none       -- Do not perform any optimization and just group requests
	//  |                 within metrics (default)
	//  |---max_insert -- Collate registers across all defined metrics and fill in
	//                    holes to optimize the number of requests.
	Optimization string

	// OptimizationMaxRegisterFill is the maximum number of registers the optimizer is allowed to insert between
	// non-consecutive registers to save requests.
	// This option is only used for the 'max_insert' optimization strategy and
	// effectively denotes the hole size between registers to fill.
	OptimizationMaxRegisterFill int

	// ByteOrder is the byte order of the registers. The default is big endian.
	//  |---ABCD -- Big Endian (Motorola)
	//  |---DCBA -- Little Endian (Intel)
	//  |---BADC -- Big Endian with byte swap
	//  |---CDAB -- Little Endian with byte swap
	ByteOrder string

	// Modbus Workarounds. Required by some devices to work correctly
	PauseAfterConnect       time.Duration // PauseAfterConnect is the pause after connect delays the first request by the specified time. This might be necessary for (slow) devices.
	OneRequestPerField      bool          // OneRequestPerField sends each field in a separate request. This might be necessary for some devices. see https://github.com/influxdata/telegraf/issues/12071.
	ReadCoilsStartingAtZero bool          // ReadCoilsStartingAtZero reads coils starting at address 0 instead of 1. This might be necessary for some devices. See https://github.com/influxdata/telegraf/issues/8905

	// StringRegisterLocation is the String byte-location in registers AFTER byte-order conversion.
	// Some device (e.g. EM340) place the string byte in only the upper
	// or lower byte location of a register
	// see https://github.com/influxdata/telegraf/issues/14748
	// Available settings:
	//   lower -- use only lower byte of the register i.e. 00XX 00XX 00XX 00XX
	//   upper -- use only upper byte of the register i.e. XX00 XX00 XX00 XX00
	// By default both bytes of the register are used i.e. XXXX XXXX.
	StringRegisterLocation string

	// Addresses is a list of Modbus addresses to read
	Addresses []ModbusDataItemWithAddress

	// Requests is the auto-generated list of requests to be made
	// They are creates based on the addresses and the optimization strategy
	requestSet requestSet

	// Internal
	Handler modbus.TCPClientHandler
	Client  modbus.Client
	Log     *service.Logger
}

type requestSet struct {
	coil     []request
	discrete []request
	holding  []request
	input    []request
}

type modbusTag struct {
	name      string
	address   uint16
	length    uint16
	omit      bool
	converter converterFunc
	value     interface{}
}

type converterFunc func([]byte) interface{}

var errAddressOverflow = errors.New("address overflow")

// ModbusConfigSpec defines the configuration options available for the ModbusInput plugin.
// It outlines the required information to establish a connection with the Modbus device and the data to be read.
var ModbusConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that reads data from Modbus devices. Created & maintained by the United Manufacturing Hub. About us: www.umh.app").
	Description("This input plugin enables Benthos to read data directly from Modbus devices using the Modbus protocol.").
	Field(service.NewStringField("controller").Description("The Modbus controller address, e.g., 'tcp://localhost:502'").Default("tcp://localhost:502")).
	Field(service.NewStringField("transmissionMode").Description("Transmission mode: 'TCP', 'RTUOverTCP', or 'ASCIIOverTCP'").Default("TCP")).
	Field(service.NewIntField("slaveID").Description("Slave ID of the Modbus device").Default(1)).
	Field(service.NewIntField("busyRetries").Description("Maximum number of retries when the device is busy").Default(3)).
	Field(service.NewDurationField("busyRetriesWait").Description("Time to wait between retries when the device is busy").Default("200ms")).
	Field(service.NewStringField("optimization").Description("Request optimization algorithm: 'none' or 'max_insert'").Default("none")).
	Field(service.NewIntField("optimizationMaxRegisterFill").Description("Maximum number of registers to insert for optimization").Default(50)).
	Field(service.NewStringField("byteOrder").Description("Byte order: 'ABCD', 'DCBA', 'BADC', or 'CDAB'").Default("ABCD")).
	Field(service.NewObjectField("workarounds",
		service.NewDurationField("pauseAfterConnect").Description("Pause after connect to delay the first request").Default("0s"),
		service.NewBoolField("oneRequestPerField").Description("Send each field in a separate request").Default(false),
		service.NewBoolField("readCoilsStartingAtZero").Description("Read coils starting at address 0 instead of 1").Default(false),
		service.NewStringField("stringRegisterLocation").Description("String byte-location in registers: 'lower', 'upper', or empty for both").Default("")).
		Description("Modbus workarounds. Required by some devices to work correctly. Should be left alone by default and must not be changed unless necessary.")).
	Field(service.NewObjectListField("addresses",
		service.NewStringField("name").Description("Field name"),
		service.NewStringField("register").Description("Register type: 'coil', 'discrete', 'holding', or 'input'").Default("holding"),
		service.NewIntField("address").Description("Address of the register to query"),
		service.NewStringField("type").Description("Data type of the field"),
		service.NewIntField("length").Description("Number of registers, only valid for STRING type").Default(1),
		service.NewIntField("bit").Description("Bit of the register, only valid for BIT type").Default(0),
		service.NewFloatField("scale").Description("Factor to scale the variable with").Default(1.0),
		service.NewStringField("output").Description("Type of resulting field: 'INT64', 'UINT64', 'FLOAT64', or 'native'").Default("native")).
		Description("List of Modbus addresses to read"))

// newModbusInput is the constructor function for ModbusInput. It parses the plugin configuration,
// establishes a connection with the Modbus device, and initializes the input plugin instance.
func newModbusInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	m := &ModbusInput{
		Log: mgr.Logger(),
	}

	var err error

	if m.Controller, err = conf.FieldString("controller"); err != nil {
		return nil, err
	}
	if m.TransmissionMode, err = conf.FieldString("transmissionMode"); err != nil {
		return nil, err
	}
	if slaveID, err := conf.FieldInt("slaveID"); err != nil {
		return nil, err
	} else {
		m.SlaveID = byte(slaveID)
	}
	if m.BusyRetries, err = conf.FieldInt("busyRetries"); err != nil {
		return nil, err
	}
	if m.BusyRetriesWait, err = conf.FieldDuration("busyRetriesWait"); err != nil {
		return nil, err
	}
	if m.Optimization, err = conf.FieldString("optimization"); err != nil {
		return nil, err
	}
	if m.OptimizationMaxRegisterFill, err = conf.FieldInt("optimizationMaxRegisterFill"); err != nil {
		return nil, err
	}
	if m.ByteOrder, err = conf.FieldString("byteOrder"); err != nil {
		return nil, err
	}
	if m.PauseAfterConnect, err = conf.FieldDuration("pauseAfterConnect"); err != nil {
		return nil, err
	}
	if m.OneRequestPerField, err = conf.FieldBool("oneRequestPerField"); err != nil {
		return nil, err
	}
	if m.ReadCoilsStartingAtZero, err = conf.FieldBool("readCoilsStartingAtZero"); err != nil {
		return nil, err
	}
	if m.StringRegisterLocation, err = conf.FieldString("stringRegisterLocation"); err != nil {
		return nil, err
	}

	// These are the general checks for the configuration
	switch m.ByteOrder {
	case "":
		m.ByteOrder = "ABCD"
	case "ABCD", "DCBA", "BADC", "CDAB", "MSW-BE", "MSW-LE", "LSW-LE", "LSW-BE":
	default:
		return nil, fmt.Errorf("unknown byte-order %q", m.ByteOrder)
	}

	// These are the checks for the workarounds

	// StringRegisterLocation
	switch m.StringRegisterLocation {
	case "", "both", "lower", "upper":
		// Do nothing as those are valid
	default:
		return nil, fmt.Errorf("invalid 'string_register_location' %q", m.StringRegisterLocation)
	}

	// Check optimization algorithm
	switch m.Optimization {
	case "", "none":
		m.Optimization = "none"
	case "max_insert":
		if m.OptimizationMaxRegisterFill == 0 {
			m.OptimizationMaxRegisterFill = 50
		}
	default:
		return nil, fmt.Errorf("unknown optimization %q", m.Optimization)
	}

	// Read in addresses
	addressesConf, err := conf.FieldObjectList("addresses")
	if err != nil {
		return nil, err
	}

	// Reject any configuration without fields as it would be pointless
	if len(addressesConf) == 0 {
		return nil, fmt.Errorf("adresses are empty")
	}

	// used to de-duplicate
	seenFields := make(map[uint64]bool)
	seed := maphash.MakeSeed()

	for _, addrConf := range addressesConf {
		item := ModbusDataItemWithAddress{}

		// Name
		if item.Name, err = addrConf.FieldString("name"); err != nil {
			return nil, err
		}

		// mandatory
		if item.Name == "" {
			return nil, fmt.Errorf("empty field name in request for slave %d", m.SlaveID)
		}

		// Register
		if item.Register, err = addrConf.FieldString("register"); err != nil {
			return nil, err
		}

		switch item.Register {
		case "":
			item.Register = "holding"
		case "coil", "discrete", "holding", "input":
		default:
			return nil, fmt.Errorf("unknown register-type %q for field %q", item.Register, item.Name)
		}

		// Address
		if addr, err := addrConf.FieldInt("address"); err != nil {
			return nil, err
		} else if addr < 0 || addr > 65535 { // Check if the value is within the range of uint16
			return nil, fmt.Errorf("value out of range for uint16: %d", addr)
		} else {
			item.Address = uint16(addr) // Convert int to uint16
		}

		if item.Type, err = addrConf.FieldString("type"); err != nil {
			return nil, err
		}

		if length, err := addrConf.FieldInt("length"); err != nil {
			return nil, err
		} else if length < 0 || length > 65535 { // Check if the value is within the range of uint16
			return nil, fmt.Errorf("value out of range for uint16: %d", length)
		} else {
			item.Length = uint16(length) // Convert int to uint16
		}

		if bit, err := addrConf.FieldInt("bit"); err != nil {
			return nil, err
		} else if bit < 0 || bit > 65535 { // Check if the value is within the range of uint16
			return nil, fmt.Errorf("value out of range for uint16: %d", bit)
		} else {
			item.Bit = uint16(bit) // Convert int to uint16
		}

		if item.Scale, err = addrConf.FieldFloat("scale"); err != nil {
			return nil, err
		}
		if item.Output, err = addrConf.FieldString("output"); err != nil {
			return nil, err
		}

		// Check the input and output type for all fields as we later need
		// it to determine the number of registers to query.
		switch item.Register {
		case "holding", "input":
			// Check the input type
			switch item.Type {
			case "":
			case "INT8L", "INT8H", "INT16", "INT32", "INT64",
				"UINT8L", "UINT8H", "UINT16", "UINT32", "UINT64",
				"FLOAT16", "FLOAT32", "FLOAT64":
				if item.Length != 0 {
					return nil, fmt.Errorf("length option cannot be used for type %q of field %q", item.Type, item.Name)
				}
				if item.Bit != 0 {
					return nil, fmt.Errorf("bit option cannot be used for type %q of field %q", item.Type, item.Name)
				}
				if item.Output == "STRING" {
					return nil, fmt.Errorf("cannot output field %q as string", item.Name)
				}
			case "STRING":
				if item.Length < 1 {
					return nil, fmt.Errorf("missing length for string field %q", item.Name)
				}
				if item.Bit != 0 {
					return nil, fmt.Errorf("bit option cannot be used for type %q of field %q", item.Type, item.Name)
				}
				if item.Scale != 0.0 {
					return nil, fmt.Errorf("scale option cannot be used for string field %q", item.Name)
				}
				if item.Output != "" && item.Output != "STRING" {
					return nil, fmt.Errorf("invalid output type %q for string field %q", item.Type, item.Name)
				}
			case "BIT":
				if item.Length != 0 {
					return nil, fmt.Errorf("length option cannot be used for type %q of field %q", item.Type, item.Name)
				}
				if item.Output == "STRING" {
					return nil, fmt.Errorf("cannot output field %q as string", item.Name)
				}
			default:
				return nil, fmt.Errorf("unknown register data-type %q for field %q", item.Type, item.Name)
			}

			// Check output type
			switch item.Output {
			case "", "INT64", "UINT64", "FLOAT64", "STRING":
			default:
				return nil, fmt.Errorf("unknown output data-type %q for field %q", item.Output, item.Name)
			}
		case "coil", "discrete":
			// Bit register types can only be UINT64 or BOOL
			switch item.Output {
			case "", "UINT16", "BOOL":
			default:
				return nil, fmt.Errorf("unknown output data-type %q for field %q", item.Output, item.Name)
			}
		}

		// Check for duplicate fields
		if _, exists := seenFields[tagID(seed, item)]; exists {
			m.Log.Warnf("Duplicate field %q %q, ignoring", item.Name, item.Address)
			continue
		} else {
			seenFields[tagID(seed, item)] = true
		}

		m.Addresses = append(m.Addresses, item)
	}

	// Parse the addresses into batches
	m.requestSet, err = m.createBatchesFromAddresses(m.Addresses)
	if err != nil {
		m.Log.Errorf("Failed to create batches: %v", err)
	}

	return service.AutoRetryNacksBatched(m), nil
}

func (m *ModbusInput) createBatchesFromAddresses(addresses []ModbusDataItemWithAddress) (requestSet, error) {

	// Create a map of requests for each register type
	collection := make(map[string][]modbusTag)

	// Collect the requested registers across metrics and transform them into
	// requests. This will produce one request per slave and register-type

	for _, item := range addresses {

		// Create a new tag
		tag, err := m.newTag(item)
		if err != nil {
			return requestSet{}, err
		}

		// Append the tag to the collection
		collection[item.Register] = append(collection[item.Register], tag)
	}

	var result requestSet

	// Create a request for each register type
	params := groupingParams{
		Optimization:      m.Optimization,
		MaxExtraRegisters: uint16(m.OptimizationMaxRegisterFill),
	}

	for register, tags := range collection {
		switch register {
		case "coil":
			params.MaxBatchSize = maxQuantityCoils
			if m.OneRequestPerField {
				params.MaxBatchSize = 1
			}
			params.EnforceFromZero = m.ReadCoilsStartingAtZero
			requests := m.groupTagsToRequests(tags, params)
			result.coil = append(result.coil, requests...)
		case "discrete":
			params.MaxBatchSize = maxQuantityDiscreteInput
			if m.OneRequestPerField {
				params.MaxBatchSize = 1
			}
			requests := m.groupTagsToRequests(tags, params)
			result.discrete = append(result.discrete, requests...)
		case "holding":
			params.MaxBatchSize = maxQuantityHoldingRegisters
			if m.OneRequestPerField {
				params.MaxBatchSize = 1
			}
			requests := m.groupTagsToRequests(tags, params)
			result.holding = append(result.holding, requests...)
		case "input":
			params.MaxBatchSize = maxQuantityInputRegisters
			if m.OneRequestPerField {
				params.MaxBatchSize = 1
			}
			requests := m.groupTagsToRequests(tags, params)
			result.input = append(result.input, requests...)
		default:
			return requestSet{}, fmt.Errorf("unknown register type %q", register)
		}
	}

	return result, nil
}

func (m *ModbusInput) newTag(item ModbusDataItemWithAddress) (modbusTag, error) {
	typed := item.Register == "holding" || item.Register == "input"

	fieldLength := uint16(1)
	if typed {
		var err error
		if fieldLength, err = determineTagLength(item.Type, item.Length); err != nil {
			return modbusTag{}, err
		}
	}

	// Check for address overflow
	if item.Address > math.MaxUint16-fieldLength {
		return modbusTag{}, fmt.Errorf("%w for field %q", errAddressOverflow, item.Name)
	}

	// Initialize the field
	f := modbusTag{
		name:    item.Name,
		address: item.Address,
		length:  fieldLength,
	}

	// Handle type conversions for coil and discrete registers
	if !typed {
		var err error
		f.converter, err = determineUntypedConverter(item.Output)
		if err != nil {
			return modbusTag{}, err
		}
		// No more processing for un-typed (coil and discrete registers) fields
		return f, nil
	}

	// Automagically determine the output type...
	if item.Output == "" {
		if item.Scale == 0.0 {
			// For non-scaling cases we should choose the output corresponding to the input class
			// i.e. INT64 for INT*, UINT64 for UINT* etc.
			var err error
			if item.Output, err = determineOutputDatatype(item.Type); err != nil {
				return modbusTag{}, err
			}
		} else {
			// For scaling cases we always want FLOAT64 by default except for
			// string fields
			if item.Type != "STRING" {
				item.Output = "FLOAT64"
			} else {
				item.Output = "STRING"
			}
		}
	}

	// Setting default byte-order
	byteOrder := m.ByteOrder
	if byteOrder == "" {
		byteOrder = "ABCD"
	}

	// Normalize the data relevant for determining the converter
	inType, err := normalizeInputDatatype(item.Type)
	if err != nil {
		return modbusTag{}, err
	}
	outType, err := normalizeOutputDatatype(item.Output)
	if err != nil {
		return modbusTag{}, err
	}
	order, err := normalizeByteOrder(byteOrder)
	if err != nil {
		return modbusTag{}, err
	}

	f.converter, err = determineConverter(inType, order, outType, item.Scale, uint8(item.Bit), m.StringRegisterLocation)
	if err != nil {
		return modbusTag{}, err
	}

	return f, nil
}

func (m *ModbusInput) Connect(ctx context.Context) error {
	m.Handler = modbus.NewTCPClientHandler(m.TcpDevice)
	m.Handler.Timeout = m.Timeout

	err := m.Handler.Connect()
	if err != nil {
		m.Log.Errorf("Failed to connect to Modbus device at %s: %v", m.TcpDevice, err)
		return err
	}

	m.Client = modbus.NewClient(m.Handler)
	m.Log.Infof("Successfully connected to Modbus device at %s", m.TcpDevice)

	return nil
}

func (m *ModbusInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if m.Client == nil {
		return nil, nil, fmt.Errorf("Modbus client is not initialized")
	}

	msgs := make(service.MessageBatch, 0)
	for i, batch := range m.Batches {

		for _, item := range batch {
			// Read the registers
			results, err := m.Client.ReadHoldingRegisters(item.Address, item.Quantity)
			if err != nil {
				return nil, nil, fmt.Errorf("Failed to read batch %d: %v", i+1, err)
			}

			// Convert the result using the converter function
			convertedData := item.ConverterFunc(results)

			// Create a message with the converted data
			msg := service.NewMessage([]byte(fmt.Sprintf("%v", convertedData)))
			msg.MetaSet("modbus_address", fmt.Sprintf("%d", item.Address))

			msgs = append(msgs, msg)
		}
	}

	time.Sleep(time.Second)

	return msgs, func(ctx context.Context, err error) error {
		return nil
	}, nil
}

func (m *ModbusInput) Close(ctx context.Context) error {
	if m.Handler != nil {
		m.Handler.Close()
	}

	return nil
}

func init() {
	err := service.RegisterBatchInput(
		"modbus", ModbusConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newModbusInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}
