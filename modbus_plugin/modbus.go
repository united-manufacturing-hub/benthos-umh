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
	"fmt"
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

	// Internal
	Handler modbus.TCPClientHandler
	Client  modbus.Client
	Log     *service.Logger
}

type converterFunc func([]byte) interface{}

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

	addressesConf, err := conf.FieldObjectList("addresses")
	if err != nil {
		return nil, err
	}

	for _, addrConf := range addressesConf {
		item := ModbusDataItemWithAddress{}
		if item.Name, err = addrConf.FieldString("name"); err != nil {
			return nil, err
		}
		if item.Register, err = addrConf.FieldString("register"); err != nil {
			return nil, err
		}
		if item.Address, err = addrConf.FieldUint16("address"); err != nil {
			return nil, err
		}
		if item.Type, err = addrConf.FieldString("type"); err != nil {
			return nil, err
		}
		if item.Length, err = addrConf.FieldUint16("length"); err != nil {
			return nil, err
		}
		if item.Bit, err = addrConf.FieldUint16("bit"); err != nil {
			return nil, err
		}
		if item.Scale, err = addrConf.FieldFloat("scale"); err != nil {
			return nil, err
		}
		if item.Output, err = addrConf.FieldString("output"); err != nil {
			return nil, err
		}
		m.Addresses = append(m.Addresses, item)
	}

	return service.AutoRetryNacksBatched(m), nil
}

func ParseModbusAddresses(addresses []string) ([][]ModbusDataItemWithAddress, error) {
	parsedAddresses := make([]ModbusDataItemWithAddress, 0, len(addresses))

	for _, address := range addresses {
		addr, qty, converterFunc, err := handleModbusAddress(address)
		if err != nil {
			return nil, fmt.Errorf("address %q: %w", address, err)
		}

		newModbusDataItemWithAddress := ModbusDataItemWithAddress{
			Address:       addr,
			Quantity:      qty,
			ConverterFunc: converterFunc,
		}

		parsedAddresses = append(parsedAddresses, newModbusDataItemWithAddress)
	}

	// Now split the addresses into batches based on a reasonable size
	batchMaxSize := 125 // Modbus typically allows up to 125 registers per request
	batches := make([][]ModbusDataItemWithAddress, 0)
	for i := 0; i < len(parsedAddresses); i += batchMaxSize {
		end := i + batchMaxSize
		if end > len(parsedAddresses) {
			end = len(parsedAddresses)
		}
		batches = append(batches, parsedAddresses[i:end])
	}

	return batches, nil
}

func handleModbusAddress(address string) (uint16, uint16, converterFunc, error) {
	var addr uint16
	var qty uint16
	n, err := fmt.Sscanf(address, "%d:%d", &addr, &qty)
	if n != 2 || err != nil {
		return 0, 0, nil, fmt.Errorf("invalid address format: %s", address)
	}

	converterFunc := determineModbusConversion(qty)
	return addr, qty, converterFunc, nil
}

func determineModbusConversion(qty uint16) converterFunc {
	return func(data []byte) interface{} {
		// Simple conversion function example
		if qty == 1 {
			return int(data[0])
		}
		return data
	}
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
