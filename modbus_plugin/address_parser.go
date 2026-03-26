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

package modbus_plugin

import (
	"fmt"
	"strconv"
	"strings"
)

// validRegisters is the set of valid Modbus register types.
var validRegisters = map[string]bool{
	"coil":     true,
	"discrete": true,
	"holding":  true,
	"input":    true,
}

// validTypes is the set of valid Modbus data types.
var validTypes = map[string]bool{
	"BIT":     true,
	"INT8L":   true,
	"INT8H":   true,
	"UINT8L":  true,
	"UINT8H":  true,
	"INT16":   true,
	"UINT16":  true,
	"INT32":   true,
	"UINT32":  true,
	"INT64":   true,
	"UINT64":  true,
	"FLOAT16": true,
	"FLOAT32": true,
	"FLOAT64": true,
	"STRING":  true,
}

// validOptionKeys is the set of valid optional key names.
var validOptionKeys = map[string]bool{
	"slaveID": true,
	"length":  true,
	"bit":     true,
	"scale":   true,
	"output":  true,
}

// validOutputTypes is the set of valid output type values.
var validOutputTypes = map[string]bool{
	"INT64":   true,
	"UINT64":  true,
	"FLOAT64": true,
	"STRING":  true,
	"BOOL":    true,
	"UINT16":  true,
}

// ParseModbusAddress parses a unified address string of the form
// "name.register.address.type[:key=value]*" into a ModbusDataItemWithAddress.
func ParseModbusAddress(addr string) (ModbusDataItemWithAddress, error) {
	var item ModbusDataItemWithAddress

	if addr == "" {
		return item, fmt.Errorf("empty address string")
	}

	// Split on ':' → first element is positional, rest are key-value options
	parts := strings.Split(addr, ":")
	positional := parts[0]
	options := parts[1:]

	// Split positional part on '.'
	segments := strings.Split(positional, ".")
	if len(segments) != 4 {
		return item, fmt.Errorf("expected exactly 4 dot-separated segments (name.register.address.type), got %d", len(segments))
	}

	// 1. Name
	name := segments[0]
	if name == "" {
		return item, fmt.Errorf("empty name in address %q", addr)
	}
	item.Name = name

	// 2. Register
	register := segments[1]
	if !validRegisters[register] {
		return item, fmt.Errorf("invalid register %q, must be one of: coil, discrete, holding, input", register)
	}
	item.Register = register

	// 3. Address
	address, err := strconv.Atoi(segments[2])
	if err != nil {
		return item, fmt.Errorf("invalid address %q: %w", segments[2], err)
	}
	if address < 0 || address > 65535 {
		return item, fmt.Errorf("address %d out of range (0-65535)", address)
	}
	item.Address = uint16(address)

	// 4. Type
	typeName := segments[3]
	if !validTypes[typeName] {
		return item, fmt.Errorf("invalid type %q", typeName)
	}
	item.Type = typeName

	// Parse optional key-value pairs
	seenKeys := make(map[string]bool)
	for _, opt := range options {
		if opt == "" {
			continue
		}

		kv := strings.SplitN(opt, "=", 2)
		if len(kv) != 2 {
			return item, fmt.Errorf("invalid option %q, expected key=value", opt)
		}

		key, value := kv[0], kv[1]
		if !validOptionKeys[key] {
			return item, fmt.Errorf("unknown option key %q", key)
		}

		if seenKeys[key] {
			return item, fmt.Errorf("duplicate option key %q", key)
		}
		seenKeys[key] = true

		switch key {
		case "slaveID":
			slaveID, err := strconv.Atoi(value)
			if err != nil {
				return item, fmt.Errorf("invalid slave value %q: %w", value, err)
			}

			if slaveID < 0 || slaveID > 255 {
				return item, fmt.Errorf("slave %d out of range (0-255)", slaveID)
			}

			item.SlaveID = byte(slaveID)
		case "length":
			length, err := strconv.Atoi(value)
			if err != nil {
				return item, fmt.Errorf("invalid length value %q: %w", value, err)
			}

			if length < 0 || length > 65535 {
				return item, fmt.Errorf("length %d out of range (0-65535)", length)
			}

			item.Length = uint16(length)
		case "bit":
			bit, err := strconv.Atoi(value)
			if err != nil {
				return item, fmt.Errorf("invalid bit value %q: %w", value, err)
			}

			if bit < 0 || bit > 7 {
				return item, fmt.Errorf("bit %d out of range (0-7)", bit)
			}

			item.Bit = uint16(bit)
		case "scale":
			scale, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return item, fmt.Errorf("invalid scale value %q: %w", value, err)
			}

			item.Scale = scale
		case "output":
			if !validOutputTypes[value] {
				return item, fmt.Errorf("invalid output type %q, must be one of: INT64, UINT64, FLOAT64, STRING, BOOL, UINT16", value)
			}

			item.Output = value
		}
	}

	// Cross-validate: length only with STRING
	if item.Length != 0 && item.Type != "STRING" {
		return item, fmt.Errorf("length option is only valid for STRING type, got %q", item.Type)
	}

	// Cross-validate: bit only with BIT
	if item.Bit != 0 && item.Type != "BIT" {
		return item, fmt.Errorf("bit option is only valid for BIT type, got %q", item.Type)
	}

	return item, nil
}

// FormatModbusAddress converts a ModbusDataItemWithAddress back to the unified
// dotted-chain address string format.
func FormatModbusAddress(item ModbusDataItemWithAddress) string {
	var b strings.Builder

	// Positional: name.register.address.type
	b.WriteString(item.Name)
	b.WriteByte('.')
	b.WriteString(item.Register)
	b.WriteByte('.')
	b.WriteString(strconv.Itoa(int(item.Address)))
	b.WriteByte('.')
	b.WriteString(item.Type)

	// Optional key-value pairs (in a stable order)
	if item.SlaveID != 0 {
		b.WriteString(":slaveID=")
		b.WriteString(strconv.Itoa(int(item.SlaveID)))
	}

	if item.Length != 0 {
		b.WriteString(":length=")
		b.WriteString(strconv.Itoa(int(item.Length)))
	}

	if item.Bit != 0 {
		b.WriteString(":bit=")
		b.WriteString(strconv.Itoa(int(item.Bit)))
	}

	if item.Scale != 0.0 {
		b.WriteString(":scale=")
		b.WriteString(strconv.FormatFloat(item.Scale, 'f', -1, 64))
	}

	if item.Output != "" {
		b.WriteString(":output=")
		b.WriteString(item.Output)
	}

	return b.String()
}
