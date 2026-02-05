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
	"hash/maphash"
	"strconv"
)

const (
	maxQuantityDiscreteInput    = uint16(2000)
	maxQuantityCoils            = uint16(2000)
	maxQuantityInputRegisters   = uint16(125)
	maxQuantityHoldingRegisters = uint16(125)
)

func normalizeInputDatatype(dataType string) (string, error) {
	switch dataType {
	case "BIT", "INT8L", "INT8H", "UINT8L", "UINT8H",
		"INT16", "UINT16", "INT32", "UINT32", "INT64", "UINT64",
		"FLOAT16", "FLOAT32", "FLOAT64", "STRING":
		return dataType, nil
	}
	return "unknown", fmt.Errorf("unknown input type %q", dataType)
}

func normalizeOutputDatatype(dataType string) (string, error) {
	switch dataType {
	case "", "native":
		return "native", nil
	case "INT64", "UINT64", "FLOAT64", "STRING":
		return dataType, nil
	}
	return "unknown", fmt.Errorf("unknown output type %q", dataType)
}

func normalizeByteOrder(byteOrder string) (string, error) {
	switch byteOrder {
	case "ABCD", "MSW-BE", "MSW": // Big endian (Motorola)
		return "ABCD", nil
	case "BADC", "MSW-LE": // Big endian with bytes swapped
		return "BADC", nil
	case "CDAB", "LSW-BE": // Little endian with bytes swapped
		return "CDAB", nil
	case "DCBA", "LSW-LE", "LSW": // Little endian (Intel)
		return "DCBA", nil
	}
	return "unknown", fmt.Errorf("unknown byte-order %q", byteOrder)
}

func tagID(seed maphash.Seed, item ModbusDataItemWithAddress) uint64 {
	var mh maphash.Hash
	mh.SetSeed(seed)

	mh.WriteString(item.Register)
	mh.WriteByte(0)
	mh.WriteString(strconv.Itoa(int(item.Address)))
	mh.WriteByte(0)

	return mh.Sum64()
}

// tagIDWithSlave generates a hash for deduplication that includes the slave ID.
// Used for first-pass dedup to catch identical YAML entries.
func tagIDWithSlave(seed maphash.Seed, item ModbusDataItemWithAddress) uint64 {
	var mh maphash.Hash
	mh.SetSeed(seed)

	mh.WriteString(item.Register)
	mh.WriteByte(0)
	mh.WriteString(strconv.Itoa(int(item.Address)))
	mh.WriteByte(0)
	mh.WriteByte(item.SlaveID)

	return mh.Sum64()
}

func determineTagLength(input string, length uint16) (uint16, error) {
	// Handle our special types
	switch input {
	case "BIT", "INT8L", "INT8H", "UINT8L", "UINT8H":
		return 1, nil
	case "INT16", "UINT16", "FLOAT16":
		return 1, nil
	case "INT32", "UINT32", "FLOAT32":
		return 2, nil
	case "INT64", "UINT64", "FLOAT64":
		return 4, nil
	case "STRING":
		return length, nil
	}
	return 0, fmt.Errorf("invalid input datatype %q for determining tag length", input)
}

func determineOutputDatatype(input string) (string, error) {
	// Handle our special types
	switch input {
	case "INT8L", "INT8H", "INT16", "INT32", "INT64":
		return "INT64", nil
	case "BIT", "UINT8L", "UINT8H", "UINT16", "UINT32", "UINT64":
		return "UINT64", nil
	case "FLOAT16", "FLOAT32", "FLOAT64":
		return "FLOAT64", nil
	case "STRING":
		return "STRING", nil
	}
	return "unknown", fmt.Errorf("invalid input datatype %q for determining output", input)
}
