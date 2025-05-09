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
)

func determineUntypedConverter(outType string) (converterFunc, error) {
	switch outType {
	case "", "UINT16":
		return func(b []byte) interface{} {
			return uint16(b[0])
		}, nil
	case "BOOL":
		return func(b []byte) interface{} {
			return b[0] != 0
		}, nil
	}
	return nil, fmt.Errorf("invalid output data-type: %s", outType)
}

func determineConverter(inType, byteOrder, outType string, scale float64, bit uint8, strloc string) (converterFunc, error) {
	switch inType {
	case "STRING":
		switch strloc {
		case "", "both":
			return determineConverterString(byteOrder)
		case "lower":
			return determineConverterStringLow(byteOrder)
		case "upper":
			return determineConverterStringHigh(byteOrder)
		}
	case "BIT":
		return determineConverterBit(byteOrder, bit)
	}

	if scale != 0.0 {
		return determineConverterScale(inType, byteOrder, outType, scale)
	}
	return determineConverterNoScale(inType, byteOrder, outType)
}

func determineConverterScale(inType, byteOrder, outType string, scale float64) (converterFunc, error) {
	switch inType {
	case "INT8L":
		return determineConverterI8LScale(outType, byteOrder, scale)
	case "INT8H":
		return determineConverterI8HScale(outType, byteOrder, scale)
	case "UINT8L":
		return determineConverterU8LScale(outType, byteOrder, scale)
	case "UINT8H":
		return determineConverterU8HScale(outType, byteOrder, scale)
	case "INT16":
		return determineConverterI16Scale(outType, byteOrder, scale)
	case "UINT16":
		return determineConverterU16Scale(outType, byteOrder, scale)
	case "INT32":
		return determineConverterI32Scale(outType, byteOrder, scale)
	case "UINT32":
		return determineConverterU32Scale(outType, byteOrder, scale)
	case "INT64":
		return determineConverterI64Scale(outType, byteOrder, scale)
	case "UINT64":
		return determineConverterU64Scale(outType, byteOrder, scale)
	case "FLOAT16":
		return determineConverterF16Scale(outType, byteOrder, scale)
	case "FLOAT32":
		return determineConverterF32Scale(outType, byteOrder, scale)
	case "FLOAT64":
		return determineConverterF64Scale(outType, byteOrder, scale)
	}
	return nil, fmt.Errorf("invalid input data-type: %s", inType)
}

func determineConverterNoScale(inType, byteOrder, outType string) (converterFunc, error) {
	switch inType {
	case "INT8L":
		return determineConverterI8L(outType, byteOrder)
	case "INT8H":
		return determineConverterI8H(outType, byteOrder)
	case "UINT8L":
		return determineConverterU8L(outType, byteOrder)
	case "UINT8H":
		return determineConverterU8H(outType, byteOrder)
	case "INT16":
		return determineConverterI16(outType, byteOrder)
	case "UINT16":
		return determineConverterU16(outType, byteOrder)
	case "INT32":
		return determineConverterI32(outType, byteOrder)
	case "UINT32":
		return determineConverterU32(outType, byteOrder)
	case "INT64":
		return determineConverterI64(outType, byteOrder)
	case "UINT64":
		return determineConverterU64(outType, byteOrder)
	case "FLOAT16":
		return determineConverterF16(outType, byteOrder)
	case "FLOAT32":
		return determineConverterF32(outType, byteOrder)
	case "FLOAT64":
		return determineConverterF64(outType, byteOrder)
	}
	return nil, fmt.Errorf("invalid input data-type: %s", inType)
}
