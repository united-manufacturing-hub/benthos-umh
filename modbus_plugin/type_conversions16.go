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
	"encoding/binary"
	"fmt"

	"github.com/x448/float16"
)

type convert16 func([]byte) uint16

func endiannessConverter16(byteOrder string) (convert16, error) {
	switch byteOrder {
	case "ABCD", "CDAB": // Big endian (Motorola)
		return binary.BigEndian.Uint16, nil
	case "DCBA", "BADC": // Little endian (Intel)
		return binary.LittleEndian.Uint16, nil
	}
	return nil, fmt.Errorf("invalid byte-order: %s", byteOrder)
}

// I16 - no scale
func determineConverterI16(outType string, byteOrder string) (converterFunc, error) {
	tohost, err := endiannessConverter16(byteOrder)
	if err != nil {
		return nil, err
	}

	switch outType {
	case "native":
		return func(b []byte) interface{} {
			return int16(tohost(b))
		}, nil
	case "INT64":
		return func(b []byte) interface{} {
			return int64(int16(tohost(b)))
		}, nil
	case "UINT64":
		return func(b []byte) interface{} {
			return uint64(int16(tohost(b)))
		}, nil
	case "FLOAT64":
		return func(b []byte) interface{} {
			return float64(int16(tohost(b)))
		}, nil
	}
	return nil, fmt.Errorf("invalid output data-type: %s", outType)
}

// U16 - no scale
func determineConverterU16(outType string, byteOrder string) (converterFunc, error) {
	tohost, err := endiannessConverter16(byteOrder)
	if err != nil {
		return nil, err
	}

	switch outType {
	case "native":
		return func(b []byte) interface{} {
			return tohost(b)
		}, nil
	case "INT64":
		return func(b []byte) interface{} {
			return int64(tohost(b))
		}, nil
	case "UINT64":
		return func(b []byte) interface{} {
			return uint64(tohost(b))
		}, nil
	case "FLOAT64":
		return func(b []byte) interface{} {
			return float64(tohost(b))
		}, nil
	}
	return nil, fmt.Errorf("invalid output data-type: %s", outType)
}

// F16 - no scale
func determineConverterF16(outType string, byteOrder string) (converterFunc, error) {
	tohost, err := endiannessConverter16(byteOrder)
	if err != nil {
		return nil, err
	}

	switch outType {
	case "native":
		return func(b []byte) interface{} {
			raw := tohost(b)
			return float16.Frombits(raw).Float32()
		}, nil
	case "FLOAT64":
		return func(b []byte) interface{} {
			raw := tohost(b)
			in := float16.Frombits(raw).Float32()
			return float64(in)
		}, nil
	}
	return nil, fmt.Errorf("invalid output data-type: %s", outType)
}

// I16 - scale
func determineConverterI16Scale(outType string, byteOrder string, scale float64) (converterFunc, error) {
	tohost, err := endiannessConverter16(byteOrder)
	if err != nil {
		return nil, err
	}

	switch outType {
	case "native":
		return func(b []byte) interface{} {
			in := int16(tohost(b))
			return int16(float64(in) * scale)
		}, nil
	case "INT64":
		return func(b []byte) interface{} {
			in := int16(tohost(b))
			return int64(float64(in) * scale)
		}, nil
	case "UINT64":
		return func(b []byte) interface{} {
			in := int16(tohost(b))
			return uint64(float64(in) * scale)
		}, nil
	case "FLOAT64":
		return func(b []byte) interface{} {
			in := int16(tohost(b))
			return float64(in) * scale
		}, nil
	}
	return nil, fmt.Errorf("invalid output data-type: %s", outType)
}

// U16 - scale
func determineConverterU16Scale(outType string, byteOrder string, scale float64) (converterFunc, error) {
	tohost, err := endiannessConverter16(byteOrder)
	if err != nil {
		return nil, err
	}

	switch outType {
	case "native":
		return func(b []byte) interface{} {
			in := tohost(b)
			return uint16(float64(in) * scale)
		}, nil
	case "INT64":
		return func(b []byte) interface{} {
			in := tohost(b)
			return int64(float64(in) * scale)
		}, nil
	case "UINT64":
		return func(b []byte) interface{} {
			in := tohost(b)
			return uint64(float64(in) * scale)
		}, nil
	case "FLOAT64":
		return func(b []byte) interface{} {
			in := tohost(b)
			return float64(in) * scale
		}, nil
	}
	return nil, fmt.Errorf("invalid output data-type: %s", outType)
}

// F16 - scale
func determineConverterF16Scale(outType string, byteOrder string, scale float64) (converterFunc, error) {
	tohost, err := endiannessConverter16(byteOrder)
	if err != nil {
		return nil, err
	}

	switch outType {
	case "native":
		return func(b []byte) interface{} {
			raw := tohost(b)
			in := float16.Frombits(raw)
			return in.Float32() * float32(scale)
		}, nil
	case "FLOAT64":
		return func(b []byte) interface{} {
			raw := tohost(b)
			in := float16.Frombits(raw)
			return float64(in.Float32()) * scale
		}, nil
	}
	return nil, fmt.Errorf("invalid output data-type: %s", outType)
}
