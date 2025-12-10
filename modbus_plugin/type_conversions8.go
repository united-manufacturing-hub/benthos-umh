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

func endiannessIndex8(byteOrder string, low bool) (int, error) {
	switch byteOrder {
	case "ABCD": // Big endian (Motorola)
		if low {
			return 1, nil
		}
		return 0, nil
	case "DCBA": // Little endian (Intel)
		if low {
			return 0, nil
		}
		return 1, nil
	}
	return -1, fmt.Errorf("invalid byte-order: %s", byteOrder)
}

// I8 lower byte - no scale
func determineConverterI8L(outType string, byteOrder string) (converterFunc, error) {
	idx, err := endiannessIndex8(byteOrder, true)
	if err != nil {
		return nil, err
	}

	switch outType {
	case "native":
		return func(b []byte) interface{} {
			return int8(b[idx])
		}, nil
	case "INT64":
		return func(b []byte) interface{} {
			return int64(int8(b[idx]))
		}, nil
	case "UINT64":
		return func(b []byte) interface{} {
			return uint64(int8(b[idx]))
		}, nil
	case "FLOAT64":
		return func(b []byte) interface{} {
			return float64(int8(b[idx]))
		}, nil
	}
	return nil, fmt.Errorf("invalid output data-type: %s", outType)
}

// I8 higher byte - no scale
func determineConverterI8H(outType string, byteOrder string) (converterFunc, error) {
	idx, err := endiannessIndex8(byteOrder, false)
	if err != nil {
		return nil, err
	}

	switch outType {
	case "native":
		return func(b []byte) interface{} {
			return int8(b[idx])
		}, nil
	case "INT64":
		return func(b []byte) interface{} {
			return int64(int8(b[idx]))
		}, nil
	case "UINT64":
		return func(b []byte) interface{} {
			return uint64(int8(b[idx]))
		}, nil
	case "FLOAT64":
		return func(b []byte) interface{} {
			return float64(int8(b[idx]))
		}, nil
	}
	return nil, fmt.Errorf("invalid output data-type: %s", outType)
}

// U8 lower byte - no scale
func determineConverterU8L(outType string, byteOrder string) (converterFunc, error) {
	idx, err := endiannessIndex8(byteOrder, true)
	if err != nil {
		return nil, err
	}

	switch outType {
	case "native":
		return func(b []byte) interface{} {
			return b[idx]
		}, nil
	case "INT64":
		return func(b []byte) interface{} {
			return int64(b[idx])
		}, nil
	case "UINT64":
		return func(b []byte) interface{} {
			return uint64(b[idx])
		}, nil
	case "FLOAT64":
		return func(b []byte) interface{} {
			return float64(b[idx])
		}, nil
	}
	return nil, fmt.Errorf("invalid output data-type: %s", outType)
}

// U8 higher byte - no scale
func determineConverterU8H(outType string, byteOrder string) (converterFunc, error) {
	idx, err := endiannessIndex8(byteOrder, false)
	if err != nil {
		return nil, err
	}

	switch outType {
	case "native":
		return func(b []byte) interface{} {
			return b[idx]
		}, nil
	case "INT64":
		return func(b []byte) interface{} {
			return int64(b[idx])
		}, nil
	case "UINT64":
		return func(b []byte) interface{} {
			return uint64(b[idx])
		}, nil
	case "FLOAT64":
		return func(b []byte) interface{} {
			return float64(b[idx])
		}, nil
	}
	return nil, fmt.Errorf("invalid output data-type: %s", outType)
}

// I8 lower byte - scale
func determineConverterI8LScale(outType string, byteOrder string, scale float64) (converterFunc, error) {
	idx, err := endiannessIndex8(byteOrder, true)
	if err != nil {
		return nil, err
	}

	switch outType {
	case "native":
		return func(b []byte) interface{} {
			in := int8(b[idx])
			return int8(float64(in) * scale)
		}, nil
	case "INT64":
		return func(b []byte) interface{} {
			in := int8(b[idx])
			return int64(float64(in) * scale)
		}, nil
	case "UINT64":
		return func(b []byte) interface{} {
			in := int8(b[idx])
			return uint64(float64(in) * scale)
		}, nil
	case "FLOAT64":
		return func(b []byte) interface{} {
			in := int8(b[idx])
			return float64(in) * scale
		}, nil
	}
	return nil, fmt.Errorf("invalid output data-type: %s", outType)
}

// I8 higher byte - scale
func determineConverterI8HScale(outType string, byteOrder string, scale float64) (converterFunc, error) {
	idx, err := endiannessIndex8(byteOrder, false)
	if err != nil {
		return nil, err
	}

	switch outType {
	case "native":
		return func(b []byte) interface{} {
			in := int8(b[idx])
			return int8(float64(in) * scale)
		}, nil
	case "INT64":
		return func(b []byte) interface{} {
			in := int8(b[idx])
			return int64(float64(in) * scale)
		}, nil
	case "UINT64":
		return func(b []byte) interface{} {
			in := int8(b[idx])
			return uint64(float64(in) * scale)
		}, nil
	case "FLOAT64":
		return func(b []byte) interface{} {
			in := int8(b[idx])
			return float64(in) * scale
		}, nil
	}
	return nil, fmt.Errorf("invalid output data-type: %s", outType)
}

// U8 lower byte - scale
func determineConverterU8LScale(outType string, byteOrder string, scale float64) (converterFunc, error) {
	idx, err := endiannessIndex8(byteOrder, true)
	if err != nil {
		return nil, err
	}

	switch outType {
	case "native":
		return func(b []byte) interface{} {
			return uint8(float64(b[idx]) * scale)
		}, nil
	case "INT64":
		return func(b []byte) interface{} {
			return int64(float64(b[idx]) * scale)
		}, nil
	case "UINT64":
		return func(b []byte) interface{} {
			return uint64(float64(b[idx]) * scale)
		}, nil
	case "FLOAT64":
		return func(b []byte) interface{} {
			return float64(b[idx]) * scale
		}, nil
	}
	return nil, fmt.Errorf("invalid output data-type: %s", outType)
}

// U8 higher byte - scale
func determineConverterU8HScale(outType string, byteOrder string, scale float64) (converterFunc, error) {
	idx, err := endiannessIndex8(byteOrder, false)
	if err != nil {
		return nil, err
	}

	switch outType {
	case "native":
		return func(b []byte) interface{} {
			return uint8(float64(b[idx]) * scale)
		}, nil
	case "INT64":
		return func(b []byte) interface{} {
			return int64(float64(b[idx]) * scale)
		}, nil
	case "UINT64":
		return func(b []byte) interface{} {
			return uint64(float64(b[idx]) * scale)
		}, nil
	case "FLOAT64":
		return func(b []byte) interface{} {
			return float64(b[idx]) * scale
		}, nil
	}
	return nil, fmt.Errorf("invalid output data-type: %s", outType)
}
