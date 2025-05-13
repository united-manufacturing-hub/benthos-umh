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

package tag_browser_plugin

import (
	"encoding/binary"
	"errors"
	"github.com/goccy/go-json"
	"math"
	"reflect"
	"unsafe"
)

// ToBytes turns v into bytes, reports its type name, and returns
// any JSON‑marshal error that occurred on the fallback path.
//
// Fast paths allocate nothing; JSON fallback allocates once.
func ToBytes(v any) ([]byte, string, error) {
	switch x := v.(type) {
	case nil:
		return nil, "nil", nil

	case []byte:
		return x, "[]byte", nil

	case string:
		sh := (*[2]uintptr)(unsafe.Pointer(&x))                  // StringHeader
		b := unsafe.Slice((*byte)(unsafe.Pointer(sh[0])), sh[1]) // zero‑copy
		return b, "string", nil

	case bool:
		if x {
			return []byte{1}, "bool", nil
		}
		return []byte{0}, "bool", nil

	case int8:
		return []byte{byte(x)}, "int8", nil
	case uint8:
		return []byte{x}, "uint8", nil

	case int16:
		var b [2]byte
		binary.LittleEndian.PutUint16(b[:], uint16(x))
		return b[:], "int16", nil
	case uint16:
		var b [2]byte
		binary.LittleEndian.PutUint16(b[:], x)
		return b[:], "uint16", nil

	case int32:
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(x))
		return b[:], "int32", nil
	case uint32:
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], x)
		return b[:], "uint32", nil

	case int64:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(x))
		return b[:], "int64", nil
	case uint64:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], x)
		return b[:], "uint64", nil

	case int:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(x))
		return b[:], "int", nil
	case uint:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(x))
		return b[:], "uint", nil

	case float32:
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], math.Float32bits(x))
		return b[:], "float32", nil
	case float64:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], math.Float64bits(x))
		return b[:], "float64", nil

	default:
		b, err := json.Marshal(x)
		return b, reflect.TypeOf(x).String(), err
	}
}

// FromBytes performs the inverse ofToBytes.
//
//	b – raw data produced byToBytes
//	typ – the type string returned byToBytes
//
// It reconstructs the original value (or the best‑effort equivalent) and
// returns it as any plus an (optional) error.
func FromBytes(b []byte, typ string) (any, error) {
	switch typ {
	case "nil":
		return nil, nil

	case "[]byte":
		return b, nil

	case "string": // zero‑copy
		return *(*string)(unsafe.Pointer(&b)), nil

	case "bool":
		if len(b) != 1 {
			return nil, errors.New("bool needs 1 byte")
		}
		return b[0] != 0, nil

	case "int8":
		if len(b) != 1 {
			return nil, errors.New("int8 needs 1 byte")
		}
		return int8(b[0]), nil
	case "uint8":
		if len(b) != 1 {
			return nil, errors.New("uint8 needs 1 byte")
		}
		return b[0], nil

	case "int16":
		if len(b) != 2 {
			return nil, errors.New("int16 needs 2 bytes")
		}
		return int16(binary.LittleEndian.Uint16(b)), nil
	case "uint16":
		if len(b) != 2 {
			return nil, errors.New("uint16 needs 2 bytes")
		}
		return binary.LittleEndian.Uint16(b), nil

	case "int32":
		if len(b) != 4 {
			return nil, errors.New("int32 needs 4 bytes")
		}
		return int32(binary.LittleEndian.Uint32(b)), nil
	case "uint32":
		if len(b) != 4 {
			return nil, errors.New("uint32 needs 4 bytes")
		}
		return binary.LittleEndian.Uint32(b), nil

	case "int64":
		if len(b) != 8 {
			return nil, errors.New("int64 needs 8 bytes")
		}
		return int64(binary.LittleEndian.Uint64(b)), nil
	case "uint64":
		if len(b) != 8 {
			return nil, errors.New("uint64 needs 8 bytes")
		}
		return binary.LittleEndian.Uint64(b), nil

	case "int":
		if len(b) != 8 {
			return nil, errors.New("int needs 8 bytes")
		}
		return int(binary.LittleEndian.Uint64(b)), nil
	case "uint":
		if len(b) != 8 {
			return nil, errors.New("uint needs 8 bytes")
		}
		return uint(binary.LittleEndian.Uint64(b)), nil

	case "float32":
		if len(b) != 4 {
			return nil, errors.New("float32 needs 4 bytes")
		}
		bits := binary.LittleEndian.Uint32(b)
		return math.Float32frombits(bits), nil
	case "float64":
		if len(b) != 8 {
			return nil, errors.New("float64 needs 8 bytes")
		}
		bits := binary.LittleEndian.Uint64(b)
		return math.Float64frombits(bits), nil

	default:
		// Unknown type → generic decode into map/array.
		var v any
		err := json.Unmarshal(b, &v)
		return v, err
	}
}
