// Copyright 2026 UMH Systems GmbH
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

package template_write_plugin

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"regexp"
	"strconv"
)

// The example protocol's address grammar: a type prefix and an offset.
//
//	INT10    16-bit signed integer at offset 10
//	BOOL3    boolean at offset 3
//	FLOAT20  32-bit float at offset 20
//	STATUS1  read-only status word; never writable
//
// A real protocol replaces this with its own grammar, for example S7's
// "DB10.DBD4" or Modbus's "holding:40001:INT16".
var addressPattern = regexp.MustCompile(`^(INT|BOOL|FLOAT|STATUS)(\d+)$`)

// point is one prepared write. It implements Point.
type devicePoint struct {
	address string
	data    []byte
}

var _ Point[devicePoint] = devicePoint{}

func (p devicePoint) Address() string { return p.address }

// SameValue compares raw bytes. That is exact for integers and bools. For
// floats it compares the bits we wrote, so it never fails on rounding.
func (p devicePoint) SameValue(other devicePoint) bool { return bytes.Equal(p.data, other.data) }

// codec turns a mapping into a point. It does no I/O.
type deviceCodec struct{}

var _ Codec[devicePoint] = deviceCodec{}

// Prepare checks the address and encodes the value. Every failure wraps an
// sentinel from protocol.go, so the engine reports it with a stable reason.
func (deviceCodec) Prepare(address string, value any) (devicePoint, error) {
	match := addressPattern.FindStringSubmatch(address)
	if match == nil {
		return devicePoint{}, fmt.Errorf("%w: %q is not an address like INT10, BOOL3 or FLOAT20", ErrInvalidValue, address)
	}
	if _, err := strconv.Atoi(match[2]); err != nil {
		return devicePoint{}, fmt.Errorf("%w: offset in %q is too large", ErrInvalidValue, address)
	}

	data, err := encode(match[1], value)
	if err != nil {
		return devicePoint{}, err
	}
	return devicePoint{address: address, data: data}, nil
}

// encode uses the value helpers in values.go, which are strict: a value that does
// not fit is an error, never a silent wrap or cut.
func encode(kind string, value any) ([]byte, error) {
	switch kind {
	case "INT":
		number, err := IntegerInRange(value, math.MinInt16, math.MaxInt16)
		if err != nil {
			return nil, err
		}
		return binary.BigEndian.AppendUint16(nil, uint16(int16(number))), nil
	case "BOOL":
		flag, err := Bool(value)
		if err != nil {
			return nil, err
		}
		if flag {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case "FLOAT":
		number, err := Float32(value)
		if err != nil {
			return nil, err
		}
		return binary.BigEndian.AppendUint32(nil, math.Float32bits(number)), nil
	default:
		return nil, fmt.Errorf("%w: %s addresses are read-only", ErrNotWritable, kind)
	}
}
