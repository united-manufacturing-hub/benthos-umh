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
	"encoding/binary"
	"math"

	"github.com/robinson/gos7"
)

var helper = &gos7.Helper{}

func determineConversion(dtype string) converterFunc {
	switch dtype {
	case "X":
		return func(buf []byte) interface{} {
			return buf[0] != 0
		}
	case "B":
		return func(buf []byte) interface{} {
			return buf[0]
		}
	case "C":
		return func(buf []byte) interface{} {
			return string(buf[0])
		}
	case "S":
		return func(buf []byte) interface{} {
			if len(buf) <= 2 {
				return ""
			}
			// Get the length of the encoded string
			length := int(buf[1])
			// Clip the string if we do not fill the whole buffer
			if length < len(buf)-2 {
				return string(buf[2 : 2+length])
			}
			return string(buf[2:])
		}
	case "W":
		return func(buf []byte) interface{} {
			return binary.BigEndian.Uint16(buf)
		}
	case "I":
		return func(buf []byte) interface{} {
			return int16(binary.BigEndian.Uint16(buf))
		}
	case "DW":
		return func(buf []byte) interface{} {
			return binary.BigEndian.Uint32(buf)
		}
	case "DI":
		return func(buf []byte) interface{} {
			return int32(binary.BigEndian.Uint32(buf))
		}
	case "R":
		return func(buf []byte) interface{} {
			x := binary.BigEndian.Uint32(buf)
			return math.Float32frombits(x)
		}
	case "DT":
		return func(buf []byte) interface{} {
			return helper.GetDateTimeAt(buf, 0).UnixNano()
		}
	}

	panic("Unknown type!")
}
