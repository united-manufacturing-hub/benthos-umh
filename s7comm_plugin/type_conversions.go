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
	"bytes"
	"encoding/binary"
	"math"
	"time"
)

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
			var result []byte
			if length < len(buf)-2 {
				result = buf[2 : 2+length]
			} else {
				result = buf[2:]
			}
			// trim the null bytes
			result = bytes.TrimRight(result, "\x00")
			return string(result)
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
			return dateTimeAt(buf).UnixNano()
		}
	}

	panic("Unknown type!")
}

// dateTimeAt decodes the 8 bytes of an S7 DATE_AND_TIME: year, month, day,
// hour, minute and second in BCD, then the milliseconds as two BCD digits
// followed by one digit in the high nibble of the last byte. The year has two
// digits, 90 and above is 19xx and below is 20xx. A field that is no valid
// date is normalized by time.Date, not refused.
func dateTimeAt(buf []byte) time.Time {
	year := decodeBCD(buf[0])
	if year < 90 {
		year += 2000
	} else {
		year += 1900
	}
	milliseconds := decodeBCD(buf[6])*10 + decodeBCD(buf[7]>>4)

	return time.Date(year, time.Month(decodeBCD(buf[1])), decodeBCD(buf[2]), decodeBCD(buf[3]), decodeBCD(buf[4]), decodeBCD(buf[5]), milliseconds*int(time.Millisecond), time.UTC)
}

// decodeBCD turns a byte of two decimal digits into its number.
func decodeBCD(value byte) int {
	return int((value>>4)*10 + (value & 0x0F))
}
