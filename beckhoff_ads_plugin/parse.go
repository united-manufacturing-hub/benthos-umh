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

package beckhoff_ads_plugin

import (
	"encoding/json"
	"strconv"
	"strings"
)

// tagKind classifies how a decoded value is emitted, matching the opcua/modbus
// convention: numbers/bools go unquoted on the wire, strings are JSON-quoted.
type tagKind int

const (
	tagNumber tagKind = iota
	tagBool
	tagString
)

// classifyBaseType maps an ADS base/primitive type to its wire kind. go-ads has
// already decoded the binary value; this only decides number vs bool vs string.
func classifyBaseType(bt string) tagKind {
	switch strings.ToUpper(strings.TrimSpace(bt)) {
	case "BOOL":
		return tagBool
	case "SINT", "INT", "DINT", "LINT", "USINT", "BYTE", "UINT", "WORD",
		"UDINT", "DWORD", "ULINT", "LWORD", "REAL", "LREAL":
		return tagNumber
	default: // STRING/WSTRING/TIME/TOD/DATE/DT/unknown
		return tagString
	}
}

// adsValueBytes turns a go-ads-decoded string into payload bytes plus a tag-type hint;
// string-like values are JSON-quoted so a numeric-looking string isn't reparsed as a number.
func adsValueBytes(typ string, decoded string) ([]byte, string) {
	switch classifyBaseType(typ) {
	case tagNumber:
		return []byte(decoded), "number"
	case tagBool:
		return []byte(decoded), "bool"
	default:
		b, err := json.Marshal(decoded)
		if err != nil {
			b = []byte(strconv.Quote(decoded))
		}
		return b, "string"
	}
}
