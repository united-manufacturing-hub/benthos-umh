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

package s7comm_plugin

// The words of an input address, as in DB1.DW20, named once. Code that branches
// on a type or an area uses these constants and not string literals, so that a
// renamed or added token is a compile error or an exhaustive-switch finding,
// and not a case that silently never matches.

// dataType is the type token of an address.
type dataType string

const (
	typeBit      dataType = "X"
	typeByte     dataType = "B"
	typeChar     dataType = "C"
	typeString   dataType = "S"
	typeWord     dataType = "W"
	typeInt      dataType = "I"
	typeDWord    dataType = "DW"
	typeDInt     dataType = "DI"
	typeReal     dataType = "R"
	typeDateTime dataType = "DT"
)

// area is the area token of an address.
type area string

const (
	areaProcessInput  area = "PE"
	areaProcessOutput area = "PA"
	areaMerker        area = "MK"
	areaDataBlock     area = "DB"
	areaCounter       area = "C"
	areaTimer         area = "T"
)

// The names of the parts of an address in addressRegexp.
const (
	groupArea   = "area"
	groupNumber = "no"
	groupType   = "type"
	groupStart  = "start"
	groupExtra  = "extra"
)

// The area codes of the S7 protocol, as they go into a request.
const (
	areaCodeProcessInput  = 0x81
	areaCodeProcessOutput = 0x82
	areaCodeMerker        = 0x83
	areaCodeDataBlock     = 0x84
	areaCodeCounter       = 0x1C
	areaCodeTimer         = 0x1D
)

// The word lengths of the S7 protocol, as they go into a request. A string is
// read as a run of characters.
const (
	wordLengthBit      = 0x01
	wordLengthByte     = 0x02
	wordLengthChar     = 0x03
	wordLengthWord     = 0x04
	wordLengthInt      = 0x05
	wordLengthDWord    = 0x06
	wordLengthDInt     = 0x07
	wordLengthReal     = 0x08
	wordLengthDateTime = 0x0F // see https://support.industry.siemens.com/cs/document/36479/date_and_time-format-for-s7-?dti=0&lc=en-DE
)
