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

package modbus_plugin

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/tag_processor_plugin/tagprocessortest"
)

// a STRING register holds two bytes, so an odd-length string ends in a NUL byte
var (
	nonNumericRegisters     = append([]byte(tagprocessortest.NonNumericString), 0x00)
	seventeenDigitRegisters = append([]byte(tagprocessortest.SeventeenDigitString), 0x00)
)

var _ = Describe("Modbus value type through tag_processor", func() {
	DescribeTable("keeps the type and value of the register value",
		func(address string, registerBytes []byte, deviceValue any) {
			input := &ModbusInput{}
			parsedAddress, err := ParseModbusAddress(address)
			Expect(err).NotTo(HaveOccurred())
			tag, err := input.newTag(parsedAddress)
			Expect(err).NotTo(HaveOccurred())

			message := input.createMessageFromValue(tag, registerBytes, parsedAddress.Register)
			Expect(message).NotTo(BeNil())

			tagprocessortest.ExpectTagProcessorKeepsValue(deviceValue, message)
		},
		Entry("numeric STRING", "tag.holding.0.STRING:length=2", []byte(tagprocessortest.NumericString), tagprocessortest.NumericString),
		Entry("non-numeric STRING", "tag.holding.0.STRING:length=2", nonNumericRegisters, tagprocessortest.NonNumericString),
		Entry("17-digit numeric STRING", "tag.holding.0.STRING:length=9", seventeenDigitRegisters, tagprocessortest.SeventeenDigitString),
		Entry("INT16", "tag.holding.0.INT16", []byte{0xFF, 0xFE}, tagprocessortest.NegativeInteger),
		Entry("UINT32", "tag.holding.0.UINT32", []byte{0x00, 0x00, 0x00, 0x2A}, tagprocessortest.PositiveInteger),
		Entry("FLOAT32", "tag.holding.0.FLOAT32", []byte{0x41, 0xBC, 0x00, 0x00}, tagprocessortest.FractionalNumber),
		Entry("coil as BOOL", "tag.coil.0.BIT:output=BOOL", []byte{0x01}, true),
	)
})
