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

// Internal (white-box) tests for the tag-name fallback. Placed in package
// modbus_plugin so they can reach the unexported getModbusTagName helper and
// createMessageFromValue; the suite's single RunSpecs picks these specs up.
package modbus_plugin

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("getModbusTagName", func() {
	It("falls back to the locator when the address is nameless", func() {
		Expect(getModbusTagName(modbusTag{name: "", unifiedAddress: "holding.100.INT16"})).To(Equal("holding.100.INT16"))
	})

	It("prefers the authored name when present", func() {
		Expect(getModbusTagName(modbusTag{name: "temperature", unifiedAddress: "temperature.holding.100.INT16"})).To(Equal("temperature"))
	})
})

var _ = Describe("createMessageFromValue tag-name metadata", func() {
	makeItem := func(name, unified string) modbusTag {
		return modbusTag{
			name:           name,
			unifiedAddress: unified,
			length:         1,
			converter:      func([]byte) interface{} { return uint16(42) },
		}
	}

	It("sets modbus_tag_name to the locator for a nameless item", func() {
		m := &ModbusInput{}
		msg := m.createMessageFromValue(makeItem("", "holding.100.INT16"), []byte{0x00, 0x2a}, "holding")
		Expect(msg).NotTo(BeNil())

		// modbus_tag_name is sanitized (dots -> underscores); the raw locator is
		// preserved in modbus_tag_name_original.
		name, ok := msg.MetaGet("modbus_tag_name")
		Expect(ok).To(BeTrue())
		Expect(name).To(Equal("holding_100_INT16"))

		orig, _ := msg.MetaGet("modbus_tag_name_original")
		Expect(orig).To(Equal("holding.100.INT16"))
	})

	It("sets modbus_tag_name to the sanitized name for a named item", func() {
		m := &ModbusInput{}
		msg := m.createMessageFromValue(makeItem("temperature", "temperature.holding.100.INT16"), []byte{0x00, 0x2a}, "holding")
		Expect(msg).NotTo(BeNil())

		name, _ := msg.MetaGet("modbus_tag_name")
		Expect(name).To(Equal("temperature"))
	})
})
