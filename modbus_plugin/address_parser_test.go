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

package modbus_plugin_test

import (
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/united-manufacturing-hub/benthos-umh/modbus_plugin"
)

var _ = Describe("ParseModbusAddress", func() {
	Context("valid addresses", func() {
		It("should parse a basic 4-segment address", func() {
			item, err := ParseModbusAddress("temperature.holding.100.INT16")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.Name).To(Equal("temperature"))
			Expect(item.Register).To(Equal("holding"))
			Expect(item.Address).To(Equal(uint16(100)))
			Expect(item.Type).To(Equal("INT16"))
			Expect(item.SlaveID).To(Equal(byte(0)))
			Expect(item.Length).To(Equal(uint16(0)))
			Expect(item.Bit).To(Equal(uint16(0)))
			Expect(item.Scale).To(Equal(0.0))
			Expect(item.Output).To(Equal(""))
		})

		It("should parse all register types", func() {
			for _, reg := range []string{"coil", "discrete", "holding", "input"} {
				item, err := ParseModbusAddress("tag." + reg + ".0.UINT16")
				Expect(err).NotTo(HaveOccurred())
				Expect(item.Register).To(Equal(reg))
			}
		})

		It("should parse all data types", func() {
			types := []string{
				"BIT", "INT8L", "INT8H", "UINT8L", "UINT8H",
				"INT16", "UINT16", "INT32", "UINT32", "INT64", "UINT64",
				"FLOAT16", "FLOAT32", "FLOAT64", "STRING",
			}
			for _, t := range types {
				addr := "tag.holding.0." + t
				if t == "STRING" {
					addr += ":length=10"
				}
				item, err := ParseModbusAddress(addr)
				Expect(err).NotTo(HaveOccurred(), "type %s", t)
				Expect(item.Type).To(Equal(t))
			}
		})

		It("should parse address with all optional keys", func() {
			item, err := ParseModbusAddress("x.holding.100.FLOAT32:scale=0.1:output=FLOAT64:slaveID=2")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.Name).To(Equal("x"))
			Expect(item.Address).To(Equal(uint16(100)))
			Expect(item.Type).To(Equal("FLOAT32"))
			Expect(item.Scale).To(Equal(0.1))
			Expect(item.Output).To(Equal("FLOAT64"))
			Expect(item.SlaveID).To(Equal(byte(2)))
		})

		It("should parse STRING with length", func() {
			item, err := ParseModbusAddress("serial.holding.200.STRING:length=10")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.Type).To(Equal("STRING"))
			Expect(item.Length).To(Equal(uint16(10)))
		})

		It("should parse BIT with bit", func() {
			item, err := ParseModbusAddress("flag.discrete.1.BIT:bit=3")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.Type).To(Equal("BIT"))
			Expect(item.Bit).To(Equal(uint16(3)))
		})

		It("should parse address at boundary 0", func() {
			item, err := ParseModbusAddress("tag.holding.0.INT16")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.Address).To(Equal(uint16(0)))
		})

		It("should parse address at boundary 65535", func() {
			item, err := ParseModbusAddress("tag.holding.65535.INT16")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.Address).To(Equal(uint16(65535)))
		})

		It("should parse slaveID=0 explicitly", func() {
			item, err := ParseModbusAddress("tag.holding.100.INT16:slaveID=0")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.SlaveID).To(Equal(byte(0)))
		})

		It("should parse names with underscores and hyphens", func() {
			item, err := ParseModbusAddress("motor_status-1.holding.100.INT16")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.Name).To(Equal("motor_status-1"))
		})

		It("should parse coil and discrete with UINT16", func() {
			item, err := ParseModbusAddress("boolIn.discrete.0.UINT16")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.Register).To(Equal("discrete"))
			Expect(item.Type).To(Equal("UINT16"))

			item, err = ParseModbusAddress("boolOut.coil.1.UINT16")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.Register).To(Equal("coil"))
			Expect(item.Type).To(Equal("UINT16"))
		})

		It("should parse scale option", func() {
			item, err := ParseModbusAddress("temperature.holding.100.INT16:scale=0.1")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.Scale).To(Equal(0.1))

			item, err = ParseModbusAddress("pressure.holding.102.FLOAT32:scale=0.01")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.Scale).To(Equal(0.01))
		})

		It("should parse output type override", func() {
			item, err := ParseModbusAddress("rawValue.holding.110.UINT16:output=FLOAT64")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.Output).To(Equal("FLOAT64"))
		})

		It("should parse scale with output combined", func() {
			item, err := ParseModbusAddress("scaledValue.holding.112.INT16:scale=0.1:output=FLOAT64")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.Scale).To(Equal(0.1))
			Expect(item.Output).To(Equal("FLOAT64"))
		})

		It("should parse per-slave addresses", func() {
			item, err := ParseModbusAddress("slave1Only.holding.200.INT16:slaveID=1")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.SlaveID).To(Equal(byte(1)))

			item, err = ParseModbusAddress("slave2Only.holding.200.FLOAT32:slaveID=2")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.SlaveID).To(Equal(byte(2)))
			Expect(item.Type).To(Equal("FLOAT32"))
		})

		It("should parse full example with all options combined", func() {
			item, err := ParseModbusAddress("fullExample.holding.300.INT16:slaveID=55:scale=0.01:output=FLOAT64")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.Name).To(Equal("fullExample"))
			Expect(item.Register).To(Equal("holding"))
			Expect(item.Address).To(Equal(uint16(300)))
			Expect(item.Type).To(Equal("INT16"))
			Expect(item.SlaveID).To(Equal(byte(55)))
			Expect(item.Scale).To(Equal(0.01))
			Expect(item.Output).To(Equal("FLOAT64"))
		})

		It("should parse STRING with large length", func() {
			item, err := ParseModbusAddress("deviceName.holding.70.STRING:length=16")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.Length).To(Equal(uint16(16)))
		})

		It("should parse BIT at different bit positions", func() {
			for bit := 0; bit <= 15; bit++ {
				addr := "statusBit.holding.50.BIT"
				if bit > 0 {
					addr += ":bit=" + strconv.Itoa(bit)
				}
				item, err := ParseModbusAddress(addr)
				Expect(err).NotTo(HaveOccurred(), "bit=%d", bit)
				Expect(item.Bit).To(Equal(uint16(bit)), "bit=%d", bit)
			}
		})

		It("should parse input register types", func() {
			item, err := ParseModbusAddress("intIn.input.1.INT16")
			Expect(err).NotTo(HaveOccurred())
			Expect(item.Register).To(Equal("input"))
			Expect(item.Type).To(Equal("INT16"))
		})
	})

	Context("invalid addresses", func() {
		It("should reject empty string", func() {
			_, err := ParseModbusAddress("")
			Expect(err).To(HaveOccurred())
		})

		It("should reject too few positional segments", func() {
			_, err := ParseModbusAddress("name.holding.100")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("exactly 4"))
		})

		It("should reject too many positional segments", func() {
			_, err := ParseModbusAddress("name.holding.100.INT16.extra")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("exactly 4"))
		})

		It("should reject empty name", func() {
			_, err := ParseModbusAddress(".holding.100.INT16")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("empty name"))
		})

		It("should reject invalid register", func() {
			_, err := ParseModbusAddress("tag.unknown.100.INT16")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid register"))
		})

		It("should reject non-numeric address", func() {
			_, err := ParseModbusAddress("tag.holding.abc.INT16")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid address"))
		})

		It("should reject address out of range", func() {
			_, err := ParseModbusAddress("tag.holding.70000.INT16")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("out of range"))
		})

		It("should reject negative address", func() {
			_, err := ParseModbusAddress("tag.holding.-1.INT16")
			Expect(err).To(HaveOccurred())
		})

		It("should reject invalid type", func() {
			_, err := ParseModbusAddress("tag.holding.100.INVALID")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid type"))
		})

		It("should reject unknown option key", func() {
			_, err := ParseModbusAddress("tag.holding.100.INT16:unknown=1")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unknown option key"))
		})

		It("should reject duplicate option key", func() {
			_, err := ParseModbusAddress("tag.holding.100.INT16:slaveID=1:slaveID=2")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("duplicate"))
		})

		It("should reject length on non-STRING type", func() {
			_, err := ParseModbusAddress("tag.holding.100.INT16:length=10")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("length option is only valid for STRING"))
		})

		It("should reject bit on non-BIT type", func() {
			_, err := ParseModbusAddress("tag.holding.100.INT16:bit=3")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("bit option is only valid for BIT"))
		})

		It("should reject slave out of range", func() {
			_, err := ParseModbusAddress("tag.holding.100.INT16:slaveID=256")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("out of range"))
		})

		It("should reject bit out of range", func() {
			_, err := ParseModbusAddress("tag.holding.100.BIT:bit=16")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("out of range"))
		})

		It("should reject invalid output type", func() {
			_, err := ParseModbusAddress("tag.holding.100.INT16:output=INVALID")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid output type"))
		})

		It("should reject malformed option (missing =)", func() {
			_, err := ParseModbusAddress("tag.holding.100.INT16:slaveID")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("expected key=value"))
		})

		It("should reject invalid scale value", func() {
			_, err := ParseModbusAddress("tag.holding.100.INT16:scale=abc")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid scale"))
		})
	})

	Context("roundtrip", func() {
		DescribeTable("FormatModbusAddress(ParseModbusAddress(s)) == s",
			func(addr string) {
				item, err := ParseModbusAddress(addr)
				Expect(err).NotTo(HaveOccurred())
				formatted := FormatModbusAddress(item)
				Expect(formatted).To(Equal(addr))
			},
			Entry("basic INT16", "temperature.holding.100.INT16"),
			Entry("with slaveID", "tag.holding.100.INT16:slaveID=2"),
			Entry("STRING with length", "serial.holding.200.STRING:length=10"),
			Entry("BIT with bit", "flag.discrete.1.BIT:bit=3"),
			Entry("with scale and output", "pressure.holding.300.FLOAT32:scale=0.1:output=FLOAT64"),
			Entry("all options", "x.holding.100.FLOAT32:slaveID=2:scale=0.1:output=FLOAT64"),
			Entry("coil register", "motor.coil.5.UINT16"),
			Entry("input register", "sensor.input.42.FLOAT32"),
			Entry("INT8L", "tag.holding.10.INT8L"),
			Entry("INT8H", "tag.holding.11.INT8H"),
			Entry("UINT8L", "tag.holding.12.UINT8L"),
			Entry("UINT8H", "tag.holding.13.UINT8H"),
			Entry("INT32", "tag.holding.20.INT32"),
			Entry("UINT32", "tag.holding.22.UINT32"),
			Entry("INT64", "tag.holding.30.INT64"),
			Entry("UINT64", "tag.holding.34.UINT64"),
			Entry("FLOAT16", "tag.holding.40.FLOAT16"),
			Entry("FLOAT32", "tag.holding.42.FLOAT32"),
			Entry("FLOAT64", "tag.holding.44.FLOAT64"),
			Entry("STRING length=16", "deviceName.holding.70.STRING:length=16"),
			Entry("full example", "fullExample.holding.300.INT16:slaveID=55:scale=0.01:output=FLOAT64"),
		)
	})
})
