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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Byte Conversion", func() {
	Describe("ToBytes", func() {
		It("handles nil value", func() {
			bytes, typ, err := ToBytes(nil)
			Expect(err).To(BeNil())
			Expect(bytes).To(BeNil())
			Expect(typ).To(Equal("nil"))
		})

		It("handles string value", func() {
			input := "hello"
			bytes, typ, err := ToBytes(input)
			Expect(err).To(BeNil())
			Expect(bytes).To(Equal([]byte("hello")))
			Expect(typ).To(Equal("string"))
		})

		It("handles boolean values", func() {
			bytes, typ, err := ToBytes(true)
			Expect(err).To(BeNil())
			Expect(bytes).To(Equal([]byte{1}))
			Expect(typ).To(Equal("bool"))

			bytes, typ, err = ToBytes(false)
			Expect(err).To(BeNil())
			Expect(bytes).To(Equal([]byte{0}))
			Expect(typ).To(Equal("bool"))
		})

		It("handles numeric values", func() {
			// int8
			bytes, typ, err := ToBytes(int8(-42))
			Expect(err).To(BeNil())
			Expect(bytes).To(Equal([]byte{214}))
			Expect(typ).To(Equal("int8"))

			// uint16
			bytes, typ, err = ToBytes(uint16(12345))
			Expect(err).To(BeNil())
			Expect(bytes).To(Equal([]byte{57, 48}))
			Expect(typ).To(Equal("uint16"))

			// float32
			bytes, typ, err = ToBytes(float32(3.14))
			Expect(err).To(BeNil())
			Expect(len(bytes)).To(Equal(4))
			Expect(typ).To(Equal("float32"))
		})

		It("handles complex types via JSON", func() {
			type TestStruct struct {
				Name  string
				Value int
			}
			input := TestStruct{Name: "test", Value: 42}
			bytes, typ, err := ToBytes(input)
			Expect(err).To(BeNil())
			Expect(typ).To(Equal("tag_browser_plugin.TestStruct"))
			Expect(bytes).To(ContainSubstring("test"))
			Expect(bytes).To(ContainSubstring("42"))
		})
	})

	Describe("FromBytes", func() {
		It("handles nil value", func() {
			result, err := FromBytes(nil, "nil")
			Expect(err).To(BeNil())
			Expect(result).To(BeNil())
		})

		It("handles string value", func() {
			input := []byte("hello")
			result, err := FromBytes(input, "string")
			Expect(err).To(BeNil())
			Expect(result).To(Equal("hello"))
		})

		It("handles boolean values", func() {
			result, err := FromBytes([]byte{1}, "bool")
			Expect(err).To(BeNil())
			Expect(result).To(Equal(true))

			result, err = FromBytes([]byte{0}, "bool")
			Expect(err).To(BeNil())
			Expect(result).To(Equal(false))
		})

		It("handles numeric values", func() {
			// int8
			result, err := FromBytes([]byte{214}, "int8")
			Expect(err).To(BeNil())
			Expect(result).To(Equal(int8(-42)))

			// uint16
			result, err = FromBytes([]byte{57, 48}, "uint16")
			Expect(err).To(BeNil())
			Expect(result).To(Equal(uint16(12345)))

			// float32
			bits := []byte{195, 245, 72, 64} // 3.14 in float32
			result, err = FromBytes(bits, "float32")
			Expect(err).To(BeNil())
			Expect(result).To(BeNumerically("~", float32(3.14), 0.0001))
		})

		It("returns error for invalid byte length", func() {
			_, err := FromBytes([]byte{1, 2}, "int8")
			Expect(err).To(MatchError("int8 needs 1 byte"))

			_, err = FromBytes([]byte{1}, "int16")
			Expect(err).To(MatchError("int16 needs 2 bytes"))
		})

		It("handles complex types via JSON", func() {
			jsonBytes := []byte(`{"Name":"test","Value":42}`)
			result, err := FromBytes(jsonBytes, "tag_browser_plugin.TestStruct")
			Expect(err).To(BeNil())
			Expect(result).To(HaveKeyWithValue("Name", "test"))
			Expect(result).To(HaveKeyWithValue("Value", float64(42)))
		})
	})

	Describe("Round trip conversion", func() {
		It("preserves values through ToBytes and FromBytes", func() {
			testCases := []any{
				nil,
				"hello",
				true,
				false,
				int8(-42),
				uint8(42),
				int16(-12345),
				uint16(12345),
				int32(-123456789),
				uint32(123456789),
				int64(-1234567890123456789),
				uint64(1234567890123456789),
				float32(3.14),
				float64(3.14159265359),
			}

			for _, tc := range testCases {
				bytes, typ, err := ToBytes(tc)
				Expect(err).To(BeNil())

				result, err := FromBytes(bytes, typ)
				Expect(err).To(BeNil())

				if tc == nil {
					Expect(result).To(BeNil())
				} else {
					Expect(result).To(Equal(tc))
				}
			}
		})
	})
})
