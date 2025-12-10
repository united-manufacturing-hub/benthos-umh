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

package main

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("JSON Schema Generator", func() {
	Context("when validating format flag", func() {
		It("should accept 'benthos' format", func() {
			err := validateFormat("benthos")
			Expect(err).ToNot(HaveOccurred())
		})

		It("should accept 'json-schema' format", func() {
			err := validateFormat("json-schema")
			Expect(err).ToNot(HaveOccurred())
		})

		It("should reject invalid format", func() {
			err := validateFormat("invalid-format")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid format"))
		})

		It("should reject empty format", func() {
			err := validateFormat("")
			Expect(err).To(HaveOccurred())
		})
	})

	Context("benthosTypeToJSONSchemaType", func() {
		Context("base type mappings (non-array)", func() {
			It("should map 'string' to JSON Schema string", func() {
				result := benthosTypeToJSONSchemaType("string", "")
				Expect(result["type"]).To(Equal("string"))
				Expect(result).NotTo(HaveKey("items"))
			})

			It("should map 'int' to JSON Schema number", func() {
				result := benthosTypeToJSONSchemaType("int", "")
				Expect(result["type"]).To(Equal("number"))
				Expect(result).NotTo(HaveKey("items"))
			})

			It("should map 'float' to JSON Schema number", func() {
				result := benthosTypeToJSONSchemaType("float", "")
				Expect(result["type"]).To(Equal("number"))
			})

			It("should map 'bool' to JSON Schema boolean", func() {
				result := benthosTypeToJSONSchemaType("bool", "")
				Expect(result["type"]).To(Equal("boolean"))
			})

			It("should map 'object' to JSON Schema object", func() {
				result := benthosTypeToJSONSchemaType("object", "")
				Expect(result["type"]).To(Equal("object"))
			})

			It("should map unknown type to string as fallback", func() {
				result := benthosTypeToJSONSchemaType("unknown", "")
				Expect(result["type"]).To(Equal("string"))
			})
		})

		Context("array type mappings (kind='array')", func() {
			It("should map int array to JSON Schema array with number items", func() {
				result := benthosTypeToJSONSchemaType("int", "array")
				Expect(result["type"]).To(Equal("array"))
				items, ok := result["items"].(map[string]interface{})
				Expect(ok).To(BeTrue(), "items should be a map")
				Expect(items["type"]).To(Equal("number"))
			})

			It("should map string array to JSON Schema array with string items", func() {
				result := benthosTypeToJSONSchemaType("string", "array")
				Expect(result["type"]).To(Equal("array"))
				items, ok := result["items"].(map[string]interface{})
				Expect(ok).To(BeTrue(), "items should be a map")
				Expect(items["type"]).To(Equal("string"))
			})

			It("should map object array to JSON Schema array with object items", func() {
				result := benthosTypeToJSONSchemaType("object", "array")
				Expect(result["type"]).To(Equal("array"))
				items, ok := result["items"].(map[string]interface{})
				Expect(ok).To(BeTrue(), "items should be a map")
				Expect(items["type"]).To(Equal("object"))
			})

			It("should map float array to JSON Schema array with number items", func() {
				result := benthosTypeToJSONSchemaType("float", "array")
				Expect(result["type"]).To(Equal("array"))
				items, ok := result["items"].(map[string]interface{})
				Expect(ok).To(BeTrue(), "items should be a map")
				Expect(items["type"]).To(Equal("number"))
			})

			It("should map bool array to JSON Schema array with boolean items", func() {
				result := benthosTypeToJSONSchemaType("bool", "array")
				Expect(result["type"]).To(Equal("array"))
				items, ok := result["items"].(map[string]interface{})
				Expect(ok).To(BeTrue(), "items should be a map")
				Expect(items["type"]).To(Equal("boolean"))
			})
		})
	})

	Context("convertFieldToJSONSchema", func() {
		It("should convert array field with kind='array' to proper schema", func() {
			field := FieldSpec{
				Name:        "slaveIDs",
				Type:        "int",
				Kind:        "array",
				Description: "List of slave IDs",
			}
			result := convertFieldToJSONSchema(field)
			Expect(result["type"]).To(Equal("array"))
			Expect(result["description"]).To(Equal("List of slave IDs"))
			items, ok := result["items"].(map[string]interface{})
			Expect(ok).To(BeTrue(), "items should be a map")
			Expect(items["type"]).To(Equal("number"))
		})

		It("should convert string array field correctly", func() {
			field := FieldSpec{
				Name:        "addresses",
				Type:        "string",
				Kind:        "array",
				Description: "List of addresses",
			}
			result := convertFieldToJSONSchema(field)
			Expect(result["type"]).To(Equal("array"))
			items, ok := result["items"].(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(items["type"]).To(Equal("string"))
		})

		It("should preserve description, default, and advanced flags", func() {
			field := FieldSpec{
				Name:        "timeout",
				Type:        "int",
				Kind:        "",
				Description: "Timeout in milliseconds",
				Default:     1000,
				Advanced:    true,
			}
			result := convertFieldToJSONSchema(field)
			Expect(result["type"]).To(Equal("number"))
			Expect(result["description"]).To(Equal("Timeout in milliseconds"))
			Expect(result["default"]).To(Equal(1000))
			Expect(result["x-advanced"]).To(Equal(true))
		})

		It("should add enum for fields with options", func() {
			field := FieldSpec{
				Name:        "mode",
				Type:        "string",
				Description: "Operation mode",
				Options:     []string{"read", "write", "both"},
			}
			result := convertFieldToJSONSchema(field)
			Expect(result["type"]).To(Equal("string"))
			Expect(result["enum"]).To(Equal([]string{"read", "write", "both"}))
		})
	})
})
