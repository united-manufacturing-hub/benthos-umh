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

	Context("convertFieldToJSONSchema", func() {
		// Group 1: Array type conversion tests
		DescribeTable("array type conversion",
			func(benthosType string, expectedItemsType string) {
				field := FieldSpec{
					Name:        "testField",
					Type:        benthosType,
					Kind:        "array",
					Description: "Test array field",
				}
				result := convertFieldToJSONSchema(field)
				Expect(result["type"]).To(Equal("array"))
				items, ok := result["items"].(map[string]interface{})
				Expect(ok).To(BeTrue(), "items should be a map")
				Expect(items["type"]).To(Equal(expectedItemsType))
			},
			Entry("int array becomes number array", "int", "number"),
			Entry("string array becomes string array", "string", "string"),
			Entry("float array becomes number array", "float", "number"),
			Entry("bool array becomes boolean array", "bool", "boolean"),
			Entry("object array becomes object array", "object", "object"),
		)

		// Group 2: Field property preservation tests
		DescribeTable("field property preservation",
			func(field FieldSpec, expectedType string, checkFn func(map[string]interface{})) {
				result := convertFieldToJSONSchema(field)
				Expect(result["type"]).To(Equal(expectedType))
				checkFn(result)
			},
			Entry("preserves description, default, and advanced flags",
				FieldSpec{
					Name:        "timeout",
					Type:        "int",
					Kind:        "",
					Description: "Timeout in milliseconds",
					Default:     1000,
					Advanced:    true,
				},
				"number",
				func(result map[string]interface{}) {
					Expect(result["description"]).To(Equal("Timeout in milliseconds"))
					Expect(result["default"]).To(Equal(1000))
					Expect(result["x-advanced"]).To(BeTrue())
				},
			),
			Entry("adds enum for fields with options",
				FieldSpec{
					Name:        "mode",
					Type:        "string",
					Description: "Operation mode",
					Options:     []string{"read", "write", "both"},
				},
				"string",
				func(result map[string]interface{}) {
					Expect(result["enum"]).To(Equal([]string{"read", "write", "both"}))
				},
			),
		)

		It("should handle nested object without array wrapper", func() {
			// Like modbus workarounds: {type: "object", kind: "", children: [...]}
			field := FieldSpec{
				Name:        "workarounds",
				Type:        "object",
				Kind:        "",
				Description: "Modbus workarounds",
				Children: []FieldSpec{
					{Name: "pauseAfterConnect", Type: "string", Description: "Pause after connect"},
					{Name: "oneRequestPerField", Type: "bool", Description: "Send each field separately"},
				},
			}
			result := convertFieldToJSONSchema(field)
			Expect(result["type"]).To(Equal("object"))
			Expect(result["description"]).To(Equal("Modbus workarounds"))

			// Properties should be at top level for non-array objects
			properties, ok := result["properties"].(map[string]interface{})
			Expect(ok).To(BeTrue(), "properties should exist at top level")
			Expect(properties).To(HaveKey("pauseAfterConnect"))
			Expect(properties).To(HaveKey("oneRequestPerField"))
		})

		It("should handle object array with children (like modbus addresses)", func() {
			// Like modbus addresses: {type: "object", kind: "array", children: [...]}
			field := FieldSpec{
				Name:        "addresses",
				Type:        "object",
				Kind:        "array",
				Description: "Register addresses to read",
				Children: []FieldSpec{
					{Name: "name", Type: "string", Description: "Field name", Required: true},
					{Name: "register", Type: "string", Description: "Register type", Default: "holding"},
					{Name: "address", Type: "int", Description: "Address of the register", Required: true},
					{Name: "type", Type: "string", Description: "Data type"},
				},
			}
			result := convertFieldToJSONSchema(field)

			// Top level should be array
			Expect(result["type"]).To(Equal("array"))
			Expect(result["description"]).To(Equal("Register addresses to read"))

			// Properties should NOT be at top level for arrays
			Expect(result).NotTo(HaveKey("properties"), "properties should NOT be at top level for arrays")

			// Items should contain the object schema with properties
			items, ok := result["items"].(map[string]interface{})
			Expect(ok).To(BeTrue(), "items should be a map")
			Expect(items["type"]).To(Equal("object"))

			// Properties should be inside items
			itemProperties, ok := items["properties"].(map[string]interface{})
			Expect(ok).To(BeTrue(), "items should have properties")
			Expect(itemProperties).To(HaveKey("name"))
			Expect(itemProperties).To(HaveKey("register"))
			Expect(itemProperties).To(HaveKey("address"))
			Expect(itemProperties).To(HaveKey("type"))

			// Required should be inside items
			itemRequired, ok := items["required"].([]string)
			Expect(ok).To(BeTrue(), "items should have required array")
			Expect(itemRequired).To(ContainElement("name"))
			Expect(itemRequired).To(ContainElement("address"))
		})
	})

	Context("additionalProperties validation", func() {
		It("should include additionalProperties false in plugin root schemas", func() {
			plugin := PluginSpec{
				Name:        "test-plugin",
				Type:        "input",
				Description: "Test plugin",
				Config: map[string]FieldSpec{
					"endpoint": {
						Name:        "endpoint",
						Type:        "string",
						Description: "Test endpoint",
						Required:    true,
					},
				},
			}

			result := convertPluginToJSONSchema(plugin)

			// Plugin root must have additionalProperties: false
			Expect(result).To(HaveKey("additionalProperties"))
			Expect(result["additionalProperties"]).To(BeFalse())
		})

		It("should include additionalProperties false in nested object schemas", func() {
			field := FieldSpec{
				Name:        "handshake",
				Type:        "object",
				Description: "Handshake configuration",
				Children: []FieldSpec{
					{Name: "enabled", Type: "bool", Description: "Enable handshake"},
					{Name: "timeout", Type: "int", Description: "Timeout in ms"},
				},
			}

			result := convertFieldToJSONSchema(field)

			// Nested objects with properties must have additionalProperties: false
			Expect(result["type"]).To(Equal("object"))
			Expect(result).To(HaveKey("properties"))
			Expect(result).To(HaveKey("additionalProperties"))
			Expect(result["additionalProperties"]).To(BeFalse())
		})

		It("should include additionalProperties false in deeply nested objects", func() {
			// Plugin with nested object containing another nested object
			plugin := PluginSpec{
				Name:        "deep-plugin",
				Type:        "input",
				Description: "Plugin with deep nesting",
				Config: map[string]FieldSpec{
					"connection": {
						Name:        "connection",
						Type:        "object",
						Description: "Connection settings",
						Children: []FieldSpec{
							{Name: "host", Type: "string"},
							{
								Name:        "security",
								Type:        "object",
								Description: "Security settings",
								Children: []FieldSpec{
									{Name: "mode", Type: "string"},
									{Name: "certificate", Type: "string"},
								},
							},
						},
					},
				},
			}

			result := convertPluginToJSONSchema(plugin)

			// Root plugin must have additionalProperties: false
			Expect(result).To(HaveKey("additionalProperties"))
			Expect(result["additionalProperties"]).To(BeFalse())

			// First level nested object (connection) must have additionalProperties: false
			properties := result["properties"].(map[string]interface{})
			connection := properties["connection"].(map[string]interface{})
			Expect(connection).To(HaveKey("additionalProperties"))
			Expect(connection["additionalProperties"]).To(BeFalse())

			// Second level nested object (security) must have additionalProperties: false
			connectionProps := connection["properties"].(map[string]interface{})
			security := connectionProps["security"].(map[string]interface{})
			Expect(security).To(HaveKey("additionalProperties"))
			Expect(security["additionalProperties"]).To(BeFalse())
		})
	})

	Context("convertPluginToJSONSchema (modbus-like complex plugin)", func() {
		It("should convert a modbus-like plugin with mixed field types", func() {
			plugin := PluginSpec{
				Name:        "modbus",
				Type:        "input",
				Source:      "benthos-umh",
				Summary:     "Reads data from Modbus devices",
				Description: "This input plugin enables Benthos to read data directly from Modbus devices.",
				Config: map[string]FieldSpec{
					"controller": {
						Name:        "controller",
						Type:        "string",
						Description: "The Modbus controller address",
						Required:    true,
						Default:     "tcp://localhost:502",
					},
					"slaveIDs": {
						Name:        "slaveIDs",
						Type:        "int",
						Kind:        "array",
						Description: "Slave IDs to poll",
					},
					"addresses": {
						Name:        "addresses",
						Type:        "object",
						Kind:        "array",
						Description: "Register addresses to read",
						Children: []FieldSpec{
							{Name: "name", Type: "string", Required: true},
							{Name: "register", Type: "string", Default: "holding"},
							{Name: "address", Type: "int", Required: true},
						},
					},
					"workarounds": {
						Name:        "workarounds",
						Type:        "object",
						Kind:        "",
						Description: "Modbus workarounds",
						Advanced:    true,
						Children: []FieldSpec{
							{Name: "pauseAfterConnect", Type: "string", Default: "0s"},
							{Name: "oneRequestPerField", Type: "bool", Default: false},
						},
					},
				},
			}

			result := convertPluginToJSONSchema(plugin)

			// Top-level checks
			Expect(result["type"]).To(Equal("object"))
			Expect(result["description"]).To(Equal("This input plugin enables Benthos to read data directly from Modbus devices."))

			properties, ok := result["properties"].(map[string]interface{})
			Expect(ok).To(BeTrue())

			// controller: simple string field
			controller, ok := properties["controller"].(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(controller["type"]).To(Equal("string"))
			Expect(controller["default"]).To(Equal("tcp://localhost:502"))

			// slaveIDs: int array (kind="array", type="int")
			slaveIDs, ok := properties["slaveIDs"].(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(slaveIDs["type"]).To(Equal("array"))
			slaveIDsItems, ok := slaveIDs["items"].(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(slaveIDsItems["type"]).To(Equal("number"))

			// addresses: object array with nested children
			addresses, ok := properties["addresses"].(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(addresses["type"]).To(Equal("array"))
			Expect(addresses).NotTo(HaveKey("properties"), "object array should not have properties at top level")
			addressItems, ok := addresses["items"].(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(addressItems["type"]).To(Equal("object"))
			addressItemProps, ok := addressItems["properties"].(map[string]interface{})
			Expect(ok).To(BeTrue(), "items should have properties for object arrays")
			Expect(addressItemProps).To(HaveKey("name"))
			Expect(addressItemProps).To(HaveKey("register"))
			Expect(addressItemProps).To(HaveKey("address"))

			// workarounds: nested object (no array wrapper)
			workarounds, ok := properties["workarounds"].(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(workarounds["type"]).To(Equal("object"))
			Expect(workarounds["x-advanced"]).To(BeTrue())
			workaroundProps, ok := workarounds["properties"].(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(workaroundProps).To(HaveKey("pauseAfterConnect"))
			Expect(workaroundProps).To(HaveKey("oneRequestPerField"))

			// Required array should include controller
			required, ok := result["required"].([]string)
			Expect(ok).To(BeTrue())
			Expect(required).To(ContainElement("controller"))
		})
	})
})
