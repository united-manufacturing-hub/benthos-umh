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

	Context("when converting Benthos types to JSON Schema types", func() {
		It("should convert string to string type", func() {
			result := benthosTypeToJSONSchemaType("string", "scalar")
			Expect(result).To(HaveKeyWithValue("type", "string"))
		})

		It("should convert int to number type", func() {
			result := benthosTypeToJSONSchemaType("int", "scalar")
			Expect(result).To(HaveKeyWithValue("type", "number"))
		})

		It("should convert float to number type", func() {
			result := benthosTypeToJSONSchemaType("float", "scalar")
			Expect(result).To(HaveKeyWithValue("type", "number"))
		})

		It("should convert number to number type", func() {
			result := benthosTypeToJSONSchemaType("number", "scalar")
			Expect(result).To(HaveKeyWithValue("type", "number"))
		})

		It("should convert bool to boolean type", func() {
			result := benthosTypeToJSONSchemaType("bool", "scalar")
			Expect(result).To(HaveKeyWithValue("type", "boolean"))
		})

		It("should convert object to object type", func() {
			result := benthosTypeToJSONSchemaType("object", "object")
			Expect(result).To(HaveKeyWithValue("type", "object"))
		})

		It("should convert array to array type", func() {
			result := benthosTypeToJSONSchemaType("array", "array")
			Expect(result).To(HaveKeyWithValue("type", "array"))
		})

		It("should convert duration to string type", func() {
			result := benthosTypeToJSONSchemaType("string", "scalar")
			// Duration fields in Benthos use string type
			Expect(result).To(HaveKeyWithValue("type", "string"))
		})
	})

	Context("when converting FieldSpec to JSON Schema property", func() {
		It("should convert simple string field", func() {
			field := FieldSpec{
				Name:        "address",
				Type:        "string",
				Kind:        "scalar",
				Description: "Connection address",
				Required:    true,
				Default:     "localhost:502",
			}

			result := convertFieldToJSONSchema(field)

			Expect(result).To(HaveKeyWithValue("type", "string"))
			Expect(result).To(HaveKeyWithValue("description", "Connection address"))
			Expect(result).To(HaveKeyWithValue("default", "localhost:502"))
		})

		It("should convert field with enum options", func() {
			field := FieldSpec{
				Name:        "mode",
				Type:        "string",
				Kind:        "scalar",
				Description: "Connection mode",
				Required:    true,
				Options:     []string{"TCP", "RTU", "ASCII"},
				Default:     "TCP",
			}

			result := convertFieldToJSONSchema(field)

			Expect(result).To(HaveKeyWithValue("type", "string"))
			Expect(result).To(HaveKey("enum"))
			enum := result["enum"].([]string)
			Expect(enum).To(ConsistOf("TCP", "RTU", "ASCII"))
			Expect(result).To(HaveKeyWithValue("default", "TCP"))
		})

		It("should preserve advanced flag as x-advanced", func() {
			field := FieldSpec{
				Name:        "timeout",
				Type:        "string",
				Kind:        "scalar",
				Description: "Connection timeout",
				Required:    false,
				Advanced:    true,
				Default:     "30s",
			}

			result := convertFieldToJSONSchema(field)

			Expect(result).To(HaveKeyWithValue("x-advanced", true))
		})

		It("should convert nested object field", func() {
			field := FieldSpec{
				Name:        "connection",
				Type:        "object",
				Kind:        "object",
				Description: "Connection settings",
				Required:    true,
				Children: []FieldSpec{
					{
						Name:        "host",
						Type:        "string",
						Kind:        "scalar",
						Description: "Host address",
						Required:    true,
					},
					{
						Name:        "port",
						Type:        "int",
						Kind:        "scalar",
						Description: "Port number",
						Required:    true,
					},
				},
			}

			result := convertFieldToJSONSchema(field)

			Expect(result).To(HaveKeyWithValue("type", "object"))
			Expect(result).To(HaveKey("properties"))
			properties := result["properties"].(map[string]interface{})
			Expect(properties).To(HaveKey("host"))
			Expect(properties).To(HaveKey("port"))
			Expect(result).To(HaveKey("required"))
			required := result["required"].([]string)
			Expect(required).To(ConsistOf("host", "port"))
		})

		It("should convert array field with items schema", func() {
			field := FieldSpec{
				Name:        "addresses",
				Type:        "array",
				Kind:        "array",
				Description: "List of addresses",
				Required:    true,
				Children: []FieldSpec{
					{
						Name:        "item",
						Type:        "string",
						Kind:        "scalar",
						Description: "Address item",
					},
				},
			}

			result := convertFieldToJSONSchema(field)

			Expect(result).To(HaveKeyWithValue("type", "array"))
			Expect(result).To(HaveKey("items"))
			items := result["items"].(map[string]interface{})
			Expect(items).To(HaveKeyWithValue("type", "string"))
		})
	})

	Context("when building required array from fields", func() {
		It("should include only required fields", func() {
			fields := map[string]FieldSpec{
				"required_field": {
					Name:     "required_field",
					Required: true,
				},
				"optional_field": {
					Name:     "optional_field",
					Required: false,
				},
				"another_required": {
					Name:     "another_required",
					Required: true,
				},
			}

			result := buildRequiredArray(fields)

			Expect(result).To(ConsistOf("required_field", "another_required"))
		})

		It("should return empty array when no required fields", func() {
			fields := map[string]FieldSpec{
				"optional1": {
					Name:     "optional1",
					Required: false,
				},
				"optional2": {
					Name:     "optional2",
					Required: false,
				},
			}

			result := buildRequiredArray(fields)

			Expect(result).To(BeEmpty())
		})
	})

	Context("when generating JSON Schema from plugin specs", func() {
		It("should generate valid JSON Schema Draft-07 structure", func() {
			plugins := &SchemaOutput{
				Inputs: map[string]PluginSpec{
					"modbus": {
						Name:        "modbus",
						Type:        "input",
						Source:      "benthos-umh",
						Summary:     "Modbus protocol input",
						Description: "Reads data from Modbus devices",
						Config: map[string]FieldSpec{
							"controller": {
								Name:        "controller",
								Type:        "string",
								Kind:        "scalar",
								Description: "Modbus controller address",
								Required:    true,
							},
						},
					},
				},
				Processors: map[string]PluginSpec{},
				Outputs:    map[string]PluginSpec{},
			}

			result, err := generateJSONSchema(plugins, "0.11.6")

			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(HaveKeyWithValue("$schema", "http://json-schema.org/draft-07/schema#"))
			Expect(result).To(HaveKey("$id"))
			idStr := result["$id"].(string)
			Expect(idStr).To(ContainSubstring("0.11.6"))
			Expect(result).To(HaveKeyWithValue("title", "Benthos UMH Configuration Schema"))
			Expect(result).To(HaveKey("definitions"))
		})

		It("should include all plugins in definitions", func() {
			plugins := &SchemaOutput{
				Inputs: map[string]PluginSpec{
					"modbus": {
						Name:   "modbus",
						Type:   "input",
						Config: map[string]FieldSpec{},
					},
					"opcua": {
						Name:   "opcua",
						Type:   "input",
						Config: map[string]FieldSpec{},
					},
				},
				Processors: map[string]PluginSpec{
					"tag_processor": {
						Name:   "tag_processor",
						Type:   "processor",
						Config: map[string]FieldSpec{},
					},
				},
				Outputs: map[string]PluginSpec{
					"uns": {
						Name:   "uns",
						Type:   "output",
						Config: map[string]FieldSpec{},
					},
				},
			}

			result, err := generateJSONSchema(plugins, "0.11.6")

			Expect(err).ToNot(HaveOccurred())
			definitions := result["definitions"].(map[string]interface{})
			Expect(definitions).To(HaveKey("modbus"))
			Expect(definitions).To(HaveKey("opcua"))
			Expect(definitions).To(HaveKey("tag_processor"))
			Expect(definitions).To(HaveKey("uns"))
		})

		It("should convert plugin config to JSON Schema properties", func() {
			plugins := &SchemaOutput{
				Inputs: map[string]PluginSpec{
					"modbus": {
						Name: "modbus",
						Type: "input",
						Config: map[string]FieldSpec{
							"controller": {
								Name:        "controller",
								Type:        "string",
								Kind:        "scalar",
								Description: "Modbus controller address",
								Required:    true,
							},
							"port": {
								Name:        "port",
								Type:        "int",
								Kind:        "scalar",
								Description: "Modbus port",
								Required:    false,
								Default:     502,
							},
						},
					},
				},
				Processors: map[string]PluginSpec{},
				Outputs:    map[string]PluginSpec{},
			}

			result, err := generateJSONSchema(plugins, "0.11.6")

			Expect(err).ToNot(HaveOccurred())
			definitions := result["definitions"].(map[string]interface{})
			modbus := definitions["modbus"].(map[string]interface{})
			Expect(modbus).To(HaveKeyWithValue("type", "object"))
			Expect(modbus).To(HaveKey("properties"))
			properties := modbus["properties"].(map[string]interface{})
			Expect(properties).To(HaveKey("controller"))
			Expect(properties).To(HaveKey("port"))
			Expect(modbus).To(HaveKey("required"))
			required := modbus["required"].([]string)
			Expect(required).To(ContainElement("controller"))
		})
	})

	Context("when generating schema with format flag", func() {
		It("should return Benthos format when format is 'benthos'", func() {
			plugins := &SchemaOutput{
				Inputs:     map[string]PluginSpec{},
				Processors: map[string]PluginSpec{},
				Outputs:    map[string]PluginSpec{},
			}

			result, err := generateSchemaWithFormat(plugins, "benthos", "0.11.6")

			Expect(err).ToNot(HaveOccurred())
			// Should return the original SchemaOutput structure
			_, isSchemaOutput := result.(*SchemaOutput)
			Expect(isSchemaOutput).To(BeTrue())
		})

		It("should return JSON Schema when format is 'json-schema'", func() {
			plugins := &SchemaOutput{
				Inputs:     map[string]PluginSpec{},
				Processors: map[string]PluginSpec{},
				Outputs:    map[string]PluginSpec{},
			}

			result, err := generateSchemaWithFormat(plugins, "json-schema", "0.11.6")

			Expect(err).ToNot(HaveOccurred())
			// Should return a map (JSON Schema structure)
			jsonSchema, isMap := result.(map[string]interface{})
			Expect(isMap).To(BeTrue())
			Expect(jsonSchema).To(HaveKey("$schema"))
		})
	})
})
