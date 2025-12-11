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
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("detectSource", func() {
	DescribeTable("should correctly identify plugin sources",
		func(pluginName, expectedSource string) {
			result := detectSource(pluginName)
			Expect(result).To(Equal(expectedSource))
		},
		// UMH plugins (auto-detected from *_plugin directories)
		Entry("opcua", "opcua", "benthos-umh"),
		Entry("modbus", "modbus", "benthos-umh"),
		Entry("s7comm", "s7comm", "benthos-umh"),
		Entry("sparkplug_b", "sparkplug_b", "benthos-umh"),
		Entry("tag_processor", "tag_processor", "benthos-umh"),
		Entry("uns", "uns", "benthos-umh"),
		// Upstream Benthos plugins
		Entry("kafka", "kafka", "upstream"),
		Entry("http_client", "http_client", "upstream"),
		Entry("unknown_plugin", "unknown_plugin", "upstream"),
	)
})

var _ = Describe("extractFields", func() {
	Context("when given a simple scalar field", func() {
		It("should extract field correctly", func() {
			configObj := map[string]interface{}{
				"children": []interface{}{
					map[string]interface{}{
						"name":        "controller",
						"type":        "string",
						"kind":        "scalar",
						"description": "Controller address",
						"is_optional": false,
						"is_advanced": false,
						"default":     "tcp://localhost:502",
					},
				},
			}

			fields := extractFields(configObj)

			Expect(fields).To(HaveLen(1))
			Expect(fields["controller"].Name).To(Equal("controller"))
			Expect(fields["controller"].Type).To(Equal("string"))
			Expect(fields["controller"].Kind).To(Equal("scalar"))
			Expect(fields["controller"].Description).To(Equal("Controller address"))
			// Field has default, so not required (Benthos will use default if omitted)
			Expect(fields["controller"].Required).To(BeFalse())
			Expect(fields["controller"].Advanced).To(BeFalse())
			Expect(fields["controller"].Default).To(Equal("tcp://localhost:502"))
			Expect(fields["controller"].Children).To(BeEmpty())
		})
	})

	Context("when given an array of primitives", func() {
		It("should have empty children", func() {
			configObj := map[string]interface{}{
				"children": []interface{}{
					map[string]interface{}{
						"name":        "slaveIDs",
						"type":        "int",
						"kind":        "array",
						"description": "Slave IDs",
						"is_optional": false,
						"is_advanced": false,
						"children":    []interface{}{},
						"default":     []interface{}{1},
					},
				},
			}

			fields := extractFields(configObj)

			Expect(fields).To(HaveLen(1))
			Expect(fields["slaveIDs"].Name).To(Equal("slaveIDs"))
			Expect(fields["slaveIDs"].Type).To(Equal("int"))
			Expect(fields["slaveIDs"].Kind).To(Equal("array"))
			Expect(fields["slaveIDs"].Children).To(BeEmpty())
			Expect(fields["slaveIDs"].Default).To(Equal([]interface{}{1}))
		})
	})

	Context("when given an array of objects", func() {
		It("should populate children with nested fields", func() {
			configObj := map[string]interface{}{
				"children": []interface{}{
					map[string]interface{}{
						"name":        "addresses",
						"type":        "object",
						"kind":        "array",
						"description": "Address list",
						"is_optional": false,
						"is_advanced": false,
						"children": []interface{}{
							map[string]interface{}{
								"name":        "name",
								"type":        "string",
								"kind":        "scalar",
								"description": "Address name",
								"is_optional": false,
								"is_advanced": false,
							},
							map[string]interface{}{
								"name":        "register",
								"type":        "string",
								"kind":        "scalar",
								"description": "Register type",
								"is_optional": false,
								"is_advanced": false,
							},
						},
					},
				},
			}

			fields := extractFields(configObj)

			Expect(fields).To(HaveLen(1))
			Expect(fields["addresses"].Name).To(Equal("addresses"))
			Expect(fields["addresses"].Type).To(Equal("object"))
			Expect(fields["addresses"].Kind).To(Equal("array"))
			Expect(fields["addresses"].Children).To(HaveLen(2))
			Expect(fields["addresses"].Children[0].Name).To(Equal("name"))
			Expect(fields["addresses"].Children[0].Type).To(Equal("string"))
			Expect(fields["addresses"].Children[1].Name).To(Equal("register"))
			Expect(fields["addresses"].Children[1].Type).To(Equal("string"))
		})
	})

	Context("when given fields with optional and advanced flags", func() {
		It("should map is_optional to Required correctly", func() {
			configObj := map[string]interface{}{
				"children": []interface{}{
					map[string]interface{}{
						"name":        "required_field",
						"type":        "string",
						"kind":        "scalar",
						"description": "Required field",
						"is_optional": false,
						"is_advanced": false,
					},
					map[string]interface{}{
						"name":        "optional_field",
						"type":        "string",
						"kind":        "scalar",
						"description": "Optional field",
						"is_optional": true,
						"is_advanced": false,
					},
				},
			}

			fields := extractFields(configObj)

			Expect(fields["required_field"].Required).To(BeTrue())
			Expect(fields["optional_field"].Required).To(BeFalse())
		})

		It("should treat fields with defaults as not required even without is_optional", func() {
			configObj := map[string]interface{}{
				"children": []interface{}{
					map[string]interface{}{
						"name":        "field_with_default",
						"type":        "int",
						"kind":        "scalar",
						"description": "Field with default but no .Optional()",
						"is_optional": false,
						"is_advanced": true,
						"default":     10000,
					},
					map[string]interface{}{
						"name":        "field_without_default",
						"type":        "string",
						"kind":        "scalar",
						"description": "Field without default",
						"is_optional": false,
						"is_advanced": false,
					},
				},
			}

			fields := extractFields(configObj)

			// Field with default should NOT be required (Benthos won't fail if omitted)
			Expect(fields["field_with_default"].Required).To(BeFalse())
			// Field without default and not optional SHOULD be required
			Expect(fields["field_without_default"].Required).To(BeTrue())
		})

		It("should set Advanced flag correctly", func() {
			configObj := map[string]interface{}{
				"children": []interface{}{
					map[string]interface{}{
						"name":        "basic_field",
						"type":        "string",
						"kind":        "scalar",
						"description": "Basic field",
						"is_optional": false,
						"is_advanced": false,
					},
					map[string]interface{}{
						"name":        "advanced_field",
						"type":        "string",
						"kind":        "scalar",
						"description": "Advanced field",
						"is_optional": false,
						"is_advanced": true,
					},
				},
			}

			fields := extractFields(configObj)

			Expect(fields["basic_field"].Advanced).To(BeFalse())
			Expect(fields["advanced_field"].Advanced).To(BeTrue())
		})
	})

	Context("when given fields with examples and options", func() {
		It("should extract examples array", func() {
			configObj := map[string]interface{}{
				"children": []interface{}{
					map[string]interface{}{
						"name":        "example_field",
						"type":        "string",
						"kind":        "scalar",
						"description": "Field with examples",
						"is_optional": false,
						"is_advanced": false,
						"examples":    []interface{}{"example1", "example2"},
					},
				},
			}

			fields := extractFields(configObj)

			Expect(fields["example_field"].Examples).To(HaveLen(2))
			Expect(fields["example_field"].Examples[0]).To(Equal("example1"))
			Expect(fields["example_field"].Examples[1]).To(Equal("example2"))
		})

		It("should extract options array", func() {
			configObj := map[string]interface{}{
				"children": []interface{}{
					map[string]interface{}{
						"name":        "option_field",
						"type":        "string",
						"kind":        "scalar",
						"description": "Field with options",
						"is_optional": false,
						"is_advanced": false,
						"options":     []interface{}{"option1", "option2", "option3"},
					},
				},
			}

			fields := extractFields(configObj)

			Expect(fields["option_field"].Options).To(HaveLen(3))
			Expect(fields["option_field"].Options[0]).To(Equal("option1"))
			Expect(fields["option_field"].Options[1]).To(Equal("option2"))
			Expect(fields["option_field"].Options[2]).To(Equal("option3"))
		})
	})
})

var _ = Describe("extractFieldsArray", func() {
	Context("when given an array of field definitions", func() {
		It("should convert to FieldSpec slice", func() {
			childFields := []interface{}{
				map[string]interface{}{
					"name":        "field1",
					"type":        "string",
					"kind":        "scalar",
					"description": "First field",
					"is_optional": false,
					"is_advanced": false,
				},
				map[string]interface{}{
					"name":        "field2",
					"type":        "int",
					"kind":        "scalar",
					"description": "Second field",
					"is_optional": true,
					"is_advanced": false,
				},
			}

			result := extractFieldsArray(childFields)

			Expect(result).To(HaveLen(2))
			Expect(result[0].Name).To(Equal("field1"))
			Expect(result[0].Type).To(Equal("string"))
			Expect(result[0].Required).To(BeTrue())
			Expect(result[1].Name).To(Equal("field2"))
			Expect(result[1].Type).To(Equal("int"))
			Expect(result[1].Required).To(BeFalse())
		})
	})
})

var _ = Describe("generateSchemas", func() {
	Context("when generating schemas", func() {
		It("should return SchemaOutput with populated maps", func() {
			result, err := generateSchemas()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.Metadata.BenthosVersion).NotTo(BeEmpty())
			// Verify maps are populated with actual plugins, not just initialized
			Expect(len(result.Inputs)).To(BeNumerically(">", 0), "Should have at least one input plugin")
			Expect(len(result.Processors)).To(BeNumerically(">", 0), "Should have at least one processor plugin")
			Expect(len(result.Outputs)).To(BeNumerically(">", 0), "Should have at least one output plugin")
		})

		It("should include UMH plugins in inputs", func() {
			result, err := generateSchemas()
			Expect(err).NotTo(HaveOccurred())

			// These UMH input plugins MUST exist - fail if missing
			umhInputs := []string{"opcua", "modbus", "s7comm", "sparkplug_b", "sensorconnect"}
			for _, pluginName := range umhInputs {
				Expect(result.Inputs).To(HaveKey(pluginName), "Missing required UMH input plugin: %s", pluginName)
				plugin := result.Inputs[pluginName]
				Expect(plugin.Source).To(Equal("benthos-umh"), "Plugin %s should be from benthos-umh", pluginName)
				Expect(plugin.Name).To(Equal(pluginName))
				Expect(plugin.Type).To(Equal("input"))
			}
		})

		It("should include UMH plugins in processors", func() {
			result, err := generateSchemas()
			Expect(err).NotTo(HaveOccurred())

			// These UMH processor plugins MUST exist - fail if missing
			umhProcessors := []string{"tag_processor", "stream_processor", "downsampler", "topic_browser"}
			for _, pluginName := range umhProcessors {
				Expect(result.Processors).To(HaveKey(pluginName), "Missing required UMH processor plugin: %s", pluginName)
				plugin := result.Processors[pluginName]
				Expect(plugin.Source).To(Equal("benthos-umh"), "Plugin %s should be from benthos-umh", pluginName)
				Expect(plugin.Name).To(Equal(pluginName))
				Expect(plugin.Type).To(Equal("processor"))
			}
		})

		It("should include UMH plugins in outputs", func() {
			result, err := generateSchemas()
			Expect(err).NotTo(HaveOccurred())

			// UNS output plugin MUST exist - fail if missing
			Expect(result.Outputs).To(HaveKey("uns"), "Missing required UMH output plugin: uns")
			plugin := result.Outputs["uns"]
			Expect(plugin.Source).To(Equal("benthos-umh"))
			Expect(plugin.Name).To(Equal("uns"))
			Expect(plugin.Type).To(Equal("output"))
		})

		It("should include upstream Benthos plugins", func() {
			result, err := generateSchemas()
			Expect(err).NotTo(HaveOccurred())

			// Check that we have some upstream plugins
			upstreamCount := 0
			for _, plugin := range result.Inputs {
				if plugin.Source == "upstream" {
					upstreamCount++
				}
			}
			Expect(upstreamCount).To(BeNumerically(">", 0), "Should have at least some upstream input plugins")
		})

		It("should set metadata timestamp", func() {
			result, err := generateSchemas()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Metadata.GeneratedAt.IsZero()).To(BeFalse())
		})

		It("should set metadata versions", func() {
			result, err := generateSchemas()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Metadata.BenthosVersion).NotTo(BeEmpty())
			Expect(result.Metadata.BenthosUMHVersion).NotTo(BeEmpty())
		})
	})
})

var _ = Describe("getBenthosVersion", func() {
	Context("when retrieving Benthos version", func() {
		It("should return a non-empty version string", func() {
			version := getBenthosVersion()
			Expect(version).NotTo(BeEmpty())
		})
	})
})

var _ = Describe("getUMHVersion", func() {
	Context("when retrieving UMH version", func() {
		It("should return a non-empty version string", func() {
			version := getUMHVersion()
			Expect(version).NotTo(BeEmpty())
		})
	})
})

var _ = Describe("Version Flag Handling", func() {
	Context("when generating versioned filename", func() {
		It("should strip 'v' prefix from version", func() {
			result := generateVersionedFilename("v0.11.6")
			Expect(result).To(Equal("benthos-schemas-v0.11.6.json"))
		})

		It("should handle version without prefix", func() {
			result := generateVersionedFilename("0.11.6")
			Expect(result).To(Equal("benthos-schemas-v0.11.6.json"))
		})
	})
})

// Verifies JSON output structure required by Management Console
var _ = Describe("JSON Output Contract", func() {
	Context("when marshaling real schema to JSON", func() {
		It("should produce valid JSON with expected structure for downstream consumers", func() {
			schema, err := generateSchemas()
			Expect(err).NotTo(HaveOccurred())

			// Marshal to JSON
			jsonBytes, err := json.Marshal(schema)
			Expect(err).NotTo(HaveOccurred())

			// Parse and verify structure
			var result map[string]interface{}
			err = json.Unmarshal(jsonBytes, &result)
			Expect(err).NotTo(HaveOccurred())

			// Verify metadata structure (required by Management Console)
			Expect(result).To(HaveKey("metadata"))
			metadata := result["metadata"].(map[string]interface{})
			Expect(metadata).To(HaveKey("benthos_version"))
			Expect(metadata).To(HaveKey("benthos_umh_version"))
			Expect(metadata).To(HaveKey("generated_at"))

			// Verify plugin collections exist
			Expect(result).To(HaveKey("inputs"))
			Expect(result).To(HaveKey("processors"))
			Expect(result).To(HaveKey("outputs"))

			// Verify at least one UMH plugin has expected field structure
			inputs := result["inputs"].(map[string]interface{})
			Expect(inputs).To(HaveKey("modbus"))
			modbus := inputs["modbus"].(map[string]interface{})
			Expect(modbus).To(HaveKey("name"))
			Expect(modbus).To(HaveKey("type"))
			Expect(modbus).To(HaveKey("source"))
			Expect(modbus).To(HaveKey("config"))
			Expect(modbus["source"]).To(Equal("benthos-umh"))

			// Verify nested config fields work correctly
			config := modbus["config"].(map[string]interface{})
			Expect(len(config)).To(BeNumerically(">", 0), "Plugin should have config fields")
		})
	})
})
