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
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSchemaExport(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Schema Export Suite")
}

var _ = Describe("SchemaOutput", func() {
	Context("when marshaling to JSON", func() {
		It("should marshal SchemaOutput with all fields correctly", func() {
			// Arrange
			testTime := time.Date(2025, 10, 21, 12, 0, 0, 0, time.UTC)
			schema := SchemaOutput{
				Metadata: Metadata{
					BenthosVersion:    "4.0.0",
					GeneratedAt:       testTime,
					BenthosUMHVersion: "1.0.0",
				},
				Inputs: map[string]PluginSpec{
					"test_input": {
						Name:        "test_input",
						Type:        "input",
						Source:      "benthos-umh",
						Summary:     "Test input plugin",
						Description: "A test input for validation",
						Config: map[string]FieldSpec{
							"address": {
								Name:        "address",
								Type:        "string",
								Kind:        "scalar",
								Description: "Connection address",
								Required:    true,
								Default:     "localhost:502",
								Examples:    []interface{}{"192.168.1.100:502"},
							},
						},
					},
				},
				Processors: map[string]PluginSpec{},
				Outputs:    map[string]PluginSpec{},
			}

			// Act
			jsonBytes, err := json.Marshal(schema)

			// Assert
			Expect(err).ToNot(HaveOccurred())

			var result map[string]interface{}
			err = json.Unmarshal(jsonBytes, &result)
			Expect(err).ToNot(HaveOccurred())

			// Verify metadata
			metadata := result["metadata"].(map[string]interface{})
			Expect(metadata["benthos_version"]).To(Equal("4.0.0"))
			Expect(metadata["benthos_umh_version"]).To(Equal("1.0.0"))

			// Verify inputs
			inputs := result["inputs"].(map[string]interface{})
			testInput := inputs["test_input"].(map[string]interface{})
			Expect(testInput["name"]).To(Equal("test_input"))
			Expect(testInput["type"]).To(Equal("input"))
			Expect(testInput["source"]).To(Equal("benthos-umh"))
			Expect(testInput["summary"]).To(Equal("Test input plugin"))
			Expect(testInput["description"]).To(Equal("A test input for validation"))

			// Verify config field
			config := testInput["config"].(map[string]interface{})
			address := config["address"].(map[string]interface{})
			Expect(address["name"]).To(Equal("address"))
			Expect(address["type"]).To(Equal("string"))
			Expect(address["kind"]).To(Equal("scalar"))
			Expect(address["description"]).To(Equal("Connection address"))
			Expect(address["required"]).To(BeTrue())
			Expect(address["default"]).To(Equal("localhost:502"))

			// Verify examples array
			examples := address["examples"].([]interface{})
			Expect(examples).To(HaveLen(1))
			Expect(examples[0]).To(Equal("192.168.1.100:502"))
		})

		It("should marshal nested FieldSpec with Children correctly", func() {
			// Arrange - Create a field with nested children (like an object with properties)
			field := FieldSpec{
				Name:        "connection",
				Type:        "object",
				Kind:        "object",
				Description: "Connection configuration",
				Required:    true,
				Default:     nil,
				Children: []FieldSpec{
					{
						Name:        "host",
						Type:        "string",
						Kind:        "scalar",
						Description: "Hostname",
						Required:    true,
						Default:     "localhost",
					},
					{
						Name:        "port",
						Type:        "int",
						Kind:        "scalar",
						Description: "Port number",
						Required:    true,
						Default:     float64(502),
					},
				},
			}

			// Act
			jsonBytes, err := json.Marshal(field)

			// Assert
			Expect(err).ToNot(HaveOccurred())

			var result map[string]interface{}
			err = json.Unmarshal(jsonBytes, &result)
			Expect(err).ToNot(HaveOccurred())

			// Verify parent field
			Expect(result["name"]).To(Equal("connection"))
			Expect(result["type"]).To(Equal("object"))
			Expect(result["kind"]).To(Equal("object"))
			Expect(result["description"]).To(Equal("Connection configuration"))
			Expect(result["required"]).To(BeTrue())

			// Verify children array exists
			children := result["children"].([]interface{})
			Expect(children).To(HaveLen(2))

			// Verify first child (host)
			host := children[0].(map[string]interface{})
			Expect(host["name"]).To(Equal("host"))
			Expect(host["type"]).To(Equal("string"))
			Expect(host["kind"]).To(Equal("scalar"))
			Expect(host["description"]).To(Equal("Hostname"))
			Expect(host["required"]).To(BeTrue())
			Expect(host["default"]).To(Equal("localhost"))

			// Verify second child (port)
			port := children[1].(map[string]interface{})
			Expect(port["name"]).To(Equal("port"))
			Expect(port["type"]).To(Equal("int"))
			Expect(port["kind"]).To(Equal("scalar"))
			Expect(port["description"]).To(Equal("Port number"))
			Expect(port["required"]).To(BeTrue())
			Expect(port["default"]).To(Equal(float64(502)))
		})

		It("should omit optional empty fields from JSON", func() {
			// Arrange - Create field with some empty optional fields
			field := FieldSpec{
				Name:        "simple_field",
				Type:        "string",
				Kind:        "scalar",
				Description: "A simple field",
				Required:    false,
				Default:     "default_value",
				// Examples: nil - should be omitted
				// Options: nil - should be omitted
				// Advanced: false - should be omitted (default value)
				// Children: nil - should be omitted
			}

			// Act
			jsonBytes, err := json.Marshal(field)

			// Assert
			Expect(err).ToNot(HaveOccurred())

			var result map[string]interface{}
			err = json.Unmarshal(jsonBytes, &result)
			Expect(err).ToNot(HaveOccurred())

			// Verify required fields are present
			Expect(result["name"]).To(Equal("simple_field"))
			Expect(result["type"]).To(Equal("string"))
			Expect(result["kind"]).To(Equal("scalar"))
			Expect(result["description"]).To(Equal("A simple field"))
			Expect(result["required"]).To(BeFalse())
			Expect(result["default"]).To(Equal("default_value"))

			// Verify optional empty fields are omitted
			_, hasExamples := result["examples"]
			Expect(hasExamples).To(BeFalse(), "examples should be omitted when empty")

			_, hasOptions := result["options"]
			Expect(hasOptions).To(BeFalse(), "options should be omitted when empty")

			_, hasAdvanced := result["advanced"]
			Expect(hasAdvanced).To(BeFalse(), "advanced should be omitted when false")

			_, hasChildren := result["children"]
			Expect(hasChildren).To(BeFalse(), "children should be omitted when empty")
		})

		It("should include optional fields when they have values", func() {
			// Arrange - Create field with all optional fields populated
			field := FieldSpec{
				Name:        "complex_field",
				Type:        "string",
				Kind:        "scalar",
				Description: "A complex field",
				Required:    true,
				Default:     "default",
				Examples:    []interface{}{"example1", "example2"},
				Options:     []string{"option1", "option2"},
				Advanced:    true,
				Children: []FieldSpec{
					{
						Name:        "child",
						Type:        "string",
						Kind:        "scalar",
						Description: "Child field",
						Required:    false,
						Default:     "child_default",
					},
				},
			}

			// Act
			jsonBytes, err := json.Marshal(field)

			// Assert
			Expect(err).ToNot(HaveOccurred())

			var result map[string]interface{}
			err = json.Unmarshal(jsonBytes, &result)
			Expect(err).ToNot(HaveOccurred())

			// Verify all optional fields are present
			examples := result["examples"].([]interface{})
			Expect(examples).To(HaveLen(2))
			Expect(examples[0]).To(Equal("example1"))

			options := result["options"].([]interface{})
			Expect(options).To(HaveLen(2))
			Expect(options[0]).To(Equal("option1"))

			Expect(result["advanced"]).To(BeTrue())

			children := result["children"].([]interface{})
			Expect(children).To(HaveLen(1))
		})

		It("should support round-trip JSON marshaling and unmarshaling", func() {
			// Arrange
			testTime := time.Date(2025, 10, 21, 12, 0, 0, 0, time.UTC)
			original := SchemaOutput{
				Metadata: Metadata{
					BenthosVersion:    "4.0.0",
					GeneratedAt:       testTime,
					BenthosUMHVersion: "1.0.0",
				},
				Inputs: map[string]PluginSpec{
					"modbus": {
						Name:        "modbus",
						Type:        "input",
						Source:      "benthos-umh",
						Summary:     "Modbus protocol input",
						Description: "Reads data from Modbus devices",
						Config: map[string]FieldSpec{
							"connection": {
								Name:        "connection",
								Type:        "object",
								Kind:        "object",
								Description: "Connection settings",
								Required:    true,
								Default:     nil,
								Children: []FieldSpec{
									{
										Name:        "host",
										Type:        "string",
										Kind:        "scalar",
										Description: "Modbus host",
										Required:    true,
										Default:     "localhost",
										Examples:    []interface{}{"192.168.1.100"},
									},
									{
										Name:        "port",
										Type:        "int",
										Kind:        "scalar",
										Description: "Modbus port",
										Required:    true,
										Default:     float64(502),
									},
								},
							},
						},
					},
				},
				Processors: map[string]PluginSpec{},
				Outputs:    map[string]PluginSpec{},
			}

			// Act - Marshal to JSON
			jsonBytes, err := json.Marshal(original)
			Expect(err).ToNot(HaveOccurred())

			// Act - Unmarshal back to struct
			var roundTrip SchemaOutput
			err = json.Unmarshal(jsonBytes, &roundTrip)
			Expect(err).ToNot(HaveOccurred())

			// Assert - Verify structure is preserved
			Expect(roundTrip.Metadata.BenthosVersion).To(Equal(original.Metadata.BenthosVersion))
			Expect(roundTrip.Metadata.BenthosUMHVersion).To(Equal(original.Metadata.BenthosUMHVersion))
			Expect(roundTrip.Metadata.GeneratedAt.Unix()).To(Equal(original.Metadata.GeneratedAt.Unix()))

			Expect(roundTrip.Inputs).To(HaveKey("modbus"))
			modbusInput := roundTrip.Inputs["modbus"]
			Expect(modbusInput.Name).To(Equal("modbus"))
			Expect(modbusInput.Type).To(Equal("input"))
			Expect(modbusInput.Source).To(Equal("benthos-umh"))
			Expect(modbusInput.Summary).To(Equal("Modbus protocol input"))
			Expect(modbusInput.Description).To(Equal("Reads data from Modbus devices"))

			// Verify nested config with children
			Expect(modbusInput.Config).To(HaveKey("connection"))
			connection := modbusInput.Config["connection"]
			Expect(connection.Name).To(Equal("connection"))
			Expect(connection.Type).To(Equal("object"))
			Expect(connection.Children).To(HaveLen(2))

			// Verify first child
			host := connection.Children[0]
			Expect(host.Name).To(Equal("host"))
			Expect(host.Type).To(Equal("string"))
			Expect(host.Default).To(Equal("localhost"))
			Expect(host.Examples).To(HaveLen(1))
			Expect(host.Examples[0]).To(Equal("192.168.1.100"))

			// Verify second child
			port := connection.Children[1]
			Expect(port.Name).To(Equal("port"))
			Expect(port.Type).To(Equal("int"))
			Expect(port.Default).To(Equal(float64(502)))
		})
	})
})
