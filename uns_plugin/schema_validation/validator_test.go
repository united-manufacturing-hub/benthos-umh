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

package schemavalidation

import (
	"fmt"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
)

var _ = Describe("Validator", func() {
	Context("when validating basic data", func() {

		schema := []byte(`
		{
   "type": "object",
   "properties": {
		"virtual_path": {
          "type": "string",
          "enum": ["vibration.x-axis"]
       },
      "fields": {
         "type": "object",
         "properties": {
            "value": {
               "type": "object",
               "properties": {
                  "timestamp_ms": {"type": "number"},
                  "value": {"type": "number"}
               },
               "required": ["timestamp_ms", "value"],
               "additionalProperties": false
            }
         },
         "additionalProperties": false
      }
   },
   "required": ["virtual_path", "fields"],
   "additionalProperties": false
}`)
		var validator *Validator

		BeforeEach(func() {
			validator = NewValidator()
			validator.LoadSchema("_sensor_data", 1, schema)
			Expect(validator.HasSchema("_sensor_data", 1)).To(BeTrue())
		})

		/*
			Test cases:
			1. Valid data & valid virtual_path
			2. Valid data & invalid virtual_path
			3. Invalid data & valid virtual_path
			4. Invalid data & invalid virtual_path
			5. Invalid payload format
		*/

		It("should pass validation for valid data & valid virtual_path", func() {
			topic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.vibration.x-axis")
			Expect(err).To(BeNil())
			result := validator.Validate(topic, []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 100}}`))
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(BeNil())
			Expect(result.ContractName).To(Equal("_sensor_data"))
			Expect(result.ContractVersion).To(Equal(uint64(1)))
		})

		It("should fail validation for valid data & invalid virtual_path", func() {
			topic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.vibration.non-existing")
			Expect(err).To(BeNil())
			result := validator.Validate(topic, []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 100}}`))
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(Not(BeNil()))
			Expect(result.ContractName).To(Equal("_sensor_data"))
			Expect(result.ContractVersion).To(Equal(uint64(1)))
		})

		It("should fail validation for invalid data & valid virtual_path", func() {
			topic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.vibration.x-axis")
			Expect(err).To(BeNil())
			result := validator.Validate(topic, []byte(`{"value": {"timestamp_ms": 1719859200000, "value": "not a number"}}`))
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(Not(BeNil()))
			Expect(result.ContractName).To(Equal("_sensor_data"))
			Expect(result.ContractVersion).To(Equal(uint64(1)))
		})

		It("should fail validation for invalid data & invalid virtual_path", func() {
			topic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.vibration.non-existing")
			Expect(err).To(BeNil())
			result := validator.Validate(topic, []byte(`{"value": {"timestamp_ms": 1719859200000, "value": "not a number"}}`))
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(Not(BeNil()))
			Expect(result.ContractName).To(Equal("_sensor_data"))
			Expect(result.ContractVersion).To(Equal(uint64(1)))
		})

		It("should fail validation for invalid payload format", func() {
			topic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.vibration.x-axis")
			Expect(err).To(BeNil())
			result := validator.Validate(topic, []byte(`{"value": {"value": 100}}`))
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(Not(BeNil()))
			Expect(result.ContractName).To(Equal("_sensor_data"))
			Expect(result.ContractVersion).To(Equal(uint64(1)))
		})
	})

	Context("when parsing data contracts", func() {
		var validator *Validator

		BeforeEach(func() {
			validator = NewValidator()
		})

		It("should parse valid contract with version", func() {
			contractName, version, err := validator.ExtractSchemaVersionFromDataContract("_sensor_data-v1")
			Expect(err).To(BeNil())
			Expect(contractName).To(Equal("_sensor_data"))
			Expect(version).To(Equal(uint64(1)))
		})

		It("should parse contract with multi-digit version", func() {
			contractName, version, err := validator.ExtractSchemaVersionFromDataContract("_pump_data-v123")
			Expect(err).To(BeNil())
			Expect(contractName).To(Equal("_pump_data"))
			Expect(version).To(Equal(uint64(123)))
		})

		It("should parse contract with complex name", func() {
			contractName, version, err := validator.ExtractSchemaVersionFromDataContract("_complex_sensor_data_v2-v42")
			Expect(err).To(BeNil())
			Expect(contractName).To(Equal("_complex_sensor_data_v2"))
			Expect(version).To(Equal(uint64(42)))
		})

		It("should fail for empty contract", func() {
			_, _, err := validator.ExtractSchemaVersionFromDataContract("")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("contract string is empty"))
		})

		It("should fail for contract without version", func() {
			_, _, err := validator.ExtractSchemaVersionFromDataContract("_sensor_data")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid data contract format"))
		})

		It("should fail for contract with invalid version format", func() {
			_, _, err := validator.ExtractSchemaVersionFromDataContract("_sensor_data-vabc")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid data contract format"))
		})

		It("should fail for contract with non-numeric version", func() {
			_, _, err := validator.ExtractSchemaVersionFromDataContract("_sensor_data-v1a2")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid data contract format"))
		})

		It("should fail for contract with negative version", func() {
			_, _, err := validator.ExtractSchemaVersionFromDataContract("_sensor_data-v-1")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid data contract format"))
		})
	})

	Context("when loading schemas", func() {
		var validator *Validator
		validSchema := []byte(`{"type": "object", "properties": {"test": {"type": "string"}}}`)

		BeforeEach(func() {
			validator = NewValidator()
		})

		It("should load valid schema successfully", func() {
			err := validator.LoadSchema("_test_contract", 1, validSchema)
			Expect(err).To(BeNil())
			Expect(validator.HasSchema("_test_contract", 1)).To(BeTrue())
		})

		It("should load multiple versions of same contract", func() {
			schema1 := []byte(`{"type": "object", "properties": {"test1": {"type": "string"}}}`)
			schema2 := []byte(`{"type": "object", "properties": {"test2": {"type": "number"}}}`)

			err := validator.LoadSchema("_test_contract", 1, schema1)
			Expect(err).To(BeNil())

			err = validator.LoadSchema("_test_contract", 2, schema2)
			Expect(err).To(BeNil())

			Expect(validator.HasSchema("_test_contract", 1)).To(BeTrue())
			Expect(validator.HasSchema("_test_contract", 2)).To(BeTrue())
		})

		It("should load multiple contracts", func() {
			err := validator.LoadSchema("_contract1", 1, validSchema)
			Expect(err).To(BeNil())

			err = validator.LoadSchema("_contract2", 1, validSchema)
			Expect(err).To(BeNil())

			Expect(validator.HasSchema("_contract1", 1)).To(BeTrue())
			Expect(validator.HasSchema("_contract2", 1)).To(BeTrue())
		})

		It("should fail for empty contract name", func() {
			err := validator.LoadSchema("", 1, validSchema)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("contract name cannot be empty"))
		})

		It("should fail for contract name without underscore prefix", func() {
			err := validator.LoadSchema("test_contract", 1, validSchema)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("contract name must start with an underscore"))
		})

		It("should fail for empty schema", func() {
			err := validator.LoadSchema("_test_contract", 1, []byte{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("schema cannot be empty"))
		})

		It("should fail for nil schema", func() {
			err := validator.LoadSchema("_test_contract", 1, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("schema cannot be empty"))
		})

		It("should fail for invalid JSON schema", func() {
			// Use completely invalid JSON syntax
			invalidSchema := []byte(`{"type": "object", "invalid": syntax}`)
			err := validator.LoadSchema("_test_contract", 1, invalidSchema)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to add schema version"))
		})

		It("should fail for malformed JSON", func() {
			malformedSchema := []byte(`{"type": "object", "properties":`)
			err := validator.LoadSchema("_test_contract", 1, malformedSchema)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to add schema version"))
		})

		It("should overwrite existing schema version", func() {
			schema1 := []byte(`{"type": "object", "properties": {"test1": {"type": "string"}}}`)
			schema2 := []byte(`{"type": "object", "properties": {"test2": {"type": "number"}}}`)

			err := validator.LoadSchema("_test_contract", 1, schema1)
			Expect(err).To(BeNil())

			// Load different schema with same version - should overwrite
			err = validator.LoadSchema("_test_contract", 1, schema2)
			Expect(err).To(BeNil())

			Expect(validator.HasSchema("_test_contract", 1)).To(BeTrue())
		})
	})

	Context("when checking schema existence", func() {
		var validator *Validator
		validSchema := []byte(`{"type": "object", "properties": {"test": {"type": "string"}}}`)

		BeforeEach(func() {
			validator = NewValidator()
			validator.LoadSchema("_existing_contract", 1, validSchema)
		})

		It("should return true for existing contract and version", func() {
			Expect(validator.HasSchema("_existing_contract", 1)).To(BeTrue())
		})

		It("should return false for non-existing contract", func() {
			Expect(validator.HasSchema("_non_existing_contract", 1)).To(BeFalse())
		})

		It("should return false for existing contract with non-existing version", func() {
			Expect(validator.HasSchema("_existing_contract", 2)).To(BeFalse())
		})

		It("should return false for empty contract name", func() {
			Expect(validator.HasSchema("", 1)).To(BeFalse())
		})
	})

	Context("when validating with edge cases", func() {
		var validator *Validator
		validSchema := []byte(`
		{
			"type": "object",
			"properties": {
				"virtual_path": {
					"type": "string",
					"enum": ["temperature", "humidity"]
				},
				"fields": {
					"type": "object",
					"properties": {
						"value": {
							"type": "object",
							"properties": {
								"timestamp_ms": {"type": "number"},
								"value": {"type": "number"}
							},
							"required": ["timestamp_ms", "value"],
							"additionalProperties": false
						}
					},
					"additionalProperties": false
				}
			},
			"required": ["virtual_path", "fields"],
			"additionalProperties": false
		}`)

		BeforeEach(func() {
			validator = NewValidator()
			validator.LoadSchema("_sensor_data", 1, validSchema)
		})

		It("should fail for nil topic", func() {
			result := validator.Validate(nil, []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 100}}`))
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("UNS topic cannot be nil"))
		})

		It("should fail for topic with empty contract", func() {
			// This would require mocking the topic.Info() to return empty contract
			// For now, we'll test the contract extraction directly
			_, _, err := validator.ExtractSchemaVersionFromDataContract("")
			Expect(err).To(HaveOccurred())
		})

		It("should bypass validation for non-existing schema", func() {
			topic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._non_existing_contract-v1.temperature")
			Expect(err).To(BeNil())
			result := validator.Validate(topic, []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 100}}`))
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.Error).To(BeNil())
			Expect(result.BypassReason).To(ContainSubstring("schema for contract '_non_existing_contract' version 1 not found"))
			Expect(result.ContractName).To(Equal("_non_existing_contract"))
			Expect(result.ContractVersion).To(Equal(uint64(1)))
		})

		It("should bypass validation when version extraction fails", func() {
			// Create a topic with malformed version format (unversioned contract)
			topic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._malformed_contract.temperature")
			Expect(err).To(BeNil())
			result := validator.Validate(topic, []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 100}}`))
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.Error).To(BeNil())
			Expect(result.BypassReason).To(ContainSubstring("unversioned contract '_malformed_contract' - bypassing validation (no latest fetching)"))
			Expect(result.ContractName).To(Equal("_malformed_contract"))
			Expect(result.ContractVersion).To(Equal(uint64(0)))
		})

		It("should fail for empty JSON payload", func() {
			topic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())
			result := validator.Validate(topic, []byte(``))
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("schema validation failed"))
		})

		It("should fail for invalid JSON payload", func() {
			topic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())
			result := validator.Validate(topic, []byte(`{"invalid": json}`))
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("schema validation failed"))
		})

		It("should handle topic with nil virtual path", func() {
			topic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())
			result := validator.Validate(topic, []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 100}}`))
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(BeNil())
		})

		It("should handle topic with virtual path", func() {
			// This would require a topic with virtual path - depends on topic implementation
			topic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.some.path.temperature")
			Expect(err).To(BeNil())
			result := validator.Validate(topic, []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 100}}`))
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("schema validation failed"))
		})

		It("should provide descriptive error messages", func() {
			topic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())
			result := validator.Validate(topic, []byte(`{"value": {"timestamp_ms": "not_a_number", "value": 100}}`))
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("schema validation failed"))
			Expect(result.Error.Error()).To(ContainSubstring("_sensor_data"))
			Expect(result.Error.Error()).To(ContainSubstring("version 1"))
		})

		It("should validate with different data types", func() {
			// Test with zero values
			topic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())
			result := validator.Validate(topic, []byte(`{"value": {"timestamp_ms": 0, "value": 0}}`))
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(BeNil())

			// Test with negative values
			result = validator.Validate(topic, []byte(`{"value": {"timestamp_ms": 1719859200000, "value": -25.5}}`))
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(BeNil())

			// Test with large numbers
			result = validator.Validate(topic, []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 999999999.999}}`))
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(BeNil())
		})
	})

	Context("when handling concurrent operations", func() {
		var validator *Validator
		validSchema := []byte(`{"type": "object", "properties": {"test": {"type": "string"}}}`)

		BeforeEach(func() {
			validator = NewValidator()
		})

		It("should handle concurrent schema loading", func() {
			var wg sync.WaitGroup
			numGoroutines := 10

			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					contractName := fmt.Sprintf("_test_contract_%d", index)
					err := validator.LoadSchema(contractName, 1, validSchema)
					Expect(err).To(BeNil())
				}(i)
			}

			wg.Wait()

			// Verify all schemas were loaded
			for i := 0; i < numGoroutines; i++ {
				contractName := fmt.Sprintf("_test_contract_%d", i)
				Expect(validator.HasSchema(contractName, 1)).To(BeTrue())
			}
		})

		It("should handle concurrent validation", func() {
			// Load a schema first
			err := validator.LoadSchema("_test_contract", 1, []byte(`
			{
				"type": "object",
				"properties": {
					"virtual_path": {
						"type": "string",
						"enum": ["test"]
					},
					"fields": {
						"type": "object",
						"properties": {
							"value": {
								"type": "object",
								"properties": {
									"timestamp_ms": {"type": "number"},
									"value": {"type": "number"}
								},
								"required": ["timestamp_ms", "value"]
							}
						}
					}
				},
				"required": ["virtual_path", "fields"]
			}`))
			Expect(err).To(BeNil())

			var wg sync.WaitGroup
			numGoroutines := 10

			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					topic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._test_contract-v1.test")
					Expect(err).To(BeNil())
					result := validator.Validate(topic, []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 100}}`))
					Expect(result.SchemaCheckPassed).To(BeTrue())
					Expect(result.SchemaCheckBypassed).To(BeFalse())
					Expect(result.Error).To(BeNil())
				}()
			}

			wg.Wait()
		})

		It("should handle concurrent schema loading and validation", func() {
			var wg sync.WaitGroup
			numGoroutines := 5

			// Load schemas concurrently
			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					contractName := fmt.Sprintf("_concurrent_test_%d", index)
					schema := []byte(fmt.Sprintf(`
					{
						"type": "object",
						"properties": {
							"virtual_path": {
								"type": "string",
								"enum": ["test_%d"]
							},
							"fields": {
								"type": "object",
								"properties": {
									"value": {
										"type": "object",
										"properties": {
											"timestamp_ms": {"type": "number"},
											"value": {"type": "number"}
										},
										"required": ["timestamp_ms", "value"]
									}
								}
							}
						},
						"required": ["virtual_path", "fields"]
					}`, index))
					err := validator.LoadSchema(contractName, 1, schema)
					Expect(err).To(BeNil())
				}(i)
			}

			wg.Wait()

			// Validate with each schema concurrently
			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					contractName := fmt.Sprintf("_concurrent_test_%d", index)
					topicString := fmt.Sprintf("umh.v1.enterprise.site.area.%s-v1.test_%d", contractName, index)
					topic, err := topic.NewUnsTopic(topicString)
					Expect(err).To(BeNil())
					result := validator.Validate(topic, []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 100}}`))
					Expect(result.SchemaCheckPassed).To(BeTrue())
					Expect(result.SchemaCheckBypassed).To(BeFalse())
					Expect(result.Error).To(BeNil())
				}(i)
			}

			wg.Wait()
		})
	})

	Context("when handling schema versions", func() {
		var validator *Validator

		BeforeEach(func() {
			validator = NewValidator()
		})

		It("should handle version zero", func() {
			schema := []byte(`{"type": "object", "properties": {"test": {"type": "string"}}}`)
			err := validator.LoadSchema("_test_contract", 0, schema)
			Expect(err).To(BeNil())
			Expect(validator.HasSchema("_test_contract", 0)).To(BeTrue())
		})

		It("should handle large version numbers", func() {
			schema := []byte(`{"type": "object", "properties": {"test": {"type": "string"}}}`)
			largeVersion := uint64(18446744073709551615) // max uint64
			err := validator.LoadSchema("_test_contract", largeVersion, schema)
			Expect(err).To(BeNil())
			Expect(validator.HasSchema("_test_contract", largeVersion)).To(BeTrue())
		})

		It("should maintain separate versions independently", func() {
			schema1 := []byte(`{"type": "object", "properties": {"v1_field": {"type": "string"}}}`)
			schema2 := []byte(`{"type": "object", "properties": {"v2_field": {"type": "number"}}}`)

			err := validator.LoadSchema("_versioned_contract", 1, schema1)
			Expect(err).To(BeNil())

			err = validator.LoadSchema("_versioned_contract", 2, schema2)
			Expect(err).To(BeNil())

			Expect(validator.HasSchema("_versioned_contract", 1)).To(BeTrue())
			Expect(validator.HasSchema("_versioned_contract", 2)).To(BeTrue())
			Expect(validator.HasSchema("_versioned_contract", 3)).To(BeFalse())
		})
	})
})
