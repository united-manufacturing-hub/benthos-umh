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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
)

var _ = Describe("Validator", func() {
	var (
		validator    *Validator
		mockRegistry *MockSchemaRegistry
	)

	BeforeEach(func() {
		mockRegistry = NewMockSchemaRegistry()
		mockRegistry.SetupTestSchemas()
		validator = NewValidatorWithRegistry(mockRegistry.URL())
	})

	AfterEach(func() {
		if validator != nil {
			validator.Close()
		}
		if mockRegistry != nil {
			mockRegistry.Close()
		}
	})

	Context("when creating a new validator", func() {
		It("should initialize correctly without registry", func() {
			validator := NewValidator()
			Expect(validator.contractCache).NotTo(BeNil())
			Expect(validator.httpClient).NotTo(BeNil())
			Expect(validator.schemaRegistryURL).To(BeEmpty())
		})

		It("should initialize correctly with registry", func() {
			validator := NewValidatorWithRegistry("http://localhost:8081")
			Expect(validator.contractCache).NotTo(BeNil())
			Expect(validator.httpClient).NotTo(BeNil())
			Expect(validator.schemaRegistryURL).To(Equal("http://localhost:8081"))
		})
	})

	Context("when validating with versioned contracts", func() {
		It("should successfully validate sensor data v1", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(BeNil())
			Expect(result.ContractName).To(Equal("_sensor_data"))
			Expect(result.ContractVersion).To(Equal(uint64(1)))
		})

		It("should successfully validate sensor data v2", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v2.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(BeNil())
			Expect(result.ContractName).To(Equal("_sensor_data"))
			Expect(result.ContractVersion).To(Equal(uint64(2)))
		})

		It("should reject invalid virtual path", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.invalid_path")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("schema validation failed"))
		})

		It("should reject invalid payload structure", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": "invalid", "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("schema validation failed"))
		})
	})

	Context("when handling non-existent schemas", func() {
		It("should bypass validation for non-existent contract", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._non_existent_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("no schemas found for contract"))
			Expect(result.ContractName).To(Equal("_non_existent"))
			Expect(result.ContractVersion).To(Equal(uint64(1)))
		})

		It("should bypass validation for non-existent version", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v999.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("no schemas found for contract"))
			Expect(result.ContractName).To(Equal("_sensor_data"))
			Expect(result.ContractVersion).To(Equal(uint64(999)))
		})
	})

	Context("when handling unversioned contracts", func() {
		It("should bypass validation for unversioned contracts", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("unversioned contract"))
			Expect(result.ContractName).To(Equal("_sensor_data"))
		})
	})

	Context("when testing cache behavior", func() {
		It("should cache successful schema fetches", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			// First validation should fetch and cache the schema
			result1 := validator.Validate(unsTopic, payload)
			Expect(result1.SchemaCheckPassed).To(BeTrue())

			// Verify schema is cached
			Expect(validator.HasSchema("_sensor_data", 1)).To(BeTrue())

			// Second validation should use cached schema
			result2 := validator.Validate(unsTopic, payload)
			Expect(result2.SchemaCheckPassed).To(BeTrue())
			Expect(result2.ContractName).To(Equal("_sensor_data"))
			Expect(result2.ContractVersion).To(Equal(uint64(1)))
		})

		It("should cache negative results (schema not found)", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._non_existent_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			// First validation should fetch and cache the negative result
			result1 := validator.Validate(unsTopic, payload)
			Expect(result1.SchemaCheckBypassed).To(BeTrue())
			Expect(result1.BypassReason).To(ContainSubstring("no schemas found for contract"))

			// Second validation should use cached negative result
			result2 := validator.Validate(unsTopic, payload)
			Expect(result2.SchemaCheckBypassed).To(BeTrue())
			Expect(result2.BypassReason).To(ContainSubstring("no schemas found for contract"))
		})

		It("should cache successful schema fetches forever", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			// Validate to populate cache
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())

			// Check cache entry exists and never expires
			validator.cacheMutex.RLock()
			entry, exists := validator.contractCache["_sensor_data-v1"]
			validator.cacheMutex.RUnlock()

			Expect(exists).To(BeTrue())
			Expect(entry.SchemaExists).To(BeTrue())
			Expect(len(entry.Schemas)).To(BeNumerically(">", 0))
			Expect(entry.IsExpired()).To(BeFalse())
			Expect(entry.ExpiresAt.IsZero()).To(BeTrue()) // Zero time means never expires
		})

		It("should cache schema misses for 10 minutes", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._non_existent_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			// Validate to populate cache with miss
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckBypassed).To(BeTrue())

			// Check cache entry exists with 10 minute expiration
			validator.cacheMutex.RLock()
			entry, exists := validator.contractCache["_non_existent-v1"]
			validator.cacheMutex.RUnlock()

			Expect(exists).To(BeTrue())
			Expect(entry.SchemaExists).To(BeFalse())
			Expect(len(entry.Schemas)).To(Equal(0))
			Expect(entry.IsExpired()).To(BeFalse())
			Expect(entry.ExpiresAt.IsZero()).To(BeFalse()) // Should have an expiration time
			Expect(entry.ExpiresAt).To(BeTemporally("~", time.Now().Add(10*time.Minute), time.Second))
		})
	})

	Context("when handling contract parsing", func() {
		It("should parse versioned contracts correctly", func() {
			contractName, version, err := validator.ExtractSchemaVersionFromDataContract("_sensor_data_v1")
			Expect(err).To(BeNil())
			Expect(contractName).To(Equal("_sensor_data"))
			Expect(version).To(Equal(uint64(1)))

			contractName, version, err = validator.ExtractSchemaVersionFromDataContract("_pump_data_v123")
			Expect(err).To(BeNil())
			Expect(contractName).To(Equal("_pump_data"))
			Expect(version).To(Equal(uint64(123)))
		})

		It("should reject invalid contract formats", func() {
			_, _, err := validator.ExtractSchemaVersionFromDataContract("invalid-contract")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid data contract format"))

			_, _, err = validator.ExtractSchemaVersionFromDataContract("_sensor_data_v")
			Expect(err).To(HaveOccurred())

			_, _, err = validator.ExtractSchemaVersionFromDataContract("")
			Expect(err).To(HaveOccurred())
		})
	})

	Context("when manually loading schemas", func() {
		It("should load and cache schemas correctly", func() {
			schemas := map[string][]byte{
				"_test_contract_v1_timeseries-number": []byte(`{"type": "object", "properties": {"test": {"type": "string"}}}`),
				"_test_contract_v1_timeseries-string": []byte(`{"type": "object", "properties": {"test": {"type": "string"}}}`),
			}

			err := validator.LoadSchemas("_test_contract", 1, schemas)
			Expect(err).To(BeNil())

			// Check if schemas were cached
			Expect(validator.HasSchema("_test_contract", 1)).To(BeTrue())

			// Verify cache entry
			validator.cacheMutex.RLock()
			entry, exists := validator.contractCache["_test_contract-v1"]
			validator.cacheMutex.RUnlock()

			Expect(exists).To(BeTrue())
			Expect(entry.SchemaExists).To(BeTrue())
			Expect(entry.Schemas).NotTo(BeNil())
			Expect(len(entry.Schemas)).To(Equal(2))
		})

		It("should validate contract name requirements", func() {
			schemas := map[string][]byte{
				"invalid_contract_v1_timeseries-number": []byte(`{"type": "object"}`),
			}

			err := validator.LoadSchemas("invalid_contract", 1, schemas)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must start with an underscore"))

			err = validator.LoadSchemas("", 1, schemas)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot be empty"))

			err = validator.LoadSchemas("_test_contract", 1, map[string][]byte{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("schemas cannot be empty"))

			err = validator.LoadSchemas("_test_contract", 1, map[string][]byte{
				"_test_contract_v1_timeseries-number": []byte{},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("schema cannot be empty for subject"))
		})
	})

	Context("when handling network errors", func() {
		It("should handle schema registry unavailable", func() {
			validator := NewValidatorWithRegistry("http://localhost:9999") // Invalid URL

			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("failed to fetch schema"))
		})
	})

	Context("when closing validator", func() {
		It("should clear cache on close", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			// Validate to populate cache
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())

			// Verify cache has entries
			validator.cacheMutex.RLock()
			cacheSize := len(validator.contractCache)
			validator.cacheMutex.RUnlock()
			Expect(cacheSize).To(BeNumerically(">", 0))

			// Close validator
			validator.Close()

			// Verify cache is cleared
			validator.cacheMutex.RLock()
			cacheSize = len(validator.contractCache)
			validator.cacheMutex.RUnlock()
			Expect(cacheSize).To(Equal(0))
		})
	})

	Context("when testing schema version mismatch issues", func() {
		It("should validate sensor data v2 where v2 schemas are registered as version 1", func() {
			// This test replicates the integration test issue where _sensor_data_v2_* schemas
			// are registered as version 1 in the schema registry (first version of that subject)
			// but we try to fetch them using version 2 (extracted from the contract name)

			// Create a fresh mock registry for this test
			freshRegistry := NewMockSchemaRegistry()

			// Register _sensor_data_v2-* schema as version 1 (first version of this subject)
			// This mimics the real Redpanda setup where each subject starts at version 1
			sensorDataV2Schema := `{
			"type": "object",
			"properties": {
				"virtual_path": {
					"type": "string",
					"enum": ["temperature", "humidity", "pressure"]
				},
				"fields": {
					"type": "object",
					"properties": {
						"timestamp_ms": {"type": "number"},
						"value": {"type": "number"}
					},
					"required": ["timestamp_ms", "value"],
					"additionalProperties": false
				}
			},
			"required": ["virtual_path", "fields"],
			"additionalProperties": false
		}`

			// Register the v2 schema as version 1 in the registry (this is the current behavior)
			freshRegistry.AddSchema("_sensor_data_v2-timeseries-number", 1, sensorDataV2Schema)

			// Create validator with this registry
			freshValidator := NewValidatorWithRegistry(freshRegistry.URL())
			defer freshValidator.Close()
			defer freshRegistry.Close()

			// Test v2 contract (which should look for _sensor_data_v2_* schemas)
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v2.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := freshValidator.Validate(unsTopic, payload)

			GinkgoWriter.Printf("V2 validation result: SchemaCheckPassed=%v, SchemaCheckBypassed=%v, BypassReason=%s, Error=%v\n",
				result.SchemaCheckPassed, result.SchemaCheckBypassed, result.BypassReason, result.Error)

			// After fix: should now work correctly by fetching the latest version
			Expect(result.SchemaCheckPassed).To(BeTrue(), "Schema validation should pass by fetching latest version")
			Expect(result.SchemaCheckBypassed).To(BeFalse(), "Schema validation should not bypass")
			Expect(result.Error).To(BeNil(), "No validation error should occur")
			Expect(result.ContractName).To(Equal("_sensor_data"))
			Expect(result.ContractVersion).To(Equal(uint64(2)))
		})
	})

	Context("when validating with multiple schemas per contract", func() {
		It("should validate against the correct schema based on payload type", func() {
			// Load multiple schemas for the same contract+version
			schemas := map[string][]byte{
				"_pump_data_v1_timeseries-number": []byte(`{
					"type": "object",
					"properties": {
						"virtual_path": {
							"type": "string",
							"enum": ["vibration.x-axis", "count"]
						},
						"fields": {
							"type": "object",
							"properties": {
								"timestamp_ms": {"type": "number"},
								"value": {"type": "number"}
							},
							"required": ["timestamp_ms", "value"],
							"additionalProperties": false
						}
					},
					"required": ["virtual_path", "fields"],
					"additionalProperties": false
				}`),
				"_pump_data_v1_timeseries-string": []byte(`{
					"type": "object",
					"properties": {
						"virtual_path": {
							"type": "string",
							"enum": ["serialNumber", "status"]
						},
						"fields": {
							"type": "object",
							"properties": {
								"timestamp_ms": {"type": "number"},
								"value": {"type": "string"}
							},
							"required": ["timestamp_ms", "value"],
							"additionalProperties": false
						}
					},
					"required": ["virtual_path", "fields"],
					"additionalProperties": false
				}`),
			}

			err := validator.LoadSchemas("_pump_data", 1, schemas)
			Expect(err).To(BeNil())

			// Test numeric value validation (should match number schema)
			numberTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._pump_data_v1.vibration.x-axis")
			Expect(err).To(BeNil())
			numberPayload := []byte(`{"timestamp_ms": 1719859200000, "value": 0.5}`)

			result := validator.Validate(numberTopic, numberPayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(BeNil())
			Expect(result.ContractName).To(Equal("_pump_data"))
			Expect(result.ContractVersion).To(Equal(uint64(1)))

			// Test string value validation (should match string schema)
			stringTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._pump_data_v1.serialNumber")
			Expect(err).To(BeNil())
			stringPayload := []byte(`{"timestamp_ms": 1719859200000, "value": "SN123456789"}`)

			result = validator.Validate(stringTopic, stringPayload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(BeNil())
			Expect(result.ContractName).To(Equal("_pump_data"))
			Expect(result.ContractVersion).To(Equal(uint64(1)))

			// Test invalid virtual path (should fail against all schemas)
			invalidTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._pump_data_v1.invalid_path")
			Expect(err).To(BeNil())

			result = validator.Validate(invalidTopic, numberPayload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("schema validation failed"))

			// Test wrong data type for number field (should fail)
			result = validator.Validate(numberTopic, stringPayload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("schema validation failed"))
		})
	})
})

// Test the logger integration
var _ = Describe("Validator Logger Integration", func() {
	var validator *Validator
	var mockLogger *service.Logger

	BeforeEach(func() {
		// Create a mock logger (this will be nil in practice but we test the nil-safety)
		mockLogger = nil
		validator = NewValidatorWithRegistryAndLogger("http://localhost:8081", mockLogger)
	})

	It("should handle nil logger gracefully", func() {
		// This should not panic even with nil logger
		validator.debugf("Test debug message: %s", "should not crash")

		// Verify logger can be set after creation
		validator.SetLogger(mockLogger)

		// This should also not panic
		validator.debugf("Another test message: %d", 42)
	})

	It("should create validator with registry and logger", func() {
		// Test with nil logger (common case)
		validator := NewValidatorWithRegistryAndLogger("http://test:8081", nil)

		Expect(validator).ToNot(BeNil())
		Expect(validator.logger).To(BeNil())
		Expect(validator.schemaRegistryURL).To(Equal("http://test:8081"))
	})
})
