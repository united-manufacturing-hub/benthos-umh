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
	"strings"
	"sync"
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
				"_test_contract_v1_timeseries-number": {},
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

	Context("when testing constructors", func() {
		It("should create validator with registry URL", func() {
			validator := NewValidatorWithRegistry("http://test:8081")
			defer validator.Close()

			Expect(validator.schemaRegistryURL).To(Equal("http://test:8081"))
			Expect(validator.logger).To(BeNil())
		})

		It("should create validator with registry and logger", func() {
			validator := NewValidatorWithRegistryAndLogger("http://test:8081", nil)
			defer validator.Close()

			Expect(validator.logger).To(BeNil())
			Expect(validator.schemaRegistryURL).To(Equal("http://test:8081"))
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

	Context("when testing cache eviction", func() {
		It("should evict oldest entries when cache exceeds max size", func() {
			// Create validator with small cache for testing
			validator := NewValidator()
			defer validator.Close()

			// Load many schemas to exceed cache size
			for i := 0; i < 1200; i++ { // Exceeds maxCacheSize of 1000
				schemas := map[string][]byte{
					fmt.Sprintf("_test_contract_%d_v1-timeseries-number", i): []byte(`{"type": "object"}`),
				}
				err := validator.LoadSchemas(fmt.Sprintf("_test_contract_%d", i), 1, schemas)
				Expect(err).To(BeNil())
			}

			// Verify cache size is limited
			validator.cacheMutex.RLock()
			cacheSize := len(validator.contractCache)
			validator.cacheMutex.RUnlock()
			Expect(cacheSize).To(BeNumerically("<=", 1000))
		})
	})

	Context("when handling edge cases", func() {
		It("should handle nil UNS topic", func() {
			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)
			result := validator.Validate(nil, payload)

			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("UNS topic cannot be nil"))
		})

		It("should handle topic with nil info", func() {
			// Create a topic that will have nil info (invalid format)
			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			// This should create a topic with nil info
			topic, err := topic.NewUnsTopic("invalid")
			if err != nil {
				// If we can't create invalid topic, skip this test
				Skip("Cannot create invalid topic for testing")
			}

			result := validator.Validate(topic, payload)

			// Should handle gracefully
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
		})

		It("should handle fetch errors from registry", func() {
			validator := NewValidatorWithRegistry("http://non-existent-registry:9999")
			defer validator.Close()

			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("failed to fetch schema"))
		})

		It("should handle registry returning non-200 status", func() {
			// Create a mock registry that returns errors
			mockRegistry := NewMockSchemaRegistry()
			mockRegistry.SimulateNetworkError(true)
			defer mockRegistry.Close()

			validator := NewValidatorWithRegistry(mockRegistry.URL())
			defer validator.Close()

			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("failed to fetch schema"))
		})

		It("should handle empty schema registry URL", func() {
			validator := NewValidator() // No registry URL
			defer validator.Close()

			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("failed to fetch schema"))
		})

		It("should handle invalid JSON in schema response", func() {
			// This is harder to test without a custom mock, but we can test the error handling
			validator := NewValidatorWithRegistry("http://localhost:0") // Invalid URL
			defer validator.Close()

			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
		})

		It("should handle validation with nil schema", func() {
			// Load a schema manually and then corrupt it
			schemas := map[string][]byte{
				"_test_contract_v1-timeseries-number": []byte(`{"type": "object"}`),
			}
			err := validator.LoadSchemas("_test_contract", 1, schemas)
			Expect(err).To(BeNil())

			// Now manually corrupt the cache to have nil schema
			validator.cacheMutex.Lock()
			entry := validator.contractCache["_test_contract-v1"]
			entry.Schemas["_test_contract_v1-timeseries-number"] = nil
			validator.cacheMutex.Unlock()

			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._test_contract_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
		})

		It("should handle schema compilation errors", func() {
			schemas := map[string][]byte{
				"_test_contract_v1-timeseries-number": []byte(`invalid json`), // Invalid JSON
			}
			err := validator.LoadSchemas("_test_contract", 1, schemas)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to add schema version"))
		})

		It("should handle contract version parsing edge cases", func() {
			// Test various invalid contract formats
			_, _, err := validator.ExtractSchemaVersionFromDataContract("_sensor_data_v0") // Version 0
			Expect(err).To(BeNil())                                                        // Version 0 should be valid

			_, _, err = validator.ExtractSchemaVersionFromDataContract("_sensor_data_v999999999999999999999")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid version number"))

			_, _, err = validator.ExtractSchemaVersionFromDataContract("_sensor_data_vabc")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid data contract format"))
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

	Context("when testing enhanced error messages", func() {
		It("should include valid virtual_path values in error message with multiple schema types", func() {
			// Create schemas with both timeseries-number and timeseries-string types
			// Each with different virtual_path enum values to test comprehensive enhancement
			schemas := map[string][]byte{
				"_sensor_data_v1-timeseries-number": []byte(`{
					"type": "object",
					"properties": {
						"virtual_path": {
							"type": "string",
							"enum": ["temperature", "pressure", "flow_rate", "rpm"]
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
				"_sensor_data_v1-timeseries-string": []byte(`{
					"type": "object",
					"properties": {
						"virtual_path": {
							"type": "string",
							"enum": ["device_id", "status", "location", "operator"]
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

			err := validator.LoadSchemas("_sensor_data", 1, schemas)
			Expect(err).To(BeNil())

			// Test with invalid virtual_path that doesn't match any schema
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.invalid_sensor_path")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())

			// Verify that the error message contains enhanced virtual_path information
			errorMsg := result.Error.Error()
			Expect(errorMsg).To(ContainSubstring("virtual_path"))
			// Can be either "does not match" (singular) or "do not match" (plural)
			matchesPattern := strings.Contains(errorMsg, "does not match") || strings.Contains(errorMsg, "do not match")
			Expect(matchesPattern).To(BeTrue(), "Expected error message to contain 'does not match' or 'do not match' but got: %s", errorMsg)

			// Should contain ALL valid virtual_path values from ALL schemas, sorted alphabetically with schema types
			// Expected format: "Valid virtual_paths are: [device_id (timeseries-string), flow_rate (timeseries-number), location (timeseries-string), operator (timeseries-string), pressure (timeseries-number), rpm (timeseries-number), status (timeseries-string), temperature (timeseries-number)]. Your virtual_path is: invalid_sensor_path"
			Expect(errorMsg).To(ContainSubstring("Valid virtual_paths are: ["))
			Expect(errorMsg).To(ContainSubstring("device_id (timeseries-string)"))
			Expect(errorMsg).To(ContainSubstring("flow_rate (timeseries-number)"))
			Expect(errorMsg).To(ContainSubstring("location (timeseries-string)"))
			Expect(errorMsg).To(ContainSubstring("operator (timeseries-string)"))
			Expect(errorMsg).To(ContainSubstring("pressure (timeseries-number)"))
			Expect(errorMsg).To(ContainSubstring("rpm (timeseries-number)"))
			Expect(errorMsg).To(ContainSubstring("status (timeseries-string)"))
			Expect(errorMsg).To(ContainSubstring("temperature (timeseries-number)"))
			Expect(errorMsg).To(ContainSubstring("Your virtual_path is: invalid_sensor_path"))
		})

		It("should enhance error message with exact format for timeseries-number schema", func() {
			// Test with only timeseries-number schema to verify exact message format
			schemas := map[string][]byte{
				"_test_contract_v1-timeseries-number": []byte(`{
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
				}`),
			}

			err := validator.LoadSchemas("_test_contract", 1, schemas)
			Expect(err).To(BeNil())

			// Test with invalid virtual_path
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._test_contract_v1.invalid_path")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())

			// Verify exact error message format
			errorMsg := result.Error.Error()
			Expect(errorMsg).To(ContainSubstring("schema validation failed for contract '_test_contract' version 1"))
			Expect(errorMsg).To(ContainSubstring("schema validation failed for subject '_test_contract_v1-timeseries-number'"))
			Expect(errorMsg).To(ContainSubstring("virtual_path"))
			Expect(errorMsg).To(ContainSubstring("does not match"))
			// Should contain all valid virtual_path values sorted alphabetically with schema types
			Expect(errorMsg).To(ContainSubstring("Valid virtual_paths are: [humidity (timeseries-number), pressure (timeseries-number), temperature (timeseries-number)]"))
			Expect(errorMsg).To(ContainSubstring("Your virtual_path is: invalid_path"))
		})

		It("should enhance error message with exact format for timeseries-string schema", func() {
			// Test with only timeseries-string schema to verify exact message format
			schemas := map[string][]byte{
				"_test_contract_v1-timeseries-string": []byte(`{
					"type": "object",
					"properties": {
						"virtual_path": {
							"type": "string",
							"enum": ["device_id", "status", "location"]
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

			err := validator.LoadSchemas("_test_contract", 1, schemas)
			Expect(err).To(BeNil())

			// Test with invalid virtual_path
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._test_contract_v1.invalid_path")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": "test_value"}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())

			// Verify exact error message format
			errorMsg := result.Error.Error()
			Expect(errorMsg).To(ContainSubstring("schema validation failed for contract '_test_contract' version 1"))
			Expect(errorMsg).To(ContainSubstring("schema validation failed for subject '_test_contract_v1-timeseries-string'"))
			Expect(errorMsg).To(ContainSubstring("virtual_path"))
			Expect(errorMsg).To(ContainSubstring("does not match"))
			// Should contain all valid virtual_path values sorted alphabetically with schema types
			Expect(errorMsg).To(ContainSubstring("Valid virtual_paths are: [device_id (timeseries-string), location (timeseries-string), status (timeseries-string)]"))
			Expect(errorMsg).To(ContainSubstring("Your virtual_path is: invalid_path"))
		})

		It("should not enhance error messages for non-virtual_path validation errors", func() {
			// Create a test schema
			schemas := map[string][]byte{
				"_test_contract_v1-timeseries-number": []byte(`{
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
				}`),
			}

			err := validator.LoadSchemas("_test_contract", 1, schemas)
			Expect(err).To(BeNil())

			// Test with valid virtual_path but invalid payload structure (wrong data type)
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._test_contract_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": "invalid_type", "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())

			// Verify that the error message does NOT contain the valid virtual_path values
			// (because this is not a virtual_path validation error)
			errorMsg := result.Error.Error()
			Expect(errorMsg).ToNot(ContainSubstring("Valid virtual_paths are"))
		})

		It("should use singular grammar when only one virtual_path is available", func() {
			// Create a test schema with only one virtual_path value
			schemas := map[string][]byte{
				"_test_contract_v1-timeseries-number": []byte(`{
					"type": "object",
					"properties": {
						"virtual_path": {
							"type": "string",
							"enum": ["temperature"]
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
			}

			err := validator.LoadSchemas("_test_contract", 1, schemas)
			Expect(err).To(BeNil())

			// Test with invalid virtual_path
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._test_contract_v1.invalid_path")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())

			// Verify singular grammar is used
			errorMsg := result.Error.Error()
			Expect(errorMsg).To(ContainSubstring("Valid virtual_paths is: [temperature (timeseries-number)]"))
			Expect(errorMsg).To(ContainSubstring("Your virtual_path is: invalid_path"))
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

	Context("when testing schema object methods", func() {
		It("should test schema HasVersion method", func() {
			schema := NewSchema("test-schema")

			// Initially should not have any versions
			Expect(schema.HasVersion(1)).To(BeFalse())

			// Add a version
			err := schema.AddVersion(1, []byte(`{"type": "object"}`))
			Expect(err).To(BeNil())

			// Now should have version 1
			Expect(schema.HasVersion(1)).To(BeTrue())
			Expect(schema.HasVersion(2)).To(BeFalse())
		})

		It("should test schema GetVersion method", func() {
			schema := NewSchema("test-schema")

			// Initially should return nil
			Expect(schema.GetVersion(1)).To(BeNil())

			// Add a version
			err := schema.AddVersion(1, []byte(`{"type": "object"}`))
			Expect(err).To(BeNil())

			// Now should return the schema
			jsonSchema := schema.GetVersion(1)
			Expect(jsonSchema).NotTo(BeNil())
		})
	})

	Context("when testing debug logging", func() {
		It("should handle debug logging with nil logger", func() {
			validator := NewValidator()
			defer validator.Close()

			// This should not panic
			validator.debugf("Test message: %s", "test")
		})

		It("should handle debug logging with logger", func() {
			// Create a logger (in practice this would be a real logger)
			// We can't easily create a real logger in tests, so we'll test with nil
			validator := NewValidatorWithRegistryAndLogger("http://localhost:8081", nil)
			defer validator.Close()

			// This should not panic
			validator.debugf("Test message: %s", "test")
		})
	})

	Describe("Closed Validator", func() {
		var validator *Validator

		BeforeEach(func() {
			validator = NewValidatorWithRegistry("http://test-registry")
		})

		It("should prevent HTTP operations after Close", func() {
			// Close the validator
			validator.Close()

			// Verify that operations requiring HTTP client fail gracefully
			subjects, err := validator.fetchAllSubjects()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("validator is closed"))
			Expect(subjects).To(BeNil())

			// Verify that fetchLatestSchemaFromRegistry also fails gracefully
			schema, exists, err := validator.fetchLatestSchemaFromRegistry("test-subject")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("validator is closed"))
			Expect(exists).To(BeFalse())
			Expect(schema).To(BeNil())
		})

		It("should allow multiple Close calls without panic", func() {
			// Multiple Close calls should be safe
			validator.Close()
			validator.Close()
			validator.Close()

			// Verify validator is still closed
			Expect(validator.isClosed()).To(BeTrue())
		})

		It("should check isClosed method", func() {
			// Initially not closed
			Expect(validator.isClosed()).To(BeFalse())

			// After Close, should be closed
			validator.Close()
			Expect(validator.isClosed()).To(BeTrue())
		})

		It("should handle Close with concurrent operations", func() {
			// Start multiple goroutines that try to close the validator
			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					validator.Close()
				}()
			}

			// Wait for all goroutines to complete
			wg.Wait()

			// Validator should be closed exactly once
			Expect(validator.isClosed()).To(BeTrue())
		})
	})
})
