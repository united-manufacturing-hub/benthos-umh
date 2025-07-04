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
			Expect(validator.schemaCache).NotTo(BeNil())
			Expect(validator.httpClient).NotTo(BeNil())
			Expect(validator.schemaRegistryURL).To(BeEmpty())
		})

		It("should initialize correctly with registry", func() {
			validator := NewValidatorWithRegistry("http://localhost:8081")
			Expect(validator.schemaCache).NotTo(BeNil())
			Expect(validator.httpClient).NotTo(BeNil())
			Expect(validator.schemaRegistryURL).To(Equal("http://localhost:8081"))
		})
	})

	Context("when validating with versioned contracts", func() {
		It("should successfully validate sensor data v1", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(BeNil())
			Expect(result.ContractName).To(Equal("_sensor_data"))
			Expect(result.ContractVersion).To(Equal(uint64(1)))
		})

		It("should successfully validate sensor data v2", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v2.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(BeNil())
			Expect(result.ContractName).To(Equal("_sensor_data"))
			Expect(result.ContractVersion).To(Equal(uint64(2)))
		})

		It("should reject invalid virtual path", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.invalid_path")
			Expect(err).To(BeNil())

			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("schema validation failed"))
		})

		It("should reject invalid payload structure", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"value": {"timestamp_ms": "invalid", "value": 25.5}}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("schema validation failed"))
		})
	})

	Context("when handling non-existent schemas", func() {
		It("should bypass validation for non-existent contract", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._non_existent-v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("does not exist in registry"))
			Expect(result.ContractName).To(Equal("_non_existent"))
			Expect(result.ContractVersion).To(Equal(uint64(1)))
		})

		It("should bypass validation for non-existent version", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v999.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("does not exist in registry"))
			Expect(result.ContractName).To(Equal("_sensor_data"))
			Expect(result.ContractVersion).To(Equal(uint64(999)))
		})
	})

	Context("when handling unversioned contracts", func() {
		It("should bypass validation for unversioned contracts", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("unversioned contract"))
			Expect(result.ContractName).To(Equal("_sensor_data"))
		})
	})

	Context("when testing cache behavior", func() {
		It("should cache successful schema fetches", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

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
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._non_existent-v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			// First validation should fetch and cache the negative result
			result1 := validator.Validate(unsTopic, payload)
			Expect(result1.SchemaCheckBypassed).To(BeTrue())
			Expect(result1.BypassReason).To(ContainSubstring("does not exist in registry"))

			// Second validation should use cached negative result
			result2 := validator.Validate(unsTopic, payload)
			Expect(result2.SchemaCheckBypassed).To(BeTrue())
			Expect(result2.BypassReason).To(ContainSubstring("does not exist in registry"))
		})

		It("should cache successful schema fetches forever", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			// Validate to populate cache
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())

			// Check cache entry exists and never expires
			validator.cacheMutex.RLock()
			entry, exists := validator.schemaCache["_sensor_data-v1"]
			validator.cacheMutex.RUnlock()

			Expect(exists).To(BeTrue())
			Expect(entry.SchemaExists).To(BeTrue())
			Expect(entry.Schema).NotTo(BeNil())
			Expect(entry.IsExpired()).To(BeFalse())
			Expect(entry.ExpiresAt.IsZero()).To(BeTrue()) // Zero time means never expires
		})

		It("should cache schema misses for 10 minutes", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._non_existent-v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			// Validate to populate cache with miss
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckBypassed).To(BeTrue())

			// Check cache entry exists with 10 minute expiration
			validator.cacheMutex.RLock()
			entry, exists := validator.schemaCache["_non_existent-v1"]
			validator.cacheMutex.RUnlock()

			Expect(exists).To(BeTrue())
			Expect(entry.SchemaExists).To(BeFalse())
			Expect(entry.Schema).To(BeNil())
			Expect(entry.IsExpired()).To(BeFalse())
			Expect(entry.ExpiresAt.IsZero()).To(BeFalse()) // Should have an expiration time
			Expect(entry.ExpiresAt).To(BeTemporally("~", time.Now().Add(10*time.Minute), time.Second))
		})
	})

	Context("when handling contract parsing", func() {
		It("should parse versioned contracts correctly", func() {
			contractName, version, err := validator.ExtractSchemaVersionFromDataContract("_sensor_data-v1")
			Expect(err).To(BeNil())
			Expect(contractName).To(Equal("_sensor_data"))
			Expect(version).To(Equal(uint64(1)))

			contractName, version, err = validator.ExtractSchemaVersionFromDataContract("_pump_data-v123")
			Expect(err).To(BeNil())
			Expect(contractName).To(Equal("_pump_data"))
			Expect(version).To(Equal(uint64(123)))
		})

		It("should reject invalid contract formats", func() {
			_, _, err := validator.ExtractSchemaVersionFromDataContract("invalid-contract")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid data contract format"))

			_, _, err = validator.ExtractSchemaVersionFromDataContract("_sensor_data-v")
			Expect(err).To(HaveOccurred())

			_, _, err = validator.ExtractSchemaVersionFromDataContract("")
			Expect(err).To(HaveOccurred())
		})
	})

	Context("when manually loading schemas", func() {
		It("should load and cache schemas correctly", func() {
			schema := []byte(`{"type": "object", "properties": {"test": {"type": "string"}}}`)

			err := validator.LoadSchema("_test_contract", 1, schema)
			Expect(err).To(BeNil())

			// Check if schema was cached
			Expect(validator.HasSchema("_test_contract", 1)).To(BeTrue())

			// Verify cache entry
			validator.cacheMutex.RLock()
			entry, exists := validator.schemaCache["_test_contract-v1"]
			validator.cacheMutex.RUnlock()

			Expect(exists).To(BeTrue())
			Expect(entry.SchemaExists).To(BeTrue())
			Expect(entry.Schema).NotTo(BeNil())
		})

		It("should validate contract name requirements", func() {
			schema := []byte(`{"type": "object"}`)

			err := validator.LoadSchema("invalid_contract", 1, schema)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must start with an underscore"))

			err = validator.LoadSchema("", 1, schema)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot be empty"))

			err = validator.LoadSchema("_test_contract", 1, []byte{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("schema cannot be empty"))
		})
	})

	Context("when handling network errors", func() {
		It("should handle schema registry unavailable", func() {
			validator := NewValidatorWithRegistry("http://localhost:9999") // Invalid URL

			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("failed to fetch schema"))
		})
	})

	Context("when closing validator", func() {
		It("should clear cache on close", func() {
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			// Validate to populate cache
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())

			// Verify cache has entries
			validator.cacheMutex.RLock()
			cacheSize := len(validator.schemaCache)
			validator.cacheMutex.RUnlock()
			Expect(cacheSize).To(BeNumerically(">", 0))

			// Close validator
			validator.Close()

			// Verify cache is cleared
			validator.cacheMutex.RLock()
			cacheSize = len(validator.schemaCache)
			validator.cacheMutex.RUnlock()
			Expect(cacheSize).To(Equal(0))
		})
	})
})
