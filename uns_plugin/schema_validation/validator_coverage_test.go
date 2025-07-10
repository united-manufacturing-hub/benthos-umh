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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
)

// Additional coverage tests to improve test coverage
var _ = Describe("Validator Coverage Tests", func() {
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

	Context("cache eviction tests", func() {
		It("should evict oldest entries when cache exceeds max size", func() {
			// Create validator with small cache for testing
			testValidator := NewValidator()
			defer testValidator.Close()

			// Load many schemas to exceed cache size
			for i := 0; i < 1200; i++ { // Exceeds maxCacheSize of 1000
				schemas := map[string][]byte{
					fmt.Sprintf("_test_contract_%d_v1-timeseries-number", i): []byte(`{"type": "object"}`),
				}
				err := testValidator.LoadSchemas(fmt.Sprintf("_test_contract_%d", i), 1, schemas)
				Expect(err).To(BeNil())
			}

			// Verify cache size is limited
			testValidator.cacheMutex.RLock()
			cacheSize := len(testValidator.contractCache)
			testValidator.cacheMutex.RUnlock()
			Expect(cacheSize).To(BeNumerically("<=", 1000))
		})
	})

	Context("edge case tests", func() {
		It("should handle nil UNS topic", func() {
			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)
			result := validator.Validate(nil, payload)

			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("UNS topic cannot be nil"))
		})

		It("should handle fetch errors from registry", func() {
			testValidator := NewValidatorWithRegistry("http://non-existent-registry:9999")
			defer testValidator.Close()

			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := testValidator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("failed to fetch schema"))
		})

		It("should handle registry returning errors", func() {
			// Create a mock registry that returns errors
			errorRegistry := NewMockSchemaRegistry()
			errorRegistry.SimulateNetworkError(true)
			defer errorRegistry.Close()

			testValidator := NewValidatorWithRegistry(errorRegistry.URL())
			defer testValidator.Close()

			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := testValidator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("failed to fetch schema"))
		})

		It("should handle empty schema registry URL", func() {
			testValidator := NewValidator() // No registry URL
			defer testValidator.Close()

			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data_v1.temperature")
			Expect(err).To(BeNil())

			payload := []byte(`{"timestamp_ms": 1719859200000, "value": 25.5}`)

			result := testValidator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("failed to fetch schema"))
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

	Context("schema object method tests", func() {
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

	Context("mock registry method tests", func() {
		It("should test mock registry GetRegisteredSubjects", func() {
			subjects := mockRegistry.GetRegisteredSubjects()
			Expect(len(subjects)).To(BeNumerically(">", 0))
			Expect(subjects).To(ContainElement("_sensor_data_v1-timeseries-number"))
		})

		It("should test mock registry GetVersionsForSubject", func() {
			versions := mockRegistry.GetVersionsForSubject("_sensor_data_v1-timeseries-number")
			Expect(len(versions)).To(Equal(1))
			Expect(versions[0]).To(Equal(1))
		})

		It("should test mock registry GetSchema", func() {
			schema := mockRegistry.GetSchema("_sensor_data_v1-timeseries-number", 1)
			Expect(schema).NotTo(BeNil())
			Expect(schema.Subject).To(Equal("_sensor_data_v1-timeseries-number"))
			Expect(schema.Version).To(Equal(1))
		})

		It("should test mock registry RemoveSchema", func() {
			// Add a test schema
			mockRegistry.AddSchema("_test_remove_v1-timeseries-number", 1, `{"type": "object"}`)

			// Verify it exists
			schema := mockRegistry.GetSchema("_test_remove_v1-timeseries-number", 1)
			Expect(schema).NotTo(BeNil())

			// Remove it
			mockRegistry.RemoveSchema("_test_remove_v1-timeseries-number", 1)

			// Verify it's gone
			schema = mockRegistry.GetSchema("_test_remove_v1-timeseries-number", 1)
			Expect(schema).To(BeNil())
		})
	})

	Context("debug logging tests", func() {
		It("should handle debug logging with nil logger", func() {
			testValidator := NewValidator()
			defer testValidator.Close()

			// This should not panic
			testValidator.debugf("Test message: %s", "test")
		})

		It("should handle debug logging with logger", func() {
			// Test with nil logger since we can't easily create a real logger in tests
			testValidator := NewValidatorWithRegistryAndLogger("http://localhost:8081", nil)
			defer testValidator.Close()

			// This should not panic
			testValidator.debugf("Test message: %s", "test")
		})
	})
})
