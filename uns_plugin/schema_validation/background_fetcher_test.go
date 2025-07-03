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

var _ = Describe("BackgroundFetcher", func() {
	var (
		fetcher      *BackgroundFetcher
		validator    *Validator
		mockRegistry *MockSchemaRegistry
	)

	BeforeEach(func() {
		validator = NewValidator()
		mockRegistry = NewMockSchemaRegistry()
		mockRegistry.SetupTestSchemas()
		fetcher = NewBackgroundFetcher(mockRegistry.URL(), validator)
	})

	AfterEach(func() {
		if fetcher != nil {
			fetcher.Stop()
		}
		if mockRegistry != nil {
			mockRegistry.Close()
		}
	})

	Context("when creating a new background fetcher", func() {
		It("should initialize correctly", func() {
			Expect(fetcher.schemaRegistryURL).To(Equal(mockRegistry.URL()))
			Expect(fetcher.validator).To(Equal(validator))
			Expect(fetcher.fetchQueue).NotTo(BeNil())
			Expect(fetcher.httpClient).NotTo(BeNil())
		})
	})

	Context("when queueing schemas", func() {
		It("should add schema to queue", func() {
			fetcher.QueueSchema("_test_contract", 1)

			fetcher.fetchQueueMutex.RLock()
			queueSize := len(fetcher.fetchQueue)
			hasKey := fetcher.fetchQueue["_test_contract-v1"]
			fetcher.fetchQueueMutex.RUnlock()

			Expect(queueSize).To(Equal(1))
			Expect(hasKey).To(BeTrue())
		})

		It("should respect maximum queue size", func() {
			// Fill the queue beyond the maximum
			for i := 0; i < maxFetchQueueSize+5; i++ {
				fetcher.QueueSchema("_test_contract", uint64(i))
			}

			fetcher.fetchQueueMutex.RLock()
			queueSize := len(fetcher.fetchQueue)
			fetcher.fetchQueueMutex.RUnlock()

			Expect(queueSize).To(Equal(maxFetchQueueSize))
		})
	})

	Context("when parsing queue keys", func() {
		It("should parse valid queue keys correctly", func() {
			contractName, version, err := fetcher.parseQueueKey("_test_contract-v123")
			Expect(err).To(BeNil())
			Expect(contractName).To(Equal("_test_contract"))
			Expect(version).To(Equal(uint64(123)))
		})

		It("should handle invalid queue keys", func() {
			_, _, err := fetcher.parseQueueKey("invalid-key")
			Expect(err).To(HaveOccurred())
		})
	})

	Context("when starting and stopping", func() {
		It("should start and stop without errors", func() {
			fetcher.Start()
			time.Sleep(100 * time.Millisecond) // Give it time to start

			fetcher.Stop()

			fetcher.stopMutex.RLock()
			stopped := fetcher.stopped
			fetcher.stopMutex.RUnlock()

			Expect(stopped).To(BeTrue())
		})
	})

	Context("when fetching schemas from registry", func() {
		It("should fetch existing schemas successfully", func() {
			// Queue a schema that exists in the mock registry
			fetcher.QueueSchema("_sensor_data", 1)

			// Process the queue manually
			fetcher.processFetchQueue()

			// Verify the schema was loaded into the validator
			Expect(validator.HasSchema("_sensor_data", 1)).To(BeTrue())
		})

		It("should handle non-existent subjects", func() {
			// Queue a schema for a subject that doesn't exist
			fetcher.QueueSchema("_non_existent", 1)

			// Process the queue manually
			fetcher.processFetchQueue()

			// Verify the schema was not loaded
			Expect(validator.HasSchema("_non_existent", 1)).To(BeFalse())

			// Verify the queue is empty (item was removed)
			fetcher.fetchQueueMutex.RLock()
			queueSize := len(fetcher.fetchQueue)
			fetcher.fetchQueueMutex.RUnlock()
			Expect(queueSize).To(Equal(0))
		})

		It("should handle non-existent versions", func() {
			// Queue a schema for a version that doesn't exist
			fetcher.QueueSchema("_sensor_data", 999)

			// Process the queue manually
			fetcher.processFetchQueue()

			// Verify the schema was not loaded
			Expect(validator.HasSchema("_sensor_data", 999)).To(BeFalse())

			// Verify the queue is empty (item was removed)
			fetcher.fetchQueueMutex.RLock()
			queueSize := len(fetcher.fetchQueue)
			fetcher.fetchQueueMutex.RUnlock()
			Expect(queueSize).To(Equal(0))
		})

		It("should handle network errors gracefully", func() {
			// Queue a valid schema first
			fetcher.QueueSchema("_sensor_data", 1)

			// Simulate network error
			mockRegistry.SimulateNetworkError(true)

			// Process the queue manually
			fetcher.processFetchQueue()

			// Verify the schema was not loaded due to network error
			Expect(validator.HasSchema("_sensor_data", 1)).To(BeFalse())

			// Verify the item was re-queued for retry
			fetcher.fetchQueueMutex.RLock()
			queueSize := len(fetcher.fetchQueue)
			hasKey := fetcher.fetchQueue["_sensor_data-v1"]
			fetcher.fetchQueueMutex.RUnlock()
			Expect(queueSize).To(Equal(1))
			Expect(hasKey).To(BeTrue())

			// Restore network and try again
			mockRegistry.SimulateNetworkError(false)
			fetcher.processFetchQueue()

			// Now it should succeed
			Expect(validator.HasSchema("_sensor_data", 1)).To(BeTrue())
		})

		It("should fetch multiple schemas concurrently", func() {
			// Queue multiple schemas
			fetcher.QueueSchema("_sensor_data", 1)
			fetcher.QueueSchema("_sensor_data", 2)
			fetcher.QueueSchema("_pump_data", 1)
			fetcher.QueueSchema("_motor_controller", 3)

			// Process the queue manually
			fetcher.processFetchQueue()

			// Verify all schemas were loaded
			Expect(validator.HasSchema("_sensor_data", 1)).To(BeTrue())
			Expect(validator.HasSchema("_sensor_data", 2)).To(BeTrue())
			Expect(validator.HasSchema("_pump_data", 1)).To(BeTrue())
			Expect(validator.HasSchema("_motor_controller", 3)).To(BeTrue())
		})
	})

	Context("when running in background", func() {
		It("should automatically process queued items", func() {
			// Start the background fetcher
			fetcher.Start()

			// Queue a schema
			fetcher.QueueSchema("_sensor_data", 1)

			// Wait for background processing (more than fetchInterval)
			time.Sleep(6 * time.Second)

			// Verify the schema was automatically loaded
			Expect(validator.HasSchema("_sensor_data", 1)).To(BeTrue())

			// Stop the fetcher
			fetcher.Stop()
		})
	})
})

var _ = Describe("Validator with Background Fetcher", func() {
	var validator *Validator

	BeforeEach(func() {
		validator = NewValidatorWithRegistry("http://localhost:8081")
	})

	AfterEach(func() {
		if validator != nil {
			validator.Close()
		}
	})

	Context("when created with registry URL", func() {
		It("should have a background fetcher", func() {
			Expect(validator.backgroundFetcher).NotTo(BeNil())
		})
	})

	Context("when created without registry URL", func() {
		It("should not have a background fetcher", func() {
			validatorWithoutRegistry := NewValidatorWithRegistry("")
			Expect(validatorWithoutRegistry.backgroundFetcher).To(BeNil())
		})
	})
})

var _ = Describe("Integration: Mock Registry + Validator", func() {
	var (
		validator    *Validator
		mockRegistry *MockSchemaRegistry
	)

	BeforeEach(func() {
		// Set up mock registry with test schemas
		mockRegistry = NewMockSchemaRegistry()
		mockRegistry.SetupTestSchemas()

		// Create validator with mock registry URL
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

	Context("when validating messages with existing schemas", func() {
		It("should validate sensor data v1 successfully", func() {
			// Load the schema first (simulating background fetch)
			schemaVersion := mockRegistry.GetSchema("_sensor_data", 1)
			Expect(schemaVersion).NotTo(BeNil())
			err := validator.LoadSchema("_sensor_data", 1, []byte(schemaVersion.Schema))
			Expect(err).To(BeNil())

			// Create a valid UNS topic
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())

			// Create valid payload
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			// Validate
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(BeNil())
			Expect(result.ContractName).To(Equal("_sensor_data"))
			Expect(result.ContractVersion).To(Equal(uint64(1)))
		})

		It("should reject sensor data v1 with invalid virtual path", func() {
			// Load the schema first
			schemaVersion := mockRegistry.GetSchema("_sensor_data", 1)
			Expect(schemaVersion).NotTo(BeNil())
			err := validator.LoadSchema("_sensor_data", 1, []byte(schemaVersion.Schema))
			Expect(err).To(BeNil())

			// Create UNS topic with invalid virtual path (humidity not allowed in v1)
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.humidity")
			Expect(err).To(BeNil())

			// Create valid payload
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 65.0}}`)

			// Validate
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
			Expect(result.Error.Error()).To(ContainSubstring("schema validation failed"))
		})

		It("should validate sensor data v2 with expanded virtual paths", func() {
			// Load the schema first
			schemaVersion := mockRegistry.GetSchema("_sensor_data", 2)
			Expect(schemaVersion).NotTo(BeNil())
			err := validator.LoadSchema("_sensor_data", 2, []byte(schemaVersion.Schema))
			Expect(err).To(BeNil())

			// Test temperature (allowed in both v1 and v2)
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v2.temperature")
			Expect(err).To(BeNil())
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.Error).To(BeNil())

			// Test humidity (allowed in v2 but not v1)
			unsTopic, err = topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v2.humidity")
			Expect(err).To(BeNil())
			payload = []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 65.0}}`)
			result = validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.Error).To(BeNil())

			// Test pressure (allowed in v2 but not v1)
			unsTopic, err = topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v2.pressure")
			Expect(err).To(BeNil())
			payload = []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 1013.25}}`)
			result = validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.Error).To(BeNil())
		})

		It("should validate pump data with complex virtual paths", func() {
			// Load the schema first
			schemaVersion := mockRegistry.GetSchema("_pump_data", 1)
			Expect(schemaVersion).NotTo(BeNil())
			err := validator.LoadSchema("_pump_data", 1, []byte(schemaVersion.Schema))
			Expect(err).To(BeNil())

			// Test vibration.x-axis
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._pump_data-v1.vibration.x-axis")
			Expect(err).To(BeNil())
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 0.5}}`)
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.Error).To(BeNil())

			// Test vibration.y-axis
			unsTopic, err = topic.NewUnsTopic("umh.v1.enterprise.site.area._pump_data-v1.vibration.y-axis")
			Expect(err).To(BeNil())
			payload = []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 0.3}}`)
			result = validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.Error).To(BeNil())

			// Test count
			unsTopic, err = topic.NewUnsTopic("umh.v1.enterprise.site.area._pump_data-v1.count")
			Expect(err).To(BeNil())
			payload = []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 1542}}`)
			result = validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.Error).To(BeNil())
		})

		It("should validate string data types correctly", func() {
			// Load the schema first
			schemaVersion := mockRegistry.GetSchema("_string_data", 1)
			Expect(schemaVersion).NotTo(BeNil())
			err := validator.LoadSchema("_string_data", 1, []byte(schemaVersion.Schema))
			Expect(err).To(BeNil())

			// Test string value (serialNumber)
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._string_data-v1.serialNumber")
			Expect(err).To(BeNil())
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": "SN123456789"}}`)
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.Error).To(BeNil())

			// Test string value (status)
			unsTopic, err = topic.NewUnsTopic("umh.v1.enterprise.site.area._string_data-v1.status")
			Expect(err).To(BeNil())
			payload = []byte(`{"value": {"timestamp_ms": 1719859200000, "value": "RUNNING"}}`)
			result = validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.Error).To(BeNil())
		})

		It("should reject invalid payload formats", func() {
			// Load the schema first
			schemaVersion := mockRegistry.GetSchema("_sensor_data", 1)
			Expect(schemaVersion).NotTo(BeNil())
			err := validator.LoadSchema("_sensor_data", 1, []byte(schemaVersion.Schema))
			Expect(err).To(BeNil())

			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())

			// Missing timestamp_ms
			payload := []byte(`{"value": {"value": 25.5}}`)
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())

			// Missing value
			payload = []byte(`{"value": {"timestamp_ms": 1719859200000}}`)
			result = validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())

			// Wrong value type
			payload = []byte(`{"value": {"timestamp_ms": 1719859200000, "value": "not_a_number"}}`)
			result = validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
		})

		It("should reject wrong data types for string fields", func() {
			// Load the schema first
			schemaVersion := mockRegistry.GetSchema("_string_data", 1)
			Expect(schemaVersion).NotTo(BeNil())
			err := validator.LoadSchema("_string_data", 1, []byte(schemaVersion.Schema))
			Expect(err).To(BeNil())

			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._string_data-v1.serialNumber")
			Expect(err).To(BeNil())

			// Provide number instead of string
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 12345}}`)
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())
		})
	})

	Context("when testing background fetching integration", func() {
		It("should queue and fetch schemas automatically", func() {
			// Create topic that will trigger background fetch
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())

			// Create valid payload
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			// First validation should bypass (schema not loaded yet)
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("schema for contract '_sensor_data' version 1 not found"))

			// Manual trigger of background fetch (in real scenario this would happen automatically)
			if validator.backgroundFetcher != nil {
				validator.backgroundFetcher.processFetchQueue()
			}

			// Now validation should pass
			result = validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.SchemaCheckBypassed).To(BeFalse())
			Expect(result.Error).To(BeNil())
		})
	})

	Context("when handling unversioned contracts", func() {
		It("should bypass validation for unversioned contracts", func() {
			// Create topic with unversioned contract
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._unversioned_contract.temperature")
			Expect(err).To(BeNil())

			// Create valid payload
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			// Should bypass validation
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("unversioned contract '_unversioned_contract' - bypassing validation (no latest fetching)"))
			Expect(result.Error).To(BeNil())
		})
	})

	Context("when handling missing schemas", func() {
		It("should bypass validation for non-existent schemas", func() {
			// Create topic with contract that doesn't exist in mock registry
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._non_existent_contract-v1.temperature")
			Expect(err).To(BeNil())

			// Create valid payload
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)

			// Should bypass validation
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.SchemaCheckBypassed).To(BeTrue())
			Expect(result.BypassReason).To(ContainSubstring("schema for contract '_non_existent_contract' version 1 not found"))
			Expect(result.Error).To(BeNil())
		})
	})

	Context("when testing schema versioning", func() {
		It("should handle different schema versions independently", func() {
			// Load both v1 and v2 schemas
			schemaV1 := mockRegistry.GetSchema("_sensor_data", 1)
			Expect(schemaV1).NotTo(BeNil())
			err := validator.LoadSchema("_sensor_data", 1, []byte(schemaV1.Schema))
			Expect(err).To(BeNil())
			schemaV2 := mockRegistry.GetSchema("_sensor_data", 2)
			Expect(schemaV2).NotTo(BeNil())
			err = validator.LoadSchema("_sensor_data", 2, []byte(schemaV2.Schema))
			Expect(err).To(BeNil())

			// Test v1 - should only allow temperature
			unsTopic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.temperature")
			Expect(err).To(BeNil())
			payload := []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 25.5}}`)
			result := validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.ContractVersion).To(Equal(uint64(1)))

			// Test v1 with humidity should fail
			unsTopic, err = topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.humidity")
			Expect(err).To(BeNil())
			result = validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeFalse())
			Expect(result.Error).To(HaveOccurred())

			// Test v2 - should allow temperature, humidity, and pressure
			unsTopic, err = topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v2.humidity")
			Expect(err).To(BeNil())
			result = validator.Validate(unsTopic, payload)
			Expect(result.SchemaCheckPassed).To(BeTrue())
			Expect(result.ContractVersion).To(Equal(uint64(2)))
		})
	})
})
