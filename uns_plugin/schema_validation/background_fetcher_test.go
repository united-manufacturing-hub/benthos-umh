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
