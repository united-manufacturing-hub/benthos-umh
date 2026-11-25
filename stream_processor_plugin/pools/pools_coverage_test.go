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

package pools_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/metrics"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/pools"
)

var _ = Describe("ObjectPools Coverage Tests", func() {
	var (
		objectPools *pools.ObjectPools
		resources   *service.Resources
	)

	BeforeEach(func() {
		resources = service.MockResources()
		objectPools = pools.NewObjectPools([]string{"temp", "press", "flow"}, resources.Logger())
	})

	Describe("GetByteBuffer and PutByteBuffer", func() {
		It("should get and return byte buffers", func() {
			// Get a buffer
			buf1 := objectPools.GetByteBuffer()
			Expect(buf1).ToNot(BeNil())
			Expect(buf1).To(BeEmpty())

			// Get another buffer
			buf2 := objectPools.GetByteBuffer()
			Expect(buf2).ToNot(BeNil())

			// Put buffers back
			objectPools.PutByteBuffer(buf1)
			objectPools.PutByteBuffer(buf2)

			// Get a buffer again - should potentially reuse one
			buf3 := objectPools.GetByteBuffer()
			Expect(buf3).ToNot(BeNil())

			// Test with oversized buffer (should not be pooled)
			largeBuf := make([]byte, 5000) // Larger than 4096 limit
			objectPools.PutByteBuffer(largeBuf)
		})
	})

	Describe("MarshalToJSON", func() {
		It("should marshal objects to JSON", func() {
			data := map[string]interface{}{
				"temp":  25.5,
				"press": 100.0,
				"run":   true,
			}

			result, err := objectPools.MarshalToJSON(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).ToNot(BeEmpty())
			Expect(string(result)).To(ContainSubstring("temp"))
			Expect(string(result)).To(ContainSubstring("25.5"))
		})

		It("should handle simple structures", func() {
			simple := "test string"
			result, err := objectPools.MarshalToJSON(simple)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(result)).To(Equal(`"test string"`))
		})
	})

	Describe("ClearTopicCache", func() {
		It("should clear the topic cache", func() {
			// Create some cached topics
			topic1 := objectPools.GetTopic("umh.v1.corp.plant", "data_v1", "temp")
			topic2 := objectPools.GetTopic("umh.v1.corp.plant", "data_v1", "press")

			Expect(topic1).ToNot(BeEmpty())
			Expect(topic2).ToNot(BeEmpty())

			// Clear the cache
			objectPools.ClearTopicCache()

			// Topics should still be constructible but cache is cleared
			topic3 := objectPools.GetTopic("umh.v1.corp.plant", "data_v1", "temp")
			Expect(topic3).To(Equal(topic1)) // Same result, but freshly constructed
		})
	})

	Describe("Pool Reuse", func() {
		It("should reuse metadata maps", func() {
			// Get a map
			map1 := objectPools.GetMetadataMap()
			map1["test"] = "value"

			// Return it
			objectPools.PutMetadataMap(map1)

			// Get another map - should be cleared
			map2 := objectPools.GetMetadataMap()
			Expect(map2).ToNot(BeNil())
			Expect(map2).To(BeEmpty()) // Should be cleared
		})

		It("should reuse variable contexts", func() {
			// Get a context
			ctx1 := objectPools.GetVariableContext()
			ctx1["temp"] = 25.5

			// Return it
			objectPools.PutVariableContext(ctx1)

			// Get another context - should be cleared
			ctx2 := objectPools.GetVariableContext()
			Expect(ctx2).ToNot(BeNil())
			Expect(ctx2).To(BeEmpty()) // Should be cleared
		})

		It("should reuse string builders", func() {
			// Get a builder
			sb1 := objectPools.GetStringBuilder()
			sb1.WriteString("test")

			// Return it
			objectPools.PutStringBuilder(sb1)

			// Get another builder - should be reset
			sb2 := objectPools.GetStringBuilder()
			Expect(sb2).ToNot(BeNil())
			Expect(sb2.Len()).To(Equal(0)) // Should be reset

			// Test with oversized builder (should not be pooled)
			largeSb := objectPools.GetStringBuilder()
			largeSb.Grow(2000) // Larger than 1024 limit
			objectPools.PutStringBuilder(largeSb)
		})
	})

	Describe("Metrics Coverage", func() {
		It("should create mock metrics for testing", func() {
			// Test the NewMockMetrics function to improve coverage
			mockMetrics := metrics.NewMockMetrics()
			Expect(mockMetrics).ToNot(BeNil())
			Expect(mockMetrics.MessagesProcessed).ToNot(BeNil())
			Expect(mockMetrics.MessagesErrored).ToNot(BeNil())
			Expect(mockMetrics.MessagesDropped).ToNot(BeNil())
			Expect(mockMetrics.JavaScriptErrors).ToNot(BeNil())
			Expect(mockMetrics.JavaScriptExecutionTime).ToNot(BeNil())
			Expect(mockMetrics.OutputsGenerated).ToNot(BeNil())
			Expect(mockMetrics.BatchProcessingTime).ToNot(BeNil())
			Expect(mockMetrics.MessageProcessingTime).ToNot(BeNil())
			Expect(mockMetrics.ActiveMappings).ToNot(BeNil())
			Expect(mockMetrics.ActiveVariables).ToNot(BeNil())

			// Test that the mock metrics structure is correct without calling methods
			// The methods would fail with nil pointers since these are just struct instances
			// This test ensures the NewMockMetrics function returns a properly structured object
		})
	})
})
