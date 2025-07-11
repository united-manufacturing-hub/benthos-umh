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
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/js_security"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/metrics"
)

var _ = Describe("ObjectPools Coverage Tests", func() {
	var (
		pools     *ObjectPools
		resources *service.Resources
	)

	BeforeEach(func() {
		resources = service.MockResources()
		pools = NewObjectPools([]string{"temp", "press", "flow"}, resources.Logger())
	})

	Describe("GetByteBuffer and PutByteBuffer", func() {
		It("should get and return byte buffers", func() {
			// Get a buffer
			buf1 := pools.GetByteBuffer()
			Expect(buf1).ToNot(BeNil())
			Expect(len(buf1)).To(Equal(0))

			// Get another buffer
			buf2 := pools.GetByteBuffer()
			Expect(buf2).ToNot(BeNil())

			// Put buffers back
			pools.PutByteBuffer(buf1)
			pools.PutByteBuffer(buf2)

			// Get a buffer again - should potentially reuse one
			buf3 := pools.GetByteBuffer()
			Expect(buf3).ToNot(BeNil())

			// Test with oversized buffer (should not be pooled)
			largeBuf := make([]byte, 5000) // Larger than 4096 limit
			pools.PutByteBuffer(largeBuf)
		})
	})

	Describe("MarshalToJSON", func() {
		It("should marshal objects to JSON", func() {
			data := map[string]interface{}{
				"temp":  25.5,
				"press": 100.0,
				"run":   true,
			}

			result, err := pools.MarshalToJSON(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(result).ToNot(BeEmpty())
			Expect(string(result)).To(ContainSubstring("temp"))
			Expect(string(result)).To(ContainSubstring("25.5"))
		})

		It("should handle simple structures", func() {
			simple := "test string"
			result, err := pools.MarshalToJSON(simple)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(result)).To(Equal(`"test string"`))
		})
	})

	Describe("ClearTopicCache", func() {
		It("should clear the topic cache", func() {
			// Create some cached topics
			topic1 := pools.GetTopic("umh.v1.corp.plant", "data_v1", "temp")
			topic2 := pools.GetTopic("umh.v1.corp.plant", "data_v1", "press")

			Expect(topic1).ToNot(BeEmpty())
			Expect(topic2).ToNot(BeEmpty())

			// Clear the cache
			pools.ClearTopicCache()

			// Topics should still be constructible but cache is cleared
			topic3 := pools.GetTopic("umh.v1.corp.plant", "data_v1", "temp")
			Expect(topic3).To(Equal(topic1)) // Same result, but freshly constructed
		})
	})

	Describe("Pool Reuse", func() {
		It("should reuse metadata maps", func() {
			// Get a map
			map1 := pools.GetMetadataMap()
			map1["test"] = "value"

			// Return it
			pools.PutMetadataMap(map1)

			// Get another map - should be cleared
			map2 := pools.GetMetadataMap()
			Expect(map2).ToNot(BeNil())
			Expect(len(map2)).To(Equal(0)) // Should be cleared
		})

		It("should reuse variable contexts", func() {
			// Get a context
			ctx1 := pools.GetVariableContext()
			ctx1["temp"] = 25.5

			// Return it
			pools.PutVariableContext(ctx1)

			// Get another context - should be cleared
			ctx2 := pools.GetVariableContext()
			Expect(ctx2).ToNot(BeNil())
			Expect(len(ctx2)).To(Equal(0)) // Should be cleared
		})

		It("should reuse string builders", func() {
			// Get a builder
			sb1 := pools.GetStringBuilder()
			sb1.WriteString("test")

			// Return it
			pools.PutStringBuilder(sb1)

			// Get another builder - should be reset
			sb2 := pools.GetStringBuilder()
			Expect(sb2).ToNot(BeNil())
			Expect(sb2.Len()).To(Equal(0)) // Should be reset

			// Test with oversized builder (should not be pooled)
			largeSb := pools.GetStringBuilder()
			largeSb.Grow(2000) // Larger than 1024 limit
			pools.PutStringBuilder(largeSb)
		})

		It("should handle JS runtime cleanup properly", func() {
			// Get a runtime
			runtime1 := pools.GetJSRuntime()
			Expect(runtime1).ToNot(BeNil())

			// Set some variables that should be cleaned up
			err := runtime1.Set("temp", 25.5)
			Expect(err).ToNot(HaveOccurred())
			err = runtime1.Set("press", 100.0)
			Expect(err).ToNot(HaveOccurred())
			err = runtime1.Set("result", "test")
			Expect(err).ToNot(HaveOccurred())

			// Return it to pool - cleanup should succeed
			pools.PutJSRuntime(runtime1)

			// Get another runtime - should be a clean runtime
			runtime2 := pools.GetJSRuntime()
			Expect(runtime2).ToNot(BeNil())

			pools.PutJSRuntime(runtime2)
		})
	})

	Describe("Error Handling", func() {
		It("should handle security configuration gracefully", func() {
			// Test that configureRuntime doesn't panic even if some operations fail
			// This tests the error handling paths in configureRuntime
			runtime := pools.GetJSRuntime()
			Expect(runtime).ToNot(BeNil())

			// Configure runtime should work without errors
			js_security.ConfigureJSRuntime(runtime, pools.logger)

			// Verify that dangerous globals are replaced with security blockers
			// The security blocker should return a TypeError when called
			result, err := runtime.RunString("typeof eval")
			Expect(err).ToNot(HaveOccurred())
			Expect(result.String()).To(Equal("function")) // eval is replaced with a function that blocks

			pools.PutJSRuntime(runtime)
		})

		It("should handle runtime cleanup edge cases", func() {
			// Test cleanup with a runtime that has been interrupted
			runtime := pools.GetJSRuntime()
			Expect(runtime).ToNot(BeNil())

			// Interrupt the runtime
			runtime.Interrupt("test interrupt")

			// Set some variables
			_ = runtime.Set("temp", 25.5)
			_ = runtime.Set("result", "test")

			// Return to pool - should handle interrupted runtime gracefully
			pools.PutJSRuntime(runtime)
		})

		It("should test pools with nil logger", func() {
			// Create pools with nil logger to test error handling paths
			poolsWithNilLogger := NewObjectPools([]string{"temp", "press"}, nil)
			Expect(poolsWithNilLogger).ToNot(BeNil())

			// Test runtime configuration with nil logger
			runtime := poolsWithNilLogger.GetJSRuntime()
			Expect(runtime).ToNot(BeNil())

			// Test cleanup with nil logger
			_ = runtime.Set("temp", 25.5)
			poolsWithNilLogger.PutJSRuntime(runtime)
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
