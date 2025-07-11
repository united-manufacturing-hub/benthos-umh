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

package stream_processor_plugin_test

import (
	"context"
	"encoding/json"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/config"
	processor2 "github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/processor"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("Metadata Preservation", func() {
	var (
		processor *processor2.StreamProcessor
		resources *service.Resources
	)

	BeforeEach(func() {
		resources = service.MockResources()

		testConfig := config.StreamProcessorConfig{
			Mode:        "timeseries",
			OutputTopic: "umh.v1.corpA.plant-A.aawd",
			Model: config.ModelConfig{
				Name:    "pump",
				Version: "v1",
			},
			Sources: map[string]string{
				"press": "umh.v1.corpA.plant-A.aawd._raw.press",
			},
			Mapping: map[string]interface{}{
				"pressure":     "press + 4.00001",
				"serialNumber": `"SN-P42-008"`,
			},
		}

		var err error
		processor, err = processor2.NewStreamProcessor(testConfig, resources.Logger(), resources.Metrics())
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		if processor != nil {
			err := processor.Close(context.TODO())
			Expect(err).To(BeNil())
		}
	})

	// Note: The createOutputMessage method is not exported, so we test metadata preservation
	// through the public API by processing actual messages and verifying the outputs
	Describe("End-to-End Metadata Preservation", func() {
		It("should preserve metadata through message processing", func() {
			// Create original message with metadata
			originalPayload := processor2.TimeseriesMessage{
				Value:       25.5,
				TimestampMs: time.Now().UnixMilli(),
			}
			payloadBytes, err := json.Marshal(originalPayload)
			Expect(err).ToNot(HaveOccurred())

			originalMsg := service.NewMessage(payloadBytes)
			originalMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")
			originalMsg.MetaSet("correlation_id", "test-correlation-123")
			originalMsg.MetaSet("source_system", "test-system")
			originalMsg.MetaSet("priority", "high")
			originalMsg.MetaSet("trace_id", "trace-456")

			// Process the message
			batches, err := processor.ProcessBatch(context.TODO(), service.MessageBatch{originalMsg})
			Expect(err).ToNot(HaveOccurred())
			Expect(batches).ToNot(BeEmpty())

			// Verify output messages preserve metadata
			outputBatch := batches[0]
			Expect(len(outputBatch)).To(BeNumerically(">=", 1))

			for _, outputMsg := range outputBatch {
				// Verify umh_topic is set correctly (will be different from original)
				topic, exists := outputMsg.MetaGet("umh_topic")
				Expect(exists).To(BeTrue())
				Expect(topic).To(ContainSubstring("umh.v1.corpA.plant-A.aawd._pump_v1"))

				// Verify other metadata is preserved
				correlationId, exists := outputMsg.MetaGet("correlation_id")
				Expect(exists).To(BeTrue())
				Expect(correlationId).To(Equal("test-correlation-123"))

				sourceSystem, exists := outputMsg.MetaGet("source_system")
				Expect(exists).To(BeTrue())
				Expect(sourceSystem).To(Equal("test-system"))

				priority, exists := outputMsg.MetaGet("priority")
				Expect(exists).To(BeTrue())
				Expect(priority).To(Equal("high"))

				traceId, exists := outputMsg.MetaGet("trace_id")
				Expect(exists).To(BeTrue())
				Expect(traceId).To(Equal("trace-456"))
			}
		})
	})
})
