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

package stream_processor_plugin

import (
	"context"
	"encoding/json"
	processor2 "github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/processor"
	"time"

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

		testConfig := StreamProcessorConfig{
			Mode:        "timeseries",
			OutputTopic: "umh.v1.corpA.plant-A.aawd",
			Model: ModelConfig{
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
		processor, err = processor2.newStreamProcessor(testConfig, resources.Logger(), resources.Metrics())
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		if processor != nil {
			err := processor.Close(context.TODO())
			Expect(err).To(BeNil())
		}
	})

	Describe("createOutputMessage", func() {
		It("should preserve metadata from original message", func() {
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

			// Extract metadata
			metadata := make(map[string]string)
			err = originalMsg.MetaWalk(func(key, value string) error {
				if key != "umh_topic" {
					metadata[key] = value
				}
				return nil
			})
			Expect(err).To(BeNil())

			// Create output message
			outputMsg, err := processor.createOutputMessage(
				metadata,
				"pressure",
				29.50001,
				originalPayload.TimestampMs,
			)
			Expect(err).ToNot(HaveOccurred())

			// Verify umh_topic is overridden
			topic, exists := outputMsg.MetaGet("umh_topic")
			Expect(exists).To(BeTrue())
			Expect(topic).To(Equal("umh.v1.corpA.plant-A.aawd._pump_v1.pressure"))

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
		})

		It("should handle messages with no additional metadata", func() {
			// Create original message with only umh_topic
			originalPayload := processor2.TimeseriesMessage{
				Value:       25.5,
				TimestampMs: time.Now().UnixMilli(),
			}
			payloadBytes, err := json.Marshal(originalPayload)
			Expect(err).ToNot(HaveOccurred())

			originalMsg := service.NewMessage(payloadBytes)
			originalMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

			// Extract metadata
			metadata := make(map[string]string)
			err = originalMsg.MetaWalk(func(key, value string) error {
				if key != "umh_topic" {
					metadata[key] = value
				}
				return nil
			})
			Expect(err).To(BeNil())

			// Create output message
			outputMsg, err := processor.createOutputMessage(
				metadata,
				"pressure",
				29.50001,
				originalPayload.TimestampMs,
			)
			Expect(err).ToNot(HaveOccurred())

			// Verify only umh_topic exists
			topic, exists := outputMsg.MetaGet("umh_topic")
			Expect(exists).To(BeTrue())
			Expect(topic).To(Equal("umh.v1.corpA.plant-A.aawd._pump_v1.pressure"))

			// Verify no other metadata exists (by checking a few common keys)
			_, exists = outputMsg.MetaGet("correlation_id")
			Expect(exists).To(BeFalse())

			_, exists = outputMsg.MetaGet("source_system")
			Expect(exists).To(BeFalse())
		})

		It("should create correct payload structure", func() {
			originalPayload := processor2.TimeseriesMessage{
				Value:       25.5,
				TimestampMs: time.Now().UnixMilli(),
			}
			payloadBytes, err := json.Marshal(originalPayload)
			Expect(err).ToNot(HaveOccurred())

			originalMsg := service.NewMessage(payloadBytes)
			originalMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

			// Extract metadata
			metadata := make(map[string]string)
			err = originalMsg.MetaWalk(func(key, value string) error {
				if key != "umh_topic" {
					metadata[key] = value
				}
				return nil
			})
			Expect(err).To(BeNil())

			// Create output message
			outputMsg, err := processor.createOutputMessage(
				metadata,
				"pressure",
				29.50001,
				originalPayload.TimestampMs,
			)
			Expect(err).ToNot(HaveOccurred())

			// Verify payload structure
			outputBytes, err := outputMsg.AsBytes()
			Expect(err).ToNot(HaveOccurred())

			var outputPayload processor2.TimeseriesMessage
			err = json.Unmarshal(outputBytes, &outputPayload)
			Expect(err).ToNot(HaveOccurred())

			Expect(outputPayload.Value).To(Equal(29.50001))
			Expect(outputPayload.TimestampMs).To(Equal(originalPayload.TimestampMs))
		})
	})
})
