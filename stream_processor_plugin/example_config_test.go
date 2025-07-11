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
	processor2 "github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/processor"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("Example Config Test", func() {
	var (
		processor *processor2.StreamProcessor
		ctx       context.Context
		cancel    context.CancelFunc
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())

		// Use the exact configuration from example_config.yaml
		config := StreamProcessorConfig{
			Mode:        "timeseries",
			OutputTopic: "umh.v1.corpA.plant-A.aawd",
			Model: ModelConfig{
				Name:    "pump",
				Version: "v1",
			},
			Sources: map[string]string{
				"press": "umh.v1.corpA.plant-A.aawd._raw.press",
				"tF":    "umh.v1.corpA.plant-A.aawd._raw.tempF",
				"r":     "umh.v1.corpA.plant-A.aawd._raw.run",
			},
			Mapping: map[string]interface{}{
				"pressure":    "press+4.00001", // dynamic - depends on 'press'
				"temperature": "tF*69/31",      // dynamic - depends on 'tF'
				"combined":    "press+tF",      // dynamic - depends on 'press' and 'tF'
				"motor": map[string]interface{}{
					"rpm": "press/4", // dynamic - depends on 'press'
				},
				"serialNumber": `"SN-P42-008"`, // static - always emitted
				"deviceType":   `"pump"`,       // static - always emitted
			},
		}

		resources := service.MockResources()
		var err error
		processor, err = processor2.newStreamProcessor(config, resources.Logger(), resources.Metrics())
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		if processor != nil {
			err := processor.Close(ctx)
			Expect(err).To(BeNil())
		}
		cancel()
	})

	// Helper function to create a message with the given topic and value
	createMessage := func(topic string, value float64) *service.Message {
		payload := processor2.TimeseriesMessage{
			Value:       value,
			TimestampMs: time.Now().UnixMilli(),
		}
		payloadBytes, err := json.Marshal(payload)
		Expect(err).ToNot(HaveOccurred())

		msg := service.NewMessage(payloadBytes)
		msg.MetaSet("umh_topic", topic)
		return msg
	}

	// Helper function to extract topics and values from output messages
	extractOutputs := func(batches []service.MessageBatch) map[string]interface{} {
		outputs := make(map[string]interface{})
		for _, batch := range batches {
			for _, msg := range batch {
				topic, exists := msg.MetaGet("umh_topic")
				Expect(exists).To(BeTrue())

				var payload processor2.TimeseriesMessage
				msgBytes, err := msg.AsBytes()
				Expect(err).ToNot(HaveOccurred())
				err = json.Unmarshal(msgBytes, &payload)
				Expect(err).ToNot(HaveOccurred())

				outputs[topic] = payload.Value
			}
		}
		return outputs
	}

	It("should process the example config scenario correctly", func() {
		// Test 1: Send press message
		// Should get: pressure, motor.rpm, serialNumber, deviceType
		By("sending press message")
		pressMsg := createMessage("umh.v1.corpA.plant-A.aawd._raw.press", 100.0)
		batches1, err := processor.ProcessBatch(ctx, service.MessageBatch{pressMsg})
		Expect(err).ToNot(HaveOccurred())

		outputs1 := extractOutputs(batches1)

		// Verify expected outputs
		Expect(outputs1).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.pressure"))
		Expect(outputs1).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.motor.rpm"))
		Expect(outputs1).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber"))
		Expect(outputs1).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.deviceType"))

		// Verify calculated values
		Expect(outputs1["umh.v1.corpA.plant-A.aawd._pump_v1.pressure"]).To(BeNumerically("~", 100.0+4.00001, 0.001))
		Expect(outputs1["umh.v1.corpA.plant-A.aawd._pump_v1.motor.rpm"]).To(BeNumerically("~", 100.0/4, 0.001))
		Expect(outputs1["umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber"]).To(Equal("SN-P42-008"))
		Expect(outputs1["umh.v1.corpA.plant-A.aawd._pump_v1.deviceType"]).To(Equal("pump"))

		// Should NOT have temperature, combined (they depend on tF which wasn't sent)
		Expect(outputs1).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.temperature"))
		Expect(outputs1).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.combined"))

		// Test 2: Send tempF message
		// Should get: temperature, combined, serialNumber, deviceType
		By("sending tempF message")
		tempMsg := createMessage("umh.v1.corpA.plant-A.aawd._raw.tempF", 86.0)
		batches2, err := processor.ProcessBatch(ctx, service.MessageBatch{tempMsg})
		Expect(err).ToNot(HaveOccurred())

		outputs2 := extractOutputs(batches2)

		// Verify expected outputs
		Expect(outputs2).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.temperature"))
		Expect(outputs2).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.combined"))
		Expect(outputs2).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber"))
		Expect(outputs2).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.deviceType"))

		// Verify calculated values
		Expect(outputs2["umh.v1.corpA.plant-A.aawd._pump_v1.temperature"]).To(BeNumerically("~", 86.0*69/31, 0.001))
		Expect(outputs2["umh.v1.corpA.plant-A.aawd._pump_v1.combined"]).To(BeNumerically("~", 100.0+86.0, 0.001)) // press + tF
		Expect(outputs2["umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber"]).To(Equal("SN-P42-008"))
		Expect(outputs2["umh.v1.corpA.plant-A.aawd._pump_v1.deviceType"]).To(Equal("pump"))

		// Should NOT have pressure, motor.rpm (they depend on press, which wasn't in this message)
		Expect(outputs2).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.pressure"))
		Expect(outputs2).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.motor.rpm"))

		// Test 3: Send run message
		// Should get: serialNumber, deviceType (only static mappings)
		By("sending run message")
		runMsg := createMessage("umh.v1.corpA.plant-A.aawd._raw.run", 1.0)
		batches3, err := processor.ProcessBatch(ctx, service.MessageBatch{runMsg})
		Expect(err).ToNot(HaveOccurred())

		outputs3 := extractOutputs(batches3)

		// Verify expected outputs - only static mappings
		Expect(outputs3).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber"))
		Expect(outputs3).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.deviceType"))

		// Verify static values
		Expect(outputs3["umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber"]).To(Equal("SN-P42-008"))
		Expect(outputs3["umh.v1.corpA.plant-A.aawd._pump_v1.deviceType"]).To(Equal("pump"))

		// Should NOT have any dynamic mappings (none depend on 'r' variable)
		Expect(outputs3).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.pressure"))
		Expect(outputs3).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.temperature"))
		Expect(outputs3).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.combined"))
		Expect(outputs3).ToNot(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.motor.rpm"))
	})

	It("should demonstrate the complete flow with all variables", func() {
		// This test shows what happens when all variables are available
		By("sending all three messages in sequence")

		// Send press
		pressMsg := createMessage("umh.v1.corpA.plant-A.aawd._raw.press", 50.0)
		_, err := processor.ProcessBatch(ctx, service.MessageBatch{pressMsg})
		Expect(err).ToNot(HaveOccurred())

		// Send tempF
		tempMsg := createMessage("umh.v1.corpA.plant-A.aawd._raw.tempF", 68.0)
		_, err = processor.ProcessBatch(ctx, service.MessageBatch{tempMsg})
		Expect(err).ToNot(HaveOccurred())

		// Send run - this should generate only static mappings
		runMsg := createMessage("umh.v1.corpA.plant-A.aawd._raw.run", 1.0)
		batches, err := processor.ProcessBatch(ctx, service.MessageBatch{runMsg})
		Expect(err).ToNot(HaveOccurred())

		outputs := extractOutputs(batches)

		// Only static mappings should be present
		Expect(len(outputs)).To(Equal(2))
		Expect(outputs).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber"))
		Expect(outputs).To(HaveKey("umh.v1.corpA.plant-A.aawd._pump_v1.deviceType"))

		Expect(outputs["umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber"]).To(Equal("SN-P42-008"))
		Expect(outputs["umh.v1.corpA.plant-A.aawd._pump_v1.deviceType"]).To(Equal("pump"))
	})
})
