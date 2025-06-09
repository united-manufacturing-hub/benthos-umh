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

package sparkplug_plugin_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	_ "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin" // Import to trigger init()
	"github.com/weekaung/sparkplugb-client/sproto"
	"google.golang.org/protobuf/proto"
)

func TestSparkplugBSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sparkplug B Suite")
}

// Helper functions for creating test data (additional to existing ones)

func uint32Ptr(u uint32) *uint32 {
	return &u
}

func boolPtr(b bool) *bool {
	return &b
}

func float32Ptr(f float32) *float32 {
	return &f
}

func float64Ptr(f float64) *float64 {
	return &f
}

// SparkplugTestData contains common test data structures
type SparkplugTestData struct {
	// Common topic patterns
	NBirthTopic string
	NDataTopic  string
	DBirthTopic string
	DDataTopic  string
	NDeathTopic string
	DDeathTopic string

	// Common payloads
	NBirthPayload *sproto.Payload
	NDataPayload  *sproto.Payload
	DBirthPayload *sproto.Payload
	DDataPayload  *sproto.Payload
	NDeathPayload *sproto.Payload
	DDeathPayload *sproto.Payload
}

// NewSparkplugTestData creates test data for Sparkplug B testing
func NewSparkplugTestData() *SparkplugTestData {
	groupID := "TestFactory"
	edgeNodeID := "Line1"
	deviceID := "Machine1"

	data := &SparkplugTestData{
		NBirthTopic: fmt.Sprintf("spBv1.0/%s/NBIRTH/%s", groupID, edgeNodeID),
		NDataTopic:  fmt.Sprintf("spBv1.0/%s/NDATA/%s", groupID, edgeNodeID),
		DBirthTopic: fmt.Sprintf("spBv1.0/%s/DBIRTH/%s/%s", groupID, edgeNodeID, deviceID),
		DDataTopic:  fmt.Sprintf("spBv1.0/%s/DDATA/%s/%s", groupID, edgeNodeID, deviceID),
		NDeathTopic: fmt.Sprintf("spBv1.0/%s/NDEATH/%s", groupID, edgeNodeID),
		DDeathTopic: fmt.Sprintf("spBv1.0/%s/DDEATH/%s/%s", groupID, edgeNodeID, deviceID),
	}

	// NBIRTH payload with bdSeq and metrics with aliases
	data.NBirthPayload = &sproto.Payload{
		Timestamp: uint64Ptr(1672531200000),
		Seq:       uint64Ptr(0),
		Metrics: []*sproto.Payload_Metric{
			{
				Name:     stringPtr("bdSeq"),
				Alias:    uint64Ptr(1),
				Value:    &sproto.Payload_Metric_LongValue{LongValue: 12345},
				Datatype: uint32Ptr(7), // Int64
			},
			{
				Name:     stringPtr("Temperature"),
				Alias:    uint64Ptr(100),
				Value:    &sproto.Payload_Metric_FloatValue{FloatValue: 25.5},
				Datatype: uint32Ptr(9), // Float
			},
			{
				Name:     stringPtr("Pressure"),
				Alias:    uint64Ptr(101),
				Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 1013.25},
				Datatype: uint32Ptr(10), // Double
			},
			{
				Name:     stringPtr("IsRunning"),
				Alias:    uint64Ptr(102),
				Value:    &sproto.Payload_Metric_BooleanValue{BooleanValue: true},
				Datatype: uint32Ptr(11), // Boolean
			},
		},
	}

	// NDATA payload using aliases
	data.NDataPayload = &sproto.Payload{
		Timestamp: uint64Ptr(1672531260000),
		Seq:       uint64Ptr(1),
		Metrics: []*sproto.Payload_Metric{
			{
				Alias:    uint64Ptr(100),
				Value:    &sproto.Payload_Metric_FloatValue{FloatValue: 26.8},
				Datatype: uint32Ptr(9),
			},
			{
				Alias:    uint64Ptr(101),
				Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 1015.50},
				Datatype: uint32Ptr(10),
			},
		},
	}

	// DBIRTH payload for device-level metrics
	data.DBirthPayload = &sproto.Payload{
		Timestamp: uint64Ptr(1672531200000),
		Seq:       uint64Ptr(0),
		Metrics: []*sproto.Payload_Metric{
			{
				Name:     stringPtr("Speed"),
				Alias:    uint64Ptr(200),
				Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 1500.0},
				Datatype: uint32Ptr(10),
			},
			{
				Name:     stringPtr("Status"),
				Alias:    uint64Ptr(201),
				Value:    &sproto.Payload_Metric_StringValue{StringValue: "RUNNING"},
				Datatype: uint32Ptr(12), // String
			},
		},
	}

	// DDATA payload using device aliases
	data.DDataPayload = &sproto.Payload{
		Timestamp: uint64Ptr(1672531320000),
		Seq:       uint64Ptr(2),
		Metrics: []*sproto.Payload_Metric{
			{
				Alias:    uint64Ptr(200),
				Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 1550.0},
				Datatype: uint32Ptr(10),
			},
		},
	}

	// NDEATH payload
	data.NDeathPayload = &sproto.Payload{
		Timestamp: uint64Ptr(1672531400000),
		Metrics: []*sproto.Payload_Metric{
			{
				Name:     stringPtr("bdSeq"),
				Value:    &sproto.Payload_Metric_LongValue{LongValue: 12345},
				Datatype: uint32Ptr(7),
			},
		},
	}

	// DDEATH payload
	data.DDeathPayload = &sproto.Payload{
		Timestamp: uint64Ptr(1672531400000),
	}

	return data
}

// CreateBenthosMessage creates a Benthos message with Sparkplug payload and MQTT topic
func CreateBenthosMessage(payload *sproto.Payload, topic string) (*service.Message, error) {
	payloadBytes, err := proto.Marshal(payload)
	if err != nil {
		return nil, err
	}

	msg := service.NewMessage(payloadBytes)
	msg.MetaSetMut("mqtt_topic", topic)
	return msg, nil
}

// ExpectMetadata checks that a message has the expected metadata values
func ExpectMetadata(msg *service.Message, expectedMeta map[string]string) {
	for key, expectedValue := range expectedMeta {
		actualValue, exists := msg.MetaGet(key)
		Expect(exists).To(BeTrue(), fmt.Sprintf("Expected metadata key '%s' to exist", key))
		Expect(actualValue).To(Equal(expectedValue), fmt.Sprintf("Expected metadata '%s' to be '%s', got '%s'", key, expectedValue, actualValue))
	}
}

// ExpectJSONField checks that a structured message has the expected JSON field value
func ExpectJSONField(msg *service.Message, fieldPath string, expectedValue interface{}) {
	structured, err := msg.AsStructured()
	Expect(err).NotTo(HaveOccurred())

	// Simple field access for now - can be extended for nested paths
	structMap, ok := structured.(map[string]interface{})
	Expect(ok).To(BeTrue(), "Message should be structured as map")

	actualValue, exists := structMap[fieldPath]
	Expect(exists).To(BeTrue(), fmt.Sprintf("Expected field '%s' to exist in message", fieldPath))
	Expect(actualValue).To(Equal(expectedValue), fmt.Sprintf("Expected field '%s' to be '%v', got '%v'", fieldPath, expectedValue, actualValue))
}

// CreateProcessorPipeline creates a Benthos pipeline with sparkplug_b_decode processor
func CreateProcessorPipeline(processorConfig string) (*service.StreamBuilder, service.MessageHandlerFunc, *[]*service.Message, error) {
	builder := service.NewStreamBuilder()

	msgHandler, err := builder.AddProducerFunc()
	if err != nil {
		return nil, nil, nil, err
	}

	err = builder.AddProcessorYAML(processorConfig)
	if err != nil {
		return nil, nil, nil, err
	}

	var messages []*service.Message
	err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
		messages = append(messages, msg)
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}

	return builder, msgHandler, &messages, nil
}

// RunPipelineWithTimeout runs a pipeline for a specified timeout
func RunPipelineWithTimeout(builder *service.StreamBuilder, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	stream, err := builder.Build()
	if err != nil {
		return nil, nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	go func() {
		_ = stream.Run(ctx)
	}()

	return ctx, cancel, nil
}

var _ = Describe("Sparkplug B Test Suite Helpers", func() {
	Describe("Test Data Creation", func() {
		It("should create valid test data", func() {
			testData := NewSparkplugTestData()

			Expect(testData.NBirthTopic).To(Equal("spBv1.0/TestFactory/NBIRTH/Line1"))
			Expect(testData.NDataTopic).To(Equal("spBv1.0/TestFactory/NDATA/Line1"))
			Expect(testData.DBirthTopic).To(Equal("spBv1.0/TestFactory/DBIRTH/Line1/Machine1"))

			Expect(testData.NBirthPayload).NotTo(BeNil())
			Expect(testData.NBirthPayload.Metrics).To(HaveLen(4))

			// Validate bdSeq metric
			bdSeqMetric := testData.NBirthPayload.Metrics[0]
			Expect(*bdSeqMetric.Name).To(Equal("bdSeq"))
			Expect(*bdSeqMetric.Alias).To(Equal(uint64(1)))
		})

		It("should create valid Benthos messages", func() {
			testData := NewSparkplugTestData()

			msg, err := CreateBenthosMessage(testData.NBirthPayload, testData.NBirthTopic)
			Expect(err).NotTo(HaveOccurred())
			Expect(msg).NotTo(BeNil())

			topic, exists := msg.MetaGet("mqtt_topic")
			Expect(exists).To(BeTrue())
			Expect(topic).To(Equal(testData.NBirthTopic))

			// Should be able to unmarshal the payload
			msgBytes, err := msg.AsBytes()
			Expect(err).NotTo(HaveOccurred())

			var payload sproto.Payload
			err = proto.Unmarshal(msgBytes, &payload)
			Expect(err).NotTo(HaveOccurred())
			Expect(payload.Metrics).To(HaveLen(4))
		})
	})
})
