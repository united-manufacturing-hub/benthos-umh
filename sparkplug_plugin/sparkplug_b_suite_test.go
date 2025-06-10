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
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	_ "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin" // Import to trigger init()
	"github.com/weekaung/sparkplugb-client/sproto"
	"google.golang.org/protobuf/proto"
)

func TestSparkplugBSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sparkplug B Suite")
}

// Helper functions for creating test data (additional to existing ones)
func stringPtr(s string) *string {
	return &s
}

func uint64Ptr(u uint64) *uint64 {
	return &u
}

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

var _ = Describe("Sparkplug B Core Components", func() {

	Describe("AliasCache", func() {
		var cache *sparkplug_plugin.AliasCache

		BeforeEach(func() {
			cache = sparkplug_plugin.NewAliasCache()
		})

		Context("Basic operations", func() {
			It("should cache aliases from BIRTH metrics", func() {
				metrics := []*sproto.Payload_Metric{
					{
						Name:  stringPtr("Temperature"),
						Alias: uint64Ptr(100),
					},
					{
						Name:  stringPtr("Pressure"),
						Alias: uint64Ptr(101),
					},
					{
						Name:  stringPtr("Speed"),
						Alias: uint64Ptr(200),
					},
				}

				count := cache.CacheAliases("TestFactory/Line1", metrics)
				Expect(count).To(Equal(3))
			})

			It("should resolve aliases in DATA metrics", func() {
				// First cache aliases
				birthMetrics := []*sproto.Payload_Metric{
					{
						Name:  stringPtr("Temperature"),
						Alias: uint64Ptr(100),
					},
					{
						Name:  stringPtr("Pressure"),
						Alias: uint64Ptr(101),
					},
				}
				cache.CacheAliases("TestFactory/Line1", birthMetrics)

				// Now test alias resolution
				dataMetrics := []*sproto.Payload_Metric{
					{
						Alias: uint64Ptr(100), // Should resolve to "Temperature"
						Value: &sproto.Payload_Metric_DoubleValue{DoubleValue: 25.5},
					},
					{
						Alias: uint64Ptr(101), // Should resolve to "Pressure"
						Value: &sproto.Payload_Metric_DoubleValue{DoubleValue: 1013.25},
					},
				}

				count := cache.ResolveAliases("TestFactory/Line1", dataMetrics)
				Expect(count).To(Equal(2))
				Expect(*dataMetrics[0].Name).To(Equal("Temperature"))
				Expect(*dataMetrics[1].Name).To(Equal("Pressure"))
			})

			It("should handle empty device key", func() {
				metrics := []*sproto.Payload_Metric{
					{
						Name:  stringPtr("Temperature"),
						Alias: uint64Ptr(100),
					},
				}

				count := cache.CacheAliases("", metrics)
				Expect(count).To(Equal(0))
			})

			It("should handle nil metrics", func() {
				count := cache.CacheAliases("TestFactory/Line1", nil)
				Expect(count).To(Equal(0))
			})

			It("should skip metrics without name or alias", func() {
				metrics := []*sproto.Payload_Metric{
					{
						Name: stringPtr("Temperature"), // Missing alias
					},
					{
						Alias: uint64Ptr(100), // Missing name
					},
					{
						Name:  stringPtr(""), // Empty name
						Alias: uint64Ptr(101),
					},
					{
						Name:  stringPtr("ValidMetric"),
						Alias: uint64Ptr(102),
					},
				}

				count := cache.CacheAliases("TestFactory/Line1", metrics)
				Expect(count).To(Equal(1)) // Only ValidMetric should be cached
			})

			It("should clear all aliases", func() {
				metrics := []*sproto.Payload_Metric{
					{
						Name:  stringPtr("Temperature"),
						Alias: uint64Ptr(100),
					},
				}
				cache.CacheAliases("TestFactory/Line1", metrics)

				cache.Clear()

				// Verify cache is empty by trying to resolve
				dataMetrics := []*sproto.Payload_Metric{
					{
						Alias: uint64Ptr(100),
						Value: &sproto.Payload_Metric_DoubleValue{DoubleValue: 25.5},
					},
				}
				count := cache.ResolveAliases("TestFactory/Line1", dataMetrics)
				Expect(count).To(Equal(0))
			})
		})

		Context("Multiple devices", func() {
			It("should maintain separate alias caches per device", func() {
				// Cache aliases for Line1
				line1Metrics := []*sproto.Payload_Metric{
					{
						Name:  stringPtr("Line1_Temperature"),
						Alias: uint64Ptr(100),
					},
				}
				cache.CacheAliases("TestFactory/Line1", line1Metrics)

				// Cache aliases for Line2 (same alias, different name)
				line2Metrics := []*sproto.Payload_Metric{
					{
						Name:  stringPtr("Line2_Temperature"),
						Alias: uint64Ptr(100),
					},
				}
				cache.CacheAliases("TestFactory/Line2", line2Metrics)

				// Test that each device resolves to its own metric name
				line1Data := []*sproto.Payload_Metric{
					{
						Alias: uint64Ptr(100),
						Value: &sproto.Payload_Metric_DoubleValue{DoubleValue: 25.5},
					},
				}
				count1 := cache.ResolveAliases("TestFactory/Line1", line1Data)
				Expect(count1).To(Equal(1))
				Expect(*line1Data[0].Name).To(Equal("Line1_Temperature"))

				line2Data := []*sproto.Payload_Metric{
					{
						Alias: uint64Ptr(100),
						Value: &sproto.Payload_Metric_DoubleValue{DoubleValue: 30.0},
					},
				}
				count2 := cache.ResolveAliases("TestFactory/Line2", line2Data)
				Expect(count2).To(Equal(1))
				Expect(*line2Data[0].Name).To(Equal("Line2_Temperature"))
			})
		})
	})

	Describe("TopicParser", func() {
		var parser *sparkplug_plugin.TopicParser

		BeforeEach(func() {
			parser = sparkplug_plugin.NewTopicParser()
		})

		Context("Topic parsing", func() {
			It("should parse valid node-level topics", func() {
				topic := "spBv1.0/TestFactory/NBIRTH/Line1"
				msgType, deviceKey, topicInfo := parser.ParseSparkplugTopicDetailed(topic)

				Expect(msgType).To(Equal("NBIRTH"))
				Expect(deviceKey).To(Equal("TestFactory/Line1"))
				Expect(topicInfo.Group).To(Equal("TestFactory"))
				Expect(topicInfo.EdgeNode).To(Equal("Line1"))
				Expect(topicInfo.Device).To(Equal(""))
			})

			It("should parse valid device-level topics", func() {
				topic := "spBv1.0/TestFactory/DBIRTH/Line1/Machine1"
				msgType, deviceKey, topicInfo := parser.ParseSparkplugTopicDetailed(topic)

				Expect(msgType).To(Equal("DBIRTH"))
				Expect(deviceKey).To(Equal("TestFactory/Line1/Machine1"))
				Expect(topicInfo.Group).To(Equal("TestFactory"))
				Expect(topicInfo.EdgeNode).To(Equal("Line1"))
				Expect(topicInfo.Device).To(Equal("Machine1"))
			})

			It("should return empty values for invalid topics", func() {
				invalidTopics := []string{
					"",                              // Empty
					"invalid/topic/format",          // Wrong namespace
					"spBv1.0/Group",                 // Too few parts
					"spBv1.0/Group/MSG",             // Still too few
					"spBv2.0/Group/NBIRTH/EdgeNode", // Wrong version
				}

				for _, topic := range invalidTopics {
					By(fmt.Sprintf("testing invalid topic: %s", topic), func() {
						msgType, deviceKey, topicInfo := parser.ParseSparkplugTopicDetailed(topic)
						Expect(msgType).To(Equal(""))
						Expect(deviceKey).To(Equal(""))
						Expect(topicInfo).To(BeNil())
					})
				}
			})
		})

		Context("Topic construction", func() {
			It("should build node-level topics", func() {
				topic := parser.BuildTopic("TestFactory", "NBIRTH", "Line1", "")
				Expect(topic).To(Equal("spBv1.0/TestFactory/NBIRTH/Line1"))
			})

			It("should build device-level topics", func() {
				topic := parser.BuildTopic("TestFactory", "DBIRTH", "Line1", "Machine1")
				Expect(topic).To(Equal("spBv1.0/TestFactory/DBIRTH/Line1/Machine1"))
			})
		})

		Context("Message type validation", func() {
			It("should validate message types correctly", func() {
				validTypes := []string{"NBIRTH", "NDATA", "NDEATH", "DBIRTH", "DDATA", "DDEATH", "NCMD", "DCMD", "STATE"}
				for _, msgType := range validTypes {
					By(fmt.Sprintf("validating message type: %s", msgType), func() {
						Expect(parser.IsValidMessageType(msgType)).To(BeTrue())
					})
				}

				invalidTypes := []string{"INVALID", "BIRTH", "DATA", ""}
				for _, msgType := range invalidTypes {
					By(fmt.Sprintf("invalidating message type: %s", msgType), func() {
						Expect(parser.IsValidMessageType(msgType)).To(BeFalse())
					})
				}
			})

			It("should identify message type categories", func() {
				// BIRTH messages
				Expect(parser.IsBirthMessage("NBIRTH")).To(BeTrue())
				Expect(parser.IsBirthMessage("DBIRTH")).To(BeTrue())
				Expect(parser.IsBirthMessage("NDATA")).To(BeFalse())

				// DATA messages
				Expect(parser.IsDataMessage("NDATA")).To(BeTrue())
				Expect(parser.IsDataMessage("DDATA")).To(BeTrue())
				Expect(parser.IsDataMessage("NBIRTH")).To(BeFalse())

				// DEATH messages
				Expect(parser.IsDeathMessage("NDEATH")).To(BeTrue())
				Expect(parser.IsDeathMessage("DDEATH")).To(BeTrue())
				Expect(parser.IsDeathMessage("NDATA")).To(BeFalse())

				// CMD messages
				Expect(parser.IsCommandMessage("NCMD")).To(BeTrue())
				Expect(parser.IsCommandMessage("DCMD")).To(BeTrue())
				Expect(parser.IsCommandMessage("NDATA")).To(BeFalse())

				// Node vs Device
				Expect(parser.IsNodeMessage("NBIRTH")).To(BeTrue())
				Expect(parser.IsNodeMessage("DBIRTH")).To(BeFalse())
				Expect(parser.IsDeviceMessage("DBIRTH")).To(BeTrue())
				Expect(parser.IsDeviceMessage("NBIRTH")).To(BeFalse())
			})
		})
	})

	Describe("SequenceManager", func() {
		var seqMgr *sparkplug_plugin.SequenceManager

		BeforeEach(func() {
			seqMgr = sparkplug_plugin.NewSequenceManager()
		})

		Context("Sequence generation", func() {
			It("should generate incrementing sequence numbers", func() {
				seq1 := seqMgr.NextSequence()
				seq2 := seqMgr.NextSequence()
				seq3 := seqMgr.NextSequence()

				Expect(seq1).To(Equal(uint8(0)))
				Expect(seq2).To(Equal(uint8(1)))
				Expect(seq3).To(Equal(uint8(2)))
			})

			It("should wrap around at 256", func() {
				// Set sequence to near the limit
				seqMgr.SetSequence(254)

				seq1 := seqMgr.NextSequence()
				seq2 := seqMgr.NextSequence()
				seq3 := seqMgr.NextSequence()

				Expect(seq1).To(Equal(uint8(254)))
				Expect(seq2).To(Equal(uint8(255)))
				Expect(seq3).To(Equal(uint8(0))) // Wrapped around
			})

			It("should validate sequence numbers correctly", func() {
				// Test normal increment
				Expect(seqMgr.IsSequenceValid(5, 6)).To(BeTrue())
				Expect(seqMgr.IsSequenceValid(5, 7)).To(BeFalse())
				Expect(seqMgr.IsSequenceValid(5, 4)).To(BeFalse())

				// Test wrap-around
				Expect(seqMgr.IsSequenceValid(255, 0)).To(BeTrue())
				Expect(seqMgr.IsSequenceValid(255, 1)).To(BeFalse())
			})

			It("should get current sequence without increment", func() {
				seqMgr.SetSequence(42)

				current1 := seqMgr.GetCurrent()
				current2 := seqMgr.GetCurrent()

				Expect(current1).To(Equal(uint8(42)))
				Expect(current2).To(Equal(uint8(42))) // Should not increment

				next := seqMgr.NextSequence()
				Expect(next).To(Equal(uint8(42))) // Now it increments

				current3 := seqMgr.GetCurrent()
				Expect(current3).To(Equal(uint8(43)))
			})
		})
	})

	Describe("TypeConverter", func() {
		var converter *sparkplug_plugin.TypeConverter

		BeforeEach(func() {
			converter = sparkplug_plugin.NewTypeConverter()
		})

		Context("Data type mapping", func() {
			It("should return correct Sparkplug data types", func() {
				typeTests := map[string]uint32{
					"int8":    1,
					"int16":   2,
					"int32":   3,
					"int64":   4,
					"uint8":   5,
					"uint16":  6,
					"uint32":  7,
					"uint64":  8,
					"float":   9,
					"double":  10,
					"boolean": 11,
					"string":  12,
				}

				for typeStr, expectedValue := range typeTests {
					By(fmt.Sprintf("testing type: %s", typeStr), func() {
						dataType := converter.GetSparkplugDataType(typeStr)
						Expect(*dataType).To(Equal(expectedValue))
					})
				}

				// Test default case
				unknownType := converter.GetSparkplugDataType("unknown")
				Expect(*unknownType).To(Equal(uint32(10))) // Should default to double
			})
		})

		Context("Value conversion", func() {
			It("should convert values to int32", func() {
				tests := map[interface{}]uint32{
					int(42):     42,
					int32(42):   42,
					int64(42):   42,
					uint32(42):  42,
					uint64(42):  42,
					float32(42): 42,
					float64(42): 42,
					"42":        42,
				}

				for input, expected := range tests {
					By(fmt.Sprintf("converting %v (%T) to int32", input, input), func() {
						result, ok := converter.ConvertToInt32(input)
						Expect(ok).To(BeTrue())
						Expect(result).To(Equal(expected))
					})
				}

				// Test invalid conversion
				_, ok := converter.ConvertToInt32("invalid")
				Expect(ok).To(BeFalse())
			})

			It("should convert values to bool", func() {
				tests := map[interface{}]bool{
					true:       true,
					false:      false,
					int(1):     true,
					int(0):     false,
					float64(1): true,
					float64(0): false,
					"true":     true,
					"false":    false,
					"1":        true,
					"0":        false,
					"1.5":      true,
					"0.0":      false,
				}

				for input, expected := range tests {
					By(fmt.Sprintf("converting %v (%T) to bool", input, input), func() {
						result, ok := converter.ConvertToBool(input)
						Expect(ok).To(BeTrue())
						Expect(result).To(Equal(expected))
					})
				}

				// Test invalid conversion
				_, ok := converter.ConvertToBool("invalid")
				Expect(ok).To(BeFalse())
			})

			It("should set metric values based on type", func() {
				metric := &sproto.Payload_Metric{}

				// Test setting different value types
				converter.SetMetricValue(metric, 42.5, "double")
				Expect(metric.Value).To(BeAssignableToTypeOf(&sproto.Payload_Metric_DoubleValue{}))
				doubleVal := metric.Value.(*sproto.Payload_Metric_DoubleValue)
				Expect(doubleVal.DoubleValue).To(Equal(42.5))

				// Test setting boolean
				converter.SetMetricValue(metric, true, "boolean")
				Expect(metric.Value).To(BeAssignableToTypeOf(&sproto.Payload_Metric_BooleanValue{}))
				boolVal := metric.Value.(*sproto.Payload_Metric_BooleanValue)
				Expect(boolVal.BooleanValue).To(BeTrue())

				// Test setting string
				converter.SetMetricValue(metric, "test", "string")
				Expect(metric.Value).To(BeAssignableToTypeOf(&sproto.Payload_Metric_StringValue{}))
				stringVal := metric.Value.(*sproto.Payload_Metric_StringValue)
				Expect(stringVal.StringValue).To(Equal("test"))

				// Test setting null value
				converter.SetMetricValue(metric, nil, "double")
				Expect(*metric.IsNull).To(BeTrue())
			})
		})
	})

	Describe("MQTTClientBuilder", func() {
		var builder *sparkplug_plugin.MQTTClientBuilder
		var mgr *service.Resources

		BeforeEach(func() {
			mgr = service.MockResources()
			builder = sparkplug_plugin.NewMQTTClientBuilder(mgr)
		})

		Context("Client creation", func() {
			It("should create MQTT client with basic configuration", func() {
				config := sparkplug_plugin.MQTTClientConfig{
					BrokerURLs:     []string{"tcp://localhost:1883", "ssl://broker.test:8883"},
					ClientID:       "test-client",
					KeepAlive:      30 * time.Second,
					ConnectTimeout: 10 * time.Second,
					CleanSession:   true,
				}

				client, err := builder.CreateClient(config)
				Expect(err).ToNot(HaveOccurred())
				Expect(client).ToNot(BeNil())
			})

			It("should create client with authentication", func() {
				config := sparkplug_plugin.MQTTClientConfig{
					BrokerURLs:     []string{"tcp://localhost:1883"},
					ClientID:       "test-client",
					Username:       "user",
					Password:       "pass",
					KeepAlive:      30 * time.Second,
					ConnectTimeout: 10 * time.Second,
					CleanSession:   true,
				}

				client, err := builder.CreateClient(config)
				Expect(err).ToNot(HaveOccurred())
				Expect(client).ToNot(BeNil())
			})

			It("should create client with Last Will Testament", func() {
				config := sparkplug_plugin.MQTTClientConfig{
					BrokerURLs:     []string{"tcp://localhost:1883"},
					ClientID:       "test-client",
					KeepAlive:      30 * time.Second,
					ConnectTimeout: 10 * time.Second,
					CleanSession:   true,
					WillTopic:      "test/will",
					WillPayload:    []byte("offline"),
					WillQoS:        1,
					WillRetain:     true,
				}

				client, err := builder.CreateClient(config)
				Expect(err).ToNot(HaveOccurred())
				Expect(client).ToNot(BeNil())
			})

			It("should fail with empty broker URLs", func() {
				config := sparkplug_plugin.MQTTClientConfig{
					BrokerURLs: []string{},
					ClientID:   "test-client",
				}

				client, err := builder.CreateClient(config)
				Expect(err).To(HaveOccurred())
				Expect(client).To(BeNil())
				Expect(err.Error()).To(ContainSubstring("at least one broker URL is required"))
			})
		})
	})
})
