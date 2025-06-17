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

package topic_browser_plugin

import (
	"context"
	"encoding/hex"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("TopicBrowserProcessor", func() {
	var processor *TopicBrowserProcessor

	BeforeEach(func() {
		processor = NewTopicBrowserProcessor(nil, nil, 0, time.Second, 10, 10000)
	})

	Describe("ProcessBatch", func() {
		It("processes a single message successfully", func() {
			// Create a message with basic metadata
			msg := service.NewMessage(nil)
			msg.MetaSet("umh_topic", "umh.v1.test-topic._historian.some_value")
			msg.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1647753600000),
				"value":        13,
			})

			// Process the message
			var err error
			processor.topicMetadataCache, err = lru.New(1)
			Expect(err).To(BeNil())
			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
			Expect(err).To(BeNil())
			Expect(result).To(HaveLen(1))
			Expect(result[0]).To(HaveLen(1))

			// Verify the output message
			outputMsg := result[0][0]
			Expect(outputMsg).NotTo(BeNil())

			// Dump to disk for testing
			/*
				bytes, err := outputMsg.AsBytes()
				Expect(err).To(BeNil())
				Expect(bytes).NotTo(BeNil())
				err = os.WriteFile("single_message.proto", bytes, 0644)
				Expect(err).To(BeNil())
			*/
		})

		It("handles empty batch", func() {
			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{})
			Expect(err).To(BeNil())
			Expect(result).To(HaveLen(0))
		})

		It("handles message with missing required metadata", func() {
			msg := service.NewMessage([]byte("test"))
			// No metadata set

			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
			Expect(err).To(BeNil())
			Expect(result).To(BeNil())
		})

		It("caches UNS map entries", func() {
			// Create two messages with the same UNS tree ID
			msg1 := service.NewMessage(nil)
			msg1.MetaSet("umh_topic", "umh.v1.test-topic._historian.some_value")
			msg1.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1647753600000),
				"value":        3,
			})

			msg2 := service.NewMessage(nil)
			msg2.MetaSet("umh_topic", "umh.v1.test-topic._historian.some_value")
			msg2.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1647753600001),
				"value":        5,
			})

			// Process both messages
			var err error
			processor.topicMetadataCache, err = lru.New(1)
			Expect(err).To(BeNil())
			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg1, msg2})
			Expect(err).To(BeNil())
			Expect(result).To(HaveLen(1))
			Expect(result[0]).To(HaveLen(1))

			// Verify the output messages
			outputMsg := result[0][0]
			Expect(outputMsg).NotTo(BeNil())

			// Dump to disk for testing

			/*
				bytes, err := outputMsg.AsBytes()
				Expect(err).To(BeNil())
				Expect(bytes).NotTo(BeNil())
				err = os.WriteFile("multiple_messages.proto", bytes, 0644)
				Expect(err).To(BeNil())
			*/

			// Parse the resulting message back
			outBytes, err := outputMsg.AsBytes()
			Expect(err).To(BeNil())
			Expect(outBytes).NotTo(BeNil())

			// The output shall look like this:
			/*
				STARTSTARTSTART
				0a720a700a1031363337626462653336643561396262125c0a0a746573742d746f7069633a0a5f686973746f7269616e4a0c0a0a736f6d655f76616c756552340a09756d685f746f7069631227756d682e76312e746573742d746f7069632e5f686973746f7269616e2e736f6d655f76616c756512be020a9c010a103136333762646265333664356139626212160a0a676f6c616e672f696e74120803000000000000001a070880b892aefa2f20012a650a340a09756d685f746f7069631227756d682e76312e746573742d746f7069632e5f686973746f7269616e2e736f6d655f76616c7565122d7b22736f6d655f76616c7565223a332c2274696d657374616d705f6d73223a313634373735333630303030307d0a9c010a103136333762646265333664356139626212160a0a676f6c616e672f696e74120805000000000000001a070881b892aefa2f20012a650a340a09756d685f746f7069631227756d682e76312e746573742d746f7069632e5f686973746f7269616e2e736f6d655f76616c7565122d7b22736f6d655f76616c7565223a352c2274696d657374616d705f6d73223a313634373735333630303030317d
				ENDDATAENDDATENDDATA
				279638000
				ENDENDENDEND
			*/

			// Let's only focus on the 2nd lin (0a70 - updated due to Name field addition)
			dataLine := strings.Split(string(outBytes), "\n")[1]
			// Expect it to begin with 0a70 (updated due to Name field addition)
			Expect(dataLine[:4]).To(Equal("0a70"))

			// Hex decode it
			hexDecoded, err := hex.DecodeString(dataLine)
			Expect(err).To(BeNil())
			Expect(hexDecoded).NotTo(BeNil())

			// Decode it
			decoded, err := ProtobufBytesToBundleWithCompression(hexDecoded)
			Expect(err).To(BeNil())
			Expect(decoded).NotTo(BeNil())

			Expect(decoded.Events.Entries).To(HaveLen(2))
			Expect(decoded.UnsMap.Entries).To(HaveLen(1))

			Expect(decoded.UnsMap.Entries).To(HaveKey("1637bdbe36d5a9bb")) // uns tree id - updated after Name field addition
			topicData := decoded.UnsMap.Entries["1637bdbe36d5a9bb"]
			Expect(topicData).NotTo(BeNil())
			Expect(topicData.Level0).To(Equal("test-topic"))
			Expect(topicData.DataContract).To(Equal("_historian"))
			// EventTag functionality was removed from protobuf schema
			Expect(topicData.Metadata).To(Not(BeEmpty()))
			Expect(topicData.Metadata).To(HaveKeyWithValue("umh_topic", "umh.v1.test-topic._historian.some_value"))

			// Now some tests for the Events
			Expect(decoded.Events.Entries).To(HaveLen(2))

			// Verify first event
			event1 := decoded.Events.Entries[0]
			Expect(event1.GetTs().GetTimestampMs()).To(Equal(int64(1647753600000)))
			Expect(event1.GetTs().GetScalarType()).To(Equal(ScalarType_NUMERIC))
			Expect(event1.GetTs().GetNumericValue()).NotTo(BeNil())
			Expect(event1.GetTs().GetNumericValue().GetValue()).To(Equal(float64(3)))
			Expect(event1.RawKafkaMsg).NotTo(BeNil())
			Expect(event1.RawKafkaMsg.Headers).To(HaveKeyWithValue("umh_topic", "umh.v1.test-topic._historian.some_value"))

			// Verify second event
			event2 := decoded.Events.Entries[1]
			Expect(event2.GetTs().GetTimestampMs()).To(Equal(int64(1647753600001)))
			Expect(event2.GetTs().GetScalarType()).To(Equal(ScalarType_NUMERIC))
			Expect(event2.GetTs().GetNumericValue()).NotTo(BeNil())
			Expect(event2.GetTs().GetNumericValue().GetValue()).To(Equal(float64(5)))
			Expect(event2.RawKafkaMsg).NotTo(BeNil())
		})

		It("caches UNS map entries accross multiple invocations", func() {
			// Create two messages with the same UNS tree ID
			msg1 := service.NewMessage(nil)
			msg1.MetaSet("umh_topic", "umh.v1.test-topic._historian.some_value")
			msg1.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1647753600000),
				"value":        3,
			})

			msg2 := service.NewMessage(nil)
			msg2.MetaSet("umh_topic", "umh.v1.test-topic._historian.some_value")
			msg2.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1647753600001),
				"value":        5,
			})

			// Process first messages
			var err error
			processor.topicMetadataCache, err = lru.New(1)
			Expect(err).To(BeNil())
			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg1})
			Expect(err).To(BeNil())
			Expect(result).To(HaveLen(1))
			Expect(result[0]).To(HaveLen(1))

			// Verify the output messages
			outputMsg := result[0][0]
			Expect(outputMsg).NotTo(BeNil())

			// Process 2nd messages
			processor.topicMetadataCache, err = lru.New(1)
			Expect(err).To(BeNil())
			result2, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg2})
			Expect(err).To(BeNil())
			Expect(result2).To(HaveLen(1))
			Expect(result2[0]).To(HaveLen(1))

			// Verify the output messages
			outputMsg2 := result2[0][0]
			Expect(outputMsg2).NotTo(BeNil())

			// Get the bytes and decode them
			outBytes2, err := outputMsg2.AsBytes()
			Expect(err).To(BeNil())
			Expect(outBytes2).NotTo(BeNil())

			// Let's only focus on the 2nd lin (0a70 - updated due to Name field addition)
			dataLine := strings.Split(string(outBytes2), "\n")[1]
			// Expect it to begin with 0a70 (updated due to Name field addition)
			Expect(dataLine[:4]).To(Equal("0a70"))

			// Hex decode it
			hexDecoded, err := hex.DecodeString(dataLine)
			Expect(err).To(BeNil())
			Expect(hexDecoded).NotTo(BeNil())

			// Decode the protobuf message
			decoded2, err := ProtobufBytesToBundleWithCompression(hexDecoded)
			Expect(err).To(BeNil())
			Expect(decoded2).NotTo(BeNil())

			// Verify the decoded bundle
			Expect(decoded2.Events.Entries).To(HaveLen(1))
			Expect(decoded2.UnsMap.Entries).To(HaveLen(1))

			// Verify the topic info
			topicInfo2 := decoded2.UnsMap.Entries["1637bdbe36d5a9bb"]
			Expect(topicInfo2).NotTo(BeNil())
			Expect(topicInfo2.Level0).To(Equal("test-topic"))
			Expect(topicInfo2.DataContract).To(Equal("_historian"))
			Expect(topicInfo2.Metadata).To(Not(BeEmpty()))
			Expect(topicInfo2.Metadata).To(HaveKeyWithValue("umh_topic", "umh.v1.test-topic._historian.some_value"))

			// Verify the event
			event := decoded2.Events.Entries[0]
			Expect(event.GetTs().GetTimestampMs()).To(Equal(int64(1647753600001)))
			Expect(event.GetTs().GetScalarType()).To(Equal(ScalarType_NUMERIC))
			Expect(event.GetTs().GetNumericValue()).NotTo(BeNil())
			Expect(event.GetTs().GetNumericValue().GetValue()).To(Equal(float64(5)))
			Expect(event.RawKafkaMsg).NotTo(BeNil())
			Expect(event.RawKafkaMsg.Headers).To(HaveKeyWithValue("umh_topic", "umh.v1.test-topic._historian.some_value"))

			// Dump to disk for testing
			/*
				bytes, err := outputMsg.AsBytes()
				Expect(err).To(BeNil())
				Expect(bytes).NotTo(BeNil())
				err = os.WriteFile("multiple_messages.proto", bytes, 0644)
				Expect(err).To(BeNil())
			*/
		})
	})
})
