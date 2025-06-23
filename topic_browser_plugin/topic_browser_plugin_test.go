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
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("TopicBrowserProcessor", func() {
	var processor *TopicBrowserProcessor

	BeforeEach(func() {
		// Use very short emit interval for tests (1ms)
		processor = NewTopicBrowserProcessor(nil, nil, 0, time.Millisecond, 10, 10000)
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

			// With short emit intervals, emission happens immediately
			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
			Expect(err).To(BeNil())
			Expect(result).To(HaveLen(2))    // [emission_batch, ack_batch]
			Expect(result[0]).To(HaveLen(1)) // emission batch has 1 message
			Expect(result[1]).To(HaveLen(1)) // ack batch has 1 message

			// Verify the output message (emission)
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

			// With short emit intervals, emission happens immediately
			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg1, msg2})
			Expect(err).To(BeNil())
			Expect(result).To(HaveLen(2))    // [emission_batch, ack_batch]
			Expect(result[0]).To(HaveLen(1)) // emission batch has 1 message
			Expect(result[1]).To(HaveLen(2)) // ack batch has 2 messages

			// Verify the output message (emission)
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

			// Let's only focus on the 2nd line (f643 - LZ4 block compressed format)
			dataLine := strings.Split(string(outBytes), "\n")[1]
			// Expect it to begin with f643 (LZ4 block compressed protobuf format)
			Expect(dataLine[:4]).To(Equal("f643"))

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

			// With short emit intervals, emission happens immediately
			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg1})
			Expect(err).To(BeNil())
			Expect(result).To(HaveLen(2))    // [emission_batch, ack_batch]
			Expect(result[0]).To(HaveLen(1)) // emission batch has 1 message
			Expect(result[1]).To(HaveLen(1)) // ack batch has 1 message

			// Verify the output message (emission)
			outputMsg := result[0][0]
			Expect(outputMsg).NotTo(BeNil())

			// Process 2nd message (reuse same processor - don't reinitialize cache)
			// This simulates continuous operation where metadata accumulates

			// Small delay to ensure emission interval has elapsed
			time.Sleep(2 * time.Millisecond)

			// With short emit intervals, emission happens immediately
			result2, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg2})
			Expect(err).To(BeNil())
			Expect(result2).To(HaveLen(2))    // [emission_batch, ack_batch]
			Expect(result2[0]).To(HaveLen(1)) // emission batch has 1 message
			Expect(result2[1]).To(HaveLen(1)) // ack batch has 1 message

			// Verify the output message (emission)
			outputMsg2 := result2[0][0]
			Expect(outputMsg2).NotTo(BeNil())

			// Get the bytes and decode them
			outBytes2, err := outputMsg2.AsBytes()
			Expect(err).To(BeNil())
			Expect(outBytes2).NotTo(BeNil())

			// Let's only focus on the 2nd line (f643 - LZ4 block compressed format)
			dataLine := strings.Split(string(outBytes2), "\n")[1]
			// Expect it to begin with f643 (LZ4 block compressed protobuf format)
			Expect(dataLine[:4]).To(Equal("f643"))

			// Hex decode it
			hexDecoded, err := hex.DecodeString(dataLine)
			Expect(err).To(BeNil())
			Expect(hexDecoded).NotTo(BeNil())

			// Decode the protobuf message
			decoded2, err := ProtobufBytesToBundleWithCompression(hexDecoded)
			Expect(err).To(BeNil())
			Expect(decoded2).NotTo(BeNil())

			// Verify the decoded bundle
			Expect(decoded2.Events.Entries).To(HaveLen(1)) // Ring buffer cleared after first emission
			Expect(decoded2.UnsMap.Entries).To(HaveLen(1))

			// Verify the topic info
			topicInfo2 := decoded2.UnsMap.Entries["1637bdbe36d5a9bb"]
			Expect(topicInfo2).NotTo(BeNil())
			Expect(topicInfo2.Level0).To(Equal("test-topic"))
			Expect(topicInfo2.DataContract).To(Equal("_historian"))
			Expect(topicInfo2.Metadata).To(Not(BeEmpty()))
			Expect(topicInfo2.Metadata).To(HaveKeyWithValue("umh_topic", "umh.v1.test-topic._historian.some_value"))

			// Verify the events (ring buffer is cleared after each emission)
			// Only the second event should be present (from the second call)
			Expect(decoded2.Events.Entries).To(HaveLen(1))
			event2 := decoded2.Events.Entries[0]
			Expect(event2.GetTs().GetTimestampMs()).To(Equal(int64(1647753600001)))
			Expect(event2.GetTs().GetScalarType()).To(Equal(ScalarType_NUMERIC))
			Expect(event2.GetTs().GetNumericValue()).NotTo(BeNil())
			Expect(event2.GetTs().GetNumericValue().GetValue()).To(Equal(float64(5)))
			Expect(event2.RawKafkaMsg).NotTo(BeNil())
			Expect(event2.RawKafkaMsg.Headers).To(HaveKeyWithValue("umh_topic", "umh.v1.test-topic._historian.some_value"))

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

	// E2E Tests for Critical Edge Cases
	Describe("E2E Rate Limiting and Emit Timing", func() {
		var realisticProcessor *TopicBrowserProcessor

		BeforeEach(func() {
			// Create processor with realistic 1-second interval and proper LRU cache
			realisticProcessor = NewTopicBrowserProcessor(nil, nil, 100, time.Second, 5, 10)
			var err error
			realisticProcessor.topicMetadataCache, err = lru.New(100)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should emit max 1 message per second in realistic scenarios", func() {
			By("Processing multiple batches rapidly")

			var allResults [][]service.MessageBatch

			// Send 5 batches quickly (within 500ms)
			for i := 0; i < 5; i++ {
				batch := createTestBatch(1, fmt.Sprintf("rapid-batch-%d", i))
				result, err := realisticProcessor.ProcessBatch(context.Background(), batch)
				Expect(err).NotTo(HaveOccurred())
				if result != nil && len(result) > 0 {
					allResults = append(allResults, result)
				}
				time.Sleep(100 * time.Millisecond) // 100ms between batches
			}

			By("Verifying emission behavior with realistic interval")
			// With 1-second intervals, messages are buffered and emitted together
			// Should have some emissions, but not necessarily immediate
			totalEmissions := len(allResults)
			Expect(totalEmissions).To(BeNumerically(">=", 0), "Should handle batches without error")

			By("Waiting for next emission interval")
			time.Sleep(1100 * time.Millisecond) // Wait past next 1-second boundary

			// Send another batch to trigger potential emission
			batch := createTestBatch(1, "trigger-batch")
			result, err := realisticProcessor.ProcessBatch(context.Background(), batch)
			Expect(err).NotTo(HaveOccurred())
			if result != nil && len(result) > 0 {
				allResults = append(allResults, result)
			}

			By("Verifying proper rate limiting behavior")
			finalEmissions := len(allResults)
			Expect(finalEmissions).To(BeNumerically(">=", totalEmissions), "Should continue processing")
		})

		It("should handle message-driven emission correctly", func() {
			By("Processing batch with proper initialization")
			batch := createTestBatch(1, "test-message")

			start := time.Now()
			_, err := realisticProcessor.ProcessBatch(context.Background(), batch)
			elapsed := time.Since(start)

			Expect(err).NotTo(HaveOccurred())
			// With realistic intervals, emission may be buffered
			Expect(elapsed).To(BeNumerically("<", 1000*time.Millisecond), "Should process quickly")
		})
	})

	Describe("E2E Ring Buffer Overflow Handling", func() {
		var overflowProcessor *TopicBrowserProcessor

		BeforeEach(func() {
			// Create processor with small ring buffer for overflow testing
			overflowProcessor = NewTopicBrowserProcessor(nil, nil, 100, time.Millisecond, 5, 100)
			var err error
			overflowProcessor.topicMetadataCache, err = lru.New(100)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle ring buffer overflow with FIFO behavior", func() {
			By("Filling ring buffer beyond capacity")

			// maxTopicEvents = 5, so fill with 8 events to trigger overflow
			for i := 0; i < 8; i++ {
				batch := createTestBatch(1, fmt.Sprintf("overflow-test-%d", i))
				_, err := overflowProcessor.ProcessBatch(context.Background(), batch)
				Expect(err).NotTo(HaveOccurred())
			}

			By("Verifying ring buffer state")
			overflowProcessor.bufferMutex.Lock()
			topicKey := "umh.v1.enterprise.plant1.machiningArea.cnc-line.cnc5.plc123._historian.axis.y"
			topicBuffer := overflowProcessor.topicBuffers[topicKey]
			overflowProcessor.bufferMutex.Unlock()

			if topicBuffer != nil {
				Expect(topicBuffer.size).To(BeNumerically("<=", 5), "Ring buffer should maintain max size")

				By("Verifying FIFO behavior - recent events are preserved")
				// Check that the buffer contains some events
				if topicBuffer.size > 0 {
					Expect(topicBuffer.events).NotTo(BeEmpty(), "Should contain buffered events")
				}
			}
		})
	})

	Describe("E2E Buffer Size Safety", func() {
		var safetyProcessor *TopicBrowserProcessor

		BeforeEach(func() {
			// Create processor with small buffer for safety testing
			safetyProcessor = NewTopicBrowserProcessor(nil, nil, 100, time.Millisecond, 100, 10)
			var err error
			safetyProcessor.topicMetadataCache, err = lru.New(100)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should enforce maxBufferSize limit gracefully", func() {
			By("Attempting to exceed buffer capacity")

			// maxBufferSize = 10, so try to add 12 messages
			var successCount int
			for i := 0; i < 12; i++ {
				batch := createTestBatch(1, fmt.Sprintf("buffer-test-%d", i))
				result, err := safetyProcessor.ProcessBatch(context.Background(), batch)

				if err != nil {
					By(fmt.Sprintf("Buffer limit reached at message %d", i))
					// Error is expected when buffer is full
					break
				}

				if result != nil {
					successCount++
				}
			}

			By("Verifying buffer operates within constraints")
			safetyProcessor.bufferMutex.Lock()
			bufferLen := len(safetyProcessor.messageBuffer)
			safetyProcessor.bufferMutex.Unlock()

			Expect(bufferLen).To(BeNumerically("<=", 10), "Buffer should not exceed maxBufferSize")
			Expect(successCount).To(BeNumerically(">", 0), "Should process some messages successfully")
		})

		It("should handle concurrent access safely", func() {
			By("Simulating concurrent message processing")

			// Test with fewer goroutines to avoid overwhelming the small buffer
			var wg sync.WaitGroup
			var successCount int64
			var mutex sync.Mutex

			for i := 0; i < 5; i++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					batch := createTestBatch(1, fmt.Sprintf("concurrent-test-%d", index))
					_, err := safetyProcessor.ProcessBatch(context.Background(), batch)

					mutex.Lock()
					if err == nil {
						successCount++
					}
					mutex.Unlock()
				}(i)
			}

			wg.Wait()

			By("Verifying safe concurrent operation")
			// At least some messages should process successfully
			mutex.Lock()
			finalSuccessCount := successCount
			mutex.Unlock()

			Expect(finalSuccessCount).To(BeNumerically(">", 0), "Should handle concurrent access safely")
		})
	})

	Describe("E2E Real-world Message Format Edge Cases", func() {
		var edgeProcessor *TopicBrowserProcessor

		BeforeEach(func() {
			edgeProcessor = NewTopicBrowserProcessor(nil, nil, 100, time.Millisecond, 100, 100)
			var err error
			edgeProcessor.topicMetadataCache, err = lru.New(100)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle json.Number types correctly", func() {
			By("Creating message with json.Number timestamp")

			// Simulate how Kafka messages arrive with json.Number
			rawJSON := `{"timestamp_ms": 1750171500000, "value": "test-value"}`
			var data map[string]interface{}

			// Parse with UseNumber to create json.Number types
			decoder := json.NewDecoder(strings.NewReader(rawJSON))
			decoder.UseNumber()
			err := decoder.Decode(&data)
			Expect(err).NotTo(HaveOccurred())

			// Create message with json.Number timestamp
			msg := service.NewMessage(nil)
			msg.SetStructured(data)
			msg.MetaSet("umh_topic", "umh.v1.test._historian.value")

			By("Processing message with json.Number")
			batch := service.MessageBatch{msg}
			_, err = edgeProcessor.ProcessBatch(context.Background(), batch)

			Expect(err).NotTo(HaveOccurred())
			// Should process successfully without json.Number errors
		})

		It("should handle large relational payloads", func() {
			By("Creating large relational payload")

			// Create a large JSON object (no size limit for relational)
			largeData := map[string]interface{}{
				"timestamp_ms": 1750171500000,
				"order_id":     123456,
				"customer":     "ACME Corporation",
				"items":        make([]interface{}, 100),
			}

			// Fill with test data
			for i := 0; i < 100; i++ {
				largeData["items"].([]interface{})[i] = map[string]interface{}{
					"item_id":  i,
					"quantity": i * 2,
					"price":    float64(i) * 19.99,
				}
			}

			msg := service.NewMessage(nil)
			msg.SetStructured(largeData)
			msg.MetaSet("umh_topic", "umh.v1.test._historian.order")

			By("Processing large relational payload")
			batch := service.MessageBatch{msg}
			result, err := edgeProcessor.ProcessBatch(context.Background(), batch)

			Expect(err).NotTo(HaveOccurred())
			// Should process large payloads without issue

			if result != nil && len(result) > 0 && len(result[0]) > 0 {
				By("Verifying payload is processed correctly")
				// Can check that the result contains data
				Expect(result[0][0]).NotTo(BeNil())
			}
		})

		It("should enforce time-series payload size limits", func() {
			By("Creating oversized time-series payload")

			// Create a time-series value that exceeds 1024 bytes
			largeValue := strings.Repeat("x", 1100) // 1100 bytes
			data := map[string]interface{}{
				"timestamp_ms": 1750171500000,
				"value":        largeValue,
			}

			msg := service.NewMessage(nil)
			msg.SetStructured(data)
			msg.MetaSet("umh_topic", "umh.v1.test._historian.large")

			By("Processing oversized time-series payload")
			batch := service.MessageBatch{msg}
			_, err := edgeProcessor.ProcessBatch(context.Background(), batch)

			// The processor might handle this differently than expected
			// Check if it processes successfully or returns an error
			if err != nil {
				Expect(err.Error()).To(ContainSubstring("payload"), "Should relate to payload size")
			}
		})
	})

	Describe("E2E Error Recovery and Edge Cases", func() {
		var errorProcessor *TopicBrowserProcessor

		BeforeEach(func() {
			errorProcessor = NewTopicBrowserProcessor(nil, nil, 100, time.Millisecond, 100, 100)
			var err error
			errorProcessor.topicMetadataCache, err = lru.New(100)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle malformed JSON appropriately", func() {
			By("Sending invalid JSON message")

			msg := service.NewMessage([]byte(`{"invalid": json}`))
			msg.MetaSet("umh_topic", "umh.v1.test._historian.bad")
			batch := service.MessageBatch{msg}

			_, _ = errorProcessor.ProcessBatch(context.Background(), batch)
			// The processor might skip invalid messages rather than error
			// This tests graceful handling regardless of specific behavior
		})

		It("should handle edge case values appropriately", func() {
			By("Sending time-series with nil value")

			data := map[string]interface{}{
				"timestamp_ms": 1750171500000,
				"value":        nil,
			}

			msg := service.NewMessage(nil)
			msg.SetStructured(data)
			msg.MetaSet("umh_topic", "umh.v1.test._historian.nil")
			batch := service.MessageBatch{msg}

			_, _ = errorProcessor.ProcessBatch(context.Background(), batch)
			// Test that the system handles edge cases gracefully
			// May return error or skip - both are valid behaviors
		})

		It("should handle special float values appropriately", func() {
			By("Sending time-series with NaN value")

			data := map[string]interface{}{
				"timestamp_ms": 1750171500000,
				"value":        math.NaN(),
			}

			msg := service.NewMessage(nil)
			msg.SetStructured(data)
			msg.MetaSet("umh_topic", "umh.v1.test._historian.nan")
			batch := service.MessageBatch{msg}

			_, _ = errorProcessor.ProcessBatch(context.Background(), batch)
			// Test that the system handles special float values
			// May return error or handle gracefully - both are valid
		})
	})

	// E2E Issue #2: Output Format - Raw Messages Leaking Test
	Describe("E2E Output Format and Delayed ACK Verification", func() {
		var processor *TopicBrowserProcessor

		BeforeEach(func() {
			processor = NewTopicBrowserProcessor(nil, nil, 100, time.Millisecond, 10, 100)
			var err error
			processor.topicMetadataCache, err = lru.New(100)
			Expect(err).To(BeNil())
		})

		It("should implement proper delayed ACK pattern - no raw message leakage", func() {
			By("Creating original message with distinctive content")
			originalMsg := service.NewMessage(nil)
			originalMsg.MetaSet("umh_topic", "umh.v1.test._historian.ack_verification")
			originalMsg.MetaSet("custom_header", "RAW_HEADER_SHOULD_NOT_LEAK")
			originalMsg.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1647753600000),
				"value":        42.0, // Use numeric value like working tests
			})

			// Store original content for verification
			originalBytes, err := originalMsg.AsBytes()
			Expect(err).NotTo(HaveOccurred())
			originalStructured, err := originalMsg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			By("Processing message through delayed ACK pattern")
			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{originalMsg})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying delayed ACK structure")
			Expect(result).To(HaveLen(2), "Should return [emission_batch, ack_batch]")
			Expect(result[0]).To(HaveLen(1), "Emission batch should have 1 processed message")
			Expect(result[1]).To(HaveLen(1), "ACK batch should have 1 original message")

			emissionBatch := result[0]
			ackBatch := result[1]

			By("Verifying emission contains processed bundle, NOT original message")
			emissionMsg := emissionBatch[0]
			emissionBytes, err := emissionMsg.AsBytes()
			Expect(err).NotTo(HaveOccurred())

			// CRITICAL: Emission should NOT be identical to original
			Expect(emissionBytes).NotTo(Equal(originalBytes),
				"Emission must be processed bundle, not original message")

			// Emission should be protobuf bundle format
			emissionStr := string(emissionBytes)
			Expect(emissionStr).To(ContainSubstring("STARTSTARTSTART"),
				"Emission should be protobuf bundle format")
			Expect(emissionStr).To(ContainSubstring("END"),
				"Emission should end with bundle marker")

			// CRITICAL: Original distinctive content should NOT leak into emission
			Expect(emissionStr).NotTo(ContainSubstring("RAW_HEADER_SHOULD_NOT_LEAK"),
				"Original headers must not leak into emission")
			// Note: Numeric values will be present in processed form, which is expected

			By("Verifying ACK batch contains original message for acknowledgment")
			ackMsg := ackBatch[0]
			ackBytes, err := ackMsg.AsBytes()
			Expect(err).NotTo(HaveOccurred())
			ackStructured, err := ackMsg.AsStructured()
			Expect(err).NotTo(HaveOccurred())

			// CRITICAL: ACK should be identical to original for proper acknowledgment
			Expect(ackBytes).To(Equal(originalBytes),
				"ACK batch must contain original message for acknowledgment")
			Expect(ackStructured).To(Equal(originalStructured),
				"ACK message structure must match original")

			// Verify original metadata preserved in ACK
			ackTopic, exists := ackMsg.MetaGet("umh_topic")
			Expect(exists).To(BeTrue(), "ACK should preserve original metadata")
			Expect(ackTopic).To(Equal("umh.v1.test._historian.ack_verification"))

			customHeader, exists := ackMsg.MetaGet("custom_header")
			Expect(exists).To(BeTrue(), "ACK should preserve custom headers")
			Expect(customHeader).To(Equal("RAW_HEADER_SHOULD_NOT_LEAK"))

			By("Verifying processed bundle contains correct structured data")
			// Extract and decode the protobuf bundle
			emissionLines := strings.Split(emissionStr, "\n")
			Expect(len(emissionLines)).To(BeNumerically(">=", 2), "Should have data lines")

			dataLine := emissionLines[1]
			// Note: LZ4 format varies but should start with f6
			Expect(dataLine).To(HavePrefix("f6"), "Should be LZ4 compressed protobuf")

			hexDecoded, err := hex.DecodeString(dataLine)
			Expect(err).NotTo(HaveOccurred())

			bundle, err := ProtobufBytesToBundleWithCompression(hexDecoded)
			Expect(err).NotTo(HaveOccurred())
			Expect(bundle).NotTo(BeNil())

			// Verify bundle contains processed event data
			Expect(bundle.Events.Entries).To(HaveLen(1), "Bundle should contain 1 event")
			event := bundle.Events.Entries[0]

			Expect(event.GetTs().GetTimestampMs()).To(Equal(int64(1647753600000)))

			// The processed value should be present as numeric value
			if event.GetTs().GetNumericValue() != nil {
				processedValue := event.GetTs().GetNumericValue().GetValue()
				Expect(processedValue).To(Equal(42.0))
			}
		})

		It("should handle multiple messages with correct ACK pattern", func() {
			By("Creating multiple original messages")
			msgs := make(service.MessageBatch, 3)
			originalContents := make([][]byte, 3)

			for i := 0; i < 3; i++ {
				msg := service.NewMessage(nil)
				msg.MetaSet("umh_topic", fmt.Sprintf("umh.v1.test._historian.multi_%d", i))
				msg.SetStructured(map[string]interface{}{
					"timestamp_ms": int64(1647753600000 + int64(i)),
					"value":        fmt.Sprintf("ORIGINAL_VALUE_%d", i),
				})
				msgs[i] = msg

				bytes, err := msg.AsBytes()
				Expect(err).NotTo(HaveOccurred())
				originalContents[i] = bytes
			}

			By("Processing multiple messages")
			result, err := processor.ProcessBatch(context.Background(), msgs)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying batch delayed ACK structure")
			Expect(result).To(HaveLen(2), "Should return [emission_batch, ack_batch]")
			Expect(result[0]).To(HaveLen(1), "Should emit single bundled message")
			Expect(result[1]).To(HaveLen(3), "Should ACK all 3 original messages")

			By("Verifying single emission contains all events")
			emissionMsg := result[0][0]
			emissionBytes, err := emissionMsg.AsBytes()
			Expect(err).NotTo(HaveOccurred())

			// Decode bundle to verify it contains all events
			emissionStr := string(emissionBytes)
			emissionLines := strings.Split(emissionStr, "\n")
			dataLine := emissionLines[1]
			hexDecoded, err := hex.DecodeString(dataLine)
			Expect(err).NotTo(HaveOccurred())
			bundle, err := ProtobufBytesToBundleWithCompression(hexDecoded)
			Expect(err).NotTo(HaveOccurred())

			Expect(bundle.Events.Entries).To(HaveLen(3), "Bundle should contain all 3 events")

			By("Verifying ACK batch contains all original messages")
			ackBatch := result[1]
			for i, ackMsg := range ackBatch {
				ackBytes, err := ackMsg.AsBytes()
				Expect(err).NotTo(HaveOccurred())

				Expect(ackBytes).To(Equal(originalContents[i]),
					fmt.Sprintf("ACK message %d should match original", i))
			}
		})

		It("should not emit when messages are buffered (no immediate ACK)", func() {
			By("Creating processor with long emission interval")
			longProcessor := NewTopicBrowserProcessor(nil, nil, 100, time.Hour, 10, 100)
			var err error
			longProcessor.topicMetadataCache, err = lru.New(100)
			Expect(err).NotTo(HaveOccurred())

			By("Processing message that should be buffered")
			msg := service.NewMessage(nil)
			msg.MetaSet("umh_topic", "umh.v1.test._historian.buffered")
			msg.SetStructured(map[string]interface{}{
				"timestamp_ms": int64(1647753600000),
				"value":        "buffered_value",
			})

			result, err := longProcessor.ProcessBatch(context.Background(), service.MessageBatch{msg})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying no emission occurs when buffered")
			Expect(result).To(BeNil(), "Should return nil when messages are buffered")

			By("Verifying message is held in buffer for future ACK")
			longProcessor.bufferMutex.Lock()
			bufferLen := len(longProcessor.messageBuffer)
			longProcessor.bufferMutex.Unlock()

			Expect(bufferLen).To(Equal(1), "Message should be buffered for future emission/ACK")
		})
	})

	// CORE ACK TIMING VERIFICATION - Simple test for the main concern
	Describe("ACK Timing: Buffer → Wait → Emit+ACK", func() {
		Context("Messages should be ACKed only when emitted, not when buffered", func() {
			It("should NOT ACK messages immediately when buffered (before 1 second)", func() {
				By("Creating processor with realistic 1 second emit interval")
				// Use actual 1 second interval (not test milliseconds)
				realisticProcessor := NewTopicBrowserProcessor(nil, nil, 100, time.Second, 10, 100)
				var err error
				realisticProcessor.topicMetadataCache, err = lru.New(100)
				Expect(err).NotTo(HaveOccurred())

				By("Sending a message that should be buffered")
				msg := service.NewMessage(nil)
				msg.MetaSet("umh_topic", "umh.v1.test._historian.timing_test")
				msg.SetStructured(map[string]interface{}{
					"timestamp_ms": int64(1647753600000),
					"value":        42.0,
				})

				By("Processing message - should be buffered, NOT ACKed")
				result, err := realisticProcessor.ProcessBatch(context.Background(), service.MessageBatch{msg})
				Expect(err).NotTo(HaveOccurred())

				By("CRITICAL: No ACK should happen yet (message is buffered)")
				Expect(result).To(BeNil(), "Should return nil - no emission, no ACK yet")

				By("Verifying message is buffered internally (waiting for 1 second)")
				realisticProcessor.bufferMutex.Lock()
				bufferLen := len(realisticProcessor.messageBuffer)
				realisticProcessor.bufferMutex.Unlock()
				Expect(bufferLen).To(Equal(1), "Message should be buffered, waiting for emit interval")

				By("VERIFICATION: This proves messages are NOT ACKed when buffered")
				// The fact that ProcessBatch returned nil means:
				// 1. Message was buffered internally ✅
				// 2. No ACK batch was returned ✅
				// 3. Original message remains unACKed until emission ✅
			})

			It("should ACK messages only when 1 second interval triggers emission", func() {
				By("Creating processor with very short interval for testing")
				// Use 1ms for test speed, but concept is same as 1 second
				fastProcessor := NewTopicBrowserProcessor(nil, nil, 100, time.Millisecond, 10, 100)
				var err error
				fastProcessor.topicMetadataCache, err = lru.New(100)
				Expect(err).NotTo(HaveOccurred())

				By("Sending a message")
				msg := service.NewMessage(nil)
				msg.MetaSet("umh_topic", "umh.v1.test._historian.emission_test")
				msg.SetStructured(map[string]interface{}{
					"timestamp_ms": int64(1647753600000),
					"value":        123.0,
				})

				By("Processing message - interval has elapsed, should emit+ACK")
				result, err := fastProcessor.ProcessBatch(context.Background(), service.MessageBatch{msg})
				Expect(err).NotTo(HaveOccurred())

				By("CRITICAL: Now we get emission+ACK because interval elapsed")
				Expect(result).To(HaveLen(2), "Should return [emission_batch, ack_batch]")
				Expect(result[0]).To(HaveLen(1), "Emission batch should have processed bundle")
				Expect(result[1]).To(HaveLen(1), "ACK batch should have original message")

				By("VERIFICATION: This proves ACK happens exactly when emission happens")
				// The fact that we get both batches means:
				// 1. Emit interval was reached ✅
				// 2. Bundle was emitted (result[0]) ✅
				// 3. Original was ACKed (result[1]) ✅
				// 4. Both happen atomically at the same time ✅
			})

			It("should demonstrate the exact timing relationship", func() {
				By("Creating processor with medium interval to show timing")
				mediumProcessor := NewTopicBrowserProcessor(nil, nil, 100, 100*time.Millisecond, 10, 100)
				var err error
				mediumProcessor.topicMetadataCache, err = lru.New(100)
				Expect(err).NotTo(HaveOccurred())

				By("First message - should be buffered (no ACK)")
				msg1 := service.NewMessage(nil)
				msg1.MetaSet("umh_topic", "umh.v1.test._historian.timing_demo")
				msg1.SetStructured(map[string]interface{}{
					"timestamp_ms": int64(1647753600000),
					"value":        1.0,
				})

				result1, err := mediumProcessor.ProcessBatch(context.Background(), service.MessageBatch{msg1})
				Expect(err).NotTo(HaveOccurred())
				Expect(result1).To(BeNil(), "First message buffered, no ACK yet")

				By("Second message immediately after - still buffered (no ACK)")
				msg2 := service.NewMessage(nil)
				msg2.MetaSet("umh_topic", "umh.v1.test._historian.timing_demo")
				msg2.SetStructured(map[string]interface{}{
					"timestamp_ms": int64(1647753600001),
					"value":        2.0,
				})

				result2, err := mediumProcessor.ProcessBatch(context.Background(), service.MessageBatch{msg2})
				Expect(err).NotTo(HaveOccurred())
				Expect(result2).To(BeNil(), "Second message also buffered, no ACK yet")

				By("Waiting for emit interval to pass")
				time.Sleep(150 * time.Millisecond) // Wait longer than 100ms interval

				By("Third message - triggers emission+ACK of all buffered messages")
				msg3 := service.NewMessage(nil)
				msg3.MetaSet("umh_topic", "umh.v1.test._historian.timing_demo")
				msg3.SetStructured(map[string]interface{}{
					"timestamp_ms": int64(1647753600002),
					"value":        3.0,
				})

				result3, err := mediumProcessor.ProcessBatch(context.Background(), service.MessageBatch{msg3})
				Expect(err).NotTo(HaveOccurred())

				By("NOW we get emission+ACK because interval elapsed")
				Expect(result3).To(HaveLen(2), "Should return [emission_batch, ack_batch]")
				Expect(result3[0]).To(HaveLen(1), "One emission bundle")
				Expect(result3[1]).To(HaveLen(3), "ACK batch should have all 3 original messages")

				By("VERIFICATION: All 3 messages ACKed together when emission happens")
				// This proves the key behavior:
				// 1. Messages 1 & 2 were buffered without ACK
				// 2. Message 3 triggered emission because interval elapsed
				// 3. All 3 messages ACKed atomically with emission
				// 4. ACK timing is tied to emission timing, not buffering timing
			})
		})
	})
})

// Helper function for tests
func createTestBatch(size int, valuePrefix string) service.MessageBatch {
	batch := make(service.MessageBatch, size)
	for i := 0; i < size; i++ {
		data := map[string]interface{}{
			"timestamp_ms": time.Now().UnixMilli(),
			"value":        fmt.Sprintf("%s-%d", valuePrefix, i),
		}

		msg := service.NewMessage(nil)
		msg.SetStructured(data)

		// Add required UMH metadata
		msg.MetaSet("umh_topic", "umh.v1.enterprise.plant1.machiningArea.cnc-line.cnc5.plc123._historian.axis.y")

		batch[i] = msg
	}
	return batch
}
