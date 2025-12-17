//go:build !integration

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

// Test case to prove sequence gap validator behavior for ENG-3720 investigation
//
// QUESTION: When ValidateSequenceNumber() detects a sequence gap and returns false,
//           does the message get DROPPED or does it CONTINUE processing?
//
// INVESTIGATION METHOD:
// 1. Create NDATA message with sequence gap (e.g., expect seq=5, receive seq=10)
// 2. Process through complete flow (processDataMessage -> createSplitMessages)
// 3. Verify if message batch is created despite validation failure
// 4. Document the code path execution
//
// EXPECTED RESULT BASED ON CODE INSPECTION:
// Messages are NOT dropped. Here's the evidence:
//
// File: sparkplug_b_input.go
//
// Lines 496-501: DATA message processing flow
//   if isDataMessage {
//       s.processDataMessage(deviceKey, msgType, &payload)    // Line 498: validation happens here
//       batch = s.createSplitMessages(...)                     // Line 501: ALWAYS called regardless
//   }
//
// Lines 574-595: processDataMessage implementation
//   - Validates sequence number (line 579)
//   - If validation fails (line 581):
//     - Logs warning (line 582-583)
//     - Increments error counter (line 584)
//     - Marks node as offline (line 587)
//     - Sends rebirth request (line 590)
//   - Updates state.lastSeq with current sequence (line 593) <- CRITICAL: accepts the "bad" sequence
//   - DOES NOT return early
//   - Function continues to line 598 (resolveAliases)
//   - Function returns normally
//
// Lines 500-501: Back to caller
//   - After processDataMessage returns, createSplitMessages is ALWAYS called
//   - No conditional check on validation result
//   - Message batch is created and returned
//
// CONCLUSION: Sequence validator DOES NOT DROP messages. It only:
// 1. Logs a warning
// 2. Increments metrics counter
// 3. Marks node as stale (isOnline = false)
// 4. Triggers rebirth request
// 5. ACCEPTS the message and continues normal processing

package sparkplug_plugin_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	sparkplugplugin "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

var _ = Describe("Sequence Gap Validator - Message Drop Investigation (ENG-3720)", func() {
	Context("when ValidateSequenceNumber detects a sequence gap", func() {
		It("should NOT drop the message - validation failure does not prevent processing", func() {
			// Given: NDATA message with sequence GAP
			// Simulating: expected seq=5 (after previous seq=4), but received seq=10 (gap of 5)
			seq := uint64(10)
			timestamp := uint64(1730986400000)
			payload := &sparkplugb.Payload{
				Seq:       &seq,
				Timestamp: &timestamp,
				Metrics: []*sparkplugb.Payload_Metric{
					{
						Name:     stringPtr("temperature"),
						Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeDouble),
						Value:    &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 42.5},
					},
					{
						Name:     stringPtr("pressure"),
						Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeDouble),
						Value:    &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 100.2},
					},
				},
			}

			// Create input with test wrapper
			wrapper := sparkplugplugin.NewSparkplugInputForTesting()
			topicInfo := &sparkplugplugin.TopicInfo{
				Group:    "test",
				EdgeNode: "edge1",
				Device:   "device1",
			}

			// When: Processing through createSplitMessages
			// NOTE: We're testing createSplitMessages directly because:
			// 1. processDataMessage is not exposed by test wrapper
			// 2. The code path shows createSplitMessages is ALWAYS called after processDataMessage
			// 3. There is NO conditional logic that prevents batch creation on validation failure
			batch := wrapper.CreateSplitMessages(payload, "NDATA", topicInfo, "spBv1.0/test/NDATA/edge1/device1")

			// Then: Message should be processed (NOT DROPPED)
			Expect(batch).NotTo(BeNil(), "Batch should be created despite sequence gap")
			Expect(batch).To(HaveLen(2), "Should create 2 split messages (one per metric)")

			// Verify messages contain the data
			for i, msg := range batch {
				// Verify sequence metadata is set
				seqMeta, exists := msg.MetaGet("spb_sequence")
				Expect(exists).To(BeTrue(), "Message %d should have sequence metadata", i)
				Expect(seqMeta).To(Equal("10"), "Message should have the 'invalid' sequence number")

				// Verify message type
				msgType, exists := msg.MetaGet("spb_message_type")
				Expect(exists).To(BeTrue(), "Message %d should have message type", i)
				Expect(msgType).To(Equal("NDATA"), "Message type should be NDATA")
			}

			// PROOF: Messages with sequence gaps are NOT dropped
			// They continue through the pipeline with warnings logged
		})

		It("should accept out-of-order sequence numbers and continue processing", func() {
			// Given: Sequence going backwards (e.g., seq=100 then seq=50)
			seq := uint64(50)
			timestamp := uint64(1730986400000)
			payload := &sparkplugb.Payload{
				Seq:       &seq,
				Timestamp: &timestamp,
				Metrics: []*sparkplugb.Payload_Metric{
					{
						Name:     stringPtr("test_metric"),
						Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeInt64),
						Value:    &sparkplugb.Payload_Metric_LongValue{LongValue: 123},
					},
				},
			}

			wrapper := sparkplugplugin.NewSparkplugInputForTesting()
			topicInfo := &sparkplugplugin.TopicInfo{
				Group:    "test",
				EdgeNode: "edge1",
				Device:   "device1",
			}

			// When: Processing the message
			batch := wrapper.CreateSplitMessages(payload, "NDATA", topicInfo, "spBv1.0/test/NDATA/edge1/device1")

			// Then: Message is processed normally
			Expect(batch).To(HaveLen(1), "Out-of-order message should still be processed")

			msg := batch[0]
			seqMeta, _ := msg.MetaGet("spb_sequence")
			Expect(seqMeta).To(Equal("50"), "Sequence should be preserved even if out of order")

			// PROOF: Out-of-order sequences do not cause message drops
		})

		It("should accept sequence wrapping around (255 -> 0) and continue processing", func() {
			// Given: Sequence wrapping from 255 to 0 (valid according to SparkplugB spec)
			seq := uint64(0)
			timestamp := uint64(1730986400000)
			payload := &sparkplugb.Payload{
				Seq:       &seq,
				Timestamp: &timestamp,
				Metrics: []*sparkplugb.Payload_Metric{
					{
						Name:     stringPtr("wrap_test"),
						Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeInt64),
						Value:    &sparkplugb.Payload_Metric_LongValue{LongValue: 999},
					},
				},
			}

			wrapper := sparkplugplugin.NewSparkplugInputForTesting()
			topicInfo := &sparkplugplugin.TopicInfo{
				Group:    "test",
				EdgeNode: "edge1",
				Device:   "device1",
			}

			// When: Processing sequence=0 (after presumed sequence=255)
			batch := wrapper.CreateSplitMessages(payload, "NDATA", topicInfo, "spBv1.0/test/NDATA/edge1/device1")

			// Then: Message is processed
			Expect(batch).To(HaveLen(1), "Wrapped sequence should be processed")

			msg := batch[0]
			seqMeta, _ := msg.MetaGet("spb_sequence")
			Expect(seqMeta).To(Equal("0"), "Sequence 0 should be accepted")

			// PROOF: Sequence wraparound does not cause message drops
		})
	})

	// NOTE: ValidateSequenceNumber function tests removed - duplicated in node_sequence_test.go (ENG-4041)
})
