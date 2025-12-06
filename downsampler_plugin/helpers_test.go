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

package downsampler_plugin_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// BehaviorTestCase represents a test case for DescribeTable
type BehaviorTestCase struct {
	Description    string
	Config         string
	InputMessages  []TestMessage
	ExpectedKept   int
	ExpectedValues []interface{}
}

// StreamTestCase represents an enhanced test case for comprehensive behavior testing
type StreamTestCase struct {
	Name            string
	Description     string
	Config          string
	Input           []TestMessage
	ExpectedOutput  []ExpectedMessage
	ExpectedMetrics map[string]int
}

// ExpectedMessage represents expected output message properties
type ExpectedMessage struct {
	Value         interface{}
	TimestampMs   int64
	Topic         string
	HasMetadata   map[string]string
	ShouldContain []string // For partial metadata checks
}

// TestMessage represents an input message
type TestMessage struct {
	Value       interface{}
	TimestampMs int64
	Topic       string
}

// SetupDownsamplerStream creates a Benthos stream with the downsampler processor
func SetupDownsamplerStream(config string) (service.MessageHandlerFunc, *[]*service.Message, func()) {
	builder := service.NewStreamBuilder()

	msgHandler, err := builder.AddProducerFunc()
	Expect(err).NotTo(HaveOccurred())

	err = builder.AddProcessorYAML(config)
	Expect(err).NotTo(HaveOccurred())

	var messages []*service.Message
	err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
		messages = append(messages, msg)
		return nil
	})
	Expect(err).NotTo(HaveOccurred())

	stream, err := builder.Build()
	Expect(err).NotTo(HaveOccurred())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

	go func() {
		_ = stream.Run(ctx)
	}()

	cleanup := func() {
		cancel()
		stream.Stop(context.Background())
	}

	return msgHandler, &messages, cleanup
}

// SendTestMessage sends a test message with proper UMH format
func SendTestMessage(msgHandler service.MessageHandlerFunc, testMsg TestMessage) {
	msg := service.NewMessage(nil)
	msg.SetStructured(map[string]interface{}{
		"value":        testMsg.Value,
		"timestamp_ms": testMsg.TimestampMs,
	})

	if testMsg.Topic != "" {
		msg.MetaSet("umh_topic", testMsg.Topic)
	}

	fmt.Printf("      📤 SENDING: value=%v, timestamp=%d, topic=%s\n",
		testMsg.Value, testMsg.TimestampMs, testMsg.Topic)

	err := msgHandler(context.Background(), msg)
	Expect(err).NotTo(HaveOccurred())
}

// VerifyBehaviorTestCase verifies the results of a behavior test case
func VerifyBehaviorTestCase(testCase BehaviorTestCase) {
	By("Setting up the downsampler stream")
	msgHandler, messages, cleanup := SetupDownsamplerStream(testCase.Config)
	DeferCleanup(cleanup)

	By("Sending all input messages")
	for _, inputMsg := range testCase.InputMessages {
		SendTestMessage(msgHandler, inputMsg)
	}

	By("Verifying the expected number of messages are kept")
	Eventually(func() int {
		return len(*messages)
	}).Should(Equal(testCase.ExpectedKept),
		"Expected %d messages to be kept", testCase.ExpectedKept)

	By("Verifying the kept message values")
	for i, expectedValue := range testCase.ExpectedValues {
		Expect(i).To(BeNumerically("<", len(*messages)),
			"Not enough messages received")

		structured, err := (*messages)[i].AsStructured()
		Expect(err).NotTo(HaveOccurred())

		payload := structured.(map[string]interface{})
		Expect(payload["value"]).To(Equal(expectedValue),
			"Message %d value should match expected", i)

		// Verify downsampler metadata
		downsampledBy, exists := (*messages)[i].MetaGet("downsampled_by")
		Expect(exists).To(BeTrue(), "Message should have downsampled_by metadata")
		Expect(downsampledBy).To(ContainSubstring("deadband"))
	}
}

// NumericBehaviorEntry creates a DescribeTable Entry for numeric threshold testing
func NumericBehaviorEntry(description string, threshold float64, values []float64, expectedKept []bool) TableEntry {
	var inputMessages []TestMessage
	var expectedValues []interface{}

	for i, value := range values {
		inputMessages = append(inputMessages, TestMessage{
			Value:       value,
			TimestampMs: int64(1000 + i*1000), // 1 second apart
			Topic:       "test.sensor",
		})

		if expectedKept[i] {
			expectedValues = append(expectedValues, value)
		}
	}

	expectedKeptCount := 0
	for _, kept := range expectedKept {
		if kept {
			expectedKeptCount++
		}
	}

	testCase := BehaviorTestCase{
		Description: description,
		Config: `
downsampler:
  default:
    deadband:
      threshold: ` + fmt.Sprintf("%.2f", threshold),
		InputMessages:  inputMessages,
		ExpectedKept:   expectedKeptCount,
		ExpectedValues: expectedValues,
	}

	return Entry(description, testCase)
}

// RunStreamTestCase executes a comprehensive stream test case
func RunStreamTestCase(testCase StreamTestCase) {
	By(fmt.Sprintf("Running test case: %s - %s", testCase.Name, testCase.Description))

	fmt.Printf("\n🧪 TEST CASE: %s\n", testCase.Name)
	fmt.Printf("📋 DESCRIPTION: %s\n", testCase.Description)
	fmt.Printf("⚙️  CONFIG:\n%s\n", testCase.Config)

	By("Setting up the downsampler stream")
	msgHandler, messages, cleanup := SetupDownsamplerStream(testCase.Config)
	DeferCleanup(cleanup)

	By("Sending all input messages")
	fmt.Printf("📤 SENDING %d INPUT MESSAGES:\n", len(testCase.Input))
	for i, inputMsg := range testCase.Input {
		fmt.Printf("  Input[%d]: ", i)
		SendTestMessage(msgHandler, inputMsg)
	}

	// Add a small delay to allow processing to complete
	time.Sleep(50 * time.Millisecond)

	// Send flush trigger messages to all topics that were used in the test
	// This forces algorithms like swinging door to emit any pending points
	By("Triggering flush for algorithms that need it")
	topicsUsed := make(map[string]bool)
	for _, inputMsg := range testCase.Input {
		if inputMsg.Topic != "" {
			topicsUsed[inputMsg.Topic] = true
		}
	}

	// Send an envelope-breaking message to each topic used
	for topic := range topicsUsed {
		flushTriggerMsg := service.NewMessage(nil)
		flushTriggerMsg.SetStructured(map[string]interface{}{
			"value":        999999.0, // Large value that will break any envelope
			"timestamp_ms": time.Now().UnixMilli(),
		})
		flushTriggerMsg.MetaSet("umh_topic", topic)
		_ = msgHandler(context.Background(), flushTriggerMsg)
	}

	// Small delay to allow flush trigger to process
	time.Sleep(50 * time.Millisecond)

	By("Verifying the expected number of messages are kept")

	// Wait for the expected messages with some extra time for potential flushed messages
	Eventually(func() int {
		// Filter out flush trigger messages (value=999999.0) from the count
		filteredMessages := make([]*service.Message, 0, len(*messages))
		for _, msg := range *messages {
			structured, _ := msg.AsStructured()
			if payload, ok := structured.(map[string]interface{}); ok {
				if value, ok := payload["value"].(float64); ok && value == 999999.0 {
					continue // Skip flush trigger messages
				}
			}
			filteredMessages = append(filteredMessages, msg)
		}

		currentCount := len(filteredMessages)
		fmt.Printf("📊 CURRENT MESSAGE COUNT: %d (expecting %d, total including flush triggers: %d)\n",
			currentCount, len(testCase.ExpectedOutput), len(*messages))
		if currentCount > 0 {
			fmt.Printf("📥 RECEIVED MESSAGES SO FAR (excluding flush triggers):\n")
			for i, msg := range filteredMessages {
				structured, _ := msg.AsStructured()
				if payload, ok := structured.(map[string]interface{}); ok {
					topic, _ := msg.MetaGet("umh_topic")
					metadata, _ := msg.MetaGet("downsampled_by")
					fmt.Printf("  [%d] value=%v, timestamp=%v, topic=%s, metadata=%s\n",
						i, payload["value"], payload["timestamp_ms"], topic, metadata)
				}
			}
		}

		// Update the messages slice to only contain non-flush-trigger messages for verification
		if len(filteredMessages) >= len(testCase.ExpectedOutput) {
			*messages = filteredMessages
		}

		return currentCount
	}).Should(Equal(len(testCase.ExpectedOutput)),
		"Expected %d messages to be kept, got %d", len(testCase.ExpectedOutput), len(*messages))

	By("Verifying the kept message values and metadata")
	fmt.Printf("✅ VERIFYING %d EXPECTED MESSAGES:\n", len(testCase.ExpectedOutput))

	for i, expectedMsg := range testCase.ExpectedOutput {
		fmt.Printf("  Expected[%d]: value=%v, timestamp=%d, topic=%s\n",
			i, expectedMsg.Value, expectedMsg.TimestampMs, expectedMsg.Topic)

		Expect(i).To(BeNumerically("<", len(*messages)),
			"Not enough messages received")

		structured, err := (*messages)[i].AsStructured()
		Expect(err).NotTo(HaveOccurred())

		payload := structured.(map[string]interface{})

		fmt.Printf("  Actual[%d]:   value=%v, timestamp=%v", i, payload["value"], payload["timestamp_ms"])
		if topic, exists := (*messages)[i].MetaGet("umh_topic"); exists {
			fmt.Printf(", topic=%s", topic)
		}
		fmt.Printf("\n")

		Expect(payload["value"]).To(Equal(expectedMsg.Value),
			"Message %d value should match expected", i)

		// Verify timestamp remains constant
		if expectedMsg.TimestampMs != 0 {
			Expect(payload["timestamp_ms"]).To(Equal(expectedMsg.TimestampMs),
				"Message %d timestamp_ms should match expected", i)
		}

		// Verify topic remains constant
		if expectedMsg.Topic != "" {
			actualTopic, exists := (*messages)[i].MetaGet("umh_topic")
			Expect(exists).To(BeTrue(), "Message %d should have umh_topic metadata", i)
			Expect(actualTopic).To(Equal(expectedMsg.Topic),
				"Message %d topic should match expected", i)
		}

		// Verify required metadata
		for key, expectedValue := range expectedMsg.HasMetadata {
			actualValue, exists := (*messages)[i].MetaGet(key)
			Expect(exists).To(BeTrue(), "Message %d should have metadata %s", i, key)
			if expectedValue != "" {
				Expect(actualValue).To(ContainSubstring(expectedValue),
					"Message %d metadata %s should contain %s, got %s", i, key, expectedValue, actualValue)
			}
		}

		// Verify partial metadata contains
		for _, shouldContain := range expectedMsg.ShouldContain {
			found := false
			// Check all metadata keys for the substring
			(*messages)[i].MetaWalk(func(key, value string) error {
				if strings.Contains(value, shouldContain) {
					found = true
				}
				return nil
			})
			Expect(found).To(BeTrue(),
				"Message %d should have metadata containing '%s'", i, shouldContain)
		}
	}

	// Note: Metrics verification would require access to processor metrics
	// For now, we skip metrics verification as it requires more complex setup
	if len(testCase.ExpectedMetrics) > 0 {
		By("Metrics verification would be implemented with processor access")
		// This could be enhanced by exposing processor metrics or using a test double
	}
}

// StringBehaviorEntry creates a DescribeTable Entry for string equality testing
func StringBehaviorEntry(description string, values []string, expectedKept []bool) TableEntry {
	var inputMessages []TestMessage
	var expectedValues []interface{}

	for i, value := range values {
		inputMessages = append(inputMessages, TestMessage{
			Value:       value,
			TimestampMs: int64(1000 + i*1000),
			Topic:       "test.status",
		})

		if expectedKept[i] {
			expectedValues = append(expectedValues, value)
		}
	}

	expectedKeptCount := 0
	for _, kept := range expectedKept {
		if kept {
			expectedKeptCount++
		}
	}

	testCase := BehaviorTestCase{
		Description: description,
		Config: `
downsampler:
  default:
    deadband:
      threshold: 1.0  # Ignored for strings
`,
		InputMessages:  inputMessages,
		ExpectedKept:   expectedKeptCount,
		ExpectedValues: expectedValues,
	}

	return Entry(description, testCase)
}

// BooleanBehaviorEntry creates a DescribeTable Entry for boolean equality testing
func BooleanBehaviorEntry(description string, values, expectedKept []bool) TableEntry {
	var inputMessages []TestMessage
	var expectedValues []interface{}

	for i, value := range values {
		inputMessages = append(inputMessages, TestMessage{
			Value:       value,
			TimestampMs: int64(1000 + i*1000),
			Topic:       "test.enabled",
		})

		if expectedKept[i] {
			expectedValues = append(expectedValues, value)
		}
	}

	expectedKeptCount := 0
	for _, kept := range expectedKept {
		if kept {
			expectedKeptCount++
		}
	}

	testCase := BehaviorTestCase{
		Description: description,
		Config: `
downsampler:
  default:
    deadband:
      threshold: 1.0  # Ignored for booleans
`,
		InputMessages:  inputMessages,
		ExpectedKept:   expectedKeptCount,
		ExpectedValues: expectedValues,
	}

	return Entry(description, testCase)
}
