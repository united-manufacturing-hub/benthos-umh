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
func BooleanBehaviorEntry(description string, values []bool, expectedKept []bool) TableEntry {
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
