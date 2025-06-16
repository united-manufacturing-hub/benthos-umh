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
	"encoding/json"
	"os"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("Flush and Recreate Functionality", func() {
	BeforeEach(func() {
		testActivated := os.Getenv("TEST_DOWNSAMPLER")
		if testActivated == "" {
			Skip("Skipping Downsampler tests: TEST_DOWNSAMPLER not set")
			return
		}
	})

	It("should flush and recreate processor when threshold changes via metadata", func() {
		builder := service.NewStreamBuilder()

		var msgHandler service.MessageHandlerFunc
		msgHandler, err := builder.AddProducerFunc()
		Expect(err).NotTo(HaveOccurred())

		// Configure downsampler with default deadband and allow metadata overrides
		err = builder.AddProcessorYAML(`
downsampler:
  allow_meta_overrides: true
  default:
    deadband:
      threshold: 1.0
      max_time: 30s
`)
		Expect(err).NotTo(HaveOccurred())

		var (
			messages []*service.Message
			mtx      sync.Mutex
		)
		err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
			mtx.Lock()
			messages = append(messages, msg)
			mtx.Unlock()
			return nil
		})
		Expect(err).NotTo(HaveOccurred())

		stream, err := builder.Build()
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		go func() {
			_ = stream.Run(ctx)
		}()

		baseTime := time.Now()

		// Phase 1: Send messages with default threshold (1.0)
		msg1 := createTimeSeriesMessage(10.0, baseTime, "test.parameter_change")
		err = msgHandler(ctx, msg1)
		Expect(err).NotTo(HaveOccurred())

		// Small change (0.5) - should be filtered with default threshold (1.0)
		msg2 := createTimeSeriesMessage(10.5, baseTime.Add(time.Second), "test.parameter_change")
		err = msgHandler(ctx, msg2)
		Expect(err).NotTo(HaveOccurred())

		// Wait for initial messages
		Eventually(func() int {
			mtx.Lock()
			n := len(messages)
			mtx.Unlock()
			return n
		}, "3s").Should(Equal(1)) // Only first message should pass

		// Phase 2: Change threshold to 0.1 via metadata - THIS IS THE KEY TEST
		msg3 := createTimeSeriesMessage(10.7, baseTime.Add(2*time.Second), "test.parameter_change")
		msg3.MetaSet("ds_threshold", "0.1") // New smaller threshold
		err = msgHandler(ctx, msg3)
		Expect(err).NotTo(HaveOccurred())

		// Small change (0.05) with new threshold - should be filtered
		msg4 := createTimeSeriesMessage(10.75, baseTime.Add(3*time.Second), "test.parameter_change")
		msg4.MetaSet("ds_threshold", "0.1")
		err = msgHandler(ctx, msg4)
		Expect(err).NotTo(HaveOccurred())

		// Larger change (0.3) with new threshold - should pass
		msg5 := createTimeSeriesMessage(11.0, baseTime.Add(4*time.Second), "test.parameter_change")
		msg5.MetaSet("ds_threshold", "0.1")
		err = msgHandler(ctx, msg5)
		Expect(err).NotTo(HaveOccurred())

		// Wait for all processing
		Eventually(func() int {
			mtx.Lock()
			n := len(messages)
			mtx.Unlock()
			return n
		}, "5s").Should(Equal(3)) // Should have: msg1, msg3 (first with new threshold), msg5

		// Verify the messages
		mtx.Lock()
		messageCount := len(messages)
		messagesCopy := make([]*service.Message, len(messages))
		copy(messagesCopy, messages)
		mtx.Unlock()

		Expect(messageCount).To(Equal(3))

		// Check first message (baseline)
		structured, err := messagesCopy[0].AsStructured()
		Expect(err).NotTo(HaveOccurred())
		data := structured.(map[string]interface{})

		var val float64
		switch v := data["value"].(type) {
		case float64:
			val = v
		case json.Number:
			val, err = v.Float64()
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(val).To(Equal(10.0))

		// Check second message (first with new threshold)
		structured, err = messagesCopy[1].AsStructured()
		Expect(err).NotTo(HaveOccurred())
		data = structured.(map[string]interface{})

		switch v := data["value"].(type) {
		case float64:
			val = v
		case json.Number:
			val, err = v.Float64()
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(val).To(Equal(10.7))

		// Check third message (passed with new threshold)
		structured, err = messagesCopy[2].AsStructured()
		Expect(err).NotTo(HaveOccurred())
		data = structured.(map[string]interface{})

		switch v := data["value"].(type) {
		case float64:
			val = v
		case json.Number:
			val, err = v.Float64()
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(val).To(Equal(11.0))

		// Verify all messages have correct algorithm metadata
		for _, msg := range messagesCopy {
			downsampledBy, exists := msg.MetaGet("downsampled_by")
			Expect(exists).To(BeTrue())
			Expect(downsampledBy).To(Equal("deadband"))
		}
	})

	It("should handle algorithm switching correctly (full recreation)", func() {
		builder := service.NewStreamBuilder()

		var msgHandler service.MessageHandlerFunc
		msgHandler, err := builder.AddProducerFunc()
		Expect(err).NotTo(HaveOccurred())

		err = builder.AddProcessorYAML(`
downsampler:
  allow_meta_overrides: true
  default:
    deadband:
      threshold: 1.0
`)
		Expect(err).NotTo(HaveOccurred())

		var (
			messages []*service.Message
			mtx      sync.Mutex
		)
		err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
			mtx.Lock()
			messages = append(messages, msg)
			mtx.Unlock()
			return nil
		})
		Expect(err).NotTo(HaveOccurred())

		stream, err := builder.Build()
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		go func() {
			_ = stream.Run(ctx)
		}()

		baseTime := time.Now()

		// Start with deadband
		msg1 := createTimeSeriesMessage(10.0, baseTime, "test.algorithm_switch")
		err = msgHandler(ctx, msg1)
		Expect(err).NotTo(HaveOccurred())

		// Switch to swinging_door
		msg2 := createTimeSeriesMessage(11.0, baseTime.Add(time.Second), "test.algorithm_switch")
		msg2.MetaSet("ds_algorithm", "swinging_door")
		msg2.MetaSet("ds_threshold", "0.5")
		err = msgHandler(ctx, msg2)
		Expect(err).NotTo(HaveOccurred())

		// Wait for processing
		Eventually(func() int {
			mtx.Lock()
			n := len(messages)
			mtx.Unlock()
			return n
		}, "5s").Should(BeNumerically(">=", 2))

		mtx.Lock()
		messagesCopy := make([]*service.Message, len(messages))
		copy(messagesCopy, messages)
		mtx.Unlock()

		// Verify first message uses deadband
		downsampledBy, exists := messagesCopy[0].MetaGet("downsampled_by")
		Expect(exists).To(BeTrue())
		Expect(downsampledBy).To(Equal("deadband"))

		// Verify second message uses swinging_door
		downsampledBy, exists = messagesCopy[1].MetaGet("downsampled_by")
		Expect(exists).To(BeTrue())
		Expect(downsampledBy).To(Equal("swinging_door"))
	})

	It("should handle max_time parameter changes", func() {
		builder := service.NewStreamBuilder()

		var msgHandler service.MessageHandlerFunc
		msgHandler, err := builder.AddProducerFunc()
		Expect(err).NotTo(HaveOccurred())

		err = builder.AddProcessorYAML(`
downsampler:
  allow_meta_overrides: true
  default:
    deadband:
      threshold: 5.0  # High threshold to prevent normal filtering
      max_time: 10s   # Default max_time
`)
		Expect(err).NotTo(HaveOccurred())

		var (
			messages []*service.Message
			mtx      sync.Mutex
		)
		err = builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
			mtx.Lock()
			messages = append(messages, msg)
			mtx.Unlock()
			return nil
		})
		Expect(err).NotTo(HaveOccurred())

		stream, err := builder.Build()
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		go func() {
			_ = stream.Run(ctx)
		}()

		baseTime := time.Now()

		// Send first message with default max_time
		msg1 := createTimeSeriesMessage(100.0, baseTime, "test.maxtime_change")
		err = msgHandler(ctx, msg1)
		Expect(err).NotTo(HaveOccurred())

		// Send second message with changed max_time
		msg2 := createTimeSeriesMessage(101.0, baseTime.Add(time.Second), "test.maxtime_change")
		msg2.MetaSet("ds_max_time", "2s") // Change max_time to 2s
		err = msgHandler(ctx, msg2)
		Expect(err).NotTo(HaveOccurred())

		// Wait for processing
		Eventually(func() int {
			mtx.Lock()
			n := len(messages)
			mtx.Unlock()
			return n
		}, "10s").Should(BeNumerically(">=", 2))

		// At minimum we should get both messages
		mtx.Lock()
		messageCount := len(messages)
		mtx.Unlock()

		Expect(messageCount).To(BeNumerically(">=", 2))
	})
})
