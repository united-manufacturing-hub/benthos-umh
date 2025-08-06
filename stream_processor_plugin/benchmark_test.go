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
	"fmt"
	"testing"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/config"
	processor2 "github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/processor"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// BenchmarkProcessBatch measures the performance of batch message processing
func BenchmarkProcessBatch(b *testing.B) {
	ctx := context.Background()

	// Create processor with test configuration
	processor, err := createTestProcessor()
	if err != nil {
		b.Fatalf("Failed to create processor: %v", err)
	}

	b.Run("StaticMapping", func(b *testing.B) {
		// Test static mapping performance - should be fastest
		inputPayload := processor2.TimeseriesMessage{
			Value:       25.5,
			TimestampMs: time.Now().UnixMilli(),
		}
		payloadBytes, _ := json.Marshal(inputPayload)

		inputMsg := service.NewMessage(payloadBytes)
		inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
			if err != nil {
				b.Fatalf("ProcessBatch failed: %v", err)
			}
		}
	})

	b.Run("DynamicMapping", func(b *testing.B) {
		// Test dynamic mapping performance - includes JS evaluation
		inputPayload := processor2.TimeseriesMessage{
			Value:       25.5,
			TimestampMs: time.Now().UnixMilli(),
		}
		payloadBytes, _ := json.Marshal(inputPayload)

		inputMsg := service.NewMessage(payloadBytes)
		inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
			if err != nil {
				b.Fatalf("ProcessBatch failed: %v", err)
			}
		}
	})

	b.Run("MixedMappings", func(b *testing.B) {
		// Test mixed static and dynamic mappings
		inputPayload := processor2.TimeseriesMessage{
			Value:       25.5,
			TimestampMs: time.Now().UnixMilli(),
		}
		payloadBytes, _ := json.Marshal(inputPayload)

		inputMsg := service.NewMessage(payloadBytes)
		inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
			if err != nil {
				b.Fatalf("ProcessBatch failed: %v", err)
			}
		}
	})

	b.Run("UnknownTopic", func(b *testing.B) {
		// Test performance with unknown topics (should be fast skip)
		inputPayload := processor2.TimeseriesMessage{
			Value:       25.5,
			TimestampMs: time.Now().UnixMilli(),
		}
		payloadBytes, _ := json.Marshal(inputPayload)

		inputMsg := service.NewMessage(payloadBytes)
		inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.unknown")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
			if err != nil {
				b.Fatalf("ProcessBatch failed: %v", err)
			}
		}
	})

	b.Run("InvalidJSON", func(b *testing.B) {
		// Test performance with invalid JSON (should be fast skip)
		inputMsg := service.NewMessage([]byte("invalid json"))
		inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
			if err != nil {
				b.Fatalf("ProcessBatch failed: %v", err)
			}
		}
	})

	b.Run("MissingMetadata", func(b *testing.B) {
		// Test performance with missing umh_topic metadata (should be fastest skip)
		inputPayload := processor2.TimeseriesMessage{
			Value:       25.5,
			TimestampMs: time.Now().UnixMilli(),
		}
		payloadBytes, _ := json.Marshal(inputPayload)

		inputMsg := service.NewMessage(payloadBytes)
		// No umh_topic metadata set

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
			if err != nil {
				b.Fatalf("ProcessBatch failed: %v", err)
			}
		}
	})
}

// BenchmarkBatchProcessing measures performance with different batch sizes
func BenchmarkBatchProcessing(b *testing.B) {
	ctx := context.Background()

	processor, err := createTestProcessor()
	if err != nil {
		b.Fatalf("Failed to create processor: %v", err)
	}

	// Test different batch sizes
	batchSizes := []int{1, 10, 100, 1000}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", size), func(b *testing.B) {
			// Create batch with mixed messages
			batch := make(service.MessageBatch, size)
			for i := 0; i < size; i++ {
				inputPayload := processor2.TimeseriesMessage{
					Value:       25.5 + float64(i),
					TimestampMs: time.Now().UnixMilli(),
				}
				payloadBytes, _ := json.Marshal(inputPayload)

				msg := service.NewMessage(payloadBytes)
				// Alternate between different topics
				switch i % 3 {
				case 0:
					msg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")
				case 1:
					msg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.tempF")
				default:
					msg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.run")
				}
				batch[i] = msg
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := processor.ProcessBatch(ctx, batch)
				if err != nil {
					b.Fatalf("ProcessBatch failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkJavaScriptExpressions measures JS expression evaluation performance
func BenchmarkJavaScriptExpressions(b *testing.B) {
	ctx := context.Background()

	// Create processor configurations with different JS complexity
	configs := []struct {
		name    string
		mapping map[string]interface{}
	}{
		{
			name: "SimpleExpression",
			mapping: map[string]interface{}{
				"result": "press + 1",
			},
		},
		{
			name: "ComplexExpression",
			mapping: map[string]interface{}{
				"result": "Math.sqrt(press * press + tF * tF) / 2.0",
			},
		},
		{
			name: "StringConcatenation",
			mapping: map[string]interface{}{
				"result": `"Temperature: " + tF + "F, Pressure: " + press + " PSI"`,
			},
		},
		{
			name: "ConditionalExpression",
			mapping: map[string]interface{}{
				"result": "press > 20 ? 'high' : 'normal'",
			},
		},
		{
			name: "MathOperations",
			mapping: map[string]interface{}{
				"result": "Math.pow(press, 2) + Math.abs(tF - 32) * 1.8",
			},
		},
	}

	for _, config := range configs {
		b.Run(config.name, func(b *testing.B) {
			// Create processor with specific mapping
			processor, err := createProcessorWithMapping(config.mapping)
			if err != nil {
				b.Fatalf("Failed to create processor: %v", err)
			}

			// Create input message
			inputPayload := processor2.TimeseriesMessage{
				Value:       25.5,
				TimestampMs: time.Now().UnixMilli(),
			}
			payloadBytes, _ := json.Marshal(inputPayload)

			inputMsg := service.NewMessage(payloadBytes)
			inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
				if err != nil {
					b.Fatalf("ProcessBatch failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkVariableResolution measures state management performance
func BenchmarkVariableResolution(b *testing.B) {
	ctx := context.Background()

	processor, err := createTestProcessor()
	if err != nil {
		b.Fatalf("Failed to create processor: %v", err)
	}

	b.Run("FirstVariable", func(b *testing.B) {
		// Test performance when setting first variable
		inputPayload := processor2.TimeseriesMessage{
			Value:       25.5,
			TimestampMs: time.Now().UnixMilli(),
		}
		payloadBytes, _ := json.Marshal(inputPayload)

		inputMsg := service.NewMessage(payloadBytes)
		inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
			if err != nil {
				b.Fatalf("ProcessBatch failed: %v", err)
			}
		}
	})

	b.Run("SubsequentVariables", func(b *testing.B) {
		// Pre-populate state with initial variables
		initialPayload := processor2.TimeseriesMessage{
			Value:       25.5,
			TimestampMs: time.Now().UnixMilli(),
		}
		initialBytes, _ := json.Marshal(initialPayload)

		pressMsg := service.NewMessage(initialBytes)
		pressMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

		tempMsg := service.NewMessage(initialBytes)
		tempMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.tempF")

		// Prime the processor
		_, _ = processor.ProcessBatch(ctx, service.MessageBatch{pressMsg, tempMsg})

		// Test performance with existing state
		inputPayload := processor2.TimeseriesMessage{
			Value:       30.0,
			TimestampMs: time.Now().UnixMilli(),
		}
		payloadBytes, _ := json.Marshal(inputPayload)

		inputMsg := service.NewMessage(payloadBytes)
		inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.run")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
			if err != nil {
				b.Fatalf("ProcessBatch failed: %v", err)
			}
		}
	})

	b.Run("VariableOverwrite", func(b *testing.B) {
		// Test performance when overwriting existing variables
		inputPayload := processor2.TimeseriesMessage{
			Value:       25.5,
			TimestampMs: time.Now().UnixMilli(),
		}
		payloadBytes, _ := json.Marshal(inputPayload)

		inputMsg := service.NewMessage(payloadBytes)
		inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

		// Pre-populate with same variable
		_, _ = processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Update with new value
			newPayload := processor2.TimeseriesMessage{
				Value:       25.5 + float64(i),
				TimestampMs: time.Now().UnixMilli(),
			}
			newBytes, _ := json.Marshal(newPayload)
			newMsg := service.NewMessage(newBytes)
			newMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

			_, err := processor.ProcessBatch(ctx, service.MessageBatch{newMsg})
			if err != nil {
				b.Fatalf("ProcessBatch failed: %v", err)
			}
		}
	})
}

// BenchmarkThroughput measures overall throughput with realistic workloads
func BenchmarkThroughput(b *testing.B) {
	ctx := context.Background()

	processor, err := createTestProcessor()
	if err != nil {
		b.Fatalf("Failed to create processor: %v", err)
	}

	b.Run("RealisticWorkload", func(b *testing.B) {
		// Create realistic message distribution
		// 60% pressure, 30% temperature, 10% run status
		messages := make([]*service.Message, 100)
		for i := 0; i < 100; i++ {
			inputPayload := processor2.TimeseriesMessage{
				Value:       25.5 + float64(i%10),
				TimestampMs: time.Now().UnixMilli(),
			}
			payloadBytes, _ := json.Marshal(inputPayload)

			msg := service.NewMessage(payloadBytes)

			if i < 60 {
				msg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")
			} else if i < 90 {
				msg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.tempF")
			} else {
				msg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.run")
			}
			messages[i] = msg
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Process in batches of 10
			for j := 0; j < 10; j++ {
				batch := service.MessageBatch{}
				for k := 0; k < 10; k++ {
					batch = append(batch, messages[j*10+k])
				}
				_, err := processor.ProcessBatch(ctx, batch)
				if err != nil {
					b.Fatalf("ProcessBatch failed: %v", err)
				}
			}
		}
	})

	b.Run("HighThroughput", func(b *testing.B) {
		// Test high-throughput scenario
		inputPayload := processor2.TimeseriesMessage{
			Value:       25.5,
			TimestampMs: time.Now().UnixMilli(),
		}
		payloadBytes, _ := json.Marshal(inputPayload)

		inputMsg := service.NewMessage(payloadBytes)
		inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

		b.ResetTimer()

		// Measure messages per second
		start := time.Now()
		for i := 0; i < b.N; i++ {
			_, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
			if err != nil {
				b.Fatalf("ProcessBatch failed: %v", err)
			}
		}
		duration := time.Since(start)

		// Calculate and report throughput
		if b.N > 0 {
			msgsPerSec := float64(b.N) / duration.Seconds()
			b.ReportMetric(msgsPerSec, "msgs/sec")
		}
	})
}

// BenchmarkMemoryUsage measures memory allocation patterns
func BenchmarkMemoryUsage(b *testing.B) {
	ctx := context.Background()

	processor, err := createTestProcessor()
	if err != nil {
		b.Fatalf("Failed to create processor: %v", err)
	}

	b.Run("MessageCreation", func(b *testing.B) {
		// Test memory allocation for message creation
		inputPayload := processor2.TimeseriesMessage{
			Value:       25.5,
			TimestampMs: time.Now().UnixMilli(),
		}
		payloadBytes, _ := json.Marshal(inputPayload)

		inputMsg := service.NewMessage(payloadBytes)
		inputMsg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := processor.ProcessBatch(ctx, service.MessageBatch{inputMsg})
			if err != nil {
				b.Fatalf("ProcessBatch failed: %v", err)
			}
		}
	})

	b.Run("StateAccumulation", func(b *testing.B) {
		// Test memory usage as state accumulates
		messages := make([]*service.Message, 1000)
		for i := 0; i < 1000; i++ {
			inputPayload := processor2.TimeseriesMessage{
				Value:       25.5 + float64(i),
				TimestampMs: time.Now().UnixMilli(),
			}
			payloadBytes, _ := json.Marshal(inputPayload)

			msg := service.NewMessage(payloadBytes)
			msg.MetaSet("umh_topic", "umh.v1.corpA.plant-A.aawd._raw.press")
			messages[i] = msg
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < len(messages); j++ {
				_, err := processor.ProcessBatch(ctx, service.MessageBatch{messages[j]})
				if err != nil {
					b.Fatalf("ProcessBatch failed: %v", err)
				}
			}
		}
	})
}

// Helper function to create processor with custom mapping
func createProcessorWithMapping(mapping map[string]interface{}) (*processor2.StreamProcessor, error) {
	cfg := config.StreamProcessorConfig{
		Mode: "timeseries",
		Model: config.ModelConfig{
			Name:    "test",
			Version: "v1",
		},
		OutputTopic: "umh.v1.corpA.plant-A.aawd",
		Sources: map[string]string{
			"press": "umh.v1.corpA.plant-A.aawd._raw.press",
			"tF":    "umh.v1.corpA.plant-A.aawd._raw.tempF",
			"r":     "umh.v1.corpA.plant-A.aawd._raw.run",
		},
		Mapping: mapping,
	}

	resources := service.MockResources()
	return processor2.NewStreamProcessor(cfg, resources.Logger(), resources.Metrics())
}

// Helper function to create test processor
func createTestProcessor() (*processor2.StreamProcessor, error) {
	cfg := config.StreamProcessorConfig{
		Mode: "timeseries",
		Model: config.ModelConfig{
			Name:    "pump",
			Version: "v1",
		},
		OutputTopic: "umh.v1.corpA.plant-A.aawd",
		Sources: map[string]string{
			"press": "umh.v1.corpA.plant-A.aawd._raw.press",
			"tF":    "umh.v1.corpA.plant-A.aawd._raw.tempF",
			"r":     "umh.v1.corpA.plant-A.aawd._raw.run",
		},
		Mapping: map[string]interface{}{
			"pressure":    "press+4.00001",
			"temperature": "tF*69/31",
			"motor": map[string]interface{}{
				"rpm": "press/4",
			},
			"status":       `r ? "active" : "inactive"`,
			"serialNumber": `"SN-P42-008"`,
			"deviceType":   `"pump"`,
		},
	}

	resources := service.MockResources()
	return processor2.NewStreamProcessor(cfg, resources.Logger(), resources.Metrics())
}
