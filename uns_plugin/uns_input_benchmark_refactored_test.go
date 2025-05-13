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

package uns_plugin

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

// mockFetches implements the Fetches interface for testing
type mockFetches struct {
	records []*kgo.Record
	err     error
}

func (m *mockFetches) Empty() bool {
	return len(m.records) == 0
}

func (m *mockFetches) Err() error {
	return m.err
}

func (m *mockFetches) EachRecord(fn func(*kgo.Record)) {
	for _, r := range m.records {
		fn(r)
	}
}

func (m *mockFetches) EachError(fn func(string, int32, error)) {
	if m.err != nil {
		fn("mock-topic", 0, m.err)
	}
}

func (m *mockFetches) Err0() error {
	return m.err
}

// mockConsumer implements MessageConsumer for testing
type mockConsumer struct {
	fetches Fetches
}

func (m *mockConsumer) Connect(...kgo.Opt) error {
	return nil
}

func (m *mockConsumer) Close() error {
	return nil
}

func (m *mockConsumer) PollFetches(context.Context) Fetches {
	return m.fetches
}

func (m *mockConsumer) CommitRecords(context.Context) error {
	return nil
}

// newMockConsumer creates a mock consumer with the given records
func newMockConsumer(records []*kgo.Record) MessageConsumer {
	return &mockConsumer{
		fetches: &mockFetches{
			records: records,
		},
	}
}

// createMockRecords creates a specified number of mock Kafka records
func createMockRecords(count int, keyPattern string, valueSize int, addHeaders bool) []*kgo.Record {
	records := make([]*kgo.Record, count)

	for i := 0; i < count; i++ {
		// Create the key based on the pattern
		key := fmt.Sprintf(keyPattern, i%10, i%5)

		// Create value with the specified size
		value := make([]byte, valueSize)
		for j := 0; j < valueSize; j++ {
			value[j] = byte((i + j) % 256)
		}

		// Create the record
		record := &kgo.Record{
			Key:   []byte(key),
			Value: value,
			Topic: "umh.messages",
		}

		// Add headers if requested
		if addHeaders {
			record.Headers = []kgo.RecordHeader{
				{Key: "source", Value: []byte("benchmark-test")},
				{Key: "timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
				{Key: "sequence", Value: []byte(fmt.Sprintf("%d", i))},
			}
		}

		records[i] = record
	}

	return records
}

// createBenchmarkInput creates a UnsInput instance for benchmarking
func createBenchmarkInput(t testing.TB, records []*kgo.Record, topicPattern string) *UnsInput {
	// Create mock consumer
	mockConsumer := newMockConsumer(records)

	// Create config
	config := UnsInputConfig{
		topic:           topicPattern,
		inputKafkaTopic: "umh.messages",
		brokerAddress:   "localhost:9092",
		consumerGroup:   "benchmark-test-consumer",
	}

	// Create logger that doesn't output during benchmarks
	logger := service.NewLogger()

	// Create input
	input, err := NewUnsInput(mockConsumer, config, logger, service.NoopMetrics{})
	if err != nil {
		t.Fatalf("Failed to create input: %v", err)
	}

	return input.(*UnsInput)
}

// BenchmarkMessageProcessorPerformance benchmarks the performance of the MessageProcessor
func BenchmarkMessageProcessorPerformance(b *testing.B) {
	testCases := []struct {
		name        string
		topicPattern string
		keyPattern  string
		matches     bool
	}{
		{
			name:        "SimpleWildcard_AllMatch",
			topicPattern: ".*",
			keyPattern:  "umh.v1.acme.berlin.area%d.tag%d",
			matches:     true,
		},
		{
			name:        "PrefixMatch_AllMatch",
			topicPattern: "umh\\.v1\\..*",
			keyPattern:  "umh.v1.acme.berlin.area%d.tag%d",
			matches:     true,
		},
		{
			name:        "ComplexPattern_AllMatch",
			topicPattern: "umh\\.v1\\.acme\\.berlin\\..*",
			keyPattern:  "umh.v1.acme.berlin.area%d.tag%d",
			matches:     true,
		},
		{
			name:        "ComplexPattern_NoMatch",
			topicPattern: "umh\\.v1\\.acme\\.munich\\..*",
			keyPattern:  "umh.v1.acme.berlin.area%d.tag%d",
			matches:     false,
		},
	}

	for _, tc := range testCases {
		// Generate a single record to test pattern matching
		record := createMockRecords(1, tc.keyPattern, 128, true)[0]

		b.Run(tc.name, func(b *testing.B) {
			// Create metrics
			metrics := NewNoOpMetrics()

			// Create processor
			processor, err := NewMessageProcessor(tc.topicPattern, metrics)
			if err != nil {
				b.Fatalf("Failed to create processor: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Process the record
				msg := processor.ProcessRecord(record)

				// Verify expected behavior
				if tc.matches && msg == nil {
					b.Fatalf("Expected match but got nil message")
				}
				if !tc.matches && msg != nil {
					b.Fatalf("Expected no match but got message")
				}
			}
		})
	}
}

// BenchmarkBatchProcessing benchmarks the batch processing performance
func BenchmarkBatchProcessing(b *testing.B) {
	testCases := []struct {
		name        string
		batchSize   int
		messageSize int
		headers     bool
	}{
		{
			name:        "Small_10msgs_NoHeaders",
			batchSize:   10,
			messageSize: 128,
			headers:     false,
		},
		{
			name:        "Medium_100msgs_NoHeaders",
			batchSize:   100,
			messageSize: 128,
			headers:     false,
		},
		{
			name:        "Medium_100msgs_WithHeaders",
			batchSize:   100,
			messageSize: 128,
			headers:     true,
		},
		{
			name:        "Large_1000msgs_NoHeaders",
			batchSize:   1000,
			messageSize: 128,
			headers:     false,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Generate records
			records := createMockRecords(tc.batchSize, "umh.v1.acme.berlin.area%d.tag%d", tc.messageSize, tc.headers)

			// Create fetches
			fetches := &mockFetches{records: records}

			// Create metrics
			metrics := NewNoOpMetrics()

			// Create processor
			processor, err := NewMessageProcessor("umh\\.v1\\..*", metrics)
			if err != nil {
				b.Fatalf("Failed to create processor: %v", err)
			}

			// Pre-allocate batch
			batch := make(service.MessageBatch, 0, tc.batchSize)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Process the batch
				resultBatch := processor.ProcessRecords(fetches, batch)

				// Reset the batch for next iteration
				batch = resultBatch[:0]
			}
		})
	}
}

// BenchmarkBatchReuse compares the performance of reusing batches vs. creating new ones
func BenchmarkBatchReuse(b *testing.B) {
	// Create test data
	batchSize := 100
	records := createMockRecords(batchSize, "umh.v1.acme.berlin.area%d.tag%d", 128, true)
	fetches := &mockFetches{records: records}
	metrics := NewNoOpMetrics()

	b.Run("WithBatchReuse", func(b *testing.B) {
		// Create processor
		processor, _ := NewMessageProcessor("umh\\.v1\\..*", metrics)

		// Pre-allocate batch
		batch := make(service.MessageBatch, 0, batchSize)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Process the batch, reusing the slice
			batch = processor.ProcessRecords(fetches, batch[:0])
		}
	})

	b.Run("WithoutBatchReuse", func(b *testing.B) {
		// Create processor
		processor, _ := NewMessageProcessor("umh\\.v1\\..*", metrics)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Create a new batch each time
			var batch service.MessageBatch

			// Process records manually to simulate not using ProcessRecords
			fetches.EachRecord(func(r *kgo.Record) {
				metrics.LogRecordReceived()

				if processor.topicRegex.Match(r.Key) {
					metrics.LogRecordFiltered()
					msg := service.NewMessage(r.Value)
					msg.MetaSet("kafka_msg_key", string(r.Key))
					msg.MetaSet("kafka_topic", r.Topic)

					for _, h := range r.Headers {
						msg.MetaSet(h.Key, string(h.Value))
					}

					batch = append(batch, msg)
				}
			})
		}
	})
}

// BenchmarkEndToEndPerformance benchmarks the end-to-end performance of the UnsInput
func BenchmarkEndToEndPerformance(b *testing.B) {
	testCases := []struct {
		name        string
		batchSize   int
		messageSize int
		pattern     string
	}{
		{
			name:        "Small_10msgs_SimplePattern",
			batchSize:   10,
			messageSize: 128,
			pattern:     ".*",
		},
		{
			name:        "Medium_100msgs_PrefixPattern",
			batchSize:   100,
			messageSize: 128,
			pattern:     "umh\\.v1\\..*",
		},
		{
			name:        "Large_1000msgs_ComplexPattern",
			batchSize:   1000,
			messageSize: 128,
			pattern:     "umh\\.v1\\.acme\\.berlin\\..*",
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Generate records
			records := createMockRecords(tc.batchSize, "umh.v1.acme.berlin.area%d.tag%d", tc.messageSize, true)

			// Create input
			input := createBenchmarkInput(b, records, tc.pattern)

			// Create context
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Read batch
				batch, ackFn, err := input.ReadBatch(ctx)
				if err != nil {
					b.Fatalf("Failed to read batch: %v", err)
				}

				// Make sure we got the expected number of messages
				if len(batch) != tc.batchSize {
					b.Fatalf("Expected %d messages, got %d", tc.batchSize, len(batch))
				}

				// Acknowledge the batch
				if err := ackFn(ctx, nil); err != nil {
					b.Fatalf("Failed to acknowledge batch: %v", err)
				}
			}
		})
	}
}

// BenchmarkForProfiling is a dedicated benchmark for generating profiling data
func BenchmarkForProfiling(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping profiling benchmark in short mode")
	}

	// Generate a large dataset with varied records
	batchSize := 10000
	records := make([]*kgo.Record, batchSize)

	// Create records with varying keys, values, and headers
	for i := 0; i < batchSize; i++ {
		// Vary the key format
		var key string
		switch i % 5 {
		case 0:
			key = fmt.Sprintf("umh.v1.acme.berlin.area%d.tag%d", i%20, i%10)
		case 1:
			key = fmt.Sprintf("umh.v1.acme.munich.zone%d.sensor%d", i%15, i%7)
		case 2:
			key = fmt.Sprintf("umh.v1.beta.newyork.floor%d.device%d", i%8, i%12)
		case 3:
			key = fmt.Sprintf("umh.v1.gamma.tokyo.section%d.unit%d", i%10, i%25)
		default:
			key = fmt.Sprintf("umh.v1.delta.london.room%d.control%d", i%30, i%5)
		}

		// Vary the value size
		valueSize := 512 + (i % 10) * 128 // 512B to 1.5KB
		value := make([]byte, valueSize)
		for j := 0; j < valueSize; j++ {
			value[j] = byte((i + j) % 256)
		}

		// Vary the number of headers
		headerCount := 2 + (i % 8) // 2 to 9 headers
		headers := make([]kgo.RecordHeader, headerCount)
		for j := 0; j < headerCount; j++ {
			headerKey := fmt.Sprintf("header%d", j)
			headerValue := []byte(fmt.Sprintf("value-%d-%d", i, j))
			headers[j] = kgo.RecordHeader{
				Key:   headerKey,
				Value: headerValue,
			}
		}

		records[i] = &kgo.Record{
			Key:     []byte(key),
			Value:   value,
			Topic:   "umh.messages",
			Headers: headers,
		}
	}

	// Create input
	input := createBenchmarkInput(b, records, "umh\\.v1\\..*")

	// Create context
	ctx := context.Background()

	b.ResetTimer()

	// Run just a few iterations to get good profiling data
	b.N = 10
	for i := 0; i < b.N; i++ {
		// Read batch
		batch, ackFn, err := input.ReadBatch(ctx)
		if err != nil {
			b.Fatalf("Failed to read batch: %v", err)
		}

		// Acknowledge the batch
		if err := ackFn(ctx, nil); err != nil {
			b.Fatalf("Failed to acknowledge batch: %v", err)
		}

		// Log progress
		b.Logf("Iteration %d: Processed %d messages", i, len(batch))
	}
}
