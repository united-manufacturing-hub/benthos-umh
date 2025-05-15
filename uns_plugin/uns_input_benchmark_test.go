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
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

// BenchmarkProcessRecord measures the performance of message processing with various filtering scenarios
func BenchmarkProcessRecord(b *testing.B) {
	// Setup metrics
	metrics := NewMockMetrics()

	b.Run("MatchingTopic", func(b *testing.B) {
		// Create processor with a specific topic regex
		processor, err := NewMessageProcessor("umh\\.v1\\.acme\\.berlin\\..*", metrics)
		if err != nil {
			b.Fatalf("Failed to create message processor: %v", err)
		}

		// Create sample record that matches the filter
		record := &kgo.Record{
			Key:   []byte("umh.v1.acme.berlin.assembly.temperature"),
			Value: []byte(`{"value": 23.5}`),
			Topic: "umh.messages",
			Headers: []kgo.RecordHeader{
				{Key: "content-type", Value: []byte("application/json")},
			},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			processor.ProcessRecord(record)
		}
	})

	b.Run("NonMatchingTopic", func(b *testing.B) {
		// Create processor with a specific topic regex
		processor, _ := NewMessageProcessor("umh\\.v1\\.acme\\.berlin\\..*", metrics)

		// Create sample record that doesn't match the filter
		record := &kgo.Record{
			Key:   []byte("umh.v1.acme.munich.assembly.temperature"),
			Value: []byte(`{"value": 23.5}`),
			Topic: "umh.messages",
			Headers: []kgo.RecordHeader{
				{Key: "content-type", Value: []byte("application/json")},
			},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			processor.ProcessRecord(record)
		}
	})

	b.Run("WildcardTopicRegex", func(b *testing.B) {
		// Create processor with a wildcard regex (matches everything)
		processor, _ := NewMessageProcessor(".*", metrics)

		// Create sample record
		record := &kgo.Record{
			Key:   []byte("umh.v1.acme.berlin.assembly.temperature"),
			Value: []byte(`{"value": 23.5}`),
			Topic: "umh.messages",
			Headers: []kgo.RecordHeader{
				{Key: "content-type", Value: []byte("application/json")},
			},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			processor.ProcessRecord(record)
		}
	})

	b.Run("ComplexTopicRegex", func(b *testing.B) {
		// Create processor with a more complex regex pattern
		processor, _ := NewMessageProcessor("umh\\.v1\\.acme\\.(berlin|munich)\\.(assembly|packaging)\\.[a-z]+", metrics)

		// Create sample record
		record := &kgo.Record{
			Key:   []byte("umh.v1.acme.berlin.assembly.temperature"),
			Value: []byte(`{"value": 23.5}`),
			Topic: "umh.messages",
			Headers: []kgo.RecordHeader{
				{Key: "content-type", Value: []byte("application/json")},
			},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			processor.ProcessRecord(record)
		}
	})

	b.Run("RecordWithManyHeaders", func(b *testing.B) {
		// Create processor with a simple regex
		processor, _ := NewMessageProcessor("umh\\.v1\\..*", metrics)

		// Create sample record with many headers
		headers := make([]kgo.RecordHeader, 20)
		for i := 0; i < 20; i++ {
			headers[i] = kgo.RecordHeader{
				Key:   "header-" + string(rune('a'+i)),
				Value: []byte("header-value-" + string(rune('a'+i))),
			}
		}

		record := &kgo.Record{
			Key:     []byte("umh.v1.acme.berlin.assembly.temperature"),
			Value:   []byte(`{"value": 23.5}`),
			Topic:   "umh.messages",
			Headers: headers,
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			processor.ProcessRecord(record)
		}
	})
}

// BenchmarkProcessRecords measures the performance of batch message processing
func BenchmarkProcessRecords(b *testing.B) {
	// Setup metrics
	metrics := NewMockMetrics()

	createMockFetches := func(recordCount int, matchingKeys bool) *MockFetches {
		records := make([]*kgo.Record, recordCount)

		for i := 0; i < recordCount; i++ {
			key := "umh.v1.acme.berlin.assembly.sensor" + string(rune('a'+i%26))
			if !matchingKeys && i%2 == 0 {
				// Every other record won't match if matchingKeys is false
				key = "umh.v2.different.key.format.sensor" + string(rune('a'+i%26))
			}

			records[i] = &kgo.Record{
				Key:   []byte(key),
				Value: []byte(`{"value": 23.5}`),
				Topic: "umh.messages",
				Headers: []kgo.RecordHeader{
					{Key: "content-type", Value: []byte("application/json")},
				},
			}
		}

		return &MockFetches{
			empty:   false,
			records: records,
		}
	}

	benchmarkSizes := []int{10, 100, 1000}

	for _, size := range benchmarkSizes {
		b.Run("AllMatching_"+strconv.Itoa(size), func(b *testing.B) {
			// Create processor with a topic regex that matches all records
			processor, _ := NewMessageProcessor("umh\\.v1\\..*", metrics)

			// Create fetches with all matching records
			fetches := createMockFetches(size, true)

			// Pre-allocate message batch
			msgBatch := make(service.MessageBatch, 0, size)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				processor.ProcessRecords(fetches, msgBatch)
			}
		})

		b.Run("HalfMatching_"+strconv.Itoa(size), func(b *testing.B) {
			// Create processor with a topic regex
			processor, _ := NewMessageProcessor("umh\\.v1\\..*", metrics)

			// Create fetches with half matching records
			fetches := createMockFetches(size, false)

			// Pre-allocate message batch
			msgBatch := make(service.MessageBatch, 0, size)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				processor.ProcessRecords(fetches, msgBatch)
			}
		})
	}
}

// BenchmarkRegexCompilation measures the performance impact of regex compilation
func BenchmarkRegexCompilation(b *testing.B) {
	patterns := []struct {
		name    string
		pattern string
	}{
		{"Simple", "umh\\.v1\\..*"},
		{"Complex", "umh\\.v1\\.[a-z]+\\.(berlin|munich)\\.(assembly|packaging)\\.[a-z]+"},
		{"VeryComplex", "umh\\.v1\\.([a-z]+)\\.(berlin|munich|frankfurt)\\.(assembly|packaging|warehouse)\\.[a-z0-9]+\\.(temperature|pressure|humidity|status|level|count)"},
	}

	for _, pattern := range patterns {
		b.Run(pattern.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = regexp.Compile(pattern.pattern)
			}
		})
	}
}

// BenchmarkMessageProcessor_Creation measures the performance of creating a message processor
func BenchmarkMessageProcessor_Creation(b *testing.B) {
	// Setup metrics
	metrics := NewMockMetrics()

	patterns := []struct {
		name    string
		pattern string
	}{
		{"Simple", "umh\\.v1\\..*"},
		{"Complex", "umh\\.v1\\.[a-z]+\\.(berlin|munich)\\.(assembly|packaging)\\.[a-z]+"},
		{"VeryComplex", "umh\\.v1\\.([a-z]+)\\.(berlin|munich|frankfurt)\\.(assembly|packaging|warehouse)\\.[a-z0-9]+\\.(temperature|pressure|humidity|status|level|count)"},
	}

	for _, pattern := range patterns {
		b.Run(pattern.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = NewMessageProcessor(pattern.pattern, metrics)
			}
		})
	}
}

// BenchmarkUnsInput_ReadBatch measures the performance of reading batches
func BenchmarkUnsInput_ReadBatch(b *testing.B) {
	sizes := []int{10, 100, 1000}

	for _, size := range sizes {
		b.Run("BatchSize_"+strconv.Itoa(size), func(b *testing.B) {
			// Setup test context
			ctx := context.Background()

			// Create mock client
			mockClient := &MockKafkaConsumerClient{
				connectFunc: func(...kgo.Opt) error {
					return nil
				},
				closeFunc: func() error {
					return nil
				},
				pollFetchesFunc: func(ctx context.Context) Fetches {
					records := make([]*kgo.Record, size)
					for i := 0; i < size; i++ {
						records[i] = &kgo.Record{
							Key:   []byte("umh.v1.acme.berlin.assembly.sensor" + string(rune('a'+i%26))),
							Value: []byte(`{"value": 23.5}`),
							Topic: "umh.messages",
							Headers: []kgo.RecordHeader{
								{Key: "content-type", Value: []byte("application/json")},
							},
						}
					}
					return &MockFetches{
						empty:   false,
						records: records,
					}
				},
				commitRecordsFunc: func(ctx context.Context) error {
					return nil
				},
			}

			// Create UnsInput with default config
			inputConfig := UnsInputConfig{
				umhTopic:        "umh\\.v1\\..*",
				inputKafkaTopic: defaultInputKafkaTopic,
				brokerAddress:   defaultBrokerAddress,
				consumerGroup:   defaultConsumerGroup,
			}
			resources := service.MockResources()
			input, _ := NewUnsInput(mockClient, inputConfig, resources.Logger(), resources.Metrics())

			// Connect
			_ = input.Connect(ctx)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, ackFn, _ := input.ReadBatch(ctx)
				if ackFn != nil {
					_ = ackFn(ctx, nil)
				}
			}

			b.StopTimer()
			_ = input.Close(ctx)
		})
	}
}

// BenchmarkAckFunction measures the performance of the acknowledgment function
func BenchmarkAckFunction(b *testing.B) {
	// Setup test context
	ctx := context.Background()

	// Create mock client with different commit behaviors
	createMockClient := func(commitDelay time.Duration) *MockKafkaConsumerClient {
		return &MockKafkaConsumerClient{
			connectFunc: func(...kgo.Opt) error {
				return nil
			},
			closeFunc: func() error {
				return nil
			},
			pollFetchesFunc: func(ctx context.Context) Fetches {
				records := []*kgo.Record{
					{
						Key:   []byte("umh.v1.acme.berlin.assembly.temperature"),
						Value: []byte(`{"value": 23.5}`),
						Topic: "umh.messages",
					},
				}
				return &MockFetches{
					empty:   false,
					records: records,
				}
			},
			commitRecordsFunc: func(ctx context.Context) error {
				if commitDelay > 0 {
					time.Sleep(commitDelay)
				}
				return nil
			},
		}
	}

	benchCases := []struct {
		name        string
		commitDelay time.Duration
	}{
		{"FastCommit", 0},
		{"SlowCommit", time.Millisecond},
	}

	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			mockClient := createMockClient(bc.commitDelay)

			// Create UnsInput
			inputConfig := UnsInputConfig{
				umhTopic:        "umh\\.v1\\..*",
				inputKafkaTopic: defaultInputKafkaTopic,
				brokerAddress:   defaultBrokerAddress,
				consumerGroup:   defaultConsumerGroup,
			}
			resources := service.MockResources()
			input, _ := NewUnsInput(mockClient, inputConfig, resources.Logger(), resources.Metrics())

			// Connect
			_ = input.Connect(ctx)

			// Get a batch and its ack function
			_, ackFn, _ := input.ReadBatch(ctx)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = ackFn(ctx, nil)
			}

			b.StopTimer()
			_ = input.Close(ctx)
		})
	}
}
