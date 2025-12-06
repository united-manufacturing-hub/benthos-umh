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
	"fmt"
	"regexp"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

// MessageProcessor handles the transformation of Kafka records to Benthos messages
type MessageProcessor struct {
	topicRegex     *regexp.Regexp
	metrics        *UnsInputMetrics
	metadataFormat MetadataFormat // controls how Kafka headers are converted
}

// NewMessageProcessor creates a new MessageProcessor with the specified topic regex patterns and metadata format
func NewMessageProcessor(topicPatterns []string, metrics *UnsInputMetrics, metadataFormat MetadataFormat) (*MessageProcessor, error) {
	if len(topicPatterns) == 0 {
		return nil, fmt.Errorf("at least one topic pattern must be provided")
	}

	// Combine all patterns into one: (?:pattern1)|(?:pattern2)|(?:pattern3)
	wrappedPatterns := make([]string, len(topicPatterns))
	for i, pattern := range topicPatterns {
		// Validate individual pattern first
		if _, err := regexp.Compile(pattern); err != nil {
			return nil, fmt.Errorf("invalid regex pattern at index %d: %s - %w", i, pattern, err)
		}
		wrappedPatterns[i] = fmt.Sprintf("(?:%s)", pattern)
	}
	combinedPattern := strings.Join(wrappedPatterns, "|")

	topicRegex, err := regexp.Compile(combinedPattern)
	if err != nil {
		return nil, fmt.Errorf("failed to compile combined regex pattern: %w", err)
	}

	return &MessageProcessor{
		topicRegex:     topicRegex,
		metrics:        metrics,
		metadataFormat: metadataFormat,
	}, nil
}

// ProcessRecord processes a single Kafka record and returns a Benthos message if it matches any of the topic regexes
// Returns nil if the record doesn't match any topic regex
func (p *MessageProcessor) ProcessRecord(record *kgo.Record) *service.Message {
	p.metrics.LogRecordReceived()

	// Check if the record key matches the combined topic regex
	if !p.topicRegex.Match(record.Key) {
		return nil
	}

	p.metrics.LogRecordFiltered()

	msg := service.NewMessage(record.Value)

	// Add headers to the meta field if present, converting based on metadataFormat
	for _, h := range record.Headers {
		switch p.metadataFormat {
		case MetadataFormatString:
			msg.MetaSetMut(h.Key, string(h.Value))
		case MetadataFormatBytes:
			msg.MetaSetMut(h.Key, h.Value)
		default:
			// Unreachable: The configuration parser should only accept valid
			// values for the MetadataFormat enum. This ensures that if a
			// new MetadataFormat variant gets added we also update this
			// part of the code.
			panic(fmt.Sprintf("Unknown MetadataFormat: %#v", p.metadataFormat))
		}
	}

	// Add kafka meta fields
	msg.MetaSetMut("kafka_msg_key", record.Key)
	msg.MetaSetMut("kafka_topic", record.Topic)
	msg.MetaSetMut("umh_topic", string(record.Key)) // UMH topic structure from Kafka message key (e.g., "umh.v1.enterprise.plant1._historian.temperature")

	// Add Kafka timestamp (when the record was written to Kafka) - this is used by the topic browser for ProducedAtMs
	msg.MetaSetMut("kafka_timestamp_ms", fmt.Sprintf("%d", record.Timestamp.UnixMilli()))

	return msg
}

// ProcessRecords processes a batch of Kafka records and returns a Benthos message batch
// It reuses the provided message batch slice to reduce allocations
func (p *MessageProcessor) ProcessRecords(fetches Fetches, msgBatch service.MessageBatch) service.MessageBatch {
	// Reset the batch but keep its capacity
	msgBatch = msgBatch[:0]

	// Process each record
	fetches.EachRecord(func(r *kgo.Record) {
		msg := p.ProcessRecord(r)
		if msg != nil {
			msgBatch = append(msgBatch, msg)
		}
	})

	return msgBatch
}
