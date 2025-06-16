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

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

// MessageProcessor handles the transformation of Kafka records to Benthos messages
type MessageProcessor struct {
	topicRegex *regexp.Regexp
	metrics    *UnsInputMetrics
}

// NewMessageProcessor creates a new MessageProcessor with the specified topic regex
func NewMessageProcessor(topicPattern string, metrics *UnsInputMetrics) (*MessageProcessor, error) {
	topicRegex, err := regexp.Compile(topicPattern)
	if err != nil {
		return nil, err
	}

	return &MessageProcessor{
		topicRegex: topicRegex,
		metrics:    metrics,
	}, nil
}

// ProcessRecord processes a single Kafka record and returns a Benthos message if it matches the topic regex
// Returns nil if the record doesn't match the topic regex
func (p *MessageProcessor) ProcessRecord(record *kgo.Record) *service.Message {
	p.metrics.LogRecordReceived()

	if !p.topicRegex.Match(record.Key) {
		return nil
	}

	p.metrics.LogRecordFiltered()

	msg := service.NewMessage(record.Value)

	// Add headers to the meta field if present
	for _, h := range record.Headers {
		msg.MetaSetMut(h.Key, h.Value)
	}

	// Add kafka meta fields
	msg.MetaSetMut("kafka_msg_key", record.Key)
	msg.MetaSetMut("kafka_topic", record.Topic)
	msg.MetaSetMut("umh_topic", record.Key) // UMH topic structure from Kafka message key (e.g., "umh.v1.enterprise.plant1._historian.temperature")

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
