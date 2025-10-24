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
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

// init registers the "uns" batch input plugin with Benthos using its configuration and constructor.
func init() {
	err := service.RegisterBatchInput("uns", RegisterConfigSpec(), newUnsInput)
	if err != nil {
		panic(err)
	}
}

// UnsInput is the primary implementation of the UNS input plugin
type UnsInput struct {
	config      UnsInputConfig
	client      MessageConsumer
	log         *service.Logger
	metrics     *UnsInputMetrics
	processor   *MessageProcessor
	batchPool   service.MessageBatch
	ackFunction service.AckFunc
}

// newUnsInput creates a new UnsInput instance from Benthos configuration
func newUnsInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	// Parse configuration
	config, err := ParseFromBenthos(conf, mgr.Logger())
	if err != nil {
		return nil, err
	}

	// Create consumer client
	client := NewConsumerClient()

	// Create the input
	return NewUnsInput(client, config, mgr.Logger(), mgr.Metrics())
}

// NewUnsInput creates a new UnsInput with the specified dependencies
// This constructor is more testable as it accepts interfaces instead of concrete types
func NewUnsInput(client MessageConsumer, config UnsInputConfig, logger *service.Logger, metricsProvider *service.Metrics) (service.BatchInput, error) {
	// Create metrics
	metrics := NewUnsInputMetrics(metricsProvider)

	// Create a message processor
	processor, err := NewMessageProcessor(config.umhTopics, metrics, config.metadataFormat)
	if err != nil {
		return nil, fmt.Errorf("failed to create message processor: %v", err)
	}

	// Create the UnsInput
	input := &UnsInput{
		config:    config,
		client:    client,
		log:       logger,
		metrics:   metrics,
		processor: processor,
		batchPool: make(service.MessageBatch, 0, 100), // Pre-allocate with reasonable capacity
	}
	// Create the ack function once
	input.ackFunction = input.createAckFunction()

	logger.Infof("Starting UNS input plugin with broker: %s, kafka_topic: %s, umh_topics: %v",
		input.config.brokerAddress, input.config.inputKafkaTopic, input.config.umhTopics)

	return input, nil
}

// Connect establishes a connection to the Kafka broker
func (u *UnsInput) Connect(ctx context.Context) error {
	u.log.Infof("Connecting to uns plugin kafka broker: %v", u.config.brokerAddress)

	if u.client == nil {
		u.client = NewConsumerClient()
	}

	u.log.Infof("creating kafka client with plugin config broker: %v, input_kafka_topic: %v, topics: %v",
		u.config.brokerAddress, u.config.inputKafkaTopic, u.config.umhTopics)

	connectStart := time.Now()
	err := u.client.Connect(
		kgo.SeedBrokers(u.config.brokerAddress),           // use the configured broker address
		kgo.AllowAutoTopicCreation(),                      // Allow creating the topic if it doesn't exist
		kgo.ClientID(defaultClientID),                     // client id for all requests sent to the broker
		kgo.ConnIdleTimeout(defaultConnIdleTimeout),       // Rough amount of time to allow connections to be idle
		kgo.DialTimeout(defaultDialTimeout),               // Timeout while connecting to the broker
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // Resets the offset to the earliest partition offset
		kgo.ConsumerGroup(u.config.consumerGroup),         // Set the consumer group id
		kgo.ConsumeTopics(u.config.inputKafkaTopic),       // Set the topics to consume
		kgo.DisableAutoCommit(),                           // Disable auto committing offsets, since we commit offsets manually via the ack function

		// High-performance settings
		kgo.FetchMaxBytes(defaultFetchMaxBytes),                   // Configures the maximum number of bytes per request the broker will return
		kgo.FetchMaxPartitionBytes(defaultFetchMaxPartitionBytes), // Limits the maximum bytes per partition the broker will return
		kgo.FetchMinBytes(defaultFetchMinBytes),                   // Sets the minimum bytes the broker should wait for before responding to the fetch
		kgo.FetchMaxWait(defaultFetchMaxWaitTime),                 // Maximum time the broker will wait for FetchMinBytes to be reached before responding
	)
	if err != nil {
		return fmt.Errorf("error while creating a kafka client with broker %s: %v", u.config.brokerAddress, err)
	}

	u.metrics.LogConnectionEstablished(connectStart)
	u.log.Infof("Connection to the kafka broker %s is successful", u.config.brokerAddress)
	return nil
}

// Close closes the connection to the Kafka broker
func (u *UnsInput) Close(ctx context.Context) error {
	if u.client != nil {
		u.client.Close()
	}
	u.metrics.LogConnectionClosed()
	u.log.Infof("uns input kafka client closed successfully")
	return nil
}

// ReadBatch reads a batch of messages from Kafka
func (u *UnsInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	batchStart := time.Now()

	// Poll for messages
	fetches := u.client.PollFetches(ctx)

	// Handle errors
	if fetches.Err() != nil {
		fetches.EachError(func(topic string, partition int32, err error) {
			u.log.Errorf("Error while fetching messages, topic: %v partition: %d, err: %v", topic, partition, err)
			u.metrics.LogPollError()
		})
		return nil, nil, fetches.Err0()
	}

	// Handle empty fetches
	if fetches.Empty() {
		return nil, nil, nil
	}

	// Process records into a message batch
	batch := u.processor.ProcessRecords(fetches, u.batchPool)

	// Log metrics
	u.metrics.LogBatchProcessed(batchStart)

	return batch, u.ackFunction, nil
}

// createAckFunction creates a function that commits offsets when a batch is acknowledged
func (u *UnsInput) createAckFunction() service.AckFunc {
	return func(ctx context.Context, err error) error {
		ackStart := time.Now()
		if err != nil {
			u.log.Errorf("Error processing the batch: %v", err)
			return err
		}

		if err := u.client.CommitRecords(ctx); err != nil {
			u.log.Errorf("Error committing the message offsets: %v", err)
			return err
		}

		u.metrics.LogCommitCompleted(ackStart)
		return nil
	}
}
