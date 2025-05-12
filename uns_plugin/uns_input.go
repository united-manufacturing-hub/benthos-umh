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
	"regexp"

	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

// init registers the "uns" batch output plugin with Benthos using its configuration and constructor.
func init() {
	service.RegisterBatchInput("uns", inputConfig(), newUnsInput)
}

func inputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Consumes messsages from the UMH platform's Kafka messaging system").
		Description(`
	The uns_plugin input consumes messages from the United Manufacturing Hub'skafka mesaging system.
	This input plugin is optimized for communication with UMH core components and handles the complexities of Kafka for you.

	All messages are read from the uns topic 'umh.messages' by default, with messages being filtered by the regular expression specified in the plugin config field 'topic'. This becomes crucial for streaming out the data of interest from the uns topic.

	By default, the plugin connects to the Kafka broker at localhost:9092 with the consumer group id specified in the plugin config. The consumer group id is usually derived from the UMH workloads like protocol converter names.
		`).
		Field(service.NewStringField("topic").
			Description(`
	Key used to filter the messages. The value set for the 'topic' field will be used to compare against the message key in kafka. The 'topic' field allows regular expressions which should be compatible with RE2 regex engine.

	The topic should follow the UMH naming convention: umh.v1.enterprise.site.area.tag
	(e.g., 'umh.v1.acme.berlin.assembly.temperature')
	(e.g., 'umh.v1.acme.berlin.+' # regex to match all areas and tags under brelin site )
		`).
			Example("umh.v1.acme.berlin.assembly.temperature").
			Example(`umh\.v1\..+`).
			Default(defaultTopicKey)).
		Field(service.NewStringField("kafka_topic").
			Description(`
	The input kafka topic to read messages from. By default the messages will be consumed from 'umh.messages' topic.
			`).
			Example("umh.messages").
			Default(defaultInputKafkaTopic)).
		Field(service.NewStringField("broker_address").
			Description(`
The Kafka broker address to connect to. This can be a single address or multiple addresses
separated by commas. For example: "localhost:9092" or "broker1:9092,broker2:9092".

In most UMH deployments, the default value is sufficient as Kafka runs on the same host.
            `).
			Default(defaultBrokerAddress)).
		Field(service.NewStringField("consumer_group").
			Description(`
	The consumer group id to be used by the plugin. The default consumer group id is uns_plugin. This is an optional plugin input and can be used by the users if one wants to read the topic with a different consumer group discarding the previous consumed offsets.
	`).
			Example("uns_consumer_group").
			Default(defaultConsumerGroup))
}

const (
	defaultInputKafkaTopic        = "umh.messages"
	defaultTopicKey               = ".*"
	defaultConsumerGroup          = "uns_plugin"
	defaultConnIdleTimeout        = 15 * time.Minute
	defaultDialTimeout            = 5 * time.Minute
	defaultFetchMaxBytes          = 10e6 // 10MB
	defaultFetchMaxPartitionBytes = 10e6 // 10MB
	defaultFetchMinBytes          = 1    // 1 Byte
	defaultFetchMaxWaitTime       = 100 * time.Millisecond
)

type unsInputConfig struct {
	topic           string
	inputKafkaTopic string
	brokerAddress   string
	consumerGroup   string
}

type unsInput struct {
	config unsInputConfig
	client MessageConsumer
	log    *service.Logger
}

func newUnsInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {

	config := unsInputConfig{}

	// Use the default Topic key .* (match any key string) to allow all messages
	config.topic = defaultTopicKey
	if conf.Contains("topic") {
		topic, err := conf.FieldString("topic")
		if err != nil {
			return nil, fmt.Errorf("error while parsing the 'topic' field from the plugin's config: %v", err)
		}
		config.topic = topic
	}

	config.inputKafkaTopic = defaultInputKafkaTopic
	if conf.Contains("kafka_topic") {
		inputKafkaTopic, err := conf.FieldString("kafka_topic")
		if err != nil {
			return nil, fmt.Errorf("error while parsing the 'kafka_topic' field from the plugin's config: %v", err)
		}
		config.inputKafkaTopic = inputKafkaTopic
	}

	config.brokerAddress = defaultBrokerAddress
	if conf.Contains("broker_address") {
		brokerAddr, err := conf.FieldString("broker_address")
		if err != nil {
			return nil, fmt.Errorf("error while parsing the 'broker_address' fro m the plugin's config: %v", err)
		}
		config.brokerAddress = brokerAddr
	}

	config.consumerGroup = defaultConsumerGroup
	if conf.Contains("consumer_group") {
		cg, err := conf.FieldString("consumer_group")
		if err != nil {
			return nil, fmt.Errorf("error while parsing the 'consumer_group' from the plugin's config: %v", err)
		}
		config.consumerGroup = cg
	}

	return newUnsInputWithClient(NewConsumerClient(), config, mgr.Logger()), nil
}

func newUnsInputWithClient(client MessageConsumer, config unsInputConfig, logger *service.Logger) service.BatchInput {
	return &unsInput{
		client: client,
		config: config,
		log:    logger,
	}
}

// Close implements service.BatchInput.
func (u *unsInput) Close(ctx context.Context) error {
	if u.client != nil {
		u.client.Close()
	}
	u.log.Infof("uns input kafka client closed successfully")
	return nil
}

// Connect implements service.BatchInput.
func (u *unsInput) Connect(context.Context) error {

	u.log.Infof("Connecting to uns plugin kafka broker: %v", u.config.brokerAddress)

	if u.client == nil {
		u.client = NewConsumerClient()
	}

	u.log.Infof("creating kafka client with plugin config broker: %v, input_kafka_topic: %v, topic: %v", u.config.brokerAddress, u.config.inputKafkaTopic, u.config.topic)

	err := u.client.Connect(
		kgo.SeedBrokers(u.config.brokerAddress),           // use configured broker address
		kgo.AllowAutoTopicCreation(),                      // Allow creating the defaultOutputTopic if it doesn't exists
		kgo.ClientID(defaultClientID),                     // client id for all requests sent to the broker
		kgo.ConnIdleTimeout(defaultConnIdleTimeout),       // Rough amount of time to allow connections to be idle. Default value at franz-go is 20
		kgo.DialTimeout(defaultDialTimeout),               // Timeout while connecting to the broker. 5 second is more than enough to connect to a local broker
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // Resets the offset to the earliest partition offset if the client encountres a "OffsetOutOfRange" problem
		kgo.ConsumerGroup(u.config.consumerGroup),         // Set the consumer group id
		kgo.ConsumeTopics(u.config.inputKafkaTopic),       // Set the topics to consume

		// Some high performance settings
		kgo.FetchMaxBytes(defaultFetchMaxBytes),
		kgo.FetchMaxPartitionBytes(defaultFetchMaxPartitionBytes),
		kgo.FetchMinBytes(defaultFetchMinBytes),
		kgo.FetchMaxWait(defaultFetchMaxWaitTime), // Override the default 5s value and wait just only for 100 Millisecond for the min bytes
	)
	if err != nil {
		return fmt.Errorf("error while creating a kafka client with broker %s: %v", u.config.brokerAddress, err)
	}

	u.log.Infof("Connection to the kafka broker %s is successful", u.config.brokerAddress)
	return nil
}

// ReadBatch reads messages from Kafka in a batch and return it to redpanda connect processing chain.
func (u *unsInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	fetches := u.client.PollFetches(ctx)

	if fetches.Empty() {
		return nil, nil, nil
	}

	// There could be multiple errors within fetches. But we do check only the first error to quickly see the existence of an error
	if fetches.Err() != nil {
		fetches.EachError(func(topic string, partition int32, err error) {
			u.log.Errorf("Error while fetching messages, topic: %v partition: %d, err: %v", topic, partition, err)

		})
		// Enough to return only the first error to notify benthos. But all theerrors are sent to the benthos logs
		return nil, nil, fetches.Err0()
	}

	// Create messageBatch
	var batch service.MessageBatch
	topicRegex, err := regexp.Compile(u.config.topic)
	if err != nil {
		return nil, nil, fmt.Errorf("error compiling topic regex: %v", err)
	}

	fetches.EachRecord(func(r *kgo.Record) {

		if topicRegex.Match(r.Key) {
			msg := service.NewMessage(r.Value)
			// Add kafka meta fields
			msg.MetaSet("kafka_key", string(r.Key))
			msg.MetaSet("kafka_topic", r.Topic)

			// Add headers to the meta field if present
			for _, h := range r.Headers {
				msg.MetaSet(h.Key, string(h.Value))
			}
			batch = append(batch, msg)
		}

	})

	// Create the ack function
	ackFn := func(ctx context.Context, err error) error {
		if err != nil {
			u.log.Errorf("Error processing the batch: %v", err)
			return err
		}

		if err := u.client.CommitRecords(ctx); err != nil {
			u.log.Errorf("Error committing the message offsests: %v", err)
			return err
		}

		return nil
	}

	return batch, ackFn, nil
}
