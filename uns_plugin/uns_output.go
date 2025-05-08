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
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultOutputTopic               = "umh.v2.messages" // by the current state, the output topic must not be changed for this plugin
	defaultOutputTopicPartitionCount = 1
	defaultBrokerAddress             = "localhost:9092"
	defaultClientID                  = "umh_core"
)

var (
	messageKeySanitizer = regexp.MustCompile(`[^a-zA-Z0-9._\-]`)
)

func outputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Writes messages to the UMH platform's Kafka messaging system").
		Description(`
The uns_plugin output sends messages to the United Manufacturing Hub's Kafka messaging system.
This output is optimized for communication with UMH core components and handles the complexities
of Kafka configuration for you.

All messages are written to a single topic (umh.v2.messages), with the key of each message
derived from the 'messageKey' field specified in this configuration. This key is crucial for
proper routing within the UMH ecosystem.

By default, the plugin connects to a Kafka broker at localhost:9092 with client ID 'umh_core',
which is suitable for most UMH deployments. These defaults work with the standard UMH
installation where Kafka runs alongside other services.

Note: This output implements batch writing to Kafka for improved performance.`).
		Field(service.NewInterpolatedStringField("message_key").
			Description(`
Key used when sending messages to the UMH output topic. This value becomes the Kafka message key,
which determines how messages are routed within the UMH ecosystem.

The message_key should follow the UMH naming convention: enterprise.site.area.tag
(e.g., 'acme.berlin.assembly.temperature')

This field supports interpolation, allowing you to set the key dynamically based on message
content or metadata. Common patterns include:
- Using metadata: ${! meta("topic") }
- Using content: ${! json("device.location") }
- Using a static value: enterprise.site.area.tag

Note: The key will be sanitized to remove any characters that are not alphanumeric, dots,
underscores, or hyphens. Invalid characters will be replaced with underscores.
            `).
			Example("${! meta(\"topic\") }").
			Example("enterprise.site.area.historian").
			Default("${! meta(\"kafka_topic\") }")).
		Field(service.NewStringField("broker_address").
			Description(`
The Kafka broker address to connect to. This can be a single address or multiple addresses
separated by commas. For example: "localhost:9092" or "broker1:9092,broker2:9092".

In most UMH deployments, the default value is sufficient as Kafka runs on the same host.
            `).
			Default(defaultBrokerAddress))
}

// Config holds the configuration for the UNS output plugin
type unsConfig struct {
	messageKey    *service.InterpolatedString
	brokerAddress string
}

type unsOutput struct {
	config unsConfig
	client MessagePublisher
	log    *service.Logger
}

func newUnsOutput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
	// maximum number of messages that can be in processing simultaneously before requiring acknowledgements
	maxInFlight := 100

	batchPolicy := service.BatchPolicy{
		Count:  100,     //max number of messages per batch
		Period: "100ms", // timeout to ensure timely delivery even if the count aren't met
	}

	config := unsConfig{}

	// Parse message_key
	messageKey, err := conf.FieldInterpolatedString("message_key")
	if err != nil {
		return nil, batchPolicy, 0, fmt.Errorf("error while parsing message_key field from the config: %v", err)
	}
	config.messageKey = messageKey

	// Parse broker_address if provided
	config.brokerAddress = defaultBrokerAddress
	if conf.Contains("broker_address") {
		brokerAddress, err := conf.FieldString("broker_address")
		if err != nil {
			return nil, batchPolicy, 0, fmt.Errorf("error while parsing broker_address field from the config: %v", err)
		}
		if brokerAddress != "" {
			config.brokerAddress = brokerAddress
		}
	}

	return newUnsOutputWithClient(NewClient(), config, mgr.Logger()), batchPolicy, maxInFlight, nil
}

// Testable constructor that accepts client
func newUnsOutputWithClient(client MessagePublisher, config unsConfig, logger *service.Logger) service.BatchOutput {
	return &unsOutput{
		client: client,
		config: config,
		log:    logger,
	}
}

// Close closes the underlying kafka client
func (o *unsOutput) Close(ctx context.Context) error {
	o.log.Infof("Attempting to close the uns kafka client")
	if o.client != nil {
		o.client.Close()
		o.client = nil
	}
	o.log.Infof("uns kafka client closed successfully")
	return nil
}

// Connect initializes the kafka client
func (o *unsOutput) Connect(ctx context.Context) error {
	o.log.Infof("Connecting to uns plugin kafka broker: %v", o.config.brokerAddress)

	if o.client == nil {
		o.client = NewClient()
	}

	// Create the kafka client
	err := o.client.Connect(
		kgo.SeedBrokers(o.config.brokerAddress),     // use configured broker address
		kgo.AllowAutoTopicCreation(),                // Allow creating the defaultOutputTopic if it doesn't exists
		kgo.ClientID(defaultClientID),               // client id for all requests sent to the broker
		kgo.ConnIdleTimeout(15*time.Minute),         // Rough amount of time to allow connections to be idle. Default value at franz-go is 20
		kgo.DialTimeout(5*time.Second),              // Timeout while connecting to the broker. 5 second is more than enough to connect to a local broker
		kgo.DefaultProduceTopic(defaultOutputTopic), // topic to produce messages to. The plugin writes messages only to a single default topic
		kgo.RequiredAcks(kgo.LeaderAck()),           // Partition leader has to send acknowledment on successful write
		kgo.MaxBufferedRecords(1000),                // Max amount of records hte client will buffer
		kgo.ProduceRequestTimeout(5*time.Second),    // Produce Request Timeout
		kgo.ProducerLinger(100*time.Millisecond),    // Duration on how long individual partitons will linger waiting for more records before triggering a Produce request
		kgo.DisableIdempotentWrite(),                // Idempotent write is disabled since the  plugin is going to communicate with a local kafka broker where one could assume less network failures
	)
	if err != nil {
		return fmt.Errorf("error while creating a kafka client with broker %s: %v", o.config.brokerAddress, err)
	}

	// Verify topic existence and partition count
	if err := o.verifyOutputTopic(ctx); err != nil {
		return err
	}

	o.log.Infof("Connection to the kafka broker %s is successful", o.config.brokerAddress)
	return nil
}

// verifyOutputTopic checks if the output topic exists and has the correct partition count
// If the topic doesn't exist, it creates it with the correct partition count
func (o *unsOutput) verifyOutputTopic(ctx context.Context) error {
	topicExists, partition, err := o.client.IsTopicExists(ctx, defaultOutputTopic)
	if err != nil {
		return fmt.Errorf("error while checking if the default output topic exists: %v", err)
	}

	if topicExists && (partition != defaultOutputTopicPartitionCount) {
		// DefaultOutputTopic exists but the partition count mismatches with what is needed.
		return fmt.Errorf("default output topic '%s' has a mismatched partition count: required %d, actual %d",
			defaultOutputTopic, defaultOutputTopicPartitionCount, partition)
	}

	if !topicExists {
		// Default output topic doesn't exists. Create it
		err = o.client.CreateTopic(ctx, defaultOutputTopic, defaultOutputTopicPartitionCount)
		if err != nil {
			return fmt.Errorf("error while creating the missing default output topic '%s': %v", defaultOutputTopic, err)
		}
		o.log.Infof("Created output topic '%s' with %d partition(s)", defaultOutputTopic, defaultOutputTopicPartitionCount)
	}

	return nil
}

// sanitizeMessageKey ensures the key contains only valid characters
// It replaces invalid characters with underscores and logs a warning if sanitization occurred
func (o *unsOutput) sanitizeMessageKey(key string) string {
	sanitizedKey := messageKeySanitizer.ReplaceAllString(key, "_")
	if key != sanitizedKey {
		o.log.Debugf("Message key contained invalid characters and was sanitized: '%s' -> '%s'", key, sanitizedKey)
	}
	return sanitizedKey
}

// extractHeaders extracts all metadata from a message except Kafka-specific ones
func (o *unsOutput) extractHeaders(msg *service.Message) (map[string][]byte, error) {
	headers := make(map[string][]byte)

	err := msg.MetaWalk(func(key, value string) error {
		// We don't want the original `kafka_` metafields generated by benthos
		if !strings.HasPrefix(key, "kafka_") {
			headers[key] = []byte(value)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error extracting message metadata: %v", err)
	}

	return headers, nil
}

// WriteBatch implements service.BatchOutput.
func (o *unsOutput) WriteBatch(ctx context.Context, msgs service.MessageBatch) error {

	records := make([]Record, 0, len(msgs))
	for i, msg := range msgs {
		key, err := o.config.messageKey.TryString(msg)
		if err != nil {
			return fmt.Errorf("failed to resolve message_key field in message %d: %v", i, err)
		}

		// TryString sets the key to "null" when the key is not set in the message
		if key == "" || key == "null" {
			return fmt.Errorf("message_key is not set or is empty in message %d, message_key is mandatory", i)
		}

		sanitizedKey := o.sanitizeMessageKey(key)

		headers, err := o.extractHeaders(msg)
		if err != nil {
			return fmt.Errorf("error processing message %d: %v", i, err)
		}

		msgAsBytes, err := msg.AsBytes()
		if err != nil {
			return fmt.Errorf("error getting content of message %d: %v", i, err)
		}

		record := Record{
			Topic:   defaultOutputTopic,
			Key:     []byte(sanitizedKey),
			Value:   msgAsBytes,
			Headers: headers,
		}

		records = append(records, record)
		o.log.Tracef("Message %d prepared with key: %s", i, sanitizedKey)
	}

	if err := o.client.ProduceSync(ctx, records); err != nil {
		return fmt.Errorf("error writing batch output to kafka: %v", err)
	}

	o.log.Debugf("Successfully sent %d messages to topic '%s'", len(records), defaultOutputTopic)
	return nil
}
