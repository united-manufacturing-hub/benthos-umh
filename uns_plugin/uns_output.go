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

// init registers the "uns" batch output plugin with Benthos using its configuration and constructor.
func init() {
	service.RegisterBatchOutput("uns", outputConfig(), newUnsOutput)
}

const (
	defaultOutputTopic               = "umh.messages" // by the current state, the output topic must not be changed for this plugin
	defaultOutputTopicPartitionCount = 1
	defaultBrokerAddress             = "localhost:9092"
	defaultClientID                  = "umh_core"
	defaultBridgeName                = "uns_default_bridge"
	defaultTopic                     = "${! meta(\"topic\") }"
)

var (
	topicSanitizer = regexp.MustCompile(`[^a-zA-Z0-9._\-]`)
)

// outputConfig returns the Benthos configuration specification for the "uns" output plugin.
//
// This configuration defines how messages are sent to the UMH platform's Kafka messaging system.
// It includes fields for specifying the Kafka message key ("topic"), broker address, and the name
// of the bridge performing data bridging ("bridged_by"). The "topic" field supports interpolation
// and is sanitized before use as the Kafka message key. Default values are provided for all fields
// to simplify deployment in standard UMH environments.
func outputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Writes messages to the UMH platform's Kafka messaging system").
		Description(`
The uns_plugin output sends messages to the United Manufacturing Hub's Kafka messaging system.
This output is optimized for communication with UMH core components and handles the complexities
of Kafka configuration for you.

All messages are written to a single topic (umh.messages), with the key of each message
derived from the 'messageKey' field specified in this configuration. This key is crucial for
proper routing within the UMH ecosystem.

By default, the plugin connects to a Kafka broker at localhost:9092 with client ID 'umh_core',
which is suitable for most UMH deployments. These defaults work with the standard UMH
installation where Kafka runs alongside other services.

Note: This output implements batch writing to Kafka for improved performance.`).
		Field(service.NewInterpolatedStringField("topic").
			Description(`
Key used when sending messages to the UMH output topic. This value becomes the Kafka message key,
which determines how messages are routed within the UMH ecosystem.

The topic should follow the UMH naming convention: umh.v1.enterprise.site.area.tag
(e.g., 'umh.v1.acme.berlin.assembly.temperature')

This field supports interpolation, allowing you to set the key dynamically based on message
content or metadata. Common patterns include:
- Using metadata: ${! meta("topic") }
- Using content: ${! json("device.location") }
- Using a static value: umh.v1.enterprise.site.area.tag

Note: The key will be sanitized to remove any characters that are not alphanumeric, dots,
underscores, or hyphens. Invalid characters will be replaced with underscores.
            `).
			Example("${! meta(\"topic\") }").
			Example("umh.v1.enterprise.site.area.historian").
			Default(defaultTopic)).
		Field(service.NewStringField("broker_address").
			Description(`
The Kafka broker address to connect to. This can be a single address or multiple addresses
separated by commas. For example: "localhost:9092" or "broker1:9092,broker2:9092".

In most UMH deployments, the default value is sufficient as Kafka runs on the same host.
            `).
			Default(defaultBrokerAddress)).
		Field(service.NewStringField("bridged_by").
			Description(`
	The name of the Bridges that does the bridging of the data. The format for the Bridge name should 'protocol-converter-{nodeName}-{protocol converter name}'
			`).
			Default(defaultBridgeName))
}

// Config holds the configuration for the UNS output plugin
type unsConfig struct {
	topic         *service.InterpolatedString
	brokerAddress string
	bridgedBy     string
}

type unsOutput struct {
	config unsConfig
	client MessagePublisher
	log    *service.Logger
}

// newUnsOutput creates a new unsOutput instance by parsing configuration fields for topic, broker address, and bridge name, returning the output, batch policy, max in-flight count, and any error encountered during parsing.
func newUnsOutput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
	// maximum number of messages that can be in processing simultaneously before requiring acknowledgements
	maxInFlight := 100

	batchPolicy := service.BatchPolicy{
		Count:  100,     //max number of messages per batch
		Period: "100ms", // timeout to ensure timely delivery even if the count aren't met
	}

	config := unsConfig{}

	// Parse topic
	config.topic, _ = service.NewInterpolatedString(defaultTopic)
	if conf.Contains("topic") {
		messageKey, err := conf.FieldInterpolatedString("topic")
		if err != nil {
			return nil, batchPolicy, 0, fmt.Errorf("error while parsing topic field from the config: %v", err)
		}
		config.topic = messageKey
	}

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

	config.bridgedBy = defaultBridgeName
	if conf.Contains("bridged_by") {
		bridgedBy, err := conf.FieldString("bridged_by")
		if err != nil {
			return nil, batchPolicy, 0, fmt.Errorf("error while parsing bridged_by field from the config: %v", err)
		}
		if bridgedBy != "" {
			config.bridgedBy = bridgedBy
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
	sanitizedKey := topicSanitizer.ReplaceAllString(key, "_")
	if key != sanitizedKey {
		o.log.Debugf("Message key contained invalid characters and was sanitized: '%s' -> '%s'", key, sanitizedKey)
	}
	return sanitizedKey
}

// extractHeaders extracts all metadata from a message except Kafka-specific ones
// kafka specific meta fields are injected into the header if the source node is kafka and they can be ignored
// There could be other meta fields set by the upstream benthos processors and those meta fields should be passed down as kafka headers
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

	// Set bridged_by config to the headers
	headers["bridged_by"] = []byte(o.config.bridgedBy)

	return headers, nil
}

// WriteBatch implements service.BatchOutput.
func (o *unsOutput) WriteBatch(ctx context.Context, msgs service.MessageBatch) error {
	if len(msgs) == 0 {
		return nil
	}

	records := make([]Record, 0, len(msgs))
	for i, msg := range msgs {
		key, err := o.config.topic.TryString(msg)
		if err != nil {
			return fmt.Errorf("failed to resolve topic field in message %d: %v", i, err)
		}

		// TryString sets the key to "null" when the key is not set in the message
		if key == "" || key == "null" {
			return fmt.Errorf("topic is not set or is empty in message %d, topic is mandatory", i)
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
