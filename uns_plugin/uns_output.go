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
            `).
			Example("${! meta(\"topic\") }").
			Example("enterprise.site.area.historian").
			Default("${! meta(\"kafka_topic\") }"))
}

type unsOutput struct {
	messageKey *service.InterpolatedString
	client     MessagePublisher
	log        *service.Logger
}

func newUnsOutput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
	// maximum number of messages that can be in processing simultaneously before requiring acknowledgements
	maxInFlight := 100

	batchPolicy := service.BatchPolicy{
		Count:  100,     //max number of messages per batch
		Period: "100ms", // timeout to ensure timely delivery even if the count aren't met
	}

	messageKey, err := conf.FieldInterpolatedString("message_key")
	if err != nil {
		return nil, batchPolicy, 0, fmt.Errorf("error while parsing message_key string from the config: %v", err)
	}

	return newUnsOutputWithClient(NewClient(), messageKey, mgr.Logger()), batchPolicy, maxInFlight, nil
}

// Testable constructor that accepts client
func newUnsOutputWithClient(client MessagePublisher, messageKey *service.InterpolatedString, logger *service.Logger) service.BatchOutput {
	return &unsOutput{
		client:     client,
		messageKey: messageKey,
		log:        logger,
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
	o.log.Infof("Connecting to umh core stream kafka broker: %v", defaultBrokerAddress)

	if o.client == nil {
		o.client = NewClient()
	}
	// Create the kafka client
	err := o.client.Connect(
		kgo.SeedBrokers(defaultBrokerAddress),       // bootstrap broker addresses
		kgo.AllowAutoTopicCreation(),                // Allow creating the defaultOutputTopic if it doesn't exists
		kgo.ClientID(defaultClientID),               // client id for all requests sent to the broker
		kgo.ConnIdleTimeout(15*time.Minute),         // Rough amount of time to allow connections to be idle. Default value at franz-go is 20
		kgo.DialTimeout(5*time.Second),              // Timeout while connecting to the broker. 5 second is more than enough to connect to a local broker
		kgo.DefaultProduceTopic(defaultOutputTopic), // topic to produce messages to. The plugin writes messages only to a single default topic
		kgo.RequiredAcks(kgo.LeaderAck()),           // Partition leader has to send acknowledment on successful write
		kgo.MaxBufferedRecords(1000),                // Max amount of records hte client will buffer
		kgo.ProduceRequestTimeout(5*time.Second),    // Produce Request Timeout
		kgo.ProducerLinger(100*time.Millisecond),    // Duration on how long individual partitons will linger waiting for more records before triggering a Produce request
		kgo.DisableIdempotentWrite(),                // Idempotent write is disabled since the  plugin is going to communicate with a local kafka broker where one could assume less network failures. With Idempotency enabled, RequiredAcks should be from all insync replicas which will increase the latency
	)
	if err != nil {
		return fmt.Errorf("error while creating a kafka client: %v", err)
	}

	// Now the client is created, let's check for the defaultOutputTopic. Create it if it doesn't exists
	topicExists, partition, err := o.client.IsTopicExists(ctx, defaultOutputTopic)
	if err != nil {
		return fmt.Errorf("error while checking if the default output topic exists: %v", err)
	}

	if topicExists && (partition != defaultOutputTopicPartitionCount) {
		// DefaultOutputTopic exists but the partition count mismatches with what is needed. This happens if the topic is created manually without proper partition specified
		return fmt.Errorf("default output topic has a mismatched partition count. required partition count: %d, actual partition count: %d", defaultOutputTopicPartitionCount, partition)
	}

	if !topicExists {
		// Default output topic doesn't exists. Create it
		err = o.client.CreateTopic(ctx, defaultOutputTopic, defaultOutputTopicPartitionCount)
		if err != nil {
			return fmt.Errorf("error while creating the missing default output topic: %v, err: %v", defaultOutputTopic, err)
		}
	}

	o.log.Infof("Connection to the kafka broker %s is successful", defaultBrokerAddress)

	return nil
}

// WriteBatch implements service.BatchOutput.
func (o *unsOutput) WriteBatch(ctx context.Context, msgs service.MessageBatch) error {

	records := make([]Record, 0, len(msgs))
	for _, msg := range msgs {
		key, err := o.messageKey.TryString(msg)
		if err != nil {
			return fmt.Errorf("failed to resolve topic field: %v", err)
		}

		// TryString sets the key to "null" when the key is not set in the message
		if key == "" || key == "null" {
			return fmt.Errorf("message key is not set in the input message. message key is mandatory for this plugin to publish messages")
		}

		// Topic key should not contain characters other than a-zA-z0-9,dot,hypen and underscore
		// If present replace them with an underscore
		sanitizedKey := messageKeySanitizer.ReplaceAllString(key, "_")

		o.log.Tracef("sending message with key: %s", sanitizedKey)

		headers := make(map[string][]byte)

		// Preserve all the meta fields from the input message except the ones starting with the `kafka_` prefix
		err = msg.MetaWalk(func(key, value string) error {
			// We don't want the original `kafka_` metafields generated by benthos
			if strings.HasPrefix(key, "kafka_") {
				return nil
			}
			headers[key] = []byte(value)
			return nil
		})
		if err != nil {
			return fmt.Errorf("error while setting the metadata as the message header: %v", err)
		}

		msgAsBytes, err := msg.AsBytes()
		if err != nil {
			return err
		}

		record := Record{
			Topic:   defaultOutputTopic,
			Key:     []byte(sanitizedKey),
			Value:   msgAsBytes,
			Headers: headers,
		}

		records = append(records, record)

	}
	if err := o.client.ProduceSync(ctx, records); err != nil {
		return fmt.Errorf("error while writing batch output to kafka: %v", err)
	}
	return nil
}
