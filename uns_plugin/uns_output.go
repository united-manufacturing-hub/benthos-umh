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
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
	schemavalidation "github.com/united-manufacturing-hub/benthos-umh/uns_plugin/schema_validation"
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
	defaultUMHTopic                  = "${! meta(\"umh_topic\") }"
)

// outputConfig returns the Benthos configuration specification for the "uns"
// batch-output plugin.
//
// ──────────────────────────────────────────────────────────────────────────────
//
//	▶  PURPOSE
//	    Publishes data into the United Manufacturing Hub Unified Namespace.
//	    Every record is written to the local Redpanda topic **umh.messages**;
//	    the *Kafka-key* (not the topic) is taken from `umh_topic`, normally
//	    `${! meta("umh_topic") }`, and is ISA-95 / UMH-style (`umh.v1.<…>`).
//
//	▶  SCOPE
//	    The plugin is intended to run **inside UMH Core**.  There it can be
//	    configured with a single line:
//
//	        output:
//	          uns: {}
//
//	    Outside UMH Core you must at least set `broker_address`.
//
//	▶  DEFAULTS & SAFETY
//	    * Broker : localhost:9092 (the embedded Redpanda)
//	    * Topic  : umh.messages (hard-wired)
//	    * Key    : ${! meta("umh_topic") }  – sanitized to `[a-zA-Z0-9._-]`
//	    * Header : `bridged_by = umh-core` (auto-overwritten by the
//	               Management Console when the container is deployed as a
//	               protocol-converter)
//
//	    If umh.messages does not exist, the plugin creates it with one
//	    partition and `cleanup.policy = compact,delete`.
//
// ──────────────────────────────────────────────────────────────────────────────
func outputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Publishes records to UMH Core's Kafka (Redpanda) topic `umh.messages`.").
		Description(`
Inside UMH Core you usually configure **nothing**:

    output:
      uns: {}

The plugin connects to Redpanda on localhost:9092, batches 100 records
or 100 ms, and writes them to *umh.messages*.  The Kafka key is taken
from '${! meta("umh_topic") }' (set automatically by the tag_processor,
Bloblang, or Node-RED JS).  Outside UMH Core you may override the broker
or add a custom 'bridged_by' header for traceability.
`).
		Field(service.NewInterpolatedStringField("umh_topic").
			Description(`
The **Kafka key** for every record.  Must follow the UMH naming pattern
'umh.v1.<enterprise>.<site>.<area>.<data_contract>[.<virtual_path>].<tag>'.

Leave it at the default '${! meta("umh_topic") }' if you use the tag_processor
or set it in Bloblang / Node-RED JS.

Any character not matching [a-zA-Z0-9._-] is replaced by '_'.
`).
			Example("${! meta(\"umh_topic\") }").
			Example("umh.v1.enterprise.site.area.historian").
			Default(defaultUMHTopic)).
		Field(service.NewStringField("broker_address").
			Description(`
Kafka / Redpanda bootstrap list.  Comma-separated if you have multiple
brokers, e.g. "broker1:9092,broker2:9092".

Default 'localhost:9092' is correct for every UMH Core installation.
`).
			Default(defaultBrokerAddress)).
		Field(service.NewStringField("bridged_by").
			Description(`
Traceability header.  Defaults to 'umh-core' but is automatically
overwritten by UMH Core when the container runs as a protocol-converter:
'protocol-converter-<INSTANCE>-<NAME>'.
`).
			Default(defaultClientID))
}

// Config holds the configuration for the UNS output plugin
type unsOutputConfig struct {
	umh_topic     *service.InterpolatedString
	brokerAddress string
	bridgedBy     string
}

type unsOutput struct {
	config    unsOutputConfig
	client    MessagePublisher
	log       *service.Logger
	validator *schemavalidation.Validator
}

// newUnsOutput creates a new unsOutput instance by parsing configuration fields for umh_topic, broker address, and bridge name, returning the output, batch policy, max in-flight count, and any error encountered during parsing.
func newUnsOutput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
	// maximum number of messages that can be in processing simultaneously before requiring acknowledgements
	maxInFlight := 100

	batchPolicy := service.BatchPolicy{
		Count:  100,     //max number of messages per batch
		Period: "100ms", // timeout to ensure timely delivery even if the count aren't met
	}

	config := unsOutputConfig{}

	// Parse topic
	config.umh_topic, _ = service.NewInterpolatedString(defaultUMHTopic)
	if conf.Contains("umh_topic") {
		messageKey, err := conf.FieldInterpolatedString("umh_topic")
		if err != nil {
			return nil, batchPolicy, 0, fmt.Errorf("error while parsing umh_topic field from the config: %v", err)
		}
		config.umh_topic = messageKey
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

	config.bridgedBy = defaultClientID
	if conf.Contains("bridged_by") {
		bridgedBy, err := conf.FieldString("bridged_by")
		if err != nil {
			return nil, batchPolicy, 0, fmt.Errorf("error while parsing bridged_by field from the config: %v", err)
		}
		if bridgedBy != "" {
			config.bridgedBy = bridgedBy
		}
	}

	// Initialize the validator
	validator := schemavalidation.Validator{}

	return newUnsOutputWithClient(NewClient(), config, mgr.Logger(), &validator), batchPolicy, maxInFlight, nil
}

// Testable constructor that accepts client
func newUnsOutputWithClient(client MessagePublisher, config unsOutputConfig, logger *service.Logger, validator *schemavalidation.Validator) service.BatchOutput {
	return &unsOutput{
		client:    client,
		config:    config,
		log:       logger,
		validator: validator,
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
		key, err := o.config.umh_topic.TryString(msg)
		if err != nil {
			return fmt.Errorf("failed to resolve umh_topic field in message %d: %v", i, err)
		}

		// TryString sets the key to "null" when the key is not set in the message
		if key == "" || key == "null" {
			return fmt.Errorf("umh_topic is not set or is empty in message %d, umh_topic is mandatory", i)
		}

		// Validate the UMH topic using the centralized topic library
		unsTopic, err := topic.NewUnsTopic(key)
		if err != nil {
			return fmt.Errorf("error validating message key in message %d: invalid UMH topic '%s': %v", i, key, err)
		}

		msgAsBytes, err := msg.AsBytes()
		if err != nil {
			return fmt.Errorf("error getting content of message %d: %v", i, err)
		}

		// Validate the payload against the schema
		err = o.validator.Validate(unsTopic, msgAsBytes)
		if err != nil {
			return fmt.Errorf("error validating message payload in message %d: %v", i, err)
		}

		headers, err := o.extractHeaders(msg)
		if err != nil {
			return fmt.Errorf("error processing message %d: %v", i, err)
		}

		record := Record{
			Topic:   defaultOutputTopic,
			Key:     []byte(key),
			Value:   msgAsBytes,
			Headers: headers,
		}

		records = append(records, record)
		o.log.Tracef("Message %d prepared with key: %s", i, key)
	}

	if err := o.client.ProduceSync(ctx, records); err != nil {
		return fmt.Errorf("error writing batch output to kafka: %v", err)
	}

	o.log.Debugf("Successfully sent %d messages to topic '%s'", len(records), defaultOutputTopic)
	return nil
}
