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

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
	"time"
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
			Example(`umh\.v1\..+`)).
		Field(service.NewStringField("input_kafka_topic").
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
			Default(defaultBrokerAddress))
}

const (
	defaultInputKafkaTopic = "umh.messages"
)

type unsInputConfig struct {
	topic           string
	inputKafkaTopic string
	brokerAddress   string
}

type unsInput struct {
	config unsInputConfig
	client MessageConsumer
	log    *service.Logger
}

func newUnsInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {

	config := unsInputConfig{}

	// Parse topic
	topic, err := conf.FieldString("topic")
	if err != nil {
		return nil, fmt.Errorf("error while parsing the 'topic' field from the plugin's config: %v", err)
	}
	config.topic = topic

	config.inputKafkaTopic = defaultInputKafkaTopic
	if conf.Contains("input_kafka_topic") {
		inputKafkaTopic, err := conf.FieldString("input_kafka_topic")
		if err != nil {
			return nil, fmt.Errorf("error while parsing the 'input_kafka_topic' field from the plugin's config: %v", err)
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

	return newUnsInputWithClient(NewClient(), config, mgr.Logger()), nil
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
	return nil
}

// Connect implements service.BatchInput.
func (o *unsInput) Connect(context.Context) error {

	o.log.Infof("Connecting to uns plugin kafka broker: %v", o.config.brokerAddress)

	if o.client == nil {
		o.client = NewConsumerClient()
	}

	err := o.client.Connect(
		kgo.SeedBrokers(o.config.brokerAddress),           // use configured broker address
		kgo.AllowAutoTopicCreation(),                      // Allow creating the defaultOutputTopic if it doesn't exists
		kgo.ClientID(defaultClientID),                     // client id for all requests sent to the broker
		kgo.ConnIdleTimeout(15*time.Minute),               // Rough amount of time to allow connections to be idle. Default value at franz-go is 20
		kgo.DialTimeout(5*time.Second),                    // Timeout while connecting to the broker. 5 second is more than enough to connect to a local broker
		kgo.ConsumeRegex(),                                // Treat the name of the topics to consume as regex. This opens the world to the plugin users to consume from multiple kafka topics
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // Resets the offset to the earliest partition offset if the client encountres a "OffsetOutOfRange" problem
		kgo.ConsumeTopics(o.config.inputKafkaTopic),       // Input topics to consume
		// Todo: Set the group.id
	)
	if err != nil {
		return fmt.Errorf("error while creating a kafka client with broker %s: %v", o.config.brokerAddress, err)
	}

	o.log.Infof("Connection to the kafka broker %s is successful", o.config.brokerAddress)
	return nil
}

// ReadBatch implements service.BatchInput.
func (u *unsInput) ReadBatch(context.Context) (service.MessageBatch, service.AckFunc, error) {
	panic("unimplemented")
}
