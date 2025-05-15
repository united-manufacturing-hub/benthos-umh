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
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Constants for default configuration values
const (
	defaultInputKafkaTopic        = "umh.messages"
	defaultTopicKey               = ".*"
	defaultConsumerGroup          = "uns_plugin"
	defaultConnIdleTimeout        = 15 * time.Minute
	defaultDialTimeout            = 10 * time.Second
	defaultFetchMaxBytes          = 100e6 // 100MB
	defaultFetchMaxPartitionBytes = 100e6 // 100MB
	defaultFetchMinBytes          = 50e6  // 50MB
	defaultFetchMaxWaitTime       = 1 * time.Second
)

// UnsInputConfig holds the configuration for the UNS input plugin
type UnsInputConfig struct {
	umhTopic        string
	inputKafkaTopic string
	brokerAddress   string
	consumerGroup   string
}

// NewDefaultUnsInputConfig creates a new input config with default values
func NewDefaultUnsInputConfig() UnsInputConfig {
	return UnsInputConfig{
		umhTopic:        defaultTopicKey,
		inputKafkaTopic: defaultInputKafkaTopic,
		brokerAddress:   defaultBrokerAddress,
		consumerGroup:   defaultConsumerGroup,
	}
}

// ParseFromBenthos parses configuration from Benthos config into UnsInputConfig
func ParseFromBenthos(conf *service.ParsedConfig) (UnsInputConfig, error) {
	config := NewDefaultUnsInputConfig()

	// Parse topic (regex pattern for message key filtering)
	if conf.Contains("umh_topic") {
		topic, err := conf.FieldString("umh_topic")
		if err != nil {
			return config, fmt.Errorf("error while parsing the 'topic' field from the plugin's config: %v", err)
		}
		config.umhTopic = topic
	}

	// Parse kafka_topic (the actual Kafka topic to consume from)
	if conf.Contains("kafka_topic") {
		inputKafkaTopic, err := conf.FieldString("kafka_topic")
		if err != nil {
			return config, fmt.Errorf("error while parsing the 'kafka_topic' field from the plugin's config: %v", err)
		}
		config.inputKafkaTopic = inputKafkaTopic
	}

	// Parse broker_address
	if conf.Contains("broker_address") {
		brokerAddr, err := conf.FieldString("broker_address")
		if err != nil {
			return config, fmt.Errorf("error while parsing the 'broker_address' from the plugin's config: %v", err)
		}
		config.brokerAddress = brokerAddr
	}

	// Parse consumer_group
	if conf.Contains("consumer_group") {
		cg, err := conf.FieldString("consumer_group")
		if err != nil {
			return config, fmt.Errorf("error while parsing the 'consumer_group' from the plugin's config: %v", err)
		}
		config.consumerGroup = cg
	}

	return config, nil
}

// RegisterConfigSpec registers the Benthos configuration specification for the UNS input plugin
func RegisterConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Consumes messsages from the UMH platform's Kafka messaging system").
		Description(`
	The uns_plugin input consumes messages from the United Manufacturing Hub's kafka messaging system.
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
