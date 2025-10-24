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
	defaultInputKafkaTopic = "umh.messages"
	defaultTopicKey        = ".*"
	defaultConsumerGroup   = "uns_plugin"
	defaultConnIdleTimeout = 15 * time.Minute
	defaultDialTimeout     = 10 * time.Second

	// MEMORY OPTIMIZATION: Reduced from 100MB to prevent OOM kills
	// Previous values caused 1.7GB+ allocations in franz-go decompression
	// New values target ~170MB peak allocation (10x reduction)
	defaultFetchMaxBytes          = 10e6 // 10MB (was 100MB)
	defaultFetchMaxPartitionBytes = 10e6 // 10MB (was 100MB)
	defaultFetchMinBytes          = 1e6  // 1MB (was 50MB)
	defaultFetchMaxWaitTime       = 1 * time.Second
)

// UnsInputConfig holds the configuration for the UNS input plugin
type UnsInputConfig struct {
	umhTopics       []string
	inputKafkaTopic string
	brokerAddress   string
	consumerGroup   string
	metadataFormat  string
}

// NewDefaultUnsInputConfig creates a new input config with default values
func NewDefaultUnsInputConfig() UnsInputConfig {
	return UnsInputConfig{
		umhTopics:       []string{defaultTopicKey},
		inputKafkaTopic: defaultInputKafkaTopic,
		brokerAddress:   defaultBrokerAddress,
		consumerGroup:   defaultConsumerGroup,
		metadataFormat:  "string",
	}
}

// ParseFromBenthos parses configuration from Benthos config into UnsInputConfig
func ParseFromBenthos(conf *service.ParsedConfig, logger *service.Logger) (UnsInputConfig, error) {
	config := NewDefaultUnsInputConfig()

	// Collect topics from all available sources
	var allTopics []string

	// Parse umh_topics (preferred) - list of regex patterns for message key filtering
	if conf.Contains("umh_topics") {
		topics, err := conf.FieldStringList("umh_topics")
		if err != nil {
			return config, fmt.Errorf("error while parsing the 'umh_topics' field from the plugin's config: %v", err)
		}
		allTopics = append(allTopics, topics...)
	}

	// Parse umh_topic (single pattern) - add to list
	if conf.Contains("umh_topic") {
		topic, err := conf.FieldString("umh_topic")
		if err != nil {
			return config, fmt.Errorf("error while parsing the 'umh_topic' field from the plugin's config: %v", err)
		}
		allTopics = append(allTopics, topic)
	}

	// Parse topic (deprecated) - add to list
	if conf.Contains("topic") {
		topic, err := conf.FieldString("topic")
		if err != nil {
			return config, fmt.Errorf("error while parsing the 'topic' field from the plugin's config: %v", err)
		}
		allTopics = append(allTopics, topic)
		if logger != nil {
			logger.Warnf("'topic' field is deprecated. Please use 'umh_topic' or 'umh_topics' instead.")
		}
	}

	// If there is no entry in allTopics, we will add the defaultTopicKey
	if len(allTopics) == 0 {
		allTopics = append(allTopics, defaultTopicKey)
	}

	// Deduplicate and set topics
	if len(allTopics) > 0 {
		// Create a map to deduplicate while preserving order
		seen := make(map[string]bool)
		var deduplicatedTopics []string
		for _, topic := range allTopics {
			if !seen[topic] {
				seen[topic] = true
				deduplicatedTopics = append(deduplicatedTopics, topic)
			}
		}
		config.umhTopics = deduplicatedTopics
	} else {
		return config, fmt.Errorf("no topics found in the plugin's config: specify at least one of 'umh_topic', 'umh_topics' or 'topic' fields")
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

	// Parse metadata_format
	if conf.Contains("metadata_format") {
		metadataFormat, err := conf.FieldString("metadata_format")
		if err != nil {
			return config, fmt.Errorf("error while parsing the 'metadata_format' from the plugin's config: %v", err)
		}
		// Validate the value
		if metadataFormat != "string" && metadataFormat != "bytes" {
			return config, fmt.Errorf("metadata_format must be 'string' or 'bytes', got '%s'", metadataFormat)
		}
		config.metadataFormat = metadataFormat
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

	All messages are read from the uns topic 'umh.messages' by default, with messages being filtered by the regular expression(s) specified in the plugin config field 'umh_topic' or 'umh_topics'. This becomes crucial for streaming out the data of interest from the uns topic.

	By default, the plugin connects to the Kafka broker at localhost:9092 with the consumer group id specified in the plugin config. The consumer group id is usually derived from the UMH workloads like protocol converter names.
		`).
		Field(service.NewStringField("umh_topic").
			Description(`
	Key used to filter the messages. The value set for the 'umh_topic' field will be used to compare against the message key in kafka. The 'umh_topic' field allows regular expressions which should be compatible with RE2 regex engine.

	The topic should follow the UMH naming convention: umh.v1.enterprise.site.area.tag
	(e.g., 'umh.v1.acme.berlin.assembly.temperature')
	(e.g., 'umh.v1.acme.berlin.+' # regex to match all areas and tags under brelin site )

	Cannot be used together with 'umh_topics'.
		`).
			Example("umh.v1.acme.berlin.assembly.temperature").
			Example(`umh\.v1\..+`).Optional()).
		Field(service.NewStringListField("umh_topics").
			Description(`
	List of keys used to filter the messages. Each value in the 'umh_topics' list will be used to compare against the message key in kafka. The 'umh_topics' field allows regular expressions which should be compatible with RE2 regex engine.

	The topics should follow the UMH naming convention: umh.v1.enterprise.site.area.tag
	(e.g., ['umh.v1.acme.berlin.assembly.temperature', 'umh.v1.acme.munich.packaging.pressure'])
	(e.g., ['umh.v1.acme.berlin.+', 'umh.v1.acme.munich.+'] # regex to match all areas and tags under both sites)

	Cannot be used together with 'umh_topic'.
		`).
			Example([]string{"umh.v1.acme.berlin.assembly.temperature", "umh.v1.acme.munich.packaging.pressure"}).
			Example([]string{`umh\.v1\.acme\.berlin\..+`, `umh\.v1\.acme\.munich\..+`})).
		Field(service.NewStringField("topic").
			Description(`
	[DEPRECATED] Use 'umh_topic' or 'umh_topics' instead. Key used to filter the messages for backwards compatibility.
		`).
			Example("umh.v1.acme.berlin.assembly.temperature").
			Example(`umh\.v1\..+`).
			Optional().
			Advanced()).
		Field(service.NewStringField("kafka_topic").
			Description(`
	The input kafka topic to read messages from. By default the messages will be consumed from 'umh.messages' topic.
			`).
			Example("umh.messages").
			Optional()).
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
			Default(defaultConsumerGroup)).
		Field(service.NewStringField("metadata_format").
			Description(`Controls how Kafka headers are stored in Benthos metadata.
- "string": Convert headers to strings (recommended, fixes byte array issue)
- "bytes": Keep headers as byte arrays (legacy behavior for backward compatibility)

Default: "string" for new configs.`).
			Default("string").
			LintRule(`root = if this == "string" || this == "bytes" { null } else { "must be 'string' or 'bytes'" }`))
}
