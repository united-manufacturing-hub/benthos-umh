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
	"github.com/redpanda-data/benthos/v4/public/service"
)

// init registers the "uns" batch output plugin with Benthos using its configuration and constructor.
func init() {
	service.RegisterBatchOutput("uns", outputConfig(), newUnsOutput)
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
}
