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

// uns_beta consumes one Kafka topic through connect's redpanda input and
// delivers only the records whose Kafka key matches umh_topics.

package uns_plugin

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// unsBetaConfigSpec returns the ConfigSpec for the "uns_beta" input.
func unsBetaConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("At-least-once consumer of the Unified Namespace (preview).").
		Description("Consumes the UNS Kafka topic and delivers only the records whose Kafka key (the UMH topic) matches umh_topics. Each message stays in flight until the pipeline acknowledges it; a rejected message is redelivered until it is accepted.").
		Field(service.NewStringField("broker_address").
			Description("Kafka broker address, for example \"localhost:9092\". Separate several addresses with commas.").
			Default("localhost:9092").
			Advanced()).
		Field(service.NewStringField("consumer_group").
			Description("Consumer group used when consuming the configured Kafka topic. Must be unique to this consumer.").
			Example("analytics_reader")).
		Field(service.NewStringField("kafka_topic").
			Description("The Kafka topic to consume.").
			Default("umh.messages").
			Advanced()).
		Field(service.NewStringListField("umh_topics").
			Description("List of RE2 regex patterns matched against each record's Kafka key (the UMH topic). Only records whose key matches at least one pattern are delivered; records with an empty or absent key never match any pattern. Patterns are unanchored; use ^...$ to match the full key. Required; to consume the whole namespace, set it explicitly to [\".*\"].").
			Example([]string{".*"}).
			Example([]string{`umh\.v1\.acme\.berlin\..+`, `umh\.v1\.acme\.munich\..+`}))
}

func init() {
	if err := service.RegisterBatchInput("uns_beta", unsBetaConfigSpec(), newUnsBeta); err != nil {
		panic(err)
	}
}

// newUnsBeta validates the config and builds the redpanda reader. Building it
// does not dial the broker, so a config error fails at startup instead of on
// every Connect retry.
func newUnsBeta(conf *service.ParsedConfig, res *service.Resources) (service.BatchInput, error) {
	brokerAddress, err := conf.FieldString("broker_address")
	if err != nil {
		return nil, err
	}
	var seedBrokers []string
	for _, b := range strings.Split(brokerAddress, ",") {
		if b = strings.TrimSpace(b); b != "" {
			seedBrokers = append(seedBrokers, b)
		}
	}
	if len(seedBrokers) == 0 {
		return nil, errors.New("broker_address must not be empty: set host:port, for example localhost:9092")
	}

	consumerGroup, err := conf.FieldString("consumer_group")
	if err != nil {
		return nil, err
	}
	if consumerGroup == "" {
		return nil, errors.New("consumer_group must not be empty: set a name unique to this consumer, for example analytics_reader")
	}

	kafkaTopic, err := conf.FieldString("kafka_topic")
	if err != nil {
		return nil, err
	}
	if !legalKafkaTopicName.MatchString(kafkaTopic) || kafkaTopic == "." || kafkaTopic == ".." {
		return nil, fmt.Errorf("kafka_topic %q is not a legal Kafka topic name: use 1-249 letters, digits, '.', '_' or '-'", kafkaTopic)
	}

	patterns, err := conf.FieldStringList("umh_topics")
	if err != nil {
		return nil, err
	}
	keyFilter, err := newBetaKeyFilter(patterns)
	if err != nil {
		return nil, err
	}

	inner, err := buildUnsBetaInner(res, seedBrokers, kafkaTopic, consumerGroup, keyFilter.re.String())
	if err != nil {
		return nil, err
	}

	return &unsBetaInput{
		inner:           inner,
		keyFilter:       keyFilter,
		keylessCounter:  res.Metrics().NewCounter("dropped_keyless"),
		filteredCounter: res.Metrics().NewCounter("filtered_records"),
	}, nil
}

// unsBetaInput wraps the redpanda reader and applies the umh_topics key filter.
type unsBetaInput struct {
	// inner is connect's redpanda reader.
	inner     service.BatchInput
	keyFilter *betaKeyFilter

	// keylessCounter counts records dropped because their key is empty.
	// TODO(ENG-5125): a producer header named "kafka_key" overrides the record
	// key in connect's metadata, so such a record can be delivered under a
	// spoofed UMH topic. Detect and drop it.
	keylessCounter *service.MetricCounter

	// filteredCounter counts keyed records that reach ReadBatch but match no
	// pattern. Connect's key_pattern pre-filter drops most of them first.
	filteredCounter *service.MetricCounter
}

func (i *unsBetaInput) Connect(ctx context.Context) error {
	return i.inner.Connect(ctx)
}

func (i *unsBetaInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	for {
		batch, ack, err := i.inner.ReadBatch(ctx)
		if err != nil {
			return batch, ack, err
		}
		// Connect's key_pattern matches an empty key under ".*", so this
		// filter is the one that enforces "an empty key never matches". A
		// delivered keyless record would be rejected by the uns output and
		// redelivered forever.
		kept := make(service.MessageBatch, 0, len(batch))
		for _, msg := range batch {
			key, ok := msg.MetaGet("kafka_key")
			// Connect's key_pattern pre-filter keeps the last dropped record of
			// a fetch as an empty placeholder without metadata, so its offset
			// still commits. Drop it.
			if !ok {
				continue
			}
			if !i.keyFilter.matches(key) {
				if key == "" {
					i.keylessCounter.Incr(1)
				} else {
					i.filteredCounter.Incr(1)
				}
				continue
			}
			// The uns output reads umh_topic as the Kafka key and rejects a
			// record without it. Strip connect's internal header slice so it
			// does not round-trip into downstream Kafka headers.
			msg.MetaSetMut("umh_topic", key)
			msg.MetaSetMut("kafka_msg_key", key)
			msg.MetaDelete(rpcnKafkaHeadersKey)
			kept = append(kept, msg)
		}
		if len(kept) > 0 {
			return kept, ack, nil
		}
		// Benthos discards an empty batch without calling its AckFunc, which
		// would leave the offset uncommitted and stall the partition. Ack the
		// filtered poll ourselves and read the next one.
		if ackErr := ack(ctx, nil); ackErr != nil {
			return nil, nil, ackErr
		}
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
	}
}

func (i *unsBetaInput) Close(ctx context.Context) error {
	return i.inner.Close(ctx)
}

// betaKeyFilter is the compiled umh_topics filter: the configured patterns
// combined into one alternation, matched against each record's Kafka key.
type betaKeyFilter struct {
	re *regexp.Regexp
}

// newBetaKeyFilter compiles the umh_topics patterns into one alternation,
// (?:p1)|(?:p2)|..., the same join the uns input uses. An empty list or
// an empty element is rejected because it would match every key.
func newBetaKeyFilter(patterns []string) (*betaKeyFilter, error) {
	if len(patterns) == 0 {
		return nil, errors.New(`umh_topics must not be empty: to read the whole namespace, set umh_topics: [".*"]`)
	}
	wrapped := make([]string, len(patterns))
	for i, pattern := range patterns {
		if strings.TrimSpace(pattern) == "" {
			return nil, fmt.Errorf("umh_topics pattern at index %d must not be empty or whitespace-only (an empty pattern matches every key)", i)
		}
		// Compile each pattern on its own so the error names the culprit.
		if _, err := regexp.Compile(pattern); err != nil {
			return nil, fmt.Errorf("invalid umh_topics pattern at index %d: %s - %w", i, pattern, err)
		}
		wrapped[i] = fmt.Sprintf("(?:%s)", pattern)
	}
	// RE2's size limit applies to the whole alternation, so the join can
	// still fail.
	re, err := regexp.Compile(strings.Join(wrapped, "|"))
	if err != nil {
		return nil, fmt.Errorf("the umh_topics patterns together are too large for RE2: use fewer or simpler patterns: %w", err)
	}
	return &betaKeyFilter{re: re}, nil
}

// matches reports whether a record with the given Kafka key is delivered. An
// empty key never matches, unlike in the uns input: the uns output would reject
// it and the record would be redelivered forever.
func (f *betaKeyFilter) matches(key string) bool {
	return key != "" && f.re.MatchString(key)
}

// legalKafkaTopicName is Kafka's own topic-name rule (kafka.common.Topic):
// 1-249 chars from [a-zA-Z0-9._-]. The reserved names "." and ".." match the
// pattern and are rejected separately in newUnsBeta.
var legalKafkaTopicName = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

// rpcnKafkaHeadersKey is the metadata key under which connect's redpanda input
// stores the raw kgo header slice. It is connect-internal and may change on a
// connect upgrade.
const rpcnKafkaHeadersKey = "__rpcn_kafka_headers"
