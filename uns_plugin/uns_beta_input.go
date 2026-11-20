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

// validateYAMLScalar rejects control characters and surrounding whitespace in
// a value that is interpolated into the translated redpanda config. A newline
// would let a user inject redpanda keys such as auto_replay_nacks: false.
func validateYAMLScalar(v string, fieldName string) error {
	for _, r := range v {
		if r == '\n' || r == '\r' || r == '\t' || r < 0x20 {
			return fmt.Errorf("%s must not contain control characters or newlines", fieldName)
		}
	}
	if strings.TrimSpace(v) != v {
		return fmt.Errorf("%s must not have leading or trailing whitespace", fieldName)
	}
	return nil
}

// unsBetaConfigSpec returns the ConfigSpec for the "uns_beta" input.
func unsBetaConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("At-least-once consumer of the Unified Namespace (preview).").
		Description("Consumes the UNS Kafka topic and delivers only the records whose Kafka key (the UMH topic) matches umh_topics. Each message stays in flight until the pipeline acknowledges it; a rejected message is redelivered until it is accepted.").
		Field(service.NewStringField("broker_address").
			Description("Kafka broker address to connect to. For example: \"localhost:9092\".").
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
			Description("List of RE2 regex patterns matched against each record's Kafka key (the umh_topic). Only records whose key matches at least one pattern are delivered; records with an empty or absent key never match any pattern. Patterns are unanchored; use ^...$ to match the full key. Required; to consume the whole namespace, set it explicitly to [\".*\"].").
			Example([]string{".*"}).
			Example([]string{`umh\.v1\.acme\.berlin\..+`, `umh\.v1\.acme\.munich\..+`}))
}

func init() {
	if err := service.RegisterBatchInput("uns_beta", unsBetaConfigSpec(), newUnsBeta); err != nil {
		panic(err)
	}
}

// newUnsBeta validates the config and returns an input that builds the
// redpanda reader on first Connect, so construction never dials a broker.
func newUnsBeta(conf *service.ParsedConfig, res *service.Resources) (service.BatchInput, error) {
	brokerAddress, err := conf.FieldString("broker_address")
	if err != nil {
		return nil, err
	}
	if err = validateYAMLScalar(brokerAddress, "broker_address"); err != nil {
		return nil, err
	}

	consumerGroup, err := conf.FieldString("consumer_group")
	if err != nil {
		return nil, err
	}
	if consumerGroup == "" {
		return nil, errors.New("consumer_group must not be empty")
	}
	if err = validateYAMLScalar(consumerGroup, "consumer_group"); err != nil {
		return nil, err
	}

	kafkaTopic, err := conf.FieldString("kafka_topic")
	if err != nil {
		return nil, err
	}
	if !legalKafkaTopicName.MatchString(kafkaTopic) || kafkaTopic == "." || kafkaTopic == ".." {
		return nil, errors.New("kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)")
	}

	patterns, err := conf.FieldStringList("umh_topics")
	if err != nil {
		return nil, err
	}
	keyFilter, err := newBetaKeyFilter(patterns)
	if err != nil {
		return nil, err
	}

	// A binary built without the connect patches cannot read. Fail here, at
	// startup, rather than on every Connect retry.
	if errUnsBetaUnavailable != nil {
		return nil, errUnsBetaUnavailable
	}

	// Tests construct the input with nil resources. A nil *MetricCounter is a
	// no-op on Incr.
	var keylessCounter, filteredCounter *service.MetricCounter
	if res != nil {
		keylessCounter = res.Metrics().NewCounter("dropped_keyless")
		filteredCounter = res.Metrics().NewCounter("filtered_records")
	}
	return &unsBetaInput{
		res:             res,
		seedBrokers:     []string{brokerAddress},
		kafkaTopic:      kafkaTopic,
		consumerGroup:   consumerGroup,
		umhTopics:       patterns,
		keyFilter:       keyFilter,
		keylessCounter:  keylessCounter,
		filteredCounter: filteredCounter,
	}, nil
}

// unsBetaInput wraps the redpanda reader and applies the umh_topics key filter.
type unsBetaInput struct {
	res           *service.Resources
	seedBrokers   []string
	kafkaTopic    string
	consumerGroup string
	umhTopics     []string
	keyFilter     *betaKeyFilter

	// keylessCounter counts dropped records with an empty kafka_key.
	// TODO(ENG-5125): connect lets a producer header named "kafka_key"
	// override the record key, so a record can be delivered under a spoofed
	// umh_topic, or counted here or in filteredCounter. Detect and drop it.
	keylessCounter *service.MetricCounter

	// filteredCounter counts keyed records that reach ReadBatch but match no
	// pattern. Connect's key_pattern pre-filter drops most of them first.
	filteredCounter *service.MetricCounter

	// inner is the redpanda reader, built on first Connect.
	inner service.BatchInput
}

func (i *unsBetaInput) Connect(ctx context.Context) error {
	if i.inner == nil {
		inner, err := buildUnsBetaInner(i.res, i.seedBrokers, i.kafkaTopic, i.consumerGroup, i.umhTopics)
		if err != nil {
			return err
		}
		i.inner = inner
	}
	return i.inner.Connect(ctx)
}

func (i *unsBetaInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if i.inner == nil {
		return nil, nil, service.ErrNotConnected
	}
	for {
		batch, ack, err := i.inner.ReadBatch(ctx)
		if err != nil {
			return batch, ack, err
		}
		// Connect's key_pattern matches an empty key under ".*", so this
		// filter is the one that enforces "an empty key never matches". A
		// delivered keyless record would be rejected by the uns output and
		// redelivered forever (ENG-5094).
		kept := make(service.MessageBatch, 0, len(batch))
		for _, msg := range batch {
			key, ok := msg.MetaGet("kafka_key")
			// The connect pre-filter keeps the last omitted record of a poll
			// as an empty placeholder without metadata, so its offset still
			// commits. Drop it.
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
	if i.inner == nil {
		return nil
	}
	return i.inner.Close(ctx)
}

// betaKeyFilter is the compiled umh_topics filter: the configured patterns
// combined into one alternation, matched against each record's Kafka key.
type betaKeyFilter struct {
	re *regexp.Regexp
}

// newBetaKeyFilter compiles the umh_topics patterns into one alternation,
// (?:p1)|(?:p2)|..., the same join the legacy uns input uses. An empty list or
// an empty element is rejected because it would match every key.
func newBetaKeyFilter(patterns []string) (*betaKeyFilter, error) {
	if len(patterns) == 0 {
		return nil, errors.New("umh_topics must not be empty")
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
		return nil, fmt.Errorf("compiling combined umh_topics pattern: %w", err)
	}
	return &betaKeyFilter{re: re}, nil
}

// matches reports whether a record with the given Kafka key is delivered. An
// empty key never matches, unlike in the legacy uns input: the uns output
// would reject it and the record would be redelivered forever (ENG-5094).
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
