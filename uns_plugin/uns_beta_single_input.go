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

// uns_beta_single is a Go-registered input that consumes a single Kafka
// topic and filters records by key, intended as a sibling of the uns_beta
// template form. It is registered but not yet functional: a valid config
// parses and builds, but reading returns a not-implemented error until
// ReadBatch is implemented.

package uns_plugin

import (
	"context"
	"errors"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// unsBetaSingleConfigSpec returns the ConfigSpec for the "uns_beta_single" input.
func unsBetaSingleConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Field(service.NewStringField("consumer_group").
			Description("Consumer group used when consuming the configured Kafka topic.")).
		Field(service.NewStringField("kafka_topic").
			Description("The Kafka topic to consume.").
			Default("umh.messages").
			Advanced()).
		Field(service.NewStringListField("umh_topics").
			Description("List of RE2 regex patterns matched against each record's Kafka key (the umh_topic). Only records whose key matches at least one pattern are delivered; records with an empty or absent key never match any pattern. Patterns are unanchored; use ^...$ to match the full key. Required; to consume the whole namespace, set it explicitly to [\".*\"]."))
}

func init() {
	if err := service.RegisterBatchInput("uns_beta_single", unsBetaSingleConfigSpec(), newUnsBetaSingle); err != nil {
		panic(err)
	}
}

// newUnsBetaSingle constructs the "uns_beta_single" input, rejecting invalid
// config. A valid config builds; the built input returns a not-implemented
// error when read.
func newUnsBetaSingle(conf *service.ParsedConfig, _ *service.Resources) (service.BatchInput, error) {
	consumerGroup, err := conf.FieldString("consumer_group")
	if err != nil {
		return nil, err
	}
	if consumerGroup == "" {
		return nil, errors.New("consumer_group must not be empty")
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
	// newBetaKeyFilter is the single source of truth for umh_topics validation
	// (non-empty list, no empty/whitespace element, each pattern compiles, the
	// combined alternation compiles within RE2 limits). Reusing it makes the
	// single form's validation identical to the uns_beta sibling's and keeps
	// the compiled filter for R1b's ReadBatch.
	keyFilter, err := newBetaKeyFilter(patterns)
	if err != nil {
		return nil, err
	}

	return &unsBetaSingleInput{keyFilter: keyFilter}, nil
}

// unsBetaSingleInput is a placeholder BatchInput returned by a successful
// build. Connect and ReadBatch both return a not-implemented error so the
// input fails fast and visibly (ConnectionFailing, not ConnectionActive)
// rather than looping on a transient retryable error — a valid config is
// still distinguishable from a validation rejection.
type unsBetaSingleInput struct {
	keyFilter *betaKeyFilter
}

func (i *unsBetaSingleInput) Connect(context.Context) error {
	return errors.New("uns_beta_single: not implemented")
}

func (i *unsBetaSingleInput) ReadBatch(context.Context) (service.MessageBatch, service.AckFunc, error) {
	return nil, nil, errors.New("uns_beta_single: not implemented")
}

func (i *unsBetaSingleInput) Close(context.Context) error { return nil }
