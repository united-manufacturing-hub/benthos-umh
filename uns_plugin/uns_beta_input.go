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

// uns_beta input (ENG-5094): a thin adapter that delegates to the official
// redpanda Connect input via OwnedInput so that NACKed batches are never
// committed away.

package uns_plugin

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"

	// Registers the official "redpanda" input that uns_beta delegates to.
	_ "github.com/redpanda-data/connect/v4/public/components/kafka"
)

func unsBetaConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Field(service.NewStringField("broker_address").
			Description("Kafka broker address to connect to. This can be a single address or multiple addresses separated by commas. For example: \"localhost:9092\" or \"broker1:9092,broker2:9092\".")).
		Field(service.NewStringField("consumer_group").
			Description("Consumer group used when consuming the configured Kafka topic.")).
		Field(service.NewStringField("kafka_topic").
			Description("The Kafka topic to consume.").
			Default(defaultInputKafkaTopic).
			Advanced().
			Examples(defaultInputKafkaTopic))
}

// legalKafkaTopicName is Kafka's own topic-name rule (kafka.common.Topic):
// 1-249 chars from [a-zA-Z0-9._-]. The reserved names "." and ".." match the
// pattern and are rejected separately in buildInnerYAML.
var legalKafkaTopicName = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

// splitBrokerAddress splits a comma-separated broker list, trimming whitespace
// and dropping empty entries (e.g. from a trailing comma).
func splitBrokerAddress(brokerAddress string) []string {
	var brokers []string
	for _, b := range strings.Split(brokerAddress, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return brokers
}

// renderRedpandaFragment renders the nested redpanda-input YAML that uns_beta
// delegates to. The fetch limits are rendered from the OOM-tuned defaultFetch*
// constants in uns_input_config.go; the byte sizes render as plain digit
// strings (SI bytes) so the redpanda byte-size parser cannot reinterpret the
// unit.
func renderRedpandaFragment(brokers []string, topic, consumerGroup string) string {
	quoted := make([]string, len(brokers))
	for i, b := range brokers {
		quoted[i] = fmt.Sprintf("%q", b)
	}
	// start_offset and commit_period match redpanda's current defaults; pinned
	// deliberately so an upstream default change cannot alter uns_beta's
	// first-connect behavior or commit cadence. auto_replay_nacks also matches
	// the upstream default, but it is pinned for a stronger reason: it IS the
	// ENG-5094 NACK-replay guarantee — never prune it even if it stays
	// identical to the default.
	return fmt.Sprintf(`
input:
  redpanda:
    seed_brokers: [%s]
    topics: [%q]
    consumer_group: %q
    start_offset: earliest
    commit_period: "5s"
    fetch_max_bytes: %q
    fetch_max_partition_bytes: %q
    fetch_min_bytes: %q
    fetch_max_wait: %q
    conn_idle_timeout: %q
    auto_replay_nacks: true
`, strings.Join(quoted, ", "), topic, consumerGroup,
		strconv.Itoa(defaultFetchMaxBytes),
		strconv.Itoa(defaultFetchMaxPartitionBytes),
		strconv.Itoa(defaultFetchMinBytes),
		defaultFetchMaxWaitTime.String(),
		defaultConnIdleTimeout.String())
}

// buildInnerYAML reads and validates the uns_beta fields off the parsed config
// and renders the redpanda fragment newUnsBetaInput delegates to. Validation
// fails fast here because failures inside the inner OwnedInput land in a noop
// logger and would otherwise be invisible.
func buildInnerYAML(conf *service.ParsedConfig) (string, error) {
	brokerAddress, err := conf.FieldString("broker_address")
	if err != nil {
		return "", err
	}
	// Split before validating: a separators-only value like " , " is non-empty
	// as a raw string but yields zero brokers, which would render
	// seed_brokers: [].
	brokers := splitBrokerAddress(brokerAddress)
	if len(brokers) == 0 {
		return "", errors.New("broker_address must contain at least one broker")
	}
	consumerGroup, err := conf.FieldString("consumer_group")
	if err != nil {
		return "", err
	}
	if consumerGroup = strings.TrimSpace(consumerGroup); consumerGroup == "" {
		// An empty group parses fine downstream but silently disables offset
		// commits (full-topic replay on every restart), so fail here.
		return "", errors.New("consumer_group must not be empty")
	}
	kafkaTopic, err := conf.FieldString("kafka_topic")
	if err != nil {
		return "", err
	}
	if kafkaTopic = strings.TrimSpace(kafkaTopic); kafkaTopic == "" {
		// An explicit empty value overrides the Benthos default (defaults
		// apply only when the field is absent) and would render topics: [""],
		// a silently dead input.
		return "", errors.New("kafka_topic must not be empty")
	}
	if !legalKafkaTopicName.MatchString(kafkaTopic) || kafkaTopic == "." || kafkaTopic == ".." {
		// Kafka only accepts topic names matching [a-zA-Z0-9._-]{1,249}, minus
		// the reserved "." and "..". An illegal name fails late and silently:
		// ':' switches the inner redpanda input into explicit topic:partition
		// mode (rejected only via a field lint the nested ParseYAML path never
		// runs), and every other illegal name is rejected by the broker after
		// connect, inside the noop-logger inner input. Whitelist here, where
		// the error reaches the user.
		return "", errors.New("kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)")
	}
	return renderRedpandaFragment(brokers, kafkaTopic, consumerGroup), nil
}

func init() {
	if err := service.RegisterBatchInput("uns_beta", unsBetaConfigSpec(), newUnsBetaInput); err != nil {
		panic(err)
	}
}

func newUnsBetaInput(conf *service.ParsedConfig, _ *service.Resources) (service.BatchInput, error) {
	innerYAML, err := buildInnerYAML(conf)
	if err != nil {
		return nil, err
	}

	// ParseYAML(innerYAML, nil) builds the inner redpanda input on a fresh
	// internal manager whose logger and metrics are noops; the public API has
	// no hook to hand it the outer Resources, so the inner input's own logs
	// and metrics are dropped.
	innerSpec := service.NewConfigSpec().Field(service.NewInputField("input"))
	parsed, err := innerSpec.ParseYAML(innerYAML, nil)
	if err != nil {
		return nil, err
	}
	inner, err := parsed.FieldInput("input")
	if err != nil {
		return nil, err
	}
	return &unsBetaInput{inner: inner}, nil
}

// unsBetaInput adapts *service.OwnedInput (which manages its own connectivity)
// to the service.BatchInput interface.
type unsBetaInput struct {
	inner *service.OwnedInput
}

// Connect is a no-op: the delegated OwnedInput connects lazily on first
// ReadBatch.
func (i *unsBetaInput) Connect(context.Context) error {
	return nil
}

func (i *unsBetaInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	return i.inner.ReadBatch(ctx)
}

func (i *unsBetaInput) Close(ctx context.Context) error {
	return i.inner.Close(ctx)
}
