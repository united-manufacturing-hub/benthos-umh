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
// template form. Unlike uns_beta (which splits translation/template from the
// Go reader), uns_beta_single does both in Go: it translates the lean 4-field
// surface into a redpanda input config, builds the redpanda input via the
// connect forwarder shim, and wraps it with the umh_topics key filter.

package uns_plugin

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// validateYAMLScalar rejects any control character or leading/trailing
// whitespace in a user string that is interpolated into the translated
// redpanda config YAML. A newline lets a user field break out of its scalar
// and inject arbitrary redpanda keys (e.g. a consumer_group of
// "x\nauto_replay_nacks: false" silently disabling the ENG-5094 NACK-replay
// guarantee). The values are also %q-quoted in the YAML, but this validation
// fails fast with a named field rather than relying on the parser. It does
// NOT forbid ':' because broker_address legitimately holds host:port pairs.
func validateYAMLScalar(v, fieldName string) error {
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

// unsBetaSingleConfigSpec returns the ConfigSpec for the "uns_beta_single" input.
func unsBetaSingleConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Field(service.NewStringField("broker_address").
			Description("Kafka broker address to connect to. For example: \"localhost:9092\".").
			Default("localhost:9092").
			Advanced()).
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
// config. A valid config builds a lazy input that constructs the redpanda
// forwarder on first Connect (so a unit test that builds without a live
// broker/resources still succeeds); the actual reader is wired only when the
// framework drives Connect with real resources.
func newUnsBetaSingle(conf *service.ParsedConfig, res *service.Resources) (service.BatchInput, error) {
	brokerAddress, err := conf.FieldString("broker_address")
	if err != nil {
		return nil, err
	}
	if err := validateYAMLScalar(brokerAddress, "broker_address"); err != nil {
		return nil, err
	}
	seedBrokers := []string{brokerAddress}

	consumerGroup, err := conf.FieldString("consumer_group")
	if err != nil {
		return nil, err
	}
	if consumerGroup == "" {
		return nil, errors.New("consumer_group must not be empty")
	}
	if err := validateYAMLScalar(consumerGroup, "consumer_group"); err != nil {
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
	// newBetaKeyFilter is the single source of truth for umh_topics validation
	// (non-empty list, no empty/whitespace element, each pattern compiles, the
	// combined alternation compiles within RE2 limits). Reusing it makes the
	// single form's validation identical to the uns_beta sibling's and keeps
	// the compiled filter for the Go-level key filter.
	keyFilter, err := newBetaKeyFilter(patterns)
	if err != nil {
		return nil, err
	}

	// The config-validation unit test constructs the input with a nil
	// *service.Resources; res.Metrics() panics on a nil receiver, so guard it.
	// A nil keylessCounter is a no-op Incr (*service.MetricCounter treats a
	// nil receiver as a no-op), so the nil-resources path stays safe.
	var keylessCounter *service.MetricCounter
	var filteredCounter *service.MetricCounter
	if res != nil {
		keylessCounter = res.Metrics().NewCounter("dropped_keyless")
		// filtered_records counts keyed records that reach ReadBatch but fail
		// the umh_topics filter (the !matches branch with key != ""), mirroring
		// uns_beta's dropCounters.filtered. The handle is stored so the counter
		// is wired — registered AND incremented — rather than a dead
		// registration whose handle is discarded and can never fire (which would
		// read as a false zero on the divergence path). The increment path is
		// not exercised by the integration suite: connect's key_pattern
		// pre-filter omits keyed non-matching records before ReadBatch in both
		// forms, so end-to-end coverage of the increment is tracked in ENG-5125.
		filteredCounter = res.Metrics().NewCounter("filtered_records")
	}
	return &unsBetaSingleInput{
		res:             res,
		seedBrokers:     seedBrokers,
		kafkaTopic:      kafkaTopic,
		consumerGroup:   consumerGroup,
		umhTopics:       patterns,
		keyFilter:       keyFilter,
		keylessCounter:  keylessCounter,
		filteredCounter: filteredCounter,
	}, nil
}

// unsBetaSingleInput is the BatchInput returned by a successful build. The
// inner redpanda reader is constructed lazily on Connect (so construction
// itself never dials a broker). A zero-value unsBetaSingleInput (inner == nil)
// returns a not-implemented error from ReadBatch, keeping the placeholder's
// "valid config is distinguishable from a validation rejection" contract.
type unsBetaSingleInput struct {
	res           *service.Resources
	seedBrokers   []string
	kafkaTopic    string
	consumerGroup string
	umhTopics     []string
	keyFilter     *betaKeyFilter

	// keylessCounter counts keyless drops to match uns_beta's dropped_keyless
	// counter (records with an empty Kafka key that reach ReadBatch and fail
	// the `key != ""` guard). Parity holds for genuine (non-spoofed) keyless
	// records only: a record whose native key is empty but carries a foreign
	// header named "kafka_key" is tallied as dropped_spoofed_key by uns_beta's
	// spoof-first classifyDrop, but is mis-counted as dropped_keyless here
	// because the single form lacks hasSpoofHeader. Of the other two per-reason
	// counters, filtered_records is registered AND incremented (the !matches
	// branch with key != ""), mirroring uns_beta's dropCounters.filtered; the
	// increment path is not exercised by the integration suite (connect's
	// key_pattern pre-filter omits keyed non-matching records before ReadBatch),
	// so end-to-end coverage is tracked in ENG-5125. dropped_spoofed_key is not
	// registered at all in the single form (no hasSpoofHeader to feed it).
	// Spoof detection is tracked in ENG-5125, which closes the gap for both
	// forms together.
	keylessCounter *service.MetricCounter

	// filteredCounter counts keyed records that reach ReadBatch but fail the
	// umh_topics filter (the !matches branch with key != ""). Mirrors uns_beta's
	// dropCounters.filtered; nil under a nil-resources build, in which case
	// *service.MetricCounter treats a nil receiver as a no-op Incr.
	filteredCounter *service.MetricCounter

	// inner is built on first Connect by buildUnsBetaSingleInner (forwarder
	// under connect_patched, stub otherwise). nil until then.
	inner service.BatchInput
}

func (i *unsBetaSingleInput) Connect(ctx context.Context) error {
	if i.inner == nil {
		inner, err := buildUnsBetaSingleInner(i.res, i.seedBrokers, i.kafkaTopic, i.consumerGroup, i.umhTopics)
		if err != nil {
			return err
		}
		i.inner = inner
	}
	return i.inner.Connect(ctx)
}

func (i *unsBetaSingleInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if i.inner == nil {
		return nil, nil, errors.New("uns_beta_single: not implemented")
	}
	for {
		batch, ack, err := i.inner.ReadBatch(ctx)
		if err != nil {
			// BatchInput does not guarantee batch == nil on error; an
			// error-accompanied batch is not delivered downstream. The ack is
			// nil on the error path — never touch it.
			return batch, ack, err
		}
		// Go-level key filter (defense-in-depth alongside connect's
		// key_pattern): drop records whose Kafka key does not match umh_topics.
		// This is the single source of truth for the contract (ConfigSpec:
		// "records with an empty or absent key never match any pattern") —
		// connect's key_pattern alone matches empty keys under ".*"
		// (regexp ".*".MatchString("") == true), which re-opens the ENG-5094
		// infinite-redelivery wedge (keyless record → uns-output rejection →
		// NACK → auto_replay_nacks redelivers forever). matches enforces
		// key != "" itself, mirroring uns_beta's filterAndAlias.
		kept := make(service.MessageBatch, 0, len(batch))
		for _, msg := range batch {
			key, ok := msg.MetaGet("kafka_key")
			// connect's key-omit pre-filter keeps a poll's LAST record as a
			// high-water placeholder (service.NewMessage(nil), no metadata)
			// when it fails key_pattern, so its offset still commits;
			// recognize it by the ABSENCE of kafka_key meta and drop it. A
			// real keyless record has ok==true, key=="" and falls through to
			// matches, which rejects it (empty key never matches).
			if !ok {
				continue
			}
			if !i.keyFilter.matches(key) {
				// A real keyless record (ok && key=="") reached this guard
				// because connect's key_pattern matched the empty key (".*"
				// matches ""); the `key != ""` guard in matches drops it.
				// Count it on the outer stream's metrics registry, mirroring
				// uns_beta's dropped_keyless counter — but only for genuine
				// keyless records. A spoofed-keyless record (native key empty,
				// foreign "kafka_key" header shadowing it — the ENG-5125 spoof
				// vector) is mis-counted here as dropped_keyless because this
				// form lacks uns_beta's spoof-first hasSpoofHeader check; uns_beta
				// tallies it as dropped_spoofed_key. TODO(ENG-5125): port
				// hasSpoofHeader/classifyDrop so spoofed records are not
				// mis-counted, and wire dropped_spoofed_key.
				// The connect high-water placeholder is recognized by the
				// ABSENCE of kafka_key meta (!ok path above), so it does not
				// reach this counter today — but that recognition depends on
				// connect's current __rpcn_kafka_headers/key-meta semantics; a
				// connect upgrade that changes them (see rpcnKafkaHeadersKey in
				// uns_beta_input.go) could surface the placeholder here. This
				// is a shared invariant (uns_beta's filterAndAlias has the same
				// dependency), so the parameterized suite cannot detect its
				// regression.
				if key == "" {
					i.keylessCounter.Incr(1)
				} else {
					// A keyed non-matching record reached ReadBatch (only
					// possible if connect's key_pattern pre-filter diverges
					// from this Go-level filter, or a connect upgrade disables
					// it). Count it on filtered_records, mirroring uns_beta's
					// dropCounters.filtered, so the divergence is observable
					// rather than a silent dead-zero.
					i.filteredCounter.Incr(1)
				}
				continue
			}
			// Stamp the umh_topic/kafka_msg_key aliases and strip the
			// connect-internal __rpcn_kafka_headers meta, mirroring uns_beta's
			// filterAndAlias. The uns output's default Kafka key is
			// meta("umh_topic") and it rejects a record whose umh_topic meta is
			// empty (uns_output.go:404), so without these aliases a delivered
			// record is rejected by the uns output and the production pipeline
			// breaks. The header strip prevents the delegated kgo header slice
			// from round-tripping into downstream Kafka headers.
			msg.MetaSetMut("umh_topic", key)
			msg.MetaSetMut("kafka_msg_key", key)
			msg.MetaDelete(rpcnKafkaHeadersKey)
			kept = append(kept, msg)
		}
		if len(kept) > 0 {
			return kept, ack, nil
		}
		// All-filtered poll: benthos's AsyncReader discards an empty batch
		// without calling its AckFunc, so the delegated redpanda checkpoint
		// would never resolve and the partition would wedge at the un-acked
		// offset (ENG-5094 / ENG-5105). Self-ack the delegated transaction so
		// the commit advances past the non-matching records, then read the
		// next poll — the consumer never sees these records.
		if ackErr := ack(ctx, nil); ackErr != nil {
			return nil, nil, ackErr
		}
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
	}
}

func (i *unsBetaSingleInput) Close(ctx context.Context) error {
	if i.inner == nil {
		return nil
	}
	return i.inner.Close(ctx)
}
