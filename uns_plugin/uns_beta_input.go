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
//
// uns_beta is two registered pieces:
//   - "uns_beta" — a benthos template (MustRegisterTemplateYAML) exposing the
//     lean 4-field user surface (broker_address, consumer_group, kafka_topic,
//     umh_topics) and synthesizing the nested redpanda config in its mapping.
//   - "uns_beta_reader" — this Go core input (internal/Deprecated). Its spec
//     declares an `input:` field that the FRAMEWORK parses on the real manager,
//     so the inner redpanda input's logs, metrics and kafka_lag route through
//     the outer stream's logger/metrics registry instead of the noop manager
//     that the old ParseYAML(nil) construction built them on.
//
// For template-produced configs the normalized scalars (seed_brokers,
// consumer_group, kafka_topic) are computed once and written to both the nested
// redpanda block and these top-level reader fields, so they match by
// construction of the template. uns_beta_reader itself is a Deprecated raw
// surface and validates only the top-level copy; the inner redpanda config is
// an opaque OwnedInput with no accessor, so this equality is a template
// convention guarded by the render/parity tests, not a Go guarantee. A
// hand-authored uns_beta_reader could set the two independently.

package uns_plugin

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"

	// Registers the official "redpanda" input that uns_beta delegates to.
	_ "github.com/redpanda-data/connect/v4/public/components/kafka"
)

// unsBetaReaderConfigSpec is the spec for the internal "uns_beta_reader" core
// input. It is Deprecated() and undocumented so the Management Console steers
// users to the "uns_beta" template instead of this raw nested-input surface.
//
// Five fields: the framework-parsed `input` (the synthesized redpanda input),
// `umh_topics`, and the three normalized scalars the template also baked into
// the nested redpanda config — seed_brokers/consumer_group/kafka_topic. The
// scalars are validation-only: the constructor rejects the same values the
// template rendered into the redpanda block, so a bad value fails at config
// build with the user-facing error rather than silently inside the inner input.
func unsBetaReaderConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Deprecated().
		Field(service.NewInputField("input")).
		Field(service.NewStringListField("umh_topics").
			Description("List of RE2 regex patterns matched against each record's Kafka key (the umh_topic). Only records whose key matches at least one pattern are delivered; records with an empty or absent key never match any pattern. Patterns are unanchored; use ^...$ to match the full key. Defaults to a single match-everything pattern.").
			Default([]any{".*"}).
			Advanced().
			Examples(
				[]any{".*"},
				[]any{`^umh\.v1\.acme\.berlin\..+$`, `^umh\.v1\.acme\.munich\..+$`})).
		// The template-supplied normalized scalars. For template-produced
		// configs the template computes each once and writes it to both the
		// nested redpanda block and these fields, so validating the top-level
		// copy here covers the value redpanda receives. That equality is a
		// template convention (the inner redpanda config is an opaque
		// OwnedInput with no accessor, so Go cannot enforce it); the
		// render/parity tests guard it.
		Field(service.NewStringListField("seed_brokers").Default([]any{})).
		Field(service.NewStringField("consumer_group").Default("")).
		Field(service.NewStringField("kafka_topic").Default(""))
}

// betaKeyFilter is the compiled umh_topics filter: the configured patterns
// combined into one alternation, matched against each record's Kafka key.
type betaKeyFilter struct {
	re *regexp.Regexp
}

// newBetaKeyFilter compiles the umh_topics patterns into a single combined
// regex, (?:p1)|(?:p2)|... — the same join NewMessageProcessor uses for the
// legacy uns input. An empty pattern list is rejected: a regex joined from
// zero patterns is the empty pattern, which matches EVERYTHING — a silent
// filter bypass when the user plausibly meant match-nothing.
func newBetaKeyFilter(patterns []string) (*betaKeyFilter, error) {
	if len(patterns) == 0 {
		return nil, errors.New("umh_topics must not be empty")
	}
	wrapped := make([]string, len(patterns))
	for i, pattern := range patterns {
		// An empty element compiles fine on its own but wraps to (?:), which
		// matches every key — the same silent filter bypass as the empty
		// list, smuggled in by a stray `- ""` or trailing `-` in YAML.
		if strings.TrimSpace(pattern) == "" {
			return nil, fmt.Errorf("umh_topics pattern at index %d must not be empty or whitespace-only (an empty pattern matches every key)", i)
		}
		// Validate each pattern individually so the error names the culprit
		// instead of pointing at the combined alternation.
		if _, err := regexp.Compile(pattern); err != nil {
			return nil, fmt.Errorf("invalid umh_topics pattern at index %d: %s - %w", i, pattern, err)
		}
		wrapped[i] = fmt.Sprintf("(?:%s)", pattern)
	}
	// The join can fail even though every pattern compiled individually:
	// RE2's program-size limit is cumulative across the alternation
	// ("expression too large"), so surface it as a config error — the legacy
	// uns input handles the identical join the same way (uns_input_processor.go).
	re, err := regexp.Compile(strings.Join(wrapped, "|"))
	if err != nil {
		return nil, fmt.Errorf("compiling combined umh_topics pattern: %w", err)
	}
	return &betaKeyFilter{re: re}, nil
}

// matches reports whether a record with the given Kafka key is delivered.
// An empty/absent key NEVER matches, regardless of patterns (`.*` matches
// ""): a keyless record delivered downstream hits a guaranteed uns-output
// rejection → NACK → infinite redelivery wedge (ENG-5094). This is a
// deliberate behavior change from the legacy uns input, which DELIVERED
// keyless records under its default ".*" pattern (ParseFromBenthos).
func (f *betaKeyFilter) matches(key string) bool {
	return key != "" && f.re.MatchString(key)
}

// legalKafkaTopicName is Kafka's own topic-name rule (kafka.common.Topic):
// 1-249 chars from [a-zA-Z0-9._-]. The reserved names "." and ".." match the
// pattern and are rejected separately in newUnsBetaReader.
var legalKafkaTopicName = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

func init() {
	if err := service.RegisterBatchInput("uns_beta_reader", unsBetaReaderConfigSpec(), newUnsBetaReader); err != nil {
		panic(err)
	}
	service.MustRegisterTemplateYAML(unsBetaTemplate)
}

// newUnsBetaReader builds the reader from the framework-parsed config. The
// template renders the normalized scalars into both the nested redpanda block
// and these top-level fields, so the reject logic here runs on the exact value
// the inner redpanda input received. Errors here reach the user because the
// framework — not a noop-logger manager — built the input.
func newUnsBetaReader(conf *service.ParsedConfig, _ *service.Resources) (service.BatchInput, error) {
	// Validate the template-normalized scalars with today's exact reject logic
	// (and exact error strings). seed_brokers arrives already split/trimmed by
	// the template's $brokers bloblang, so Go only checks the resulting length.
	seedBrokers, err := conf.FieldStringList("seed_brokers")
	if err != nil {
		return nil, err
	}
	if len(seedBrokers) == 0 {
		// A separators-only broker_address yields zero brokers after the
		// template's split/trim/drop-empty; an empty seed_brokers falls back to
		// the global redpanda block, so reject it here.
		return nil, errors.New("broker_address must contain at least one broker")
	}

	consumerGroup, err := conf.FieldString("consumer_group")
	if err != nil {
		return nil, err
	}
	if consumerGroup == "" {
		// An empty group silently disables offset commits (full-topic replay on
		// every restart) AND suppresses kafka_lag (it only registers when the
		// group is non-empty), so fail here.
		return nil, errors.New("consumer_group must not be empty")
	}

	kafkaTopic, err := conf.FieldString("kafka_topic")
	if err != nil {
		return nil, err
	}
	if kafkaTopic == "" {
		// An explicit empty value (or a whitespace-only one the template
		// trimmed to "") would render topics: [""], a silently dead input.
		return nil, errors.New("kafka_topic must not be empty")
	}
	if !legalKafkaTopicName.MatchString(kafkaTopic) || kafkaTopic == "." || kafkaTopic == ".." {
		// Kafka only accepts topic names matching [a-zA-Z0-9._-]{1,249}, minus
		// the reserved "." and "..". The whitelist regex matches "." and "..",
		// so the reserved-name reject must stay explicit. ':' would switch the
		// inner redpanda input into explicit topic:partition mode; every other
		// illegal name is rejected by the broker after connect. Whitelist here,
		// where the error reaches the user.
		return nil, errors.New("kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)")
	}

	// Build the filter before the inner input so a bad umh_topics fails fast
	// without leaving an unclosed OwnedInput behind.
	patterns, err := conf.FieldStringList("umh_topics")
	if err != nil {
		return nil, err
	}
	keyFilter, err := newBetaKeyFilter(patterns)
	if err != nil {
		return nil, err
	}

	// The framework parsed the `input:` field on the REAL manager, so this
	// OwnedInput's logs, metrics and kafka_lag route through the outer stream.
	inner, err := conf.FieldInput("input")
	if err != nil {
		return nil, err
	}

	return newUnsBetaInputFor(inner, keyFilter), nil
}

// readBatcher is the part of *service.OwnedInput that unsBetaInput drives: a
// batch read and a close. Typing the field as an interface (rather than the
// concrete *service.OwnedInput) lets the unit tests substitute a fake that
// scripts ReadBatch return sequences, so the self-ack loop can be exercised
// deterministically without a broker. *service.OwnedInput satisfies it.
//
// A connectivity-test method is added when the reconnect logic lands.
type readBatcher interface {
	ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error)
	Close(ctx context.Context) error
}

// mustBetaKeyFilter compiles a filter from patterns that are known-valid at
// compile time; a failure is a programmer error, not runtime input, so it
// panics. Used for the match-everything fallback below.
func mustBetaKeyFilter(patterns []string) *betaKeyFilter {
	f, err := newBetaKeyFilter(patterns)
	if err != nil {
		panic(fmt.Sprintf("mustBetaKeyFilter: %v", err))
	}
	return f
}

// matchEverythingFilter is the [".*"] fallback the constructor substitutes for a
// nil keyFilter. ".*" delivers every keyed record but still drops keyless ones
// (matches requires key != ""), so the keyless-drop contract holds.
var matchEverythingFilter = mustBetaKeyFilter([]string{".*"})

// newUnsBetaInputFor builds the adapter from an already-constructed delegated
// reader and compiled key filter. A nil keyFilter defaults to the
// match-everything [".*"] filter, so the unsBetaInput.keyFilter field is never
// nil and ReadBatch can call i.keyFilter.matches unconditionally — no caller
// (production or test) can build an instance that panics on a nil filter.
func newUnsBetaInputFor(inner readBatcher, keyFilter *betaKeyFilter) *unsBetaInput {
	if keyFilter == nil {
		keyFilter = matchEverythingFilter
	}
	return &unsBetaInput{inner: inner, keyFilter: keyFilter}
}

// unsBetaInput adapts the delegated input (which manages its own connectivity)
// to the service.BatchInput interface, filtering records by the umh_topics key
// regex.
type unsBetaInput struct {
	inner readBatcher
	// keyFilter is the compiled umh_topics filter; always non-nil (the field
	// defaults to [".*"], which delivers every keyed record).
	keyFilter *betaKeyFilter
}

// Connect is a no-op: the delegated OwnedInput connects lazily on first
// ReadBatch.
func (i *unsBetaInput) Connect(context.Context) error {
	return nil
}

// filterAndAlias applies the umh_topics filter to a delegated poll and stamps
// the UMH aliases onto the kept records. It keeps only records whose Kafka key
// matches a configured pattern; a keyless record never matches (see
// betaKeyFilter.matches). Dropped records stay covered by the shared delegated
// ack: a batch with at least one kept record commits past the dropped offsets
// when the kept subset is acked.
//
// Trust boundary: kafka_key is trusted as the record key here, but a
// producer header literally named "kafka_key" shadows it (the
// delegated input applies headers after the native fields). The
// shadowed value controls BOTH the drop/deliver decision and the
// aliases: a spoofed header can drop (and commit past) a legitimately
// keyed record, or deliver a keyless one. Detecting the spoof IS
// possible at this layer — the delegated input preserves the original
// kgo header slice under __rpcn_kafka_headers (deleted below) — but
// recovering the shadowed real key is not, so hardening is deferred to
// ENG-5125. The uns output strips kafka_-prefixed
// metadata before producing, so the shadowing header cannot arise from
// uns-output hops.
// Mutating these messages after ReadBatch returns is safe with
// auto_replay_nacks: the batch is a shallow copy and MetaSetMut/MetaDelete
// clone the metadata map before writing, so the snapshot the retry list
// replays on NACK is untouched. Only store immutable values (key is a string).
func filterAndAlias(batch service.MessageBatch, f *betaKeyFilter) service.MessageBatch {
	kept := make(service.MessageBatch, 0, len(batch))
	for _, msg := range batch {
		key, _ := msg.MetaGet("kafka_key")
		if !f.matches(key) {
			continue
		}
		// The filter guarantees a non-empty matching key here, so the
		// aliases are stamped unconditionally.
		msg.MetaSetMut("umh_topic", key)
		msg.MetaSetMut("kafka_msg_key", key)
		// The delegated input stores the original kgo header slice under
		// __rpcn_kafka_headers; left in place it round-trips into
		// downstream Kafka headers and compounds per hop through the uns
		// output, so it is stripped here.
		msg.MetaDelete("__rpcn_kafka_headers")
		kept = append(kept, msg)
	}
	return kept
}

func (i *unsBetaInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	for {
		batch, ack, err := i.inner.ReadBatch(ctx)
		if err != nil {
			// BatchInput does not guarantee batch == nil on error; an
			// error-accompanied batch is not delivered downstream, so the
			// aliases are not set on it. The ack is nil on the error path —
			// never touch it.
			return batch, ack, err
		}
		kept := filterAndAlias(batch, i.keyFilter)
		if len(kept) > 0 {
			return kept, ack, nil
		}
		// All-filtered poll: every record failed the umh_topics filter, so the
		// kept batch is empty. Benthos's AsyncReader discards an empty batch
		// without calling its AckFunc, so the delegated redpanda checkpoint
		// would never resolve and the partition would wedge at the un-acked
		// offset (ENG-5094 / ENG-5105). Self-ack the delegated transaction so
		// the commit advances past the non-matching records, then read the next
		// poll — the consumer never sees these records.
		if ackErr := ack(ctx, nil); ackErr != nil {
			// A failed empty-ack is a commit failure: return it — the caller
			// must see commit failures.
			return nil, nil, ackErr
		}
		// Check ctx.Err() before re-polling — a cancelled context would
		// otherwise spin the loop indefinitely against a delegated input that
		// keeps returning all-filtered polls.
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
	}
}

func (i *unsBetaInput) Close(ctx context.Context) error {
	return i.inner.Close(ctx)
}
