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
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	// The "pure" components register the "none" tracer that StreamBuilder.Build
	// defaults to; the render test below builds a (never-run) stream.
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
)

// renderUnsBetaReader expands a `uns_beta: {…}` config through the registered
// template and returns the rendered uns_beta_reader config as a map. It does
// this without the production registry: it clones the global environment,
// swaps in a capturing uns_beta_reader that records its parsed config (via
// FieldAny) and aborts construction, then builds a StreamBuilder so the
// framework renders the template's mapping with the real bloblang env. The
// capture aborts with a sentinel error, so Build/Run failing with that sentinel
// is the success path. Both the nested redpanda block and the top-level reader
// scalars are read off this map — NOT off the *OwnedInput (opaque) or the
// reader struct (holds only inner+keyFilter).
func renderUnsBetaReader(unsBetaYAML string) map[string]any {
	GinkgoHelper()
	const sentinel = "render-capture-abort"
	env := service.NewEnvironment().Without("uns_beta_reader")
	var captured map[string]any
	err := env.RegisterBatchInput("uns_beta_reader", unsBetaReaderConfigSpec(),
		func(conf *service.ParsedConfig, _ *service.Resources) (service.BatchInput, error) {
			v, e := conf.FieldAny()
			Expect(e).NotTo(HaveOccurred())
			m, ok := v.(map[string]any)
			Expect(ok).To(BeTrue(), "rendered reader config is not a map: %T", v)
			captured = m
			return nil, errors.New(sentinel)
		})
	Expect(err).NotTo(HaveOccurred())

	sb := env.NewStreamBuilder()
	Expect(sb.AddInputYAML(unsBetaYAML)).To(Succeed())
	Expect(sb.AddConsumerFunc(func(context.Context, *service.Message) error { return nil })).To(Succeed())
	stream, err := sb.Build()
	Expect(err).NotTo(HaveOccurred())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // never actually consume; capture fires at input construction
	_ = stream.Run(ctx)

	Expect(captured).NotTo(BeNil(), "uns_beta_reader was never constructed — the template did not render")
	return captured
}

// redpandaOf returns the nested input.redpanda map from a rendered reader config.
func redpandaOf(reader map[string]any) map[string]any {
	GinkgoHelper()
	input, ok := reader["input"].(map[string]any)
	Expect(ok).To(BeTrue(), "reader has no input mapping: %#v", reader)
	rp, ok := input["redpanda"].(map[string]any)
	Expect(ok).To(BeTrue(), "reader.input has no redpanda mapping: %#v", input)
	return rp
}

// The "uns_beta" template synthesizes the nested redpanda config from the lean
// 4-field user surface. These specs expand the template and assert the rendered
// config: the OOM-tuned fetch limits (as STRINGS, the upstream string-field
// contract), the ENG-5094 pins, the normalize-once-reference-twice parity, and
// the trim/clean normalization that moved out of the old Go splitBrokerAddress.
var _ = Describe("uns_beta template rendering", Label("uns_beta"), func() {
	It("renders the configured fields and pins into the redpanda block", func() {
		reader := renderUnsBetaReader(`
uns_beta:
  broker_address: "broker1:9092, broker2:9092,"
  consumer_group: "grp"
  kafka_topic: "custom.messages"
`)
		rp := redpandaOf(reader)

		// csv split + trim + empty-entry drop (was splitBrokerAddress): the
		// trailing comma must not produce an empty seed_brokers element.
		Expect(rp["seed_brokers"]).To(Equal([]any{"broker1:9092", "broker2:9092"}))
		Expect(rp["topics"]).To(Equal([]any{"custom.messages"}))
		Expect(rp["consumer_group"]).To(Equal("grp"))
		Expect(rp["start_offset"]).To(Equal("earliest"))

		// The five OOM-tuned fetch/conn values are re-derived here from the
		// defaultFetch* constants in uns_input_config.go (the ones documented
		// "Reduced from 100MB to prevent OOM kills"), so retuning any constant
		// fails this test until the template literal is updated to match —
		// keeping the uns_beta template and the legacy uns path on the same OOM
		// tuning during the deprecation window. They are emitted AS STRINGS
		// (upstream NewStringField/NewDurationField): a bare int would
		// fail/coerce. commit_period and start_offset are independent pins of
		// redpanda's own defaults, not UMH constants, so commit_period stays the
		// literal "5s".
		for field, want := range map[string]string{
			"fetch_max_bytes":           strconv.FormatFloat(defaultFetchMaxBytes, 'f', -1, 64),
			"fetch_max_partition_bytes": strconv.FormatFloat(defaultFetchMaxPartitionBytes, 'f', -1, 64),
			"fetch_min_bytes":           strconv.FormatFloat(defaultFetchMinBytes, 'f', -1, 64),
			"fetch_max_wait":            defaultFetchMaxWaitTime.String(),
			"commit_period":             "5s",
			"conn_idle_timeout":         defaultConnIdleTimeout.String(),
		} {
			Expect(rp[field]).To(BeAssignableToTypeOf(""), "field %s must render as a YAML string", field)
			Expect(rp[field]).To(Equal(want), "field %s", field)
		}
		Expect(rp["auto_replay_nacks"]).To(Equal(true))
	})

	// Parity: each normalized scalar is computed once and referenced twice (top
	// level + nested redpanda). Read both off the rendered map and assert byte
	// equality. Honest scope: both operands derive from the same $-var, so the
	// cross-equality alone only guards a FUTURE mapping edit that breaks the
	// single-computation convention. The assertions below also pin concrete
	// values on BOTH sides, so an edit that re-normalizes only one side (leaving
	// the cross-equality intact but changing the value) still fails.
	It("renders each normalized scalar identically at the top level and in the redpanda block", func() {
		reader := renderUnsBetaReader(`
uns_beta:
  broker_address: "broker1:9092, broker2:9092,"
  consumer_group: "grp"
  kafka_topic: "custom.messages"
`)
		rp := redpandaOf(reader)
		Expect(reader["consumer_group"]).To(Equal(rp["consumer_group"]))
		Expect(rp["consumer_group"]).To(Equal("grp"))
		Expect(reader["consumer_group"]).To(Equal("grp"))
		Expect(reader["seed_brokers"]).To(Equal(rp["seed_brokers"]))
		Expect(rp["seed_brokers"]).To(Equal([]any{"broker1:9092", "broker2:9092"}))
		Expect(reader["seed_brokers"]).To(Equal([]any{"broker1:9092", "broker2:9092"}))
		topics, ok := rp["topics"].([]any)
		Expect(ok).To(BeTrue())
		Expect(topics).NotTo(BeEmpty())
		Expect(reader["kafka_topic"]).To(Equal(topics[0]))
		Expect(reader["kafka_topic"]).To(Equal("custom.messages"))
	})

	// Normalization pins — these three cases moved here from the validation
	// DescribeTable (they exercise the template's trim/clean, which is no longer
	// in the reader). A mutant that skips trimming would render padded values
	// and fail here.
	It("trims whitespace-padded consumer_group and kafka_topic before rendering", func() {
		reader := renderUnsBetaReader(`
uns_beta:
  broker_address: "localhost:9092"
  consumer_group: " g "
  kafka_topic: " custom.messages "
`)
		rp := redpandaOf(reader)
		Expect(rp["consumer_group"]).To(Equal("g"))
		Expect(rp["topics"]).To(Equal([]any{"custom.messages"}))
		// And the top-level reader scalars carry the same trimmed values.
		Expect(reader["consumer_group"]).To(Equal("g"))
		Expect(reader["kafka_topic"]).To(Equal("custom.messages"))
	})

	It("splits, trims, and drops empty entries from a multi-broker address", func() {
		reader := renderUnsBetaReader(`
uns_beta:
  broker_address: "b1:9092, b2:9092,"
  consumer_group: "g"
`)
		rp := redpandaOf(reader)
		Expect(rp["seed_brokers"]).To(Equal([]any{"b1:9092", "b2:9092"}))
		Expect(reader["seed_brokers"]).To(Equal([]any{"b1:9092", "b2:9092"}))
	})

	// Template default: an omitted kafka_topic falls back to umh.messages,
	// exactly as the Go .Default() the legacy spec used (open question #3).
	It("falls back to the default topic when kafka_topic is omitted", func() {
		reader := renderUnsBetaReader(`
uns_beta:
  broker_address: "localhost:9092"
  consumer_group: "g"
`)
		rp := redpandaOf(reader)
		Expect(rp["topics"]).To(Equal([]any{defaultInputKafkaTopic}))
	})
})

// validReaderBody builds a minimal valid uns_beta_reader body: a redpanda
// `input:` block (so FieldInput succeeds and the constructor reaches scalar
// validation) plus the normalized scalars under test. These are DIRECT
// uns_beta_reader-constructor unit tests — NOT driven through the template or
// StreamBuilder, which would wrap the Go error in *componentErr and surface it
// only at Run(), making exact MatchError impossible. The reader validates
// ALREADY-normalized scalars (the template normalizes), so each case feeds the
// post-normalization value directly (e.g. seed_brokers: [] for the empty case).
func validReaderBody(scalars string) string {
	return `
input:
  redpanda:
    seed_brokers: ["localhost:9092"]
    topics: ["umh.messages"]
    consumer_group: "g"
` + scalars
}

var _ = Describe("uns_beta_reader config validation", Label("uns_beta"), func() {
	// Each entry parses a body against uns_beta_reader's OWN spec and constructs
	// through newUnsBetaReader, asserting the BARE error (no framework wrapping).
	DescribeTable("newUnsBetaReader rejects the normalized scalars",
		func(scalars, wantErr string) {
			parsed, err := unsBetaReaderConfigSpec().ParseYAML(validReaderBody(scalars), nil)
			Expect(err).NotTo(HaveOccurred())
			_, err = newUnsBetaReader(parsed, nil)
			Expect(err).To(MatchError(wantErr))
		},
		// An empty seed_brokers (a separators-only broker_address normalizes to
		// this) falls back to the global redpanda block — reject it.
		Entry("empty seed_brokers",
			"seed_brokers: []\nconsumer_group: \"g\"\nkafka_topic: \"umh.messages\"",
			"broker_address must contain at least one broker"),
		// An empty group silently disables offset commits (full-topic replay on
		// every restart) AND suppresses kafka_lag — the ENG-5094 failure class.
		Entry("empty consumer_group",
			"seed_brokers: [\"localhost:9092\"]\nconsumer_group: \"\"\nkafka_topic: \"umh.messages\"",
			"consumer_group must not be empty"),
		// An empty kafka_topic (explicit or whitespace-only trimmed to "") would
		// render topics: [""], a silently dead input.
		Entry("empty kafka_topic",
			"seed_brokers: [\"localhost:9092\"]\nconsumer_group: \"g\"\nkafka_topic: \"\"",
			"kafka_topic must not be empty"),
		// ':' switches the inner redpanda input into explicit topic:partition
		// mode; the legal-name whitelist subsumes it.
		Entry("kafka_topic with partition syntax",
			"seed_brokers: [\"localhost:9092\"]\nconsumer_group: \"g\"\nkafka_topic: \"umh.messages:0\"",
			"kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)"),
		Entry("kafka_topic with comma",
			"seed_brokers: [\"localhost:9092\"]\nconsumer_group: \"g\"\nkafka_topic: \"a,b\"",
			"kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)"),
		Entry("kafka_topic with a space",
			"seed_brokers: [\"localhost:9092\"]\nconsumer_group: \"g\"\nkafka_topic: \"umh messages\"",
			"kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)"),
		Entry("kafka_topic with '#'",
			"seed_brokers: [\"localhost:9092\"]\nconsumer_group: \"g\"\nkafka_topic: \"umh#messages\"",
			"kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)"),
		Entry("kafka_topic of 250 chars",
			"seed_brokers: [\"localhost:9092\"]\nconsumer_group: \"g\"\nkafka_topic: \""+strings.Repeat("a", 250)+"\"",
			"kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)"),
		// The whitelist regex matches "." and "..", so the reserved-name reject
		// must stay explicit.
		Entry("kafka_topic of exactly '.'",
			"seed_brokers: [\"localhost:9092\"]\nconsumer_group: \"g\"\nkafka_topic: \".\"",
			"kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)"),
		Entry("kafka_topic of exactly '..'",
			"seed_brokers: [\"localhost:9092\"]\nconsumer_group: \"g\"\nkafka_topic: \"..\"",
			"kafka_topic must be a legal Kafka topic name (letters, digits, '.', '_', '-'; max 249 chars)"),
		// An explicit empty list overrides the [".*"] default; a regex joined
		// from zero patterns matches EVERYTHING — a silent filter bypass.
		Entry("explicit empty umh_topics",
			"seed_brokers: [\"localhost:9092\"]\nconsumer_group: \"g\"\nkafka_topic: \"umh.messages\"\numh_topics: []",
			"umh_topics must not be empty"),
	)

	It("rejects an invalid umh_topics pattern through the constructor", func() {
		parsed, err := unsBetaReaderConfigSpec().ParseYAML(validReaderBody(`
seed_brokers: ["localhost:9092"]
consumer_group: "g"
kafka_topic: "umh.messages"
umh_topics:
  - "["
`), nil)
		Expect(err).NotTo(HaveOccurred())
		_, err = newUnsBetaReader(parsed, nil)
		Expect(err).To(HaveOccurred(), "an invalid pattern must error, never panic or silently pass-all")
		Expect(err.Error()).To(ContainSubstring("invalid umh_topics pattern at index 0"))
	})

	// Forces the join arm in newBetaKeyFilter ("compiling combined umh_topics
	// pattern"): reachable only when every pattern compiles individually but
	// the combined alternation exceeds RE2's program-size limit ("expression
	// too large") — the per-pattern loop cannot catch it, so a "[" style
	// invalid pattern never reaches it. The sizes are empirical for the Go
	// regexp parser: (?:<literal>){1000} costs ~len(literal)*1000 size units,
	// a single pattern with a 2000-char literal compiles alone (the limit
	// sits near a 3300-char literal), and two of them joined blow past it.
	It("rejects umh_topics whose combined alternation exceeds the regex size limit", func() {
		p1 := "(?:" + strings.Repeat("a", 2000) + "){1000}"
		p2 := "(?:" + strings.Repeat("b", 2000) + "){1000}"
		// Self-documentation: each pattern must pass the per-pattern compile
		// loop on its own, or this spec is not exercising the join arm.
		_, err := regexp.Compile(p1)
		Expect(err).NotTo(HaveOccurred(), "p1 must compile individually")
		_, err = regexp.Compile(p2)
		Expect(err).NotTo(HaveOccurred(), "p2 must compile individually")

		parsed, err := unsBetaReaderConfigSpec().ParseYAML(validReaderBody(fmt.Sprintf(`
seed_brokers: ["localhost:9092"]
consumer_group: "g"
kafka_topic: "umh.messages"
umh_topics:
  - "%s"
  - "%s"
`, p1, p2)), nil)
		Expect(err).NotTo(HaveOccurred())
		_, err = newUnsBetaReader(parsed, nil)
		Expect(err).To(HaveOccurred(), "the oversized combined alternation must surface as a config error, never panic or silently pass-all")
		Expect(err.Error()).To(ContainSubstring("compiling combined umh_topics pattern"))
	})
})

// The umh_topics key filter: patterns are combined into one alternation —
// (?:p1)|(?:p2), the same join NewMessageProcessor uses for the legacy uns
// input — and matched against each record's Kafka key. Keyless contract: an
// empty key never matches, regardless of patterns.
var _ = Describe("uns_beta umh_topics key filter construction", Label("uns_beta"), func() {
	It("compiles multiple patterns and matches a key against any of them", func() {
		f, err := newBetaKeyFilter([]string{`^umh\.v1\.acme\..+$`, `^umh\.v1\.umh\..+$`})
		Expect(err).NotTo(HaveOccurred())
		Expect(f.matches("umh.v1.acme.berlin.temp")).To(BeTrue(), "first pattern must match")
		Expect(f.matches("umh.v1.umh.cologne.temp")).To(BeTrue(), "second pattern must match")
		Expect(f.matches("umh.v1.other.x")).To(BeFalse(), "a key matching no pattern must be rejected")
	})

	It("never matches an empty key, even against a match-everything pattern", func() {
		f, err := newBetaKeyFilter([]string{".*"})
		Expect(err).NotTo(HaveOccurred())
		Expect(f.matches("anything")).To(BeTrue())
		// `.*` matches "" as a regex; the filter must override that (why: see
		// the betaKeyFilter.matches doc, ENG-5094).
		Expect(f.matches("")).To(BeFalse(), "an empty key must never match, regardless of patterns")
	})

	It("errors on an invalid pattern instead of panicking or passing-all", func() {
		_, err := newBetaKeyFilter([]string{"["})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("invalid umh_topics pattern at index 0"))
	})

	It("errors on an empty pattern list", func() {
		_, err := newBetaKeyFilter(nil)
		Expect(err).To(MatchError("umh_topics must not be empty"))
	})

	// An empty element compiles individually, wraps to (?:), and the
	// alternation then matches every key — a stray `- ""` or trailing `-` in
	// YAML would silently disable a selective filter.
	It("errors on an empty or whitespace-only pattern element", func() {
		_, err := newBetaKeyFilter([]string{`^umh\.v1\.acme\..+$`, ""})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("umh_topics pattern at index 1 must not be empty"))

		_, err = newBetaKeyFilter([]string{"  "})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("umh_topics pattern at index 0 must not be empty"))
	})

	// Pins the [".*"] default: an omitted umh_topics must parse to exactly
	// one match-everything pattern, so unfiltered configs (the capstone and
	// metadata-contract specs among them) keep their keyed traffic flowing.
	It("defaults an omitted umh_topics to a single match-everything pattern", func() {
		conf, err := unsBetaReaderConfigSpec().ParseYAML(validReaderBody(`
seed_brokers: ["localhost:9092"]
consumer_group: "g"
kafka_topic: "umh.messages"
`), nil)
		Expect(err).NotTo(HaveOccurred())
		patterns, err := conf.FieldStringList("umh_topics")
		Expect(err).NotTo(HaveOccurred())
		Expect(patterns).To(Equal([]string{".*"}))
		f, err := newBetaKeyFilter(patterns)
		Expect(err).NotTo(HaveOccurred())
		Expect(f.matches("any.key.at.all")).To(BeTrue(), "the default must deliver every keyed record")
	})
})

// scriptedStep is one programmed return of the fake inner's ReadBatch.
type scriptedStep struct {
	batch service.MessageBatch
	err   error
	// ackErr is the error the step's AckFunc returns when ReadBatch's self-ack
	// (or the consumer) calls it; nil means the ack succeeds.
	ackErr error
}

// scriptedInner is a fake readBatcher that returns a programmed sequence of
// (batch, ack, err) tuples, recording the index of every step whose ack was
// invoked. It lets the self-ack loop be exercised without a broker: an
// all-filtered batch is one whose records carry no matching kafka_key.
type scriptedInner struct {
	steps   []scriptedStep
	pos     int
	acked   []int // indexes of steps whose ack was called, in call order
	overran bool  // set when ReadBatch is called past the end of the script
}

func (s *scriptedInner) ReadBatch(_ context.Context) (service.MessageBatch, service.AckFunc, error) {
	// Record an overrun as a sentinel and return an error rather than calling
	// Expect here: this fake's ReadBatch is exactly what a future
	// cancellation/concurrency spec drives from a background goroutine, where a
	// Gomega Expect would panic OFF the spec goroutine and Ginkgo would report
	// an unattributed panic that masks the real assertion. The spec body asserts
	// s.overran == false after the call instead.
	if s.pos >= len(s.steps) {
		s.overran = true
		return nil, nil, errors.New("scriptedInner ReadBatch called past the end of its script")
	}
	step := s.steps[s.pos]
	idx := s.pos
	s.pos++
	if step.err != nil {
		// On the error path the AckFunc is nil — ReadBatch must never touch it.
		return nil, nil, step.err
	}
	ack := func(context.Context, error) error {
		s.acked = append(s.acked, idx)
		return step.ackErr
	}
	return step.batch, ack, nil
}

func (s *scriptedInner) Close(context.Context) error { return nil }

// keyedBatch builds a one-record batch whose kafka_key is set, so the filter's
// keep/drop decision is deterministic.
func keyedBatch(key string) service.MessageBatch {
	m := service.NewMessage([]byte(`{"v":1}`))
	m.MetaSetMut("kafka_key", key)
	return service.MessageBatch{m}
}

// ackRecordingInner is a fake readBatcher that hands back one deliverable batch
// and a delegated AckFunc that records the error it is invoked with — so a spec
// can assert the AckFunc uns_beta returns to the consumer is the delegated one,
// forwarded unchanged (no wrap, no swallow). Uses the constructor
// newUnsBetaInputFor and the readBatcher interface.
type ackRecordingInner struct {
	batch        service.MessageBatch
	gotAckCalled bool
	gotAckErr    error
}

func (a *ackRecordingInner) ReadBatch(context.Context) (service.MessageBatch, service.AckFunc, error) {
	ack := func(_ context.Context, err error) error {
		a.gotAckCalled = true
		a.gotAckErr = err
		return nil
	}
	return a.batch, ack, nil
}

func (a *ackRecordingInner) Close(context.Context) error { return nil }

// ACK PASS-THROUGH PIN (ENG-5094/ENG-5105): a deliverable poll's ack
// is the delegated input's own AckFunc, forwarded to the consumer UNCHANGED.
// ReadBatch returns kept, ack, nil (unwrapped); this pins that behavior so a
// future wrap/swallow of the ack outcome breaks the test. Broker-free unit pin.
var _ = Describe("uns_beta ack pass-through", Label("uns_beta"), func() {
	var _ readBatcher = (*ackRecordingInner)(nil) // compile-time check: ackRecordingInner satisfies readBatcher

	It("forwards the delegated input's ack outcome unchanged", func() {
		filter, err := newBetaKeyFilter([]string{`^keep\..+$`})
		Expect(err).NotTo(HaveOccurred())
		inner := &ackRecordingInner{batch: keyedBatch("keep.0")}
		// Route through newUnsBetaInputFor so no nil-keyFilter instance is built.
		i := newUnsBetaInputFor(inner, filter)

		kept, ack, err := i.ReadBatch(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(kept).To(HaveLen(1))
		Expect(ack).NotTo(BeNil())

		// Invoking the returned AckFunc with a sentinel error must call the
		// delegated ack with EXACTLY that error — not nil, not a wrapped copy.
		sentinel := errors.New("downstream nacked")
		Expect(ack(context.Background(), sentinel)).To(Succeed())
		Expect(inner.gotAckCalled).To(BeTrue(), "the returned ack must delegate to the inner ack")
		Expect(inner.gotAckErr).To(MatchError(sentinel), "the ack error must pass through unchanged")

		// And nil passes through as nil (a successful commit).
		inner.gotAckCalled = false
		inner.gotAckErr = errors.New("stale")
		Expect(ack(context.Background(), nil)).To(Succeed())
		Expect(inner.gotAckCalled).To(BeTrue())
		Expect(inner.gotAckErr).To(BeNil(), "a nil ack outcome must pass through as nil")
	})
})

// filterAndAlias is the pure filter+alias+strip step extracted from the
// ReadBatch loop. These broker-free specs exercise it directly: the
// kept-record alias stamping (umh_topic, kafka_msg_key),
// the __rpcn_kafka_headers strip, and the drop of keyless / non-matching
// records. The metadata-contract specs (uns_beta_integration_test.go) pin the
// same contract end to end through a broker; these pin it at the unit boundary
// so a mutation that drops an alias or the strip is caught without a broker.
var _ = Describe("uns_beta filterAndAlias", Label("uns_beta"), func() {
	var filter *betaKeyFilter
	BeforeEach(func() {
		var err error
		filter, err = newBetaKeyFilter([]string{`^keep\..+$`})
		Expect(err).NotTo(HaveOccurred())
	})

	It("stamps the aliases on a kept record and strips __rpcn_kafka_headers", func() {
		m := service.NewMessage([]byte(`{"v":1}`))
		m.MetaSetMut("kafka_key", "keep.0")
		m.MetaSetMut("__rpcn_kafka_headers", "raw-kgo-slice")

		kept := filterAndAlias(service.MessageBatch{m}, filter)

		Expect(kept).To(HaveLen(1))
		topic, _ := kept[0].MetaGet("umh_topic")
		Expect(topic).To(Equal("keep.0"), "umh_topic must alias the record's kafka_key")
		msgKey, _ := kept[0].MetaGet("kafka_msg_key")
		Expect(msgKey).To(Equal("keep.0"), "kafka_msg_key must alias the record's kafka_key")
		_, hasHeaders := kept[0].MetaGet("__rpcn_kafka_headers")
		Expect(hasHeaders).To(BeFalse(), "__rpcn_kafka_headers must be stripped so it cannot compound per hop")
	})

	It("drops non-matching and keyless records, keeping only matches", func() {
		match := service.NewMessage([]byte(`{"v":1}`))
		match.MetaSetMut("kafka_key", "keep.match")
		nonMatch := service.NewMessage([]byte(`{"v":2}`))
		nonMatch.MetaSetMut("kafka_key", "drop.other")
		keyless := service.NewMessage([]byte(`{"v":3}`)) // no kafka_key set

		kept := filterAndAlias(service.MessageBatch{match, nonMatch, keyless}, filter)

		Expect(kept).To(HaveLen(1), "only the matching record survives")
		topic, _ := kept[0].MetaGet("umh_topic")
		Expect(topic).To(Equal("keep.match"))
	})
})

// The self-ack loop: when a delegated poll is entirely filtered out, ReadBatch
// must ack that empty batch itself (so the committed offset advances) and read
// the next poll — without surfacing an empty batch to the consumer. These
// broker-free specs script the inner reader directly to pin the loop's exact
// behavior: consecutive all-filtered polls, a surfaced self-ack error, and
// cancellation during the re-poll.
var _ = Describe("uns_beta all-filtered self-ack loop", Label("uns_beta"), func() {
	var filter *betaKeyFilter
	BeforeEach(func() {
		var err error
		filter, err = newBetaKeyFilter([]string{`^keep\..+$`})
		Expect(err).NotTo(HaveOccurred())
	})

	It("self-acks consecutive all-filtered polls before returning the deliverable batch", func() {
		// Two consecutive all-filtered polls (the routine production shape under
		// a selective filter, and the restart-surviving wedge a single-retry
		// mutant would reintroduce) then a deliverable one.
		inner := &scriptedInner{steps: []scriptedStep{
			{batch: keyedBatch("drop.0")},
			{batch: keyedBatch("drop.1")},
			{batch: keyedBatch("keep.2")},
		}}
		i := newUnsBetaInputFor(inner, filter)

		kept, ack, err := i.ReadBatch(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(ack).NotTo(BeNil())
		Expect(kept).To(HaveLen(1))
		// Both all-filtered polls were self-acked, in order, before the
		// deliverable batch returned. The deliverable batch's own ack is the
		// one handed back to the caller and is NOT auto-fired here. This spec
		// pins the ack ORDERING only; the alias-stamping path is covered by the
		// metadata-contract specs (uns_beta_integration_test.go).
		Expect(inner.acked).To(Equal([]int{0, 1}))
		Expect(inner.overran).To(BeFalse(), "ReadBatch must stop at the deliverable batch, not read past the script")
	})

	It("returns the delegated error unchanged and never touches the nil ack", func() {
		sentinel := errors.New("delegated read failed")
		inner := &scriptedInner{steps: []scriptedStep{{err: sentinel}}}
		i := newUnsBetaInputFor(inner, filter)

		kept, ack, err := i.ReadBatch(context.Background())
		Expect(err).To(MatchError(sentinel))
		Expect(kept).To(BeNil())
		Expect(ack).To(BeNil())
		Expect(inner.overran).To(BeFalse())
	})

	It("surfaces a failed self-ack rather than swallowing it (a commit failure)", func() {
		ackErr := errors.New("commit failed")
		inner := &scriptedInner{steps: []scriptedStep{{batch: keyedBatch("drop.0"), ackErr: ackErr}}}
		i := newUnsBetaInputFor(inner, filter)

		kept, ack, err := i.ReadBatch(context.Background())
		Expect(err).To(MatchError(ackErr))
		Expect(kept).To(BeNil())
		Expect(ack).To(BeNil())
		Expect(inner.acked).To(Equal([]int{0}), "the self-ack must have been attempted")
		Expect(inner.overran).To(BeFalse())
	})

	It("returns ctx.Err() after self-acking when the context is cancelled during the re-poll", func() {
		ctx, cancel := context.WithCancel(context.Background())
		inner := &scriptedInner{steps: []scriptedStep{{batch: keyedBatch("drop.0")}}}
		i := newUnsBetaInputFor(inner, filter)
		// Cancelled before the call: the first poll is all-filtered, so the loop
		// self-acks it, then honors cancellation before re-polling. The scripted
		// inner ignores its context (its ReadBatch never returns ctx.Err()), so
		// this pins the LOOP's own ordering — self-ack then ctx.Err() check —
		// independent of inner behavior. In production the real redpanda inner
		// returns ctx.Err() from its own ReadBatch, so a pre-cancelled context
		// usually exits via the error path instead; this spec covers the loop's
		// post-self-ack guard, not the production exit point.
		cancel()

		kept, ack, err := i.ReadBatch(ctx)
		Expect(err).To(MatchError(context.Canceled))
		Expect(kept).To(BeNil())
		Expect(ack).To(BeNil())
		Expect(inner.acked).To(Equal([]int{0}), "the all-filtered poll was acked before honoring cancellation")
		Expect(inner.overran).To(BeFalse())
	})
})

// deprecatedFlags renders the LEGACY `uns` input's config spec to the same JSON
// view the schema-export consumes (ConfigView.FormatJSON → config.children[]),
// and returns each top-level field's `is_deprecated` flag keyed by field name.
// This reads the STRUCTURED flag benthos emits for `.Deprecated()`, not the
// field description — it is the exact path cmd/schema-export/exporter.go uses
// (extractFields → getBool(fieldMap, "is_deprecated")), so a pass here means
// the schema export and the Management Console see the field as deprecated.
func deprecatedFlags(spec *service.ConfigSpec) map[string]bool {
	GinkgoHelper()
	// Register the spec into a throwaway environment so we can obtain a
	// *service.ConfigView for it (the same handle WalkInputs hands schema-export).
	env := service.NewEnvironment()
	const name = "uns_deprecation_probe"
	Expect(env.RegisterBatchInput(name, spec,
		func(*service.ParsedConfig, *service.Resources) (service.BatchInput, error) {
			return nil, errors.New("never constructed")
		})).To(Succeed())

	var jsonData []byte
	env.WalkInputs(func(n string, view *service.ConfigView) {
		if n != name {
			return
		}
		b, err := view.FormatJSON()
		Expect(err).NotTo(HaveOccurred())
		jsonData = b
	})
	Expect(jsonData).NotTo(BeNil(), "probe input %q was not registered/walked", name)

	var raw map[string]any
	Expect(json.Unmarshal(jsonData, &raw)).To(Succeed())
	cfg, ok := raw["config"].(map[string]any)
	Expect(ok).To(BeTrue(), "spec JSON has no config object: %s", jsonData)
	children, ok := cfg["children"].([]any)
	Expect(ok).To(BeTrue(), "spec config has no children: %#v", cfg)

	flags := map[string]bool{}
	for _, c := range children {
		fm, ok := c.(map[string]any)
		if !ok {
			continue
		}
		fieldName, _ := fm["name"].(string)
		if fieldName == "" {
			continue
		}
		dep, _ := fm["is_deprecated"].(bool)
		flags[fieldName] = dep
	}
	return flags
}

var _ = Describe("legacy uns input config deprecation markers (R9)", func() {
	// The new uns_beta surface uses umh_topics (the list form) exclusively and
	// always stores headers as strings, so the three fields below are legacy-only
	// and must carry the structured Deprecated flag the schema-export exports.
	It("marks umh_topic, topic, and metadata_format as deprecated", func() {
		flags := deprecatedFlags(RegisterConfigSpec())

		for _, f := range []string{"umh_topic", "topic", "metadata_format"} {
			present, ok := flags[f]
			Expect(ok).To(BeTrue(), "field %q not found in uns config spec", f)
			Expect(present).To(BeTrue(), "field %q must carry the .Deprecated() flag (is_deprecated)", f)
		}
	})

	It("does NOT mark the preferred umh_topics field as deprecated", func() {
		flags := deprecatedFlags(RegisterConfigSpec())

		present, ok := flags["umh_topics"]
		Expect(ok).To(BeTrue(), "field \"umh_topics\" not found in uns config spec")
		Expect(present).To(BeFalse(), "umh_topics is the preferred field and must NOT be deprecated")
	})
})
