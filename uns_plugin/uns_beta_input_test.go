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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	// The "pure" components register the "none" tracer that StreamBuilder.Build
	// defaults to; the render test below builds a (never-run) stream.
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

// capturingLogger is a *service.Logger backed by a slog handler that records
// every log line into a mutex-guarded buffer, so a broker-free spec can assert
// that a specific message (e.g. the kafka_key-spoof Warn) was emitted.
type capturingLogger struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func newCapturingLogger() (*service.Logger, *capturingLogger) {
	c := &capturingLogger{}
	h := slog.NewTextHandler(&lockedWriter{c: c}, &slog.HandlerOptions{Level: slog.LevelDebug})
	return service.NewLoggerFromSlog(slog.New(h)), c
}

func (c *capturingLogger) contents() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

// lockedWriter serializes writes into the capturingLogger's buffer.
type lockedWriter struct{ c *capturingLogger }

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	return w.c.buf.Write(p)
}

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
  umh_topics: [".*"]
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
		Expect(rp["auto_replay_nacks"]).To(BeTrue())
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
  umh_topics: [".*"]
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
  umh_topics: [".*"]
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
  umh_topics: [".*"]
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
  umh_topics: [".*"]
`)
		rp := redpandaOf(reader)
		Expect(rp["topics"]).To(Equal([]any{defaultInputKafkaTopic}))
	})

	It("falls back to the embedded broker when broker_address is omitted", func() {
		reader := renderUnsBetaReader(`
uns_beta:
  consumer_group: "g"
  umh_topics: [".*"]
`)
		rp := redpandaOf(reader)
		Expect(rp["seed_brokers"]).To(Equal([]any{"localhost:9092"}))
	})

	// umh_topics is mandatory: a consumer must declare what it wants rather than
	// silently subscribing to the whole namespace. Omitting it renders an empty
	// list (no implicit [".*"]), which the reader rejects at startup — before the
	// inner input is built (newUnsBetaReader compiles the filter first).
	It("rejects an omitted umh_topics instead of subscribing to everything", func() {
		sb := service.NewEnvironment().NewStreamBuilder()
		Expect(sb.AddInputYAML(`
uns_beta:
  consumer_group: "g"
`)).To(Succeed())
		Expect(sb.AddConsumerFunc(func(context.Context, *service.Message) error { return nil })).To(Succeed())
		stream, err := sb.Build()
		Expect(err).NotTo(HaveOccurred())
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		Expect(stream.Run(ctx)).To(MatchError(ContainSubstring("umh_topics must not be empty")))
	})

	// Safety property: connect's key_pattern pre-filter MUST be the same regex as
	// uns_beta's own keep set, so connect never omits a record uns_beta would have
	// kept. The pattern is built in two places that can silently drift — the
	// bloblang template (umh_topics.map_each(p -> "(?:"+p+")").join("|")) and the
	// Go betaKeyFilter (fmt.Sprintf("(?:%s)",p) + strings.Join("|")). Pin that the
	// rendered redpanda.key_pattern EQUALS the Go-side combined regex built from
	// the same umh_topics, reconstructed the way newBetaKeyFilter does it
	// (re.String() returns the exact source the patterns were joined into).
	It("renders redpanda.key_pattern equal to the Go-side betaKeyFilter combined regex", func() {
		patterns := []string{`^umh\.v1\.acme\..*`, `^umh\.v1\.beta\..*`, `specific-key`}
		// Single-quoted YAML scalars so the regex backslashes are literal (a
		// double-quoted YAML scalar would treat \. as an escape and fail to parse).
		reader := renderUnsBetaReader(`
uns_beta:
  broker_address: "localhost:9092"
  consumer_group: "g"
  umh_topics:
    - '` + patterns[0] + `'
    - '` + patterns[1] + `'
    - '` + patterns[2] + `'
`)
		rp := redpandaOf(reader)

		filter, err := newBetaKeyFilter(patterns)
		Expect(err).NotTo(HaveOccurred())
		Expect(rp["key_pattern"]).To(Equal(filter.re.String()),
			"template key_pattern drifted from the Go betaKeyFilter join — connect's pre-filter is no longer a faithful superset of uns_beta's keep set")
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
	// connTest is the result Connect's probe sees; nil means not-supported (the
	// neutral default for the broker-free self-ack/filter specs).
	connTest service.ConnectionTestResults
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

// ConnectionTest satisfies the readBatcher interface's connectivity probe. It
// returns the scripted result if one is set, else not-supported (the neutral
// default for the broker-free ReadBatch specs): a stray Connect call then falls
// through to the lazy connect-on-read path.
func (s *scriptedInner) ConnectionTest(context.Context) service.ConnectionTestResults {
	if s.connTest != nil {
		return s.connTest
	}
	return service.ConnectionTestNotSupported().AsList()
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

func (a *ackRecordingInner) ConnectionTest(context.Context) service.ConnectionTestResults {
	return service.ConnectionTestNotSupported().AsList()
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
		Expect(inner.gotAckErr).ToNot(HaveOccurred(), "a nil ack outcome must pass through as nil")
	})
})

// filterAndAlias is the filter+alias+strip+classify step of the ReadBatch loop.
// These broker-free specs exercise it directly: the kept-record alias stamping
// (umh_topic, kafka_msg_key), the rpcnKafkaHeadersKey strip, and the drop of
// keyless / non-matching records. The metadata-contract specs
// (uns_beta_integration_test.go) pin the same contract end to end through a
// broker; these pin it at the unit boundary so a mutation that drops an alias or
// the strip is caught without a broker. filterAndAlias is now a method on
// *unsBetaInput (it also returns the per-class dropTally), so the specs route
// through newUnsBetaInputFor.
var _ = Describe("uns_beta filterAndAlias", Label("uns_beta"), func() {
	var input *unsBetaInput
	BeforeEach(func() {
		filter, err := newBetaKeyFilter([]string{`^keep\..+$`})
		Expect(err).NotTo(HaveOccurred())
		input = newUnsBetaInputFor(nil, filter)
	})

	It("stamps the aliases on a kept record and strips "+rpcnKafkaHeadersKey, func() {
		m := service.NewMessage([]byte(`{"v":1}`))
		m.MetaSetMut("kafka_key", "keep.0")
		m.MetaSetMut(rpcnKafkaHeadersKey, "raw-kgo-slice")

		kept, _ := input.filterAndAlias(service.MessageBatch{m})

		Expect(kept).To(HaveLen(1))
		topic, _ := kept[0].MetaGet("umh_topic")
		Expect(topic).To(Equal("keep.0"), "umh_topic must alias the record's kafka_key")
		msgKey, _ := kept[0].MetaGet("kafka_msg_key")
		Expect(msgKey).To(Equal("keep.0"), "kafka_msg_key must alias the record's kafka_key")
		_, hasHeaders := kept[0].MetaGet(rpcnKafkaHeadersKey)
		Expect(hasHeaders).To(BeFalse(), rpcnKafkaHeadersKey+" must be stripped so it cannot compound per hop")
	})

	It("drops non-matching and keyless records, keeping only matches", func() {
		match := service.NewMessage([]byte(`{"v":1}`))
		match.MetaSetMut("kafka_key", "keep.match")
		nonMatch := service.NewMessage([]byte(`{"v":2}`))
		nonMatch.MetaSetMut("kafka_key", "drop.other")
		keyless := service.NewMessage([]byte(`{"v":3}`)) // no kafka_key set

		kept, _ := input.filterAndAlias(service.MessageBatch{match, nonMatch, keyless})

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

// Connect probes connectivity via the delegated input's ConnectionTest under a
// connectProbeTimeout-bounded context (kgo.Ping walks the seed brokers serially,
// so the local timeout caps the probe regardless of broker count) so an
// unreachable broker fails fast instead of starting a stream that delivers
// nothing. These broker-free specs script the probe result to pin the Connect
// decision deterministically: a failed test propagates, a not-supported test is
// treated as success (falls back to lazy connect-on-read), and a successful test
// returns nil. The connectProbeTimeout bound itself is exercised end-to-end
// against a real closed port in uns_beta_integration_test.go. (ENG-5094 / ENG-5105)
var _ = Describe("uns_beta Connect connectivity probe", Label("uns_beta"), func() {
	It("surfaces a failed connection test as a Connect error", func() {
		probeErr := errors.New("dial tcp 127.0.0.1:1: connect: connection refused")
		inner := &scriptedInner{connTest: service.ConnectionTestFailed(probeErr).AsList()}
		i := newUnsBetaInputFor(inner, nil)

		Expect(i.Connect(context.Background())).To(MatchError(probeErr),
			"Connect must return the unreachable-broker failure instead of starting a stream that delivers nothing")
	})

	It("returns nil when the delegated input does not support a connection test", func() {
		inner := &scriptedInner{connTest: service.ConnectionTestNotSupported().AsList()}
		i := newUnsBetaInputFor(inner, nil)

		Expect(i.Connect(context.Background())).To(Succeed(),
			"a delegated input that cannot self-test must fall back to lazy connect-on-read, not a hard failure")
	})

	It("returns nil when the connection test succeeds", func() {
		inner := &scriptedInner{connTest: service.ConnectionTestSucceeded().AsList()}
		i := newUnsBetaInputFor(inner, nil)

		Expect(i.Connect(context.Background())).To(Succeed())
	})
})

// The per-record drop classification is mutually exclusive and spoof-first: a
// dropped record carrying a raw "kafka_key" producer header counts as spoofed,
// a dropped record with an empty/absent key counts as keyless, and any other
// dropped record (a real keyed record that matched no pattern) counts as
// filtered. These broker-free specs drive filterAndAlias directly and assert the
// returned dropTally, pinning the classification ORDER without a broker (the
// counter increments themselves are pinned end-to-end against a broker in
// uns_beta_integration_test.go). The spoof-first order is the load-bearing fix:
// a spoofed kafka_key:"" header overwrites the native key in post-header
// metadata, so a spoofed record looks keyless via MetaGet — classifying from the
// raw header slice first keeps it counted as spoofed. (ENG-5094 / ENG-5105)
var _ = Describe("uns_beta drop classification", Label("uns_beta"), func() {
	var input *unsBetaInput
	BeforeEach(func() {
		// A selective filter so the keyed-but-non-matching record is a real
		// filtered drop, not a match.
		filter, err := newBetaKeyFilter([]string{`^umh\.v1\.keep\..+$`})
		Expect(err).NotTo(HaveOccurred())
		input = newUnsBetaInputFor(nil, filter)
	})

	It("classifies a spoofed-header drop as spoofed even when the post-header key is empty", func() {
		// The spoof header shadows the native key: the delegated input applies
		// headers after the native fields, so MetaGet("kafka_key") returns the
		// header's empty value and the record looks keyless. Classifying from the
		// raw rpcnKafkaHeadersKey slice first keeps it spoofed, not keyless.
		spoofed := service.NewMessage([]byte(`{"v":1}`))
		spoofed.MetaSetMut("kafka_key", "") // post-header shadowed value (empty)
		spoofed.MetaSetMut(rpcnKafkaHeadersKey, []kgo.RecordHeader{
			{Key: "kafka_key", Value: []byte("")},
		})

		kept, tally := input.filterAndAlias(service.MessageBatch{spoofed})

		Expect(kept).To(BeEmpty())
		Expect(tally.spoofed).To(Equal(1), "a raw kafka_key header must classify the drop as spoofed")
		Expect(tally.keyless).To(Equal(0), "a spoofed record must NOT be counted as keyless")
		Expect(tally.filtered).To(Equal(0))
	})

	It("classifies an empty-key drop with no spoof header as keyless", func() {
		keyless := service.NewMessage([]byte(`{"v":1}`))
		// A GENUINELY keyless real record: connect's FranzRecordToMessageV1 always
		// sets kafka_key (present-but-empty for an empty key). The present-but-empty
		// meta is what distinguishes it from the connect high-water placeholder
		// (which carries NO kafka_key meta and is dropped without classification).
		keyless.MetaSetMut("kafka_key", "")

		kept, tally := input.filterAndAlias(service.MessageBatch{keyless})

		Expect(kept).To(BeEmpty())
		Expect(tally.keyless).To(Equal(1))
		Expect(tally.spoofed).To(Equal(0))
		Expect(tally.filtered).To(Equal(0))
	})

	It("drops the connect high-water placeholder (no kafka_key meta) without counting any class", func() {
		// The connect key-omit pre-filter keeps a poll's last non-matching record
		// as a no-metadata placeholder (service.NewMessage(nil)) so its offset still
		// commits. It reaches uns_beta but must be dropped here WITHOUT touching a
		// per-reason counter — it is already accounted for by connect's
		// key_omitted_records. The absent kafka_key meta is the recognizer.
		placeholder := service.NewMessage(nil) // no metadata at all

		kept, tally := input.filterAndAlias(service.MessageBatch{placeholder})

		Expect(kept).To(BeEmpty(), "the placeholder must never be delivered")
		Expect(tally.keyless).To(Equal(0), "the placeholder must NOT be miscounted as keyless")
		Expect(tally.spoofed).To(Equal(0))
		Expect(tally.filtered).To(Equal(0))
	})

	It("classifies a real keyed non-matching drop as filtered", func() {
		nonMatch := service.NewMessage([]byte(`{"v":1}`))
		nonMatch.MetaSetMut("kafka_key", "umh.v1.other.topic") // real key, no pattern match

		kept, tally := input.filterAndAlias(service.MessageBatch{nonMatch})

		Expect(kept).To(BeEmpty())
		Expect(tally.filtered).To(Equal(1))
		Expect(tally.keyless).To(Equal(0))
		Expect(tally.spoofed).To(Equal(0))
	})

	It("counts each class independently across a mixed drop batch and counts kept records nowhere", func() {
		match := service.NewMessage([]byte(`{"v":1}`))
		match.MetaSetMut("kafka_key", "umh.v1.keep.a") // delivered, counted nowhere
		filtered := service.NewMessage([]byte(`{"v":2}`))
		filtered.MetaSetMut("kafka_key", "umh.v1.drop.b")
		keyless := service.NewMessage([]byte(`{"v":3}`))
		keyless.MetaSetMut("kafka_key", "") // genuinely keyless real record (present-but-empty)
		spoofed := service.NewMessage([]byte(`{"v":4}`))
		spoofed.MetaSetMut("kafka_key", "")
		spoofed.MetaSetMut(rpcnKafkaHeadersKey, []kgo.RecordHeader{{Key: "kafka_key", Value: []byte("x")}})

		kept, tally := input.filterAndAlias(service.MessageBatch{match, filtered, keyless, spoofed})

		Expect(kept).To(HaveLen(1), "only the matching record is delivered")
		Expect(tally.filtered).To(Equal(1))
		Expect(tally.keyless).To(Equal(1))
		Expect(tally.spoofed).To(Equal(1))
	})
})

// A present-but-wrong-type value under rpcnKafkaHeadersKey means the
// connect-internal header contract changed, which silently disables spoof
// classification. uns_beta keeps a one-shot Warn for that (it signals an upstream
// connect-contract break, NOT a per-record loss). These broker-free specs drive
// ReadBatch with a scripted inner and assert the Warn fires at most once.
// (ENG-5094 / ENG-5105)
var _ = Describe("uns_beta wrong-type header guard", Label("uns_beta"), func() {
	var filter *betaKeyFilter
	BeforeEach(func() {
		var err error
		// A selective filter so the wrong-type records are DROPPED (the
		// classification path that consults the raw header slice runs only on
		// dropped records); the keep.* record on the third poll is delivered.
		filter, err = newBetaKeyFilter([]string{`^umh\.v1\.keep\..+$`})
		Expect(err).NotTo(HaveOccurred())
	})

	It("warns once when the rpcn-headers metadata holds an unexpected type", func() {
		logger, capture := newCapturingLogger()
		// A DROPPED record whose rpcnKafkaHeadersKey value is present but NOT a
		// []kgo.RecordHeader — the connect-internal contract changed. The
		// classification reads the raw slice and surfaces the wrong type once.
		// Two such polls confirm the Warn is one-shot. A matching keyed record on
		// the third lets ReadBatch return.
		wrongType1 := service.NewMessage([]byte(`{"v":1}`))
		wrongType1.MetaSetMut("kafka_key", "umh.v1.drop.a")
		wrongType1.MetaSetMut(rpcnKafkaHeadersKey, "not-a-header-slice")
		wrongType2 := service.NewMessage([]byte(`{"v":2}`))
		wrongType2.MetaSetMut("kafka_key", "umh.v1.drop.b")
		wrongType2.MetaSetMut(rpcnKafkaHeadersKey, 42)
		inner := &scriptedInner{steps: []scriptedStep{
			{batch: service.MessageBatch{wrongType1}},
			{batch: service.MessageBatch{wrongType2}},
			{batch: keyedBatch("umh.v1.keep.keep")},
		}}
		i := newUnsBetaInputFor(inner, filter, withLogger(logger))

		// One ReadBatch call drains both all-filtered wrong-type polls (self-acked
		// and looped internally) and returns the deliverable keep.* batch. Both
		// wrong-type records are classified along the way, so the one-shot guard
		// has seen both yet must have fired exactly once.
		kept, _, err := i.ReadBatch(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(kept).To(HaveLen(1), "the matching keep.* record on the third poll must be delivered")

		out := capture.contents()
		Expect(out).To(ContainSubstring("unexpected type"), "a present-but-wrong-type rpcn-headers value must be flagged")
		Expect(strings.Count(out, "unexpected type")).To(Equal(1), "the wrong-type Warn must be rate-limited to once per input")
	})
})

// MC autocomplete shows the `uns_beta` TEMPLATE's summary. This reads the
// summary off the same schema view the Management Console consumes — the
// registered `uns_beta` input's FormatJSON `summary` (WalkInputs surfaces the
// template as a walkable input) — so a future edit that drifts the summary fails
// loudly. (ENG-5105)
var _ = Describe("uns_beta template summary", Label("uns_beta"), func() {
	It("matches the summary MC autocomplete shows", func() {
		var summary string
		found := false
		service.GlobalEnvironment().WalkInputs(func(n string, view *service.ConfigView) {
			if n != "uns_beta" {
				return
			}
			b, err := view.FormatJSON()
			Expect(err).NotTo(HaveOccurred())
			var raw map[string]any
			Expect(json.Unmarshal(b, &raw)).To(Succeed())
			summary, _ = raw["summary"].(string)
			found = true
		})
		Expect(found).To(BeTrue(), "uns_beta template was not registered as a walkable input")
		Expect(summary).To(Equal("Preview of the reliable UNS input; will replace `uns`."))
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
