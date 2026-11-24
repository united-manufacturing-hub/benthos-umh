// Copyright 2026 UMH Systems GmbH
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

package errclass_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/errclass"
)

var (
	errTimeout = errors.New("timeout")
	errClosed  = errors.New("closed")

	// other is a second component, whose Kinds component must not accept.
	other = errclass.NewComponent("other")

	foreignKind       = other.UserKind("foreign")
	foreignOnlyKind   = other.UserKind("foreign_only")
	foreignWithKind   = other.UserKind("foreign_with")
	foreignEnrichKind = other.UMHKind("foreign_enrich")

	component     = errclass.NewComponent("test")
	kindTimedOut  = component.UserKind("timed_out")
	kindClosed    = component.UserKind("closed")
	kindBadConfig = component.UserKind("bad_config")
	kindRetrying  = component.UserKind("retrying")
	kindDataLoss  = component.UMHKind("data_loss")

	classifier = component.Classifier(
		errclass.MessageIs("socket closed", kindClosed),
		errclass.ErrorIs(errTimeout, kindTimedOut),
		errclass.ErrorIs(errClosed, kindClosed),
		errclass.MessageIs("retrying", kindRetrying),
	)
)

// record is one line a Logger wrote, with its fields.
type record struct {
	level slog.Level
	msg   string
	attrs map[string]any
}

type captureHandler struct {
	recs  *[]record
	attrs []slog.Attr
}

func (h captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h captureHandler) WithGroup(string) slog.Handler            { return h }
func (h captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return captureHandler{recs: h.recs, attrs: append(append([]slog.Attr(nil), h.attrs...), attrs...)}
}

func (h captureHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := map[string]any{}
	for _, a := range h.attrs {
		attrs[a.Key] = a.Value.Any()
	}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	*h.recs = append(*h.recs, record{level: r.Level, msg: r.Message, attrs: attrs})
	return nil
}

func captureOut() (*service.Logger, *[]record) {
	recs := &[]record{}
	return service.NewLoggerFromSlog(slog.New(captureHandler{recs: recs})), recs
}

// capture returns the component Logger, writing to a capture.
func capture() (*errclass.Logger, *[]record) {
	out, recs := captureOut()
	return errclass.NewLogger(service.MockResources(service.MockResourcesOptUseLogger(out)), classifier), recs
}

var _ = Describe("Classifier.Classify", func() {
	DescribeTable("picks the Kind",
		func(err error, want *errclass.Kind) {
			Expect(classifier.Classify(err)).To(BeIdenticalTo(want))
		},
		Entry("a matching rule", fmt.Errorf("read: %w", errTimeout), kindTimedOut),
		Entry("an earlier rule, over a later one that the error lists first",
			fmt.Errorf("%w: %w", errClosed, errTimeout), kindTimedOut),
		Entry("a later rule, where no earlier one matches", fmt.Errorf("read: %w", errClosed), kindClosed),
		Entry("the Kind the error was created with", kindBadConfig.Errorf("bad %q", "x"), kindBadConfig),
		Entry("the Kind the error was created with, over a rule matching inside it", kindBadConfig.Errorf("x: %w", errClosed), kindBadConfig),
		Entry("a rule, over a Kind of another component", foreignKind.Errorf("x: %w", errClosed), kindClosed),
		Entry("unclassified, over a Kind of another component", foreignOnlyKind.Errorf("x"), component.Unclassified()),
		Entry("an error whose text equals a MessageIs rule before the error rules", errors.New("socket closed"), component.Unclassified()),
		Entry("an error whose text equals a MessageIs rule after the error rules", errors.New("retrying"), component.Unclassified()),
		Entry("nothing known", errors.New("boom"), component.Unclassified()),
		Entry("no error", nil, component.Unclassified()),
	)

	It("keeps the text and chain of a created error", func() {
		err := kindBadConfig.Errorf("parse: %w", errClosed)
		Expect(err.Error()).To(Equal("parse: closed"))
		Expect(err).To(MatchError(errClosed))
	})

	It("names the unclassified Kind after the component", func() {
		Expect(component.Unclassified().String()).To(Equal("test::unclassified"))
	})
})

var _ = Describe("Logger", func() {
	It("logs an error with its text as the message and its Kind", func() {
		log, recs := capture()
		log.With("symbol", "a").Warn(fmt.Errorf("read %s: %w", "a", errTimeout))

		Expect(*recs).To(HaveLen(1))
		Expect((*recs)[0].level).To(Equal(slog.LevelWarn))
		Expect((*recs)[0].msg).To(Equal("read a: timeout"))
		Expect((*recs)[0].attrs).To(SatisfyAll(
			HaveKeyWithValue("error_kind", "test::timed_out"),
			HaveKeyWithValue("error_owner", "user"),
			HaveKeyWithValue("symbol", "a"),
		))
	})

	It("logs at error level with Error", func() {
		log, recs := capture()
		log.Error(kindDataLoss.Errorf("lost %d values", 3))

		Expect((*recs)[0].level).To(Equal(slog.LevelError))
		Expect((*recs)[0].msg).To(Equal("lost 3 values"))
		Expect((*recs)[0].attrs).To(SatisfyAll(
			HaveKeyWithValue("error_kind", "test::data_loss"),
			HaveKeyWithValue("error_owner", "umh"),
		))
	})

	It("logs an error nothing knows and a Kind of another component as unclassified", func() {
		log, recs := capture()
		log.Warn(errors.New("boom"))
		log.Error(foreignWithKind.Errorf("x"))

		Expect(*recs).To(HaveLen(2))
		for _, r := range *recs {
			Expect(r.attrs).To(SatisfyAll(
				HaveKeyWithValue("error_kind", "test::unclassified"),
				HaveKeyWithValue("error_owner", "unknown"),
			))
		}
	})

	DescribeTable("logs a nil error at error level as unclassified, naming the caller",
		func(logNil func(*errclass.Logger), method string) {
			log, recs := capture()
			logNil(log)

			Expect(*recs).To(HaveLen(1))
			Expect((*recs)[0].level).To(Equal(slog.LevelError))
			Expect((*recs)[0].msg).To(SatisfyAll(
				HavePrefix("errclass: "+method+" called with a nil error at "),
				ContainSubstring("errclass_test.go:"),
			))
			Expect((*recs)[0].attrs).To(SatisfyAll(
				HaveKeyWithValue("error_kind", "test::unclassified"),
				HaveKeyWithValue("error_owner", "unknown"),
			))
		},
		Entry("Error", func(l *errclass.Logger) { l.Error(nil) }, "Error"),
		Entry("Warn", func(l *errclass.Logger) { l.Warn(nil) }, "Warn"),
	)

	It("does not classify an error by its text, which only a library's log records are", func() {
		log, recs := capture()
		log.Warn(errors.New("socket closed"))

		Expect((*recs)[0].attrs).To(HaveKeyWithValue("error_kind", "test::unclassified"))
	})

	It("leaves Debug and Info lines untagged", func() {
		log, recs := capture()
		log.Debugf("d %d", 1)
		log.Debug("d")
		log.Info("i")
		log.Infof("i %d", 1)

		Expect(*recs).To(HaveLen(4))
		for _, r := range *recs {
			Expect(r.level).To(BeNumerically("<", slog.LevelWarn))
			Expect(r.attrs).NotTo(HaveKey("error_kind"))
			Expect(r.attrs).NotTo(HaveKey("error_owner"))
		}
	})
})

// errValuer is a slog.LogValuer that resolves to an error.
type errValuer struct{ err error }

func (v errValuer) LogValue() slog.Value { return slog.AnyValue(v.err) }

var _ = Describe("Logger.Slog", func() {
	It("classifies a record by the error among its fields, which slog keeps typed", func() {
		log, recs := capture()
		log.Slog().With("error", fmt.Errorf("read: %w", errTimeout)).Warn("read failed")

		Expect((*recs)[0].attrs).To(SatisfyAll(
			HaveKeyWithValue("error_kind", "test::timed_out"),
			HaveKeyWithValue("error_owner", "user"),
		))
	})

	DescribeTable("tries the rules in order",
		func(msg string, args []any, want *errclass.Kind) {
			log, recs := capture()
			log.Slog().Error(msg, args...)

			Expect((*recs)[0].attrs).To(HaveKeyWithValue("error_kind", want.String()))
		},
		Entry("a MessageIs rule before the error rules wins over the error",
			"socket closed", []any{"error", errTimeout}, kindClosed),
		Entry("an error rule before a MessageIs rule wins over the message",
			"retrying", []any{"error", fmt.Errorf("dial: %w", errTimeout)}, kindTimedOut),
		Entry("an earlier error rule wins over a later one",
			"read failed", []any{"error", fmt.Errorf("%w: %w", errClosed, errTimeout)}, kindTimedOut),
		Entry("a later error rule, where no earlier one matches",
			"read failed", []any{"error", errClosed}, kindClosed),
		Entry("a MessageIs rule after the error rules, where no error rule matches the error",
			"retrying", []any{"error", errors.New("boom")}, kindRetrying),
		Entry("a MessageIs rule after the error rules, for a record without an error",
			"retrying", []any{}, kindRetrying),
		Entry("the Kind the record's error was created with, over every rule",
			"socket closed", []any{"error", kindBadConfig.Errorf("x")}, kindBadConfig),
	)

	It("tags a record nothing classified as unclassified, and leaves Info untagged", func() {
		log, recs := capture()
		log.Slog().Warn("nobody classified this")
		log.Slog().Info("i")

		Expect((*recs)[0].attrs).To(HaveKeyWithValue("error_kind", "test::unclassified"))
		Expect((*recs)[1].attrs).NotTo(HaveKey("error_kind"))
	})

	It("keeps the fields of the Logger", func() {
		log, recs := capture()
		log.With("symbol", "a").Slog().Warn("x")

		Expect((*recs)[0].attrs).To(HaveKeyWithValue("symbol", "a"))
	})

	It("maps slog levels onto Benthos levels and drops trace", func() {
		log, recs := capture()
		l := log.Slog()
		l.Log(context.Background(), slog.LevelDebug-4, "trace")
		l.Debug("debug")
		l.Info("info")
		l.Warn("warn")
		l.Error("error")
		l.Log(context.Background(), slog.LevelError+4, "above error")

		levels := make([]slog.Level, 0, len(*recs))
		for _, r := range *recs {
			levels = append(levels, r.level)
		}
		Expect(levels).To(Equal([]slog.Level{
			slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError, slog.LevelError,
		}))
	})

	It("does not let two loggers derived from one parent share fields", func() {
		log, recs := capture()
		parent := log.Slog().With("a", "1")
		left := parent.With("b", "left")
		right := parent.With("b", "right")

		left.Info("l")
		right.Info("r")

		Expect((*recs)[0].attrs).To(HaveKeyWithValue("b", "left"))
		Expect((*recs)[1].attrs).To(HaveKeyWithValue("b", "right"))
	})

	It("puts the group in front of the keys of the fields added after it", func() {
		log, recs := capture()
		log.Slog().With("before", 1).WithGroup("conn").With("addr", "x").Info("m", "port", 2)

		Expect((*recs)[0].attrs).To(SatisfyAll(
			HaveKeyWithValue("before", "1"),
			HaveKeyWithValue("conn.addr", "x"),
			HaveKeyWithValue("conn.port", "2"),
		))
	})

	It("chains nested groups", func() {
		log, recs := capture()
		log.Slog().WithGroup("a").WithGroup("b").Info("m", "key", 1)

		Expect((*recs)[0].attrs).To(HaveKeyWithValue("a.b.key", "1"))
	})

	It("ignores a group without a name", func() {
		log, recs := capture()
		log.Slog().WithGroup("").Info("m", "key", 1)

		Expect((*recs)[0].attrs).To(HaveKeyWithValue("key", "1"))
	})

	It("expands a group field into prefixed keys, and inlines one without a key", func() {
		log, recs := capture()
		log.Slog().WithGroup("g").Info("m",
			slog.Group("conn", "addr", "x", slog.Group("tls", "on", true)),
			slog.Group("", "inline", 1),
		)

		Expect((*recs)[0].attrs).To(SatisfyAll(
			HaveKeyWithValue("g.conn.addr", "x"),
			HaveKeyWithValue("g.conn.tls.on", "true"),
			HaveKeyWithValue("g.inline", "1"),
		))
	})

	It("does not let two loggers derived from one parent share fields or groups", func() {
		log, recs := capture()
		parent := log.Slog().WithGroup("p")
		left := parent.With("a", "left").WithGroup("l")
		right := parent.With("a", "right")

		left.Info("l", "k", 1)
		right.Info("r", "k", 2)

		Expect((*recs)[0].attrs).To(SatisfyAll(
			HaveKeyWithValue("p.a", "left"),
			HaveKeyWithValue("p.l.k", "1"),
		))
		Expect((*recs)[1].attrs).To(SatisfyAll(
			HaveKeyWithValue("p.a", "right"),
			HaveKeyWithValue("p.k", "2"),
			Not(HaveKey("p.l.k")),
		))
	})

	It("classifies by an error inside a group", func() {
		log, recs := capture()
		log.Slog().WithGroup("g").Warn("read failed", "error", fmt.Errorf("read: %w", errTimeout))
		log.Slog().Warn("read failed", slog.Group("op", "error", errClosed))

		Expect((*recs)[0].attrs).To(HaveKeyWithValue("error_kind", "test::timed_out"))
		Expect((*recs)[1].attrs).To(HaveKeyWithValue("error_kind", "test::closed"))
	})

	It("resolves a LogValuer, and classifies by the error it resolves to", func() {
		log, recs := capture()
		log.Slog().Warn("read failed", "error", errValuer{err: errTimeout})

		Expect((*recs)[0].attrs).To(SatisfyAll(
			HaveKeyWithValue("error", "timeout"),
			HaveKeyWithValue("error_kind", "test::timed_out"),
		))
	})
})

var _ = Describe("Classifier.EnrichForLog", func() {
	It("puts the error_kind and class in front of the text once and keeps the chain", func() {
		err := classifier.EnrichForLog(classifier.EnrichForLog(fmt.Errorf("connect: %w", errClosed)))

		Expect(err.Error()).To(Equal("[error_kind=test::closed error_owner=user] connect: closed"))
		Expect(err).To(MatchError(errClosed))
		Expect(classifier.Classify(err)).To(BeIdenticalTo(kindClosed))
	})

	It("leaves nil alone", func() {
		Expect(classifier.EnrichForLog(nil)).To(Succeed())
	})

	It("ignores a Kind of another component", func() {
		err := classifier.EnrichForLog(foreignEnrichKind.Errorf("x"))

		Expect(err.Error()).To(Equal("[error_kind=test::unclassified error_owner=unknown] x"))
	})
})

// componentCount makes the names from newComponentName unique, so a spec that
// declares a Component also passes when Ginkgo runs it again (--repeat).
var componentCount atomic.Int64

func newComponentName() string { return fmt.Sprintf("p_%d", componentCount.Add(1)) }

var _ = Describe("Component", func() {
	It("prefixes every Kind with the component name", func() {
		name := newComponentName()
		c := errclass.NewComponent(name)

		Expect(c.UserKind("bad_config").String()).To(Equal(name + "::bad_config"))
		Expect(c.Unclassified().String()).To(Equal(name + "::unclassified"))
	})

	c := errclass.NewComponent("p_name")
	DescribeTable("panics on a malformed name, so the component fails as soon as it loads",
		func(name string) {
			Expect(func() { c.UserKind(name) }).To(Panic())
		},
		Entry("empty", ""),
		Entry("upper case", "BadConfig"),
		Entry("a separator of its own", "ns::bad_config"),
		Entry("leading digit", "1st"),
		Entry("dash", "bad-config"),
	)

	It("panics on a name declared twice, whatever its owner", func() {
		c := errclass.NewComponent(newComponentName())
		c.UserKind("dropped")
		Expect(func() { c.UMHKind("dropped") }).To(Panic())
		Expect(func() { c.UserKind("unclassified") }).To(Panic())
	})

	It("panics on a malformed component name", func() {
		Expect(func() { errclass.NewComponent("Plc") }).To(Panic())
		Expect(func() { errclass.NewComponent("plc::x") }).To(Panic())
	})

	It("panics on a component name declared twice, so two components cannot share a Kind name", func() {
		name := newComponentName()
		errclass.NewComponent(name)
		Expect(func() { errclass.NewComponent(name) }).To(Panic())
	})

	It("panics on a MessageIs rule with an empty message", func() {
		Expect(func() { errclass.MessageIs("", kindClosed) }).To(Panic())
	})
})

var _ = Describe("Kind.Owner", func() {
	It("returns the owner the Kind was declared with", func() {
		Expect(kindClosed.Owner()).To(Equal("user"))
		Expect(kindDataLoss.Owner()).To(Equal("umh"))
		Expect(component.Unclassified().Owner()).To(Equal("unknown"))
	})
})

var _ = Describe("Classifier.Messages", func() {
	It("returns the messages of the MessageIs rules, in order", func() {
		Expect(classifier.Messages()).To(Equal([]string{"socket closed", "retrying"}))
	})
})
