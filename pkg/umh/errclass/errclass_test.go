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

	// other is a second plugin, whose Kinds plugin must not accept.
	other = errclass.NewPlugin("other")

	foreignKind       = other.User("foreign")
	foreignOnlyKind   = other.User("foreign_only")
	foreignWithKind   = other.User("foreign_with")
	foreignEnrichKind = other.UMH("foreign_enrich")

	plugin        = errclass.NewPlugin("test")
	kindTimedOut  = plugin.User("timed_out")
	kindClosed    = plugin.User("closed")
	kindBadConfig = plugin.User("bad_config")
	kindDataLoss  = plugin.UMH("data_loss")

	classifier = plugin.Classifier(
		errclass.Message("socket closed", kindClosed),
		errclass.Is(errTimeout, kindTimedOut),
		errclass.Is(errClosed, kindClosed),
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

// capture returns the plugin Logger, writing to a capture.
func capture() (*service.Logger, *[]record) {
	out, recs := captureOut()
	return classifier.Logger(service.MockResources(service.MockResourcesOptUseLogger(out))), recs
}

var _ = Describe("Classifier.Classify", func() {
	DescribeTable("picks the Kind",
		func(err error, want *errclass.Kind) {
			Expect(classifier.Classify(err)).To(BeIdenticalTo(want))
		},
		Entry("a matching rule", fmt.Errorf("read: %w", errTimeout), kindTimedOut),
		Entry("the first matching rule", fmt.Errorf("%w: %w", errTimeout, errClosed), kindTimedOut),
		Entry("the Kind the error was created with", kindBadConfig.Errorf("bad %q", "x"), kindBadConfig),
		Entry("the Kind the error was created with, over a rule matching inside it", kindBadConfig.Errorf("x: %w", errClosed), kindBadConfig),
		Entry("a rule, over a Kind of another plugin", foreignKind.Errorf("x: %w", errClosed), kindClosed),
		Entry("unclassified, over a Kind of another plugin", foreignOnlyKind.Errorf("x"), errclass.Unclassified(plugin)),
		Entry("an error whose text equals a Message rule", errors.New("socket closed"), errclass.Unclassified(plugin)),
		Entry("nothing known", errors.New("boom"), errclass.Unclassified(plugin)),
		Entry("no error", nil, errclass.Unclassified(plugin)),
	)

	DescribeTable("IsKind and IsClassified agree with it",
		func(err error, kind *errclass.Kind, isKind, isClassified bool) {
			Expect(classifier.IsKind(err, kind)).To(Equal(isKind))
			Expect(classifier.IsClassified(err)).To(Equal(isClassified))
		},
		Entry("a matching rule", fmt.Errorf("read: %w", errTimeout), kindTimedOut, true, true),
		Entry("another Kind", fmt.Errorf("read: %w", errTimeout), kindClosed, false, true),
		Entry("the Kind the error was created with", kindBadConfig.Errorf("x"), kindBadConfig, true, true),
		Entry("only a Kind of another plugin", foreignOnlyKind.Errorf("x"), foreignOnlyKind, false, false),
		Entry("nothing known", errors.New("boom"), kindTimedOut, false, false),
		Entry("no error", nil, kindTimedOut, false, false),
	)

	It("keeps the text and chain of a created error", func() {
		err := kindBadConfig.Errorf("parse: %w", errClosed)
		Expect(err.Error()).To(Equal("parse: closed"))
		Expect(err).To(MatchError(errClosed))
	})
})

var _ = Describe("Logger", func() {
	It("tags a line with the Kind given to With", func() {
		log, recs := capture()
		classifier.With(log.With("symbol", "a"), kindBadConfig).Warnf("symbol %s is misspelled", "a")

		Expect(*recs).To(HaveLen(1))
		Expect((*recs)[0].level).To(Equal(slog.LevelWarn))
		Expect((*recs)[0].msg).To(Equal("symbol a is misspelled"))
		Expect((*recs)[0].attrs).To(SatisfyAll(
			HaveKeyWithValue("error_kind", "test::bad_config"),
			HaveKeyWithValue("error_owner", "user"),
			HaveKeyWithValue("symbol", "a"),
		))
	})

	It("lets a later With override an earlier one, and writes one error_kind", func() {
		log, recs := capture()
		classifier.With(classifier.With(log, kindBadConfig), kindDataLoss).Error("x")

		Expect((*recs)[0].attrs).To(SatisfyAll(
			HaveKeyWithValue("error_kind", "test::data_loss"),
			HaveKeyWithValue("error_owner", "umh"),
		))
	})

	It("accepts an error_kind field set by hand, if it names a Kind of the plugin", func() {
		log, recs := capture()
		log.With("error_kind", "test::bad_config").Warn("x")

		Expect((*recs)[0].attrs).To(SatisfyAll(
			HaveKeyWithValue("error_kind", "test::bad_config"),
			HaveKeyWithValue("error_owner", "user"),
		))
	})

	It("tags every other Warn and Error line as unclassified", func() {
		log, recs := capture()
		log.Warn("nobody classified this")
		log.With("error_kind", "test::no_such_kind").Error("a Kind name the plugin did not declare")

		Expect(*recs).To(HaveLen(2))
		for _, r := range *recs {
			Expect(r.attrs).To(SatisfyAll(
				HaveKeyWithValue("error_kind", "test::unclassified"),
				HaveKeyWithValue("error_owner", "unknown"),
			))
		}
	})

	It("tags a line with a nil Kind or a Kind of another plugin as unclassified", func() {
		log, recs := capture()
		classifier.With(log, nil).Warn("nil Kind")
		classifier.With(log, foreignWithKind).Warn("foreign Kind")

		Expect(*recs).To(HaveLen(2))
		for _, r := range *recs {
			Expect(r.attrs).To(SatisfyAll(
				HaveKeyWithValue("error_kind", "test::unclassified"),
				HaveKeyWithValue("error_owner", "unknown"),
			))
		}
	})

	It("gives a line the Kind of its error with WithErr", func() {
		log, recs := capture()
		classifier.WithErr(log, fmt.Errorf("read: %w", errTimeout)).Warn("read failed")
		classifier.WithErr(log, errors.New("boom")).Warn("read failed")

		Expect((*recs)[0].attrs).To(HaveKeyWithValue("error_kind", "test::timed_out"))
		Expect((*recs)[1].attrs).To(HaveKeyWithValue("error_kind", "test::unclassified"))
	})

	It("classifies a line by its message", func() {
		log, recs := capture()
		log.Warn("socket closed")

		Expect((*recs)[0].attrs).To(HaveKeyWithValue("error_kind", "test::closed"))
	})

	It("leaves Debug and Info lines untagged, also on a logger that has a Kind", func() {
		log, recs := capture()
		log.Debugf("d %d", 1)
		classifier.With(log, kindBadConfig).Info("i")

		Expect(*recs).To(HaveLen(2))
		Expect((*recs)[0].level).To(Equal(slog.LevelDebug))
		Expect((*recs)[1].level).To(Equal(slog.LevelInfo))
		for _, r := range *recs {
			Expect(r.attrs).NotTo(HaveKey("error_kind"))
			Expect(r.attrs).NotTo(HaveKey("error_owner"))
		}
	})
})

var _ = Describe("Slog", func() {
	It("classifies a record by the error among its fields, which slog keeps typed", func() {
		out, recs := captureOut()
		classifier.Slog(out).With("error", fmt.Errorf("read: %w", errTimeout)).Warn("read failed")

		Expect((*recs)[0].attrs).To(HaveKeyWithValue("error_kind", "test::timed_out"))
	})

	It("tries the rules in order, so a Message rule listed first wins over the error", func() {
		out, recs := captureOut()
		classifier.Slog(out).Error("socket closed", "error", errTimeout)

		Expect((*recs)[0].attrs).To(HaveKeyWithValue("error_kind", "test::closed"))
	})

	It("keeps the Kind through a second classifying logger", func() {
		log, recs := capture()
		classifier.Slog(log).Error("x", "error", errTimeout)

		Expect((*recs)[0].attrs).To(SatisfyAll(
			HaveKeyWithValue("error_kind", "test::timed_out"),
			HaveKeyWithValue("error_owner", "user"),
		))
	})

	It("maps slog levels onto Benthos levels and drops trace", func() {
		out, recs := captureOut()
		l := classifier.Slog(out)
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
		out, recs := captureOut()
		parent := classifier.Slog(out).With("a", "1")
		left := parent.With("b", "left")
		right := parent.With("b", "right")

		left.Info("l")
		right.Info("r")

		Expect((*recs)[0].attrs).To(HaveKeyWithValue("b", "left"))
		Expect((*recs)[1].attrs).To(HaveKeyWithValue("b", "right"))
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

	It("ignores a Kind of another plugin", func() {
		err := classifier.EnrichForLog(foreignEnrichKind.Errorf("x"))

		Expect(err.Error()).To(Equal("[error_kind=test::unclassified error_owner=unknown] x"))
	})
})

// pluginCount makes the names from newPluginName unique, so a spec that
// declares a Plugin also passes when Ginkgo runs it again (--repeat).
var pluginCount atomic.Int64

func newPluginName() string { return fmt.Sprintf("p_%d", pluginCount.Add(1)) }

var _ = Describe("Plugin", func() {
	It("prefixes every Kind with the plugin name", func() {
		name := newPluginName()
		p := errclass.NewPlugin(name)

		Expect(p.User("bad_config").String()).To(Equal(name + "::bad_config"))
		Expect(errclass.Unclassified(p).String()).To(Equal(name + "::unclassified"))
	})

	p := errclass.NewPlugin("p_name")
	DescribeTable("panics on a malformed name, so the plugin fails as soon as it loads",
		func(name string) {
			Expect(func() { p.User(name) }).To(Panic())
		},
		Entry("empty", ""),
		Entry("upper case", "BadConfig"),
		Entry("a separator of its own", "ns::bad_config"),
		Entry("leading digit", "1st"),
		Entry("dash", "bad-config"),
	)

	It("panics on a name declared twice, whatever its owner", func() {
		p := errclass.NewPlugin(newPluginName())
		p.User("dropped")
		Expect(func() { p.UMH("dropped") }).To(Panic())
		Expect(func() { p.User("unclassified") }).To(Panic())
	})

	It("panics on a malformed plugin name", func() {
		Expect(func() { errclass.NewPlugin("Plc") }).To(Panic())
		Expect(func() { errclass.NewPlugin("plc::x") }).To(Panic())
	})

	It("panics on a plugin name declared twice, so two plugins cannot share a Kind name", func() {
		name := newPluginName()
		errclass.NewPlugin(name)
		Expect(func() { errclass.NewPlugin(name) }).To(Panic())
	})
})
