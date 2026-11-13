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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/errclass"
)

var (
	errTimeout = errors.New("timeout")
	errClosed  = errors.New("closed")

	plugin           = errclass.NewPlugin("test")
	kindTimedOut     = plugin.Define("timed_out", errclass.Known)
	kindClosed       = plugin.Define("closed", errclass.Known)
	kindBadConfig    = plugin.Define("bad_config", errclass.Known)
	kindUnclassified = plugin.Define("unclassified", errclass.Unknown)

	classifier = &errclass.Classifier{
		Rules: []errclass.Rule{
			{Matches: errclass.Is(errTimeout), Kind: kindTimedOut},
			{Matches: errclass.Is(errClosed), Kind: kindClosed},
		},
		Unclassified: kindUnclassified,
	}
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

func capture() (errclass.Logger, *[]record) {
	recs := &[]record{}
	return classifier.Logger(service.NewLoggerFromSlog(slog.New(captureHandler{recs: recs}))), recs
}

var _ = Describe("Classifier.Classify", func() {
	DescribeTable("picks the Kind",
		func(err error, site, want errclass.Kind) {
			Expect(classifier.Classify(err, site)).To(Equal(want))
		},
		Entry("a matching error wins over the call site", fmt.Errorf("read: %w", errTimeout), kindBadConfig, kindTimedOut),
		Entry("the first matching rule wins", fmt.Errorf("%w: %w", errTimeout, errClosed), kindBadConfig, kindTimedOut),
		Entry("an unmatched error falls back to the call site", errors.New("boom"), kindBadConfig, kindBadConfig),
		Entry("no error falls back to the call site", nil, kindBadConfig, kindBadConfig),
		Entry("a zero call-site Kind means unclassified", errors.New("boom"), errclass.Kind{}, kindUnclassified),
	)

	It("works on a nil Classifier, so a zero Logger does not panic", func() {
		var c *errclass.Classifier
		Expect(c.Classify(errTimeout, kindBadConfig)).To(Equal(kindBadConfig))
	})
})

var _ = Describe("Logger", func() {
	It("tags a Warnf line with the call site's Kind", func() {
		log, recs := capture()
		log.With("symbol", "a").Warnf(kindBadConfig, "symbol %s is misspelled", "a")

		Expect(*recs).To(HaveLen(1))
		Expect((*recs)[0].level).To(Equal(slog.LevelWarn))
		Expect((*recs)[0].msg).To(Equal("symbol a is misspelled"))
		Expect((*recs)[0].attrs).To(SatisfyAll(
			HaveKeyWithValue("error_id", "test::bad_config"),
			HaveKeyWithValue("error_class", "known"),
			HaveKeyWithValue("symbol", "a"),
		))
	})

	It("classifies the error of a WarnErr or ErrorErr line and appends it to the text", func() {
		log, recs := capture()
		log.WarnErr(fmt.Errorf("read: %w", errTimeout), kindBadConfig, "reading %s failed", "a")
		log.ErrorErr(errors.New("boom"), kindBadConfig, "reading %s failed", "b")

		Expect(*recs).To(HaveLen(2))
		Expect((*recs)[0].level).To(Equal(slog.LevelWarn))
		Expect((*recs)[0].msg).To(Equal("reading a failed: read: timeout"))
		Expect((*recs)[0].attrs).To(SatisfyAll(
			HaveKeyWithValue("error_id", "test::timed_out"),
			HaveKeyWithValue("error_class", "known"),
		))
		Expect((*recs)[1].level).To(Equal(slog.LevelError))
		Expect((*recs)[1].msg).To(Equal("reading b failed: boom"))
		Expect((*recs)[1].attrs).To(HaveKeyWithValue("error_id", "test::bad_config"))
	})

	It("tags a line under unclassified when the call site passes a zero Kind", func() {
		log, recs := capture()
		log.Errorf(errclass.Kind{}, "nothing known")

		Expect((*recs)[0].attrs).To(HaveKeyWithValue("error_id", "test::unclassified"))
	})

	It("leaves Debug and Info lines untagged", func() {
		log, recs := capture()
		log.Debugf("d %d", 1)
		log.Info("i")

		Expect(*recs).To(HaveLen(2))
		for _, r := range *recs {
			Expect(r.attrs).NotTo(HaveKey("error_id"))
		}
	})

	It("discards everything as a zero value", func() {
		var log errclass.Logger
		Expect(func() { log.With("a", 1).ErrorErr(errTimeout, kindBadConfig, "x") }).NotTo(Panic())
	})
})

var _ = Describe("Classifier.WithPrefix", func() {
	It("puts the error_id and class in front of the text once and keeps the chain", func() {
		err := classifier.WithPrefix(classifier.WithPrefix(fmt.Errorf("connect: %w", errClosed)))

		Expect(err.Error()).To(Equal("[error_id=test::closed error_class=known] connect: closed"))
		Expect(err).To(MatchError(errClosed))
	})

	It("leaves nil alone", func() {
		Expect(classifier.WithPrefix(nil)).To(Succeed())
	})
})

var _ = Describe("Plugin", func() {
	It("builds the Kind under the plugin name with its class", func() {
		kind := errclass.NewPlugin("p_build").Define("bad_config", errclass.Known)
		log, recs := capture()

		log.Warnf(kind, "bad config")

		Expect(kind.String()).To(Equal("p_build::bad_config"))
		Expect((*recs)[0].attrs).To(HaveKeyWithValue("error_class", "known"))
	})

	p := errclass.NewPlugin("p_name")
	DescribeTable("panics on a malformed name, so the plugin fails as soon as it loads",
		func(name string) {
			Expect(func() { p.Define(name, errclass.Known) }).To(Panic())
		},
		Entry("empty", ""),
		Entry("upper case", "BadConfig"),
		Entry("a separator of its own", "ns::bad_config"),
		Entry("leading digit", "1st"),
		Entry("dash", "bad-config"),
	)

	It("panics on a class that is not one of the three", func() {
		p := errclass.NewPlugin("p_class")
		Expect(func() { p.Define("bad_config", errclass.Class("fatal")) }).To(Panic())
	})

	It("panics on a name defined twice, whatever its class", func() {
		p := errclass.NewPlugin("p_twice")
		p.Define("dropped", errclass.Known)
		Expect(func() { p.Define("dropped", errclass.Bug) }).To(Panic())
	})

	It("panics on a malformed plugin name", func() {
		Expect(func() { errclass.NewPlugin("Plc") }).To(Panic())
		Expect(func() { errclass.NewPlugin("plc::x") }).To(Panic())
	})

	It("panics on a plugin name declared twice, so two plugins cannot share an error_id", func() {
		errclass.NewPlugin("p_twice_declared")
		Expect(func() { errclass.NewPlugin("p_twice_declared") }).To(Panic())
	})
})
