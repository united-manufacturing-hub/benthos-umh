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
	"net"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/errclass"
)

var (
	errTimeout = errors.New("timeout")
	errClosed  = errors.New("closed")

	timedOut     = errclass.Class{ID: "test::timed_out", Class: errclass.Operational}
	closed       = errclass.Class{ID: "test::closed", Class: errclass.Operational}
	badConfig    = errclass.Class{ID: "test::bad_config", Class: errclass.Known}
	unclassified = errclass.Class{ID: "test::unclassified", Class: errclass.Unknown}

	classifier = &errclass.Classifier{
		Rules: []errclass.Rule{
			{Matches: errclass.Is(errTimeout), Class: timedOut},
			{Matches: errclass.Is(errClosed), Class: closed},
		},
		Unclassified: unclassified,
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
	DescribeTable("picks the class",
		func(err error, fallback, want errclass.Class) {
			Expect(classifier.Classify(err, fallback)).To(Equal(want))
		},
		Entry("a matching error wins over the call site", fmt.Errorf("read: %w", errTimeout), badConfig, timedOut),
		Entry("the first matching rule wins", fmt.Errorf("%w: %w", errTimeout, errClosed), badConfig, timedOut),
		Entry("an unmatched error falls back to the call site", errors.New("boom"), badConfig, badConfig),
		Entry("no error falls back to the call site", nil, badConfig, badConfig),
		Entry("a zero fallback means unclassified", errors.New("boom"), errclass.Class{}, unclassified),
	)

	It("works on a nil Classifier, so a zero Logger does not panic", func() {
		var c *errclass.Classifier
		Expect(c.Classify(errTimeout, badConfig)).To(Equal(badConfig))
	})
})

var _ = Describe("Logger", func() {
	It("tags Warn and Error lines with the classified ID and class", func() {
		log, recs := capture()
		log.With("symbol", "a").Warnf(errTimeout, badConfig, "read %s failed: %v", "a", errTimeout)
		log.Errorf(nil, badConfig, "bad")

		Expect(*recs).To(HaveLen(2))
		Expect((*recs)[0].level).To(Equal(slog.LevelWarn))
		Expect((*recs)[0].msg).To(Equal("read a failed: timeout"))
		Expect((*recs)[0].attrs).To(SatisfyAll(
			HaveKeyWithValue("error_id", "test::timed_out"),
			HaveKeyWithValue("error_class", errclass.Operational),
			HaveKeyWithValue("symbol", "a"),
		))
		Expect((*recs)[1].level).To(Equal(slog.LevelError))
		Expect((*recs)[1].attrs).To(HaveKeyWithValue("error_id", "test::bad_config"))
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
		Expect(func() { log.With("a", 1).Errorf(errTimeout, badConfig, "x") }).NotTo(Panic())
	})
})

var _ = Describe("Classifier.WithPrefix", func() {
	It("puts the class in front of the text once and keeps the chain", func() {
		err := classifier.WithPrefix(classifier.WithPrefix(fmt.Errorf("connect: %w", errClosed)))

		Expect(err.Error()).To(Equal("[error_id=test::closed error_class=operational] connect: closed"))
		Expect(err).To(MatchError(errClosed))
	})

	It("leaves nil alone", func() {
		Expect(classifier.WithPrefix(nil)).To(Succeed())
	})
})

var _ = Describe("Class.Validate", func() {
	DescribeTable("checks the ID format, the plugin prefix and the class",
		func(c errclass.Class, ok bool) {
			if ok {
				Expect(c.Validate("test")).To(Succeed())
			} else {
				Expect(c.Validate("test")).NotTo(Succeed())
			}
		},
		Entry("valid", badConfig, true),
		Entry("no namespace", errclass.Class{ID: "bad_config", Class: errclass.Known}, false),
		Entry("upper case", errclass.Class{ID: "test::BadConfig", Class: errclass.Known}, false),
		Entry("another plugin's prefix", errclass.Class{ID: "opcua::bad_config", Class: errclass.Known}, false),
		Entry("unknown class", errclass.Class{ID: "test::bad_config", Class: "fatal"}, false),
	)
})

var _ = Describe("IsDialFailure", func() {
	It("matches a refused dial, not a failed read", func() {
		Expect(errclass.IsDialFailure(fmt.Errorf("x: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("refused")}))).To(BeTrue())
		Expect(errclass.IsDialFailure(&net.OpError{Op: "read", Net: "tcp", Err: errors.New("reset")})).To(BeFalse())
	})
})
