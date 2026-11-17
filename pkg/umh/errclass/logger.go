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

package errclass

import (
	"context"
	"log/slog"
	"slices"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// With returns l with kind as the Kind of every Warn and Error line it writes:
//
//	classifier.With(log, kindReadGap).Warnf("No value for %q", name)
//
// A later With overrides an earlier one. A nil Kind or a Kind of another
// plugin tags the lines as the plugin's Unclassified Kind, so the mistake
// shows up as an unclassified line instead of a crash on an error path.
func (c *Classifier) With(l *service.Logger, kind *Kind) *service.Logger {
	if !c.plugin.owns(kind) {
		kind = c.plugin.unclassified
	}
	return l.With(errorKind, kind.name)
}

// WithErr returns l with the Kind of err, as Classify finds it, as the Kind of
// every Warn and Error line it writes:
//
//	classifier.WithErr(log, err).Errorf("Read failed: %v", err)
func (c *Classifier) WithErr(l *service.Logger, err error) *service.Logger {
	return c.With(l, c.Classify(err))
}

const (
	errorKind  = "error_kind"
	errorClass = "error_class"
)

// Logger returns the logger of mgr, changed so that every Warn and Error line
// carries error_kind and error_class. Use it in place of mgr.Logger(). A line
// gets its Kind from, in order:
//  1. With or WithErr,
//  2. the Classifier's Message rules, and its Is and Match rules applied to an
//     error value among the fields (see Slog),
//  3. the plugin's Unclassified Kind.
//
// Debug and Info lines carry neither field. Trace lines are written at debug
// level: the logger goes through Benthos' slog adapter, which has no trace
// level.
func (c *Classifier) Logger(mgr *service.Resources) *service.Logger {
	return service.NewLoggerFromSlog(c.Slog(mgr.Logger())) //nolint:forbidigo // the one place a plugin's logger is created
}

// Slog returns a *slog.Logger that writes to l, classified as by Logger. Hand
// it to libraries that log through log/slog. Unlike *service.Logger, which
// turns every field into a string, slog keeps an error value an error, so the
// Is and Match rules can classify it.
func (c *Classifier) Slog(l *service.Logger) *slog.Logger {
	return slog.New(&handler{out: l, c: c})
}

// handler is a slog.Handler that writes to a *service.Logger and adds
// error_kind and error_class to every record at Warn and above.
type handler struct {
	out   *service.Logger
	c     *Classifier
	attrs []slog.Attr
}

// Enabled drops records below debug, such as a library's trace level, which
// Benthos has no level for. out applies the level Benthos is configured with
// to the rest.
func (h *handler) Enabled(_ context.Context, level slog.Level) bool { return level >= slog.LevelDebug }

func (h *handler) Handle(_ context.Context, r slog.Record) error {
	attrs := slices.Clone(h.attrs)
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a)
		return true
	})
	var kind *Kind
	if r.Level >= slog.LevelWarn {
		kind = h.kind(r.Message, attrs)
	}
	attrs = slices.DeleteFunc(attrs, func(a slog.Attr) bool { return a.Key == errorKind || a.Key == errorClass })
	if kind != nil {
		attrs = append(attrs, slog.String(errorKind, kind.name), slog.String(errorClass, string(kind.class)))
	}
	kvs := make([]any, 0, 2*len(attrs))
	for _, a := range attrs {
		kvs = append(kvs, a.Key, a.Value.Any())
	}
	l := h.out.With(kvs...)
	switch {
	case r.Level >= slog.LevelError:
		l.Error(r.Message)
	case r.Level >= slog.LevelWarn:
		l.Warn(r.Message)
	case r.Level >= slog.LevelInfo:
		l.Info(r.Message)
	default:
		l.Debug(r.Message)
	}
	return nil
}

// kind returns the Kind of a record with msg and attrs, trying in order:
//  1. the last error_kind field, set by With or by a handler this record
//     already passed. *service.Logger turns it into text, so it is looked up
//     among the plugin's Kinds; a name the plugin did not declare is ignored.
//  2. the rules, then Unclassified.
func (h *handler) kind(msg string, attrs []slog.Attr) *Kind {
	for _, a := range slices.Backward(attrs) {
		if a.Key == errorKind {
			if k := h.c.plugin.kinds[a.Value.String()]; k != nil {
				return k
			}
			break
		}
	}
	if kind := h.c.matchLine(msg, firstError(attrs)); kind != nil {
		return kind
	}
	return h.c.plugin.unclassified
}

func firstError(attrs []slog.Attr) error {
	for _, a := range attrs {
		if err, ok := a.Value.Any().(error); ok {
			return err
		}
	}
	return nil
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Clone: appending in place would let two handlers derived from the same
	// parent overwrite each other's attributes in the shared backing array.
	return &handler{out: h.out, c: h.c, attrs: append(slices.Clone(h.attrs), attrs...)}
}

// WithGroup is a no-op: Benthos log fields are flat.
func (h *handler) WithGroup(string) slog.Handler { return h }
