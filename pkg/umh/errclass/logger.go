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
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
	"slices"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	errorKind  = "error_kind"
	errorOwner = "error_owner"
)

// Logger is the logger of one component instance. Warn and Error take an
// error, not a message, and add its error_kind and error_owner to the line.
// Debug and Info lines carry neither field.
//
// Create it once, in the component's constructor, with NewLogger.
type Logger struct {
	out *service.Logger
	c   *Classifier
}

// NewLogger returns a Logger that writes to mgr.Logger() and classifies
// errors with c. mgr is the *service.Resources that Benthos passes to the
// component's constructor. Use NewLogger instead of mgr.Logger().
func NewLogger(mgr *service.Resources, c *Classifier) *Logger {
	return &Logger{out: mgr.Logger(), c: c} //nolint:forbidigo // the one place a component's logger is created
}

// Error logs err at error level, with the Kind that Classifier.Classify
// returns for err. The text of err is the line's message. To add context,
// wrap err where it is returned:
//
//	return fmt.Errorf("connect to %s: %w", addr, err)
//	...
//	log.Error(err)
//
// A nil err is a bug in the caller. Error then logs an unclassified line at
// error level, with the caller's file and line in the message.
func (l *Logger) Error(err error) {
	if err == nil {
		l.nilError("Error")
		return
	}
	l.tagged(l.c.Classify(err)).Error(err.Error())
}

// Warn logs err at warn level, like Error. To warn without an error at hand,
// create one with its Kind:
//
//	log.Warn(kindReadGap.Errorf("no value for %q", name))
//
// A nil err is handled as by Error, at error level.
func (l *Logger) Warn(err error) {
	if err == nil {
		l.nilError("Warn")
		return
	}
	l.tagged(l.c.Classify(err)).Warn(err.Error())
}

// nilError logs that method was called with a nil error. It must be called
// directly from Error or Warn: it reports the caller of that method.
func (l *Logger) nilError(method string) {
	where := "an unknown location"
	if _, file, line, ok := runtime.Caller(2); ok {
		where = fmt.Sprintf("%s:%d", filepath.Base(file), line)
	}
	l.tagged(l.c.component.unclassified).Error(fmt.Sprintf("errclass: %s called with a nil error at %s", method, where))
}

func (l *Logger) tagged(kind *Kind) *service.Logger {
	return l.out.With(errorKind, kind.name, errorOwner, kind.Owner())
}

// With returns a Logger that adds the given key-value pairs as fields to
// every line it writes:
//
//	log.With("hint", hint).Error(err)
func (l *Logger) With(keyValues ...any) *Logger {
	return &Logger{out: l.out.With(keyValues...), c: l.c}
}

// Debug logs msg at debug level.
func (l *Logger) Debug(msg string) { l.out.Debug(msg) }

// Debugf logs a formatted message at debug level.
func (l *Logger) Debugf(format string, args ...any) { l.out.Debugf(format, args...) }

// Info logs msg at info level.
func (l *Logger) Info(msg string) { l.out.Info(msg) }

// Infof logs a formatted message at info level.
func (l *Logger) Infof(format string, args ...any) { l.out.Infof(format, args...) }

// Slog returns l as a *slog.Logger, for a library that logs through log/slog.
// The Classifier classifies the library's warning and error records by its
// rules, in order. A MessageIs rule matches the record's message. The other
// rules match the first error value among the record's fields; slog passes
// that value on as an error, not as text. Records below debug
// level, such as a library's trace level, are dropped, because Benthos has no
// level for them.
//
// Benthos log fields are flat. So the key of a field in a slog group gets the
// group names in front, joined by dots, e.g. "conn.addr".
func (l *Logger) Slog() *slog.Logger {
	return slog.New(&handler{out: l.out, c: l.c})
}

// handler is a slog.Handler that writes to a *service.Logger and adds
// error_kind and error_owner to every record at Warn and above.
type handler struct {
	out *service.Logger
	c   *Classifier
	// attrs are the attributes added with WithAttrs, flattened: their keys
	// carry the groups open when they were added.
	attrs []slog.Attr
	// prefix is the open groups, each followed by a dot, e.g. "a.b.".
	prefix string
}

// Enabled drops records below debug. out applies the level Benthos is
// configured with to the rest.
func (h *handler) Enabled(_ context.Context, level slog.Level) bool { return level >= slog.LevelDebug }

func (h *handler) Handle(_ context.Context, r slog.Record) error {
	attrs := slices.Clone(h.attrs)
	r.Attrs(func(a slog.Attr) bool {
		attrs = appendFlat(attrs, h.prefix, a)
		return true
	})
	kvs := make([]any, 0, 2*len(attrs)+4)
	for _, a := range attrs {
		kvs = append(kvs, a.Key, a.Value.Any())
	}
	if r.Level >= slog.LevelWarn {
		kind := h.c.classifyRecord(r.Message, firstError(attrs))
		kvs = append(kvs, errorKind, kind.name, errorOwner, kind.Owner())
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

// firstError returns the first error value among attrs, which appendFlat has
// resolved, or nil.
func firstError(attrs []slog.Attr) error {
	for _, a := range attrs {
		if err, ok := a.Value.Any().(error); ok {
			return err
		}
	}
	return nil
}

// appendFlat appends a to dst with prefix in front of its key, after resolving
// a LogValuer. A group is expanded into its attributes, with its key and a dot
// added to the prefix; a group without a key is inlined. Empty attributes and
// empty groups are dropped, as slog handlers do.
func appendFlat(dst []slog.Attr, prefix string, a slog.Attr) []slog.Attr {
	v := a.Value.Resolve()
	if v.Kind() == slog.KindGroup {
		if a.Key != "" {
			prefix += a.Key + "."
		}
		for _, ga := range v.Group() {
			dst = appendFlat(dst, prefix, ga)
		}
		return dst
	}
	if a.Key == "" && v.Any() == nil {
		return dst
	}
	return append(dst, slog.Attr{Key: prefix + a.Key, Value: v})
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Clone: appending in place would let two handlers derived from the same
	// parent overwrite each other's attributes in the shared backing array.
	flat := slices.Clone(h.attrs)
	for _, a := range attrs {
		flat = appendFlat(flat, h.prefix, a)
	}
	return &handler{out: h.out, c: h.c, attrs: flat, prefix: h.prefix}
}

// WithGroup returns a handler that puts name and a dot in front of the keys of
// the attributes added after it. An empty name returns h unchanged.
func (h *handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &handler{out: h.out, c: h.c, attrs: h.attrs, prefix: h.prefix + name + "."}
}
