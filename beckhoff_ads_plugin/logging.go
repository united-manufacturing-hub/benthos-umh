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

package beckhoff_ads_plugin

import (
	"context"
	"log/slog"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// benthosLogHandler bridges go-ads slog records to a Benthos service.Logger.
// Levels are taken as go-ads sets them; a second level policy here would drift.
// Records at Warn and above get error_id and error_class (see error_class.go).
type benthosLogHandler struct {
	logger *service.Logger
	attrs  []slog.Attr
}

func (h *benthosLogHandler) Enabled(_ context.Context, level slog.Level) bool {
	// Suppress trace (LevelTrace = -8); forward debug and above to benthos,
	// which then applies its own configured level filter.
	return level >= slog.LevelDebug
}

func (h *benthosLogHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := append([]slog.Attr(nil), h.attrs...)
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a)
		return true
	})
	kvs := make([]any, 0, 2*len(attrs)+4)
	for _, a := range attrs {
		kvs = append(kvs, a.Key, a.Value.Any())
	}
	if r.Level >= slog.LevelWarn {
		c, ok := goADSMessages[r.Message]
		if !ok {
			c = classify(recordError(attrs), goADSMessageFallbacks[r.Message])
		}
		kvs = append(kvs, "error_id", c.ID, "error_class", c.Class)
	}

	l := h.logger
	if len(kvs) > 0 {
		l = l.With(kvs...)
	}

	switch {
	case r.Level >= slog.LevelError:
		l.Errorf("%s", r.Message)
	case r.Level >= slog.LevelWarn:
		l.Warnf("%s", r.Message)
	case r.Level >= slog.LevelInfo:
		l.Infof("%s", r.Message)
	default:
		l.Debugf("%s", r.Message)
	}
	return nil
}

// recordError returns the first attribute that holds an error value; go-ads
// logs its errors as slog.Any("error", err), not as strings.
func recordError(attrs []slog.Attr) error {
	for _, a := range attrs {
		if err, ok := a.Value.Any().(error); ok {
			return err
		}
	}
	return nil
}

func (h *benthosLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Clone: appending in place would let two handlers derived from the same
	// parent overwrite each other's attributes via the shared backing array.
	merged := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	merged = append(append(merged, h.attrs...), attrs...)
	return &benthosLogHandler{logger: h.logger, attrs: merged}
}

func (h *benthosLogHandler) WithGroup(_ string) slog.Handler { return h }
