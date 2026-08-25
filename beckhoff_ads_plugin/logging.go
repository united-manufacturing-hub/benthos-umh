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

// benthosLogHandler is a slog.Handler bridging go-ads v2 log records to a
// Benthos service.Logger; trace records are suppressed, verbosity follows the benthos log level.
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
	var kvs []any
	perSymbol := false
	for _, a := range h.attrs {
		kvs = append(kvs, a.Key, a.Value.Any())
		perSymbol = perSymbol || a.Key == "symbol" || a.Key == "handle"
	}
	r.Attrs(func(a slog.Attr) bool {
		kvs = append(kvs, a.Key, a.Value.Any())
		perSymbol = perSymbol || a.Key == "symbol" || a.Key == "handle"
		return true
	})

	l := h.logger
	if len(kvs) > 0 {
		l = l.With(kvs...)
	}

	switch {
	case r.Level >= slog.LevelError:
		l.Errorf("%s", r.Message)
	case r.Level >= slog.LevelWarn:
		l.Warnf("%s", r.Message)
	// A per-symbol/per-handle line scales with the config and buries the rest;
	// failures carry their own Warn, so only the success chatter moves to debug.
	case r.Level >= slog.LevelInfo && !perSymbol:
		l.Infof("%s", r.Message)
	default:
		l.Debugf("%s", r.Message)
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
