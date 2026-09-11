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
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// benthosLogHandler bridges go-ads slog records to a Benthos service.Logger.
// Levels are taken as go-ads sets them, with the single exception in Handle.
type benthosLogHandler struct {
	logger *service.Logger
	attrs  []slog.Attr
}

func (h *benthosLogHandler) Enabled(_ context.Context, level slog.Level) bool {
	// Suppress trace (LevelTrace = -8); forward debug and above to benthos,
	// which then applies its own configured level filter.
	return level >= slog.LevelDebug
}

// unresolvedBaseTypeMessage is go-ads' warning for a symbol whose base type it
// could not resolve.
const unresolvedBaseTypeMessage = "cannot resolve the base type"

func (h *benthosLogHandler) Handle(_ context.Context, r slog.Record) error {
	var kvs []any
	var dataType string
	for _, a := range h.attrs {
		kvs = append(kvs, a.Key, a.Value.Any())
	}
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "dataType" {
			dataType = a.Value.String()
		}
		kvs = append(kvs, a.Key, a.Value.Any())
		return true
	})

	l := h.logger
	if len(kvs) > 0 {
		l = l.With(kvs...)
	}

	// The one level override: go-ads cannot resolve a STRING member's base type
	// through the datatype table, and its hint asks for the symbol load that is
	// already on. stringBaseType supplies the answer, so the warning is noise.
	level := r.Level
	if level == slog.LevelWarn &&
		strings.Contains(r.Message, unresolvedBaseTypeMessage) &&
		stringBaseType(dataType) != "" {
		level = slog.LevelDebug
	}

	switch {
	case level >= slog.LevelError:
		l.Errorf("%s", r.Message)
	case level >= slog.LevelWarn:
		if strings.Contains(r.Message, "reconnect dial") {
			l.Errorf("%s", r.Message)
		} else {
			l.Warnf("%s", r.Message)
		}
	case level >= slog.LevelInfo:
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
