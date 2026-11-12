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
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Logger is a *service.Logger whose Warn and Error lines carry error_id and
// error_class. Its zero value discards everything.
type Logger struct {
	l *service.Logger
	c *Classifier
}

// With returns a Logger with the key-value pairs added to every line.
func (l Logger) With(keyValuePairs ...any) Logger {
	return Logger{l: l.l.With(keyValuePairs...), c: l.c}
}

// Debug logs msg at debug level, without error_id and error_class.
func (l Logger) Debug(msg string) { l.l.Debug(msg) }

// Debugf logs a formatted message at debug level, like Debug.
func (l Logger) Debugf(format string, args ...any) { l.l.Debugf(format, args...) }

// Info logs msg at info level, without error_id and error_class.
func (l Logger) Info(msg string) { l.l.Info(msg) }

// Infof logs a formatted message at info level, like Info.
func (l Logger) Infof(format string, args ...any) { l.l.Infof(format, args...) }

// Warnf logs a warning under id, the meaning the call site gives the line.
func (l Logger) Warnf(id ID, format string, args ...any) {
	l.tagged(l.c.Classify(nil, id)).Warnf(format, args...) //nolint:forbidigo // the one place a Warn line gets its ID
}

// Errorf logs an error under id, like Warnf.
func (l Logger) Errorf(id ID, format string, args ...any) {
	l.tagged(l.c.Classify(nil, id)).Errorf(format, args...) //nolint:forbidigo // the one place an Error line gets its ID
}

// WarnErr logs a warning that reports err, with ": <err>" appended to the
// text. A rule that matches err wins over id; id applies when none does.
func (l Logger) WarnErr(err error, id ID, format string, args ...any) {
	l.tagged(l.c.Classify(err, id)).Warnf("%s", withErr(err, format, args)) //nolint:forbidigo // the one place a Warn line gets its ID
}

// ErrorErr logs an error that reports err, like WarnErr.
func (l Logger) ErrorErr(err error, id ID, format string, args ...any) {
	l.tagged(l.c.Classify(err, id)).Errorf("%s", withErr(err, format, args)) //nolint:forbidigo // the one place an Error line gets its ID
}

func (l Logger) tagged(id ID) *service.Logger {
	return l.l.With("error_id", id.name, "error_class", string(id.class))
}

func withErr(err error, format string, args []any) string {
	msg := fmt.Sprintf(format, args...)
	if err == nil {
		return msg
	}
	return msg + ": " + err.Error()
}
