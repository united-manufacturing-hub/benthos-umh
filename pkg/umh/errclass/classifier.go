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
	"errors"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Rule maps the errors it matches onto an ID.
type Rule struct {
	Matches func(error) bool
	ID      ID
}

// Classifier holds one plugin's rules.
type Classifier struct {
	// Rules are tried in order and the first match wins, so a more specific
	// cause has to come before a cause it wraps.
	Rules []Rule
	// Unclassified is the plugin's ID for a line nothing classified. Its class
	// should be Unknown.
	Unclassified ID
}

// Classify returns the ID for a line that reports err. If a rule matches err,
// that rule wins: the error knows the cause better than the call site.
// Otherwise site applies, the ID the call site stands for. A zero site means
// Unclassified, so a missed lookup in a map can be passed straight in.
func (c *Classifier) Classify(err error, site ID) ID {
	if c == nil {
		return site
	}
	if err != nil {
		for _, r := range c.Rules {
			if r.Matches(err) {
				return r.ID
			}
		}
	}
	if site == (ID{}) {
		return c.Unclassified
	}
	return site
}

// Logger wraps l so that every Warn and Error line it writes is classified.
func (c *Classifier) Logger(l *service.Logger) Logger {
	return Logger{l: l, c: c}
}

// WithPrefix puts err's ID and class in front of its text, once. Use it on
// errors handed back to Benthos, whose own log line ("Failed to connect to
// ...: ...") would otherwise carry neither. errors.Is and errors.As still see
// err.
func (c *Classifier) WithPrefix(err error) error {
	if err == nil {
		return nil
	}
	var ce *classifiedError
	if errors.As(err, &ce) {
		return err
	}
	return &classifiedError{id: c.Classify(err, ID{}), err: err}
}

// classifiedError is an error whose text starts with its ID and class.
type classifiedError struct {
	id  ID
	err error
}

func (e *classifiedError) Error() string {
	return fmt.Sprintf("[error_id=%s error_class=%s] %v", e.id.name, e.id.class, e.err)
}

func (e *classifiedError) Unwrap() error { return e.err }

// Is matches target and anything wrapping it, by errors.Is.
func Is(target error) func(error) bool {
	return func(err error) bool { return errors.Is(err, target) }
}
