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

// Package errclass tags every Warn and Error line a plugin emits with
// error_id and error_class. umh-core reads only those two fields to decide what
// reaches Sentry, so an ID is part of the Sentry fingerprint and must not change
// once shipped. The contract, with each plugin's IDs:
// https://linear.app/united-manufacturing-hub/document/plugin-error-classes-contract-between-benthos-umh-and-umh-core-e51f0744258b
//
// A plugin declares its IDs with a Namespace, its Rules and a Classifier, and
// logs through the Logger that Classifier returns. Every Warn and Error method
// of Logger takes an ID, so a Warn or Error line cannot be written without one.
// The forbidigo rule in .golangci.yaml forbids calling Warn and Error on a
// *service.Logger directly.
package errclass

import (
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Class is the error_class of a line. umh-core reports Bug and Unknown to
// Sentry.
type Class string

const (
	// Bug: only UMH can fix it.
	Bug Class = "bug"
	// Known: the cause is outside UMH's code: the customer's configuration,
	// device or network. That includes faults that recover by themselves, such
	// as a dropped connection.
	Known Class = "known"
	// Unknown: nothing matched, so it is reported until it gets an ID.
	Unknown Class = "unknown"
)

// ID is one row of a plugin's contract: the error_id, e.g. ads::route_fault,
// and its Class. Build it with a Namespace, which makes it well-formed.
type ID struct {
	name  string
	class Class
}

// String is the error_id, e.g. "ads::route_fault".
func (id ID) String() string { return id.name }

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// prefixes holds every prefix a Namespace was created for: two plugins under
// one prefix would share their Sentry fingerprints.
var prefixes = struct {
	sync.Mutex
	seen map[string]bool
}{seen: map[string]bool{}}

// Namespace builds the IDs of one plugin under its prefix, e.g. "ads", so the
// same cause in two plugins gets two Sentry fingerprints. The prefix is not the
// Sentry feature tag; umh-core sets that per plugin. Declare a plugin's
// Namespace and IDs as package variables: a malformed or duplicate prefix or
// name then panics when the package loads, so any test of the plugin fails on
// it.
type Namespace struct {
	prefix string
	names  map[string]bool
}

// NewNamespace returns the Namespace for prefix, which must be lowercase
// snake_case and not taken by another Namespace.
func NewNamespace(prefix string) *Namespace {
	if !namePattern.MatchString(prefix) {
		panic(fmt.Sprintf("errclass: prefix %q is not lowercase snake_case", prefix))
	}
	prefixes.Lock()
	defer prefixes.Unlock()
	if prefixes.seen[prefix] {
		panic(fmt.Sprintf("errclass: prefix %q is declared twice", prefix))
	}
	prefixes.seen[prefix] = true
	return &Namespace{prefix: prefix, names: map[string]bool{}}
}

// ID returns the ID prefix::name with class. name must be lowercase snake_case
// and not taken in this Namespace.
func (n *Namespace) ID(name string, class Class) ID {
	if !namePattern.MatchString(name) {
		panic(fmt.Sprintf("errclass: name %q under %q is not lowercase snake_case", name, n.prefix))
	}
	switch class {
	case Bug, Known, Unknown:
	default:
		panic(fmt.Sprintf("errclass: %s::%s has unknown class %q", n.prefix, name, class))
	}
	if n.names[name] {
		panic(fmt.Sprintf("errclass: %s::%s is declared twice", n.prefix, name))
	}
	n.names[name] = true
	return ID{name: n.prefix + "::" + name, class: class}
}

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

func (l Logger) Debug(msg string)                  { l.l.Debug(msg) }
func (l Logger) Debugf(format string, args ...any) { l.l.Debugf(format, args...) }
func (l Logger) Info(msg string)                   { l.l.Info(msg) }
func (l Logger) Infof(format string, args ...any)  { l.l.Infof(format, args...) }

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

// Is matches target and anything wrapping it, by errors.Is.
func Is(target error) func(error) bool {
	return func(err error) bool { return errors.Is(err, target) }
}
