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
// once shipped. The contract, with each plugin's IDs, is the Linear document
// "Plugin error classes: contract between benthos-umh and umh-core".
//
// A plugin declares its IDs, its Rules and a Classifier, and logs through the
// Logger that Classifier returns. Logger.Warnf and Logger.Errorf take the error
// and the class of the call site, so a Warn or Error line cannot be written
// without a class. The forbidigo rule in .golangci.yml forbids calling Warn and
// Error on a *service.Logger directly.
package errclass

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// The four classes. umh-core reports Bug and Unknown to Sentry.
const (
	// Bug: only UMH can fix it.
	Bug = "bug"
	// Unknown: nothing matched, so it is reported until it gets an ID.
	Unknown = "unknown"
	// Known: the customer can fix it (configuration, device state, network).
	Known = "known"
	// Operational: transient, a retry fixes it.
	Operational = "operational"
)

// Class is one row of a plugin's contract: a stable ID and the class it
// belongs to. IDs are namespaced by plugin, e.g. "ads::route_fault", so the
// same cause in two plugins gets two Sentry fingerprints.
type Class struct {
	ID    string
	Class string
}

var idPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(::[a-z][a-z0-9_]*)+$`)

// Validate reports whether c has a well-formed ID under prefix and one of the
// four classes. Each plugin calls it on all its IDs in a test.
func (c Class) Validate(prefix string) error {
	if !idPattern.MatchString(c.ID) {
		return fmt.Errorf("error_id %q does not match %s", c.ID, idPattern)
	}
	if !strings.HasPrefix(c.ID, prefix+"::") {
		return fmt.Errorf("error_id %q is not under %q", c.ID, prefix+"::")
	}
	switch c.Class {
	case Bug, Unknown, Known, Operational:
		return nil
	}
	return fmt.Errorf("error_id %q has unknown error_class %q", c.ID, c.Class)
}

// Rule maps the errors it matches onto a Class.
type Rule struct {
	Matches func(error) bool
	Class   Class
}

// Classifier holds one plugin's rules.
type Classifier struct {
	// Rules are tried in order and the first match wins, so a more specific
	// cause has to come before a cause it wraps.
	Rules []Rule
	// Unclassified is the plugin's ID for a line nothing classified. Its class
	// should be Unknown.
	Unclassified Class
}

// Classify returns the Class for a line that reports err. If a rule matches
// err, that rule wins: the error knows the cause better than the call site.
// Otherwise fallback applies, the class the call site stands for. A zero
// fallback means Unclassified, so a missed lookup in a map can be passed
// straight in.
func (c *Classifier) Classify(err error, fallback Class) Class {
	if c == nil {
		return fallback
	}
	if err != nil {
		for _, r := range c.Rules {
			if r.Matches(err) {
				return r.Class
			}
		}
	}
	if fallback == (Class{}) {
		return c.Unclassified
	}
	return fallback
}

// Logger wraps l so that every Warn and Error line it writes is classified.
func (c *Classifier) Logger(l *service.Logger) Logger {
	return Logger{l: l, c: c}
}

// WithPrefix puts err's class in front of its text, once. Use it on errors
// handed back to Benthos, whose own log line ("Failed to connect to ...: ...")
// would otherwise carry no class. errors.Is and errors.As still see err.
func (c *Classifier) WithPrefix(err error) error {
	if err == nil {
		return nil
	}
	var ce *ClassifiedError
	if errors.As(err, &ce) {
		return err
	}
	return &ClassifiedError{Class: c.Classify(err, Class{}), Err: err}
}

// ClassifiedError is an error whose text starts with its class.
type ClassifiedError struct {
	Class Class
	Err   error
}

func (e *ClassifiedError) Error() string {
	return fmt.Sprintf("[error_id=%s error_class=%s] %v", e.Class.ID, e.Class.Class, e.Err)
}

func (e *ClassifiedError) Unwrap() error { return e.Err }

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

// Warnf logs a warning classified by Classify(err, fallback). err is the error
// the line reports, or nil; pass it to the format arguments too if the text
// should show it.
func (l Logger) Warnf(err error, fallback Class, format string, args ...any) {
	l.classified(err, fallback).Warnf(format, args...) //nolint:forbidigo // the one place a Warn line gets its class
}

// Errorf logs an error classified by Classify(err, fallback), like Warnf.
func (l Logger) Errorf(err error, fallback Class, format string, args ...any) {
	l.classified(err, fallback).Errorf(format, args...) //nolint:forbidigo // the one place an Error line gets its class
}

func (l Logger) classified(err error, fallback Class) *service.Logger {
	c := l.c.Classify(err, fallback)
	return l.l.With("error_id", c.ID, "error_class", c.Class)
}

// Is matches target and anything wrapping it, by errors.Is.
func Is(target error) func(error) bool {
	return func(err error) bool { return errors.Is(err, target) }
}

// IsDialFailure reports a TCP or UDP connection that could not be opened.
func IsDialFailure(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}
