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
)

// Rule gives a Kind to the errors or log messages it matches. Create it with
// Is, Match or Message.
type Rule struct {
	// msg is the message of a Message rule.
	msg string
	// matches is the check of an Is or Match rule.
	matches func(error) bool
	kind    *Kind
}

// Is returns the Rule that gives kind to every error for which
// errors.Is(err, target) holds.
func Is(target error, kind *Kind) Rule {
	return Rule{matches: func(err error) bool { return errors.Is(err, target) }, kind: kind}
}

// Match returns the Rule that gives kind to every error matches reports, for
// errors that need errors.As or a look at their fields.
func Match(matches func(error) bool, kind *Kind) Rule {
	return Rule{matches: matches, kind: kind}
}

// Message returns the Rule that gives kind to every Warn and Error line whose
// message is exactly msg. It is meant for libraries that log through the
// *slog.Logger from Classifier.Slog, where the message is often all there is.
func Message(msg string, kind *Kind) Rule {
	if msg == "" {
		panic("errclass: Message rule with an empty message")
	}
	return Rule{msg: msg, kind: kind}
}

func (r Rule) matchesError(err error) bool {
	return r.matches != nil && err != nil && r.matches(err)
}

func (r Rule) matchesMessage(msg string) bool {
	return r.matches == nil && r.msg == msg
}

// Classifier holds one plugin's Rules. Create it with Plugin.Classifier.
type Classifier struct {
	plugin *Plugin
	rules  []Rule
}

// Classifier returns a Classifier with rules, tried in order. The first match
// wins, so a rule for a specific cause has to come before a rule for a cause
// that the specific one wraps.
func (p *Plugin) Classifier(rules ...Rule) *Classifier {
	return &Classifier{plugin: p, rules: rules}
}

// Classify returns the Kind of err, trying in order:
//  1. the Kind err was created with by Kind.Errorf, if it is one of this
//     plugin's Kinds,
//  2. the first Is or Match Rule that matches err,
//  3. the plugin's Unclassified Kind, also for a nil err.
//
// An error nothing knows is reported as unclassified until it gets a Rule of
// its own; that is how new causes surface.
//
// Use it where the Kind decides what the code does next. To give a log line
// the Kind of its error, use WithErr.
func (c *Classifier) Classify(err error) *Kind {
	if kind := c.matchError(err); kind != nil {
		return kind
	}
	return c.plugin.unclassified
}

// matchError returns the Kind of err, trying the Kind err was created with
// and then the Is and Match rules, or nil.
func (c *Classifier) matchError(err error) *Kind {
	if kind := c.createdKind(err); kind != nil {
		return kind
	}
	for _, r := range c.rules {
		if r.matchesError(err) {
			return r.kind
		}
	}
	return nil
}

// matchLine returns the Kind of a log line with msg that reports err, trying
// the Kind err was created with and then every rule in order, or nil.
func (c *Classifier) matchLine(msg string, err error) *Kind {
	if kind := c.createdKind(err); kind != nil {
		return kind
	}
	for _, r := range c.rules {
		if r.matchesMessage(msg) || r.matchesError(err) {
			return r.kind
		}
	}
	return nil
}

// createdKind returns the Kind err was created with by Kind.Errorf, if it is
// one of this plugin's Kinds, or nil.
func (c *Classifier) createdKind(err error) *Kind {
	var ke *kindError
	if errors.As(err, &ke) && c.plugin.owns(ke.kind) {
		return ke.kind
	}
	return nil
}

// EnrichForLog returns err with its error_kind and error_class in front of its
// text, e.g. "[error_kind=ads::route_fault error_class=known] connect: ...".
//
// Use it on errors returned to Benthos, such as from Connect: Benthos logs them
// itself ("Failed to connect to ...: <err>"), without the fields the Logger
// adds. Enriching an enriched error changes nothing. errors.Is, errors.As and
// Classify still see err.
func (c *Classifier) EnrichForLog(err error) error {
	if err == nil {
		return nil
	}
	var ee *enrichedError
	if errors.As(err, &ee) {
		return err
	}
	return &enrichedError{kind: c.Classify(err), err: err}
}

type enrichedError struct {
	kind *Kind
	err  error
}

func (e *enrichedError) Error() string {
	return fmt.Sprintf("[error_kind=%s error_class=%s] %v", e.kind.name, e.kind.class, e.err)
}

func (e *enrichedError) Unwrap() error { return e.err }
