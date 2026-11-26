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
	"strings"
)

// Rule gives a Kind to the errors or log records it matches. Create it with
// ErrorIs, ErrorMatches, ErrorContains or MessageIs, and pass it to
// Component.Classifier.
type Rule struct {
	kind *Kind
	// matches is set for a rule that matches an error.
	matches func(error) bool
	// msg is set for a rule that matches the message of a log record.
	msg string
}

// ErrorIs returns the Rule that gives kind to every error for which
// errors.Is(err, target) holds.
func ErrorIs(target error, kind *Kind) Rule {
	return Rule{kind: kind, matches: func(err error) bool { return errors.Is(err, target) }}
}

// ErrorMatches returns the Rule that gives kind to every error for which
// matches returns true. Use it where errors.Is is not enough, for example to
// call errors.As or to look at the error's fields.
func ErrorMatches(matches func(error) bool, kind *Kind) Rule {
	return Rule{kind: kind, matches: matches}
}

// ErrorContains returns the Rule that gives kind to every error that has an
// error in its chain whose text contains substr. Use it for a library that
// reports a failure only as text, without a sentinel error or a type to
// match. Like the other error rules, it never applies to the message of a log
// record. ErrorContains panics if substr is empty.
func ErrorContains(substr string, kind *Kind) Rule {
	if substr == "" {
		panic("errclass: ErrorContains rule with an empty substring")
	}
	contains := func(err error) bool { return strings.Contains(err.Error(), substr) }
	return Rule{kind: kind, matches: func(err error) bool { return walk(err, contains) }}
}

// MessageIs returns the Rule that gives kind to every warning and error
// record whose message is exactly msg. It applies only to the records of a
// library that logs through Logger.Slog, which often carry no error to match.
// It never applies to errors passed to Classifier.Classify, Logger.Error or
// Logger.Warn. MessageIs panics if msg is empty.
func MessageIs(msg string, kind *Kind) Rule {
	if msg == "" {
		panic("errclass: MessageIs rule with an empty message")
	}
	return Rule{kind: kind, msg: msg}
}

// Classifier gives the errors and log records of one component their Kind.
// Create it with Component.Classifier.
type Classifier struct {
	component *Component
	rules     []Rule
}

// Classifier returns a Classifier that tries rules in order. The first rule
// that matches wins. An error can wrap another error that matches a different
// rule. Put the rule for the wrapping error first, because it names the more
// specific cause.
//
// An error is matched by the ErrorIs, ErrorMatches and ErrorContains rules
// only. A log record of a library is matched by every rule: a MessageIs rule
// matches the record's message, and the other rules match the error among its
// fields. So a MessageIs rule placed before the error rules decides by the
// message alone, and one placed after them applies only if no error rule
// matched.
//
// Classifier panics if a rule has no Kind, a Kind of another component, or
// neither an error to match nor a message, such as a zero Rule. Declare the
// Classifier as a package variable, so that such a rule fails every test of
// the component.
func (c *Component) Classifier(rules ...Rule) *Classifier {
	for i, r := range rules {
		switch {
		case r.kind == nil:
			panic(fmt.Sprintf("errclass: rule %d of component %q has no Kind", i, c.name))
		case !c.owns(r.kind):
			panic(fmt.Sprintf("errclass: rule %d of component %q has the Kind %s of another component", i, c.name, r.kind.name))
		case r.matches == nil && r.msg == "":
			panic(fmt.Sprintf("errclass: rule %d of component %q matches neither an error nor a message; create rules with ErrorIs, ErrorMatches, ErrorContains or MessageIs", i, c.name))
		}
	}
	return &Classifier{component: c, rules: rules}
}

// Messages returns the messages of the MessageIs rules, in order. A test can
// check them against the source of the library that logs them, so that a
// reworded message fails the test.
func (c *Classifier) Messages() []string {
	var msgs []string
	for _, r := range c.rules {
		if r.matches == nil {
			msgs = append(msgs, r.msg)
		}
	}
	return msgs
}

// Classify returns the Kind of err. It returns the first of:
//  1. the Kind err was created with by Kind.Errorf, if it is one of this
//     component's Kinds,
//  2. the Kind of the first error rule that matches err,
//  3. the component's unclassified Kind, "<component>::unclassified". This is
//     also the Kind of a nil err.
//
// Logger.Error and Logger.Warn log err with this Kind. Tests use Classify to
// check which Kind an error is logged with:
//
//	Expect(classifier.Classify(err)).To(Equal(kindRouteFault))
func (c *Classifier) Classify(err error) *Kind {
	if kind := c.createdKind(err); kind != nil {
		return kind
	}
	if err != nil {
		for _, r := range c.rules {
			if r.matches != nil && r.matches(err) {
				return r.kind
			}
		}
	}
	return c.component.unclassified
}

// ClassifyMessage returns the Kind of a library's log line with the message
// msg that carries err, which may be nil. It returns the first of the Kind err
// was created with, the Kind of the first rule that matches msg or err, and
// the component's unclassified Kind.
//
// Logger.Slog classifies the records of a library that logs through log/slog
// with it. For a library that logs through anything else, call it from the
// library's log callback and log the line with the Kind it returns:
//
//	kind := classifier.ClassifyMessage(msg, err)
//	log.Warn(kind.Errorf("%s: %w", msg, err))
func (c *Classifier) ClassifyMessage(msg string, err error) *Kind {
	if kind := c.createdKind(err); kind != nil {
		return kind
	}
	for _, r := range c.rules {
		if r.matches == nil {
			if r.msg == msg {
				return r.kind
			}
		} else if err != nil && r.matches(err) {
			return r.kind
		}
	}
	return c.component.unclassified
}

// createdKind returns the Kind err was created with by Kind.Errorf, if it is
// one of this component's Kinds, or nil. It searches the whole tree of err,
// outer errors first, so a Kind of another component that wraps one of this
// component's errors does not hide its Kind.
func (c *Classifier) createdKind(err error) *Kind {
	var kind *Kind
	walk(err, func(e error) bool {
		if ke, ok := e.(*kindError); ok && c.component.owns(ke.kind) { //nolint:errorlint // walk visits every wrapped error itself
			kind = ke.kind
			return true
		}
		return false
	})
	return kind
}

// walk calls visit for err and every error it wraps, depth-first and outer
// errors first, following both Unwrap() error and Unwrap() []error, as
// errors.Is does. It stops and returns true as soon as visit returns true.
func walk(err error, visit func(error) bool) bool {
	for err != nil {
		if visit(err) {
			return true
		}
		switch u := err.(type) { //nolint:errorlint // walk unwraps err itself
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		case interface{ Unwrap() []error }:
			for _, e := range u.Unwrap() {
				if walk(e, visit) {
					return true
				}
			}
			return false
		default:
			return false
		}
	}
	return false
}

// EnrichForLog returns err with its error_kind and error_owner in front of its
// text, e.g. "[error_kind=ads::route_fault error_owner=user] connect: ...".
// It returns nil for a nil err, and an already enriched err unchanged.
// errors.Is, errors.As and Classify still see err.
//
// Use it on an error that the component returns to Benthos, such as from
// Connect. Benthos logs that error itself, e.g. "Failed to connect to ...:
// <err>", without the fields that the Logger adds.
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

func (e *enrichedError) Error() string { return e.prefix() + e.err.Error() }

// prefix is the text in front of the text of the wrapped error.
func (e *enrichedError) prefix() string {
	return fmt.Sprintf("[error_kind=%s error_owner=%s] ", e.kind.name, e.kind.Owner())
}

func (e *enrichedError) Unwrap() error { return e.err }
