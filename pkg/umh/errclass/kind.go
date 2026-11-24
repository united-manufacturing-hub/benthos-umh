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
	"regexp"
	"sync"
)

// Kind is one cause of a warning or error that a component logs. Its name,
// e.g. "ads::route_fault", is logged as error_kind, and its owner as
// error_owner. Declare a Kind as a package variable, with Component.UserKind
// or Component.UMHKind. There is one *Kind per name, so == tells whether two
// Kinds are the same.
type Kind struct {
	name  string
	owner owner
}

// String returns the name of k, e.g. "ads::route_fault".
func (k *Kind) String() string { return k.name }

// Owner returns the owner of k, as logged in error_owner: "user", "umh" or
// "unknown".
func (k *Kind) Owner() string { return string(k.owner) }

// Errorf returns fmt.Errorf(format, args...), tagged with k. Use it for an
// error the component creates itself. Classifier.Classify returns k for the
// result, also after further wrapping, and even if an error it wraps matches
// a rule. errors.Is and errors.As still see the wrapped errors.
func (k *Kind) Errorf(format string, args ...any) error {
	return &kindError{kind: k, err: fmt.Errorf(format, args...)}
}

type kindError struct {
	kind *Kind
	err  error
}

func (e *kindError) Error() string { return e.err.Error() }
func (e *kindError) Unwrap() error { return e.err }

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// componentNames holds the name of every Component created so far. Two
// components with one name would log the same error_kind for different causes.
var componentNames = struct {
	sync.Mutex
	seen map[string]bool
}{seen: map[string]bool{}}

// Component stands for one Benthos component, such as the "ads" input, and
// holds the Kinds it logs. Its name is the name the component registers with
// Benthos. That name is the part of every Kind name before "::".
//
// Declare the Component and its Kinds only as package variables. A malformed or
// duplicate name then panics when the package loads, and every test of the
// component fails. The Kinds are read without a lock while the component logs,
// so no Kind may be declared after the package has loaded.
type Component struct {
	// name is the component name, e.g. "ads".
	name string
	// kinds holds every Kind declared on this Component, by name.
	kinds map[string]*Kind
	// unclassified is "<component>::unclassified", the Kind of every warning
	// and error that no rule matched.
	unclassified *Kind
}

// NewComponent returns the Component for the Benthos component registered as
// name, e.g. "ads". It panics if name is not lowercase snake_case or if a
// Component with that name already exists.
func NewComponent(name string) *Component {
	if !namePattern.MatchString(name) {
		panic(fmt.Sprintf("errclass: component name %q is not lowercase snake_case", name))
	}
	componentNames.Lock()
	defer componentNames.Unlock()
	if componentNames.seen[name] {
		panic(fmt.Sprintf("errclass: component name %q is declared twice", name))
	}
	componentNames.seen[name] = true
	c := &Component{name: name, kinds: map[string]*Kind{}}
	c.unclassified = c.declare("unclassified", ownerUnknown)
	return c
}

// UserKind declares the Kind "<component>::<name>" with the owner user, for a
// cause outside UMH's code. The user can fix such a cause, or it recovers by
// itself. UserKind panics if name is not lowercase snake_case or if c already
// has a Kind with that name.
func (c *Component) UserKind(name string) *Kind { return c.declare(name, ownerUser) }

// UMHKind declares the Kind "<component>::<name>" with the owner umh, for a
// cause only UMH can fix. UMHKind panics if name is not lowercase snake_case
// or if c already has a Kind with that name.
func (c *Component) UMHKind(name string) *Kind { return c.declare(name, ownerUMH) }

// Unclassified returns the Kind "<component>::unclassified", with the owner
// unknown. Classifier.Classify returns it for an error that no rule matches.
func (c *Component) Unclassified() *Kind { return c.unclassified }

// owns reports whether k is one of c's Kinds.
func (c *Component) owns(k *Kind) bool { return k != nil && c.kinds[k.name] == k }

func (c *Component) declare(name string, o owner) *Kind {
	if !namePattern.MatchString(name) {
		panic(fmt.Sprintf("errclass: kind name %q in component %q is not lowercase snake_case", name, c.name))
	}
	full := c.name + "::" + name
	if _, ok := c.kinds[full]; ok {
		panic(fmt.Sprintf("errclass: %s is declared twice", full))
	}
	k := &Kind{name: full, owner: o}
	c.kinds[full] = k
	return k
}
