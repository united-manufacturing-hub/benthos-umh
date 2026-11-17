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

// Kind is one cause a plugin can log. It has a name, e.g. "ads::route_fault",
// logged as error_kind, and a Class, logged as error_class. Create it with
// Plugin.Known or Plugin.Bug, as a package variable. Each Kind exists once, so
// Kinds compare with ==.
type Kind struct {
	name  string
	class Class
}

// String returns the name of k, e.g. "ads::route_fault".
func (k *Kind) String() string { return k.name }

// Class returns the class of k.
func (k *Kind) Class() Class { return k.class }

// Errorf is fmt.Errorf for an error the plugin creates itself, tagged with k.
// Classifier.Classify returns k for it, whatever errors it wraps: the call site
// that names a Kind decides, as with Classifier.With. errors.Is and errors.As
// see the wrapped errors as usual.
//
// Where an error wrapped inside may have a better Kind, ask Classify first and
// tag only what it does not know:
//
//	if classifier.Classify(err) == kindUnclassified {
//		return kindSessionCreateFailed.Errorf("creating the session failed: %w", err)
//	}
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

// pluginNames holds every name a Plugin was created for: two plugins under one
// name would log the same error_kind for different causes.
var pluginNames = struct {
	sync.Mutex
	seen map[string]bool
}{seen: map[string]bool{}}

// Plugin declares the Kinds of one Benthos plugin, under its plugin name: the
// name the plugin registers with Benthos, e.g. "ads", which is the part of
// every Kind name before "::". It makes sure no Kind is declared twice.
//
// Declare the Plugin and its Kinds only as package variables. A malformed or
// duplicate name then panics when the package loads, so every test of the
// plugin fails on it. The Kinds are read without locking while the plugin
// logs, so none may be declared later.
type Plugin struct {
	// name is the plugin name, e.g. "ads".
	name string
	// kinds holds every Kind declared on this Plugin, by name.
	kinds        map[string]*Kind
	unclassified *Kind
}

// NewPlugin returns the Plugin for the plugin that registers with Benthos as
// name, e.g. "ads". name must be lowercase snake_case and not taken by another
// Plugin.
func NewPlugin(name string) *Plugin {
	if !namePattern.MatchString(name) {
		panic(fmt.Sprintf("errclass: plugin name %q is not lowercase snake_case", name))
	}
	pluginNames.Lock()
	defer pluginNames.Unlock()
	if pluginNames.seen[name] {
		panic(fmt.Sprintf("errclass: plugin name %q is declared twice", name))
	}
	pluginNames.seen[name] = true
	p := &Plugin{name: name, kinds: map[string]*Kind{}}
	p.unclassified = p.declare("unclassified", Unknown)
	return p
}

// Known declares the Kind "<plugin>::<name>" for a cause outside UMH's code.
// name must be lowercase snake_case and not declared on p before.
func (p *Plugin) Known(name string) *Kind { return p.declare(name, Known) }

// Bug declares the Kind "<plugin>::<name>" for a cause only UMH can fix.
// name must be lowercase snake_case and not declared on p before.
func (p *Plugin) Bug(name string) *Kind { return p.declare(name, Bug) }

// Unclassified returns "<plugin>::unclassified", the Kind of every Warn and
// Error line nothing else classified. Its class is Unknown.
func (p *Plugin) Unclassified() *Kind { return p.unclassified }

// owns reports whether k is one of p's Kinds.
func (p *Plugin) owns(k *Kind) bool { return k != nil && p.kinds[k.name] == k }

func (p *Plugin) declare(name string, class Class) *Kind {
	if !namePattern.MatchString(name) {
		panic(fmt.Sprintf("errclass: kind name %q in plugin %q is not lowercase snake_case", name, p.name))
	}
	full := p.name + "::" + name
	if _, ok := p.kinds[full]; ok {
		panic(fmt.Sprintf("errclass: %s is declared twice", full))
	}
	k := &Kind{name: full, class: class}
	p.kinds[full] = k
	return k
}
