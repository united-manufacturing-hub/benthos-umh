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

// Kind is one row of a plugin's contract: the error_id, e.g. ads::route_fault,
// and its class. Build it with Plugin.Define, which makes it well-formed.
type Kind struct {
	name  string
	class Class
}

// String is the error_id, e.g. "ads::route_fault".
func (kind Kind) String() string { return kind.name }

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// pluginNames holds every name a Plugin was created for: two plugins under one
// name would log the same error_id for different causes.
var pluginNames = struct {
	sync.Mutex
	seen map[string]bool
}{seen: map[string]bool{}}

// Plugin defines the Kinds of one plugin. Every error_id starts with the
// plugin's name, e.g. "ads::route_fault", so the same cause in two plugins gets
// two Kinds. Declare a plugin's Plugin and Kinds as package variables: a
// malformed or duplicate name then panics when the package loads, so any test of the plugin
// fails on it.
type Plugin struct {
	// name is the prefix of every error_id: the name the plugin registers with
	// Benthos, e.g. "ads".
	name string
	// seenNames holds every name already defined for this plugin, so the
	// same Kind cannot be defined twice.
	seenNames map[string]bool
}

// NewPlugin returns the Plugin for name, the name the plugin registers with
// Benthos. name must be lowercase snake_case and not taken by another Plugin.
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
	return &Plugin{name: name, seenNames: map[string]bool{}}
}

// Define returns the Kind pluginname::name with class. name must be lowercase
// snake_case and not taken in this Plugin.
func (p *Plugin) Define(name string, class Class) Kind {
	if !namePattern.MatchString(name) {
		panic(fmt.Sprintf("errclass: name %q under %q is not lowercase snake_case", name, p.name))
	}
	switch class {
	case Bug, Known, Unknown:
	default:
		panic(fmt.Sprintf("errclass: %s::%s has unknown class %q", p.name, name, class))
	}
	if p.seenNames[name] {
		panic(fmt.Sprintf("errclass: %s::%s is defined twice", p.name, name))
	}
	p.seenNames[name] = true
	return Kind{name: p.name + "::" + name, class: class}
}
