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

// Package compat provides compatibility flags: the standard way for a
// benthos-umh plugin to keep its previous behavior working until a customer
// opts in to a new behavior.
//
// A plugin uses the package in three steps. First, declare the plugin's
// flags once, at package level:
//
//	var (
//		compatibility  = compat.New()
//		correctTagPath = compatibility.Declare("correct_tag_path",
//			"Publish a tag under its browse path without its own name.")
//	)
//
// Second, add the config field to the plugin's config spec:
//
//	spec := service.NewConfigSpec().
//		Field(service.NewStringField("endpoint")).
//		Field(compatibility.Field())
//
// Third, read the flags in the plugin's constructor, where a logger is
// available:
//
//	flags, err := compatibility.Parse(conf, mgr.Logger())
//	if err != nil {
//		return nil, err
//	}
//	if flags.Enabled(correctTagPath) {
//		// new behavior
//	}
//
// Every flag defaults to false: a missing compatibility block, a missing key
// and an explicit false all leave the plugin on its previous behavior. Keys
// the plugin does not recognize are ignored rather than rejected, because
// retired flags stay in customer configs forever.
package compat

import (
	"fmt"
	"strings"
)

// Flag is the handle for one compatibility flag, returned by Declare.
//
// Flag is deliberately opaque: it can only be obtained from Declare, and it is
// only meaningful to the Flags set it was declared on. Passing a Flag to the
// wrong Flags set panics rather than silently reporting the flag as disabled.
type Flag struct {
	name        string
	description string
	flags       *Flags
}

// Flags is the set of compatibility flags that one plugin recognizes. Each
// plugin builds a single Flags value with New and Declare, and passes its
// flags to Parse and Set.Enabled.
type Flags struct {
	flags   map[string]*Flag
	names   []string // declaration order, kept for stable output
	fielded bool     // Field was called; later Declares would be missing from its description
}

// New returns an empty set of compatibility flags for one plugin.
func New() *Flags {
	return &Flags{flags: make(map[string]*Flag)}
}

// Declare declares a compatibility flag and returns its handle. The name is
// the key a customer writes in the compatibility block; the description is
// the flag's help text in the Management Console and in the log.
//
// Declare panics if name or description is empty, if name was already
// declared in any casing, or if Field was already called on f, because Field's help text
// would not list the flag.
func (f *Flags) Declare(name string, description string) *Flag {
	if f.flags == nil {
		f.flags = make(map[string]*Flag)
	}
	if f.fielded {
		panic(fmt.Sprintf("Declare called for the compatibility flag %q after Field: declare every flag before building the config spec", name))
	}
	if name == "" {
		panic("Declare called with an empty flag name: a compatibility flag needs a name the customer can write in the compatibility block")
	}
	if description == "" {
		panic(fmt.Sprintf("Declare called with an empty description for the compatibility flag %q: the description is shown in the Management Console and in the log when the flag is enabled", name))
	}
	for _, existing := range f.names {
		if strings.EqualFold(existing, name) {
			panic(fmt.Sprintf("compatibility flag %q was declared twice on the same Flags (as %q and %q; names are compared ignoring case)", name, existing, name))
		}
	}
	fl := &Flag{name: name, description: description, flags: f}
	f.flags[name] = fl
	f.names = append(f.names, name)
	return fl
}
