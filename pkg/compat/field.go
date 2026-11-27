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

package compat

import (
	"fmt"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Field returns the compatibility config field for a plugin's config spec:
//
//	spec := service.NewConfigSpec().
//		Field(service.NewStringField("endpoint")).
//		Field(compatibility.Field())
//
// The field is an optional, advanced map of arbitrary keys, so that retired
// compatibility keys keep validating, and it carries one line of help text per
// declared flag. The same Flags value can safely be reused across specs.
//
// The description is a snapshot of the flags declared at the moment Field is
// called, not a live view. A package-level spec variable such as
//
//	var spec = service.NewConfigSpec().Field(compatibility.Field())
//
// is initialized before any init function that later declares flags, so its
// description would claim the plugin has no compatibility flags even though
// Parse still accepts them: the new behavior would be undiscoverable. Declare
// every flag before calling Field — the standard pattern is to declare the
// flags and build the spec in the same init function.
func (f *Flags) Field() *service.ConfigField {
	return service.NewAnyMapField("compatibility").
		Optional().
		Advanced().
		Description(f.fieldDescription())
}

// fieldDescription renders the user-facing help text for the compatibility
// field, built from the declared flags.
func (f *Flags) fieldDescription() string {
	var b strings.Builder
	b.WriteString("Compatibility flags let you opt in to this plugin's new behavior before it becomes the default. Every flag defaults to false, meaning the plugin keeps its previous behavior until you explicitly set it to true here. Keys are matched exactly, including case. Keys this plugin does not recognize are ignored, so retired keys from an older configuration keep this config valid. The YAML merge key (\"<<\") is not supported: write the flags out explicitly.")
	switch len(f.names) {
	case 0:
		b.WriteString(" This plugin currently has no compatibility flags.")
	case 1:
		b.WriteString(" The available flag is:")
	default:
		b.WriteString(" The available flags are:")
	}
	for _, name := range f.names {
		fmt.Fprintf(&b, "\n- %s: %s", name, f.flags[name].description)
	}
	return b.String()
}
