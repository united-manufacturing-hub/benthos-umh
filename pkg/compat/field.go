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

// Field returns the optional, advanced compatibility field for a plugin's
// config spec. It accepts any key, so that retired keys keep validating, and
// its help text lists the declared flags. After Field, Declare panics.
func (f *Flags) Field() *service.ConfigField {
	f.fielded = true
	field := service.NewAnyMapField("compatibility").
		Optional().
		Advanced().
		Description(f.fieldDescription())
	if len(f.names) > 0 {
		example := make(map[string]any, len(f.names))
		for _, name := range f.names {
			example[name] = true
		}
		field = field.Examples(example)
	}
	return field
}

// fieldDescription renders the user-facing help text for the compatibility
// field, built from the declared flags.
func (f *Flags) fieldDescription() string {
	var b strings.Builder
	b.WriteString("Compatibility flags let you opt in to this plugin's new behavior before it becomes the default. Every flag defaults to false, meaning the plugin keeps its previous behavior until you explicitly set it to true here. Keys are case-sensitive, and a key that differs from a flag only in case is rejected. Other keys this plugin does not recognize are ignored, so retired keys from an older configuration keep this config valid. The YAML merge key (\"<<\") is not supported: write the flags out explicitly.")
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
