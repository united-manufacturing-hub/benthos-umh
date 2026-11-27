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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Set is the result of Parse: it reports which of the plugin's flags the
// customer enabled. Its zero value is valid and reports every flag as
// disabled, which is always the plugin's previous behavior.
type Set struct {
	flags   *Flags
	enabled map[string]bool
}

// Enabled reports whether the customer enabled f. It panics if f is nil or
// belongs to another Flags, because reporting false would silently keep the
// legacy behavior.
func (s Set) Enabled(f *Flag) bool {
	if f == nil {
		panic("Set.Enabled called with a nil *Flag; pass a flag returned by Declare")
	}
	if s.flags == nil {
		return false
	}
	if f.flags != s.flags {
		panic(fmt.Sprintf("Set.Enabled called with the flag %q, which was not declared on the Flags this Set was parsed from", f.name))
	}
	return s.enabled[f.name]
}

// Parse reads the compatibility block from a plugin's parsed config. Unknown
// keys are ignored and logged at Info. It returns an error for a recognized
// key that is not a boolean, for a key that matches a flag except for case,
// and for the YAML merge key "<<", which Benthos does not expand here. log may
// be nil.
func (f *Flags) Parse(conf *service.ParsedConfig, log *service.Logger) (Set, error) {
	if conf == nil {
		return Set{flags: f}, nil
	}
	m, err := conf.FieldAnyMap("compatibility")
	if err != nil {
		// Not reachable through the config Field, but reached when a plugin
		// forgets to add Field() to its config spec: the AnyMap field type
		// yields an empty map even when the block is absent, and malformed
		// blocks (a list or a scalar) are rejected by the config parser
		// first. In that case this error names the missing field.
		return Set{}, fmt.Errorf("failed to read the compatibility block: %w", err)
	}

	set := Set{flags: f, enabled: make(map[string]bool, len(m))}
	var unknownKeys []string
	for key, entry := range m {
		if key == "<<" {
			return Set{}, fmt.Errorf("the compatibility block contains the YAML merge key \"<<\" (from a line like \"<<: *anchor\"). Benthos does not expand merge keys, so the flags merged in would silently not be applied. Write the compatibility flags out explicitly in the compatibility block instead")
		}
		fl, ok := f.flags[key]
		if !ok {
			if name, near := f.caseInsensitiveMatch(key); near {
				return Set{}, fmt.Errorf("the compatibility key %q is not recognized, but matches the compatibility flag %q except for case. Keys are case-sensitive: write it as %q", key, name, name)
			}
			unknownKeys = append(unknownKeys, key)
			continue
		}
		v, err := entry.FieldAny()
		if err != nil {
			return Set{}, fmt.Errorf("failed to read the compatibility key %q: %w", key, err)
		}
		b, ok := v.(bool)
		if !ok {
			return Set{}, fmt.Errorf("the compatibility key %q must be true or false, got %s", key, describeValue(v))
		}
		if b {
			set.enabled[fl.name] = true
			log.Infof("The compatibility flag %q is enabled: %s", fl.name, fl.description)
		}
	}
	if len(unknownKeys) > 0 {
		// One line for all of them: customer configs carry a handful of
		// retired keys forever, and one line per key would flood the log on
		// every construction.
		sort.Strings(unknownKeys)
		quoted := make([]string, len(unknownKeys))
		for i, k := range unknownKeys {
			quoted[i] = fmt.Sprintf("%q", k)
		}
		log.Infof("Ignoring the unknown compatibility keys %s. They have no effect. This plugin recognizes the following keys: %s.", strings.Join(quoted, ", "), f.recognizedKeys())
	}
	return set, nil
}

// caseInsensitiveMatch returns the declared flag that key matches except for
// case, if any.
func (f *Flags) caseInsensitiveMatch(key string) (string, bool) {
	for _, name := range f.names {
		if strings.EqualFold(key, name) {
			return name, true
		}
	}
	return "", false
}

// recognizedKeys renders the declared flag names for log messages. A Flags
// with no declarations renders as a hint to that effect rather than an empty
// list.
func (f *Flags) recognizedKeys() string {
	if len(f.names) == 0 {
		// An unrecognized key against a Flags with no declarations is
		// already nonsense; still, never emit an empty list.
		return "(none; this plugin has no compatibility flags)"
	}
	return strings.Join(f.names, ", ")
}

// describeValue names a non-boolean config value in error messages, in words
// an operator can read rather than Go's type syntax, and shows the value the
// customer actually wrote.
func describeValue(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case string:
		return fmt.Sprintf("the string %q", t)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("the integer %d", t)
	case float32, float64, json.Number:
		return fmt.Sprintf("the number %v", t)
	case map[string]any:
		return "an object"
	case []any:
		return "a list"
	case time.Time:
		return fmt.Sprintf("the date %s", t.Format("2006-01-02"))
	default:
		return fmt.Sprintf("the value %v", t)
	}
}
