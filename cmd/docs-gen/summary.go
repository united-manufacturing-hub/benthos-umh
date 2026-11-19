// Copyright 2025 UMH Systems GmbH
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

package main

import (
	"fmt"
	"strings"
)

// updateSummary writes the navigation entries of one section into
// docs/SUMMARY.md. The generator owns every entry that links into the
// section's generated directory: it removes them and inserts the current
// ones just before the section's link to the upstream catalog.
func updateSummary(summary string, s section, entries []entry) (string, error) {
	lines := strings.Split(summary, "\n")
	owned := "](" + s.pageDir() + "/"
	anchor := "/components/" + s.TypeDir + "/about/"

	kept := make([]string, 0, len(lines))
	for _, l := range lines {
		if !strings.Contains(l, owned) {
			kept = append(kept, l)
		}
	}

	at := -1
	for i, l := range kept {
		if strings.Contains(l, anchor) {
			at = i
			break
		}
	}
	if at < 0 {
		return "", fmt.Errorf("SUMMARY.md has no entry linking to %s, so the %s cannot be placed", anchor, s.Plural())
	}
	indent := kept[at][:len(kept[at])-len(strings.TrimLeft(kept[at], " "))]

	block := []string{fmt.Sprintf("%s- [Redpanda Connect %s](%s/README.md)", indent, s.Plural(), s.pageDir())}
	for _, e := range entries {
		block = append(block, fmt.Sprintf("%s  - [%s](%s/%s.md)", indent, e.Title, s.pageDir(), e.Name))
	}

	out := append([]string{}, kept[:at]...)
	out = append(out, block...)
	out = append(out, kept[at:]...)
	return strings.Join(out, "\n"), nil
}
