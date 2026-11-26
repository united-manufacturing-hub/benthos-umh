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

package js_runtime_test

import (
	"strings"
	"testing"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/js_runtime"
)

// TestCompile_AUsersFirstLineIsLineFour pins the one thing about the wrap a
// customer can see.
//
// The three plugins this package replaced each wrapped a script so that the
// user's first line landed on line 4, and that number is what appears in
// every error they get, in their logs, and in the drop reasons. Shortening
// the prefix moves every line number by two, silently: nothing fails, the
// scripts still run, and the next person to debug a two-hundred-line
// function is looking at the wrong line.
//
// Columns are deliberately NOT pinned. The old wrap indented the source with
// tabs and goja counted them, so a column here reads three lower. That
// pointed at whitespace, not at anything in the script.
func TestCompile_AUsersFirstLineIsLineFour(t *testing.T) {
	// A script whose very first line throws. Nothing before it.
	const source = "throw new Error('boom');"

	rt := js_runtime.NewRunner(js_runtime.Options{})
	defer rt.Close()

	prog, err := js_runtime.Compile("probe.js", source)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, runErr := rt.Run(t.Context(), prog, nil)
	if runErr == nil {
		t.Fatal("the script was supposed to throw")
	}
	if !strings.Contains(runErr.Error(), "probe.js:4:") {
		t.Errorf("a user's first line should report as line 4, got %v", runErr)
	}
}
