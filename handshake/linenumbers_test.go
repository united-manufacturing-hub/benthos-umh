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

package handshake

import (
	"strings"
	"testing"
)

// TestCompile_AUsersFirstLineIsLineFour pins the number a customer sees when
// their handshake script goes wrong.
//
// A script that throws reaches the flow as "handshake ended persistent:
// Error: boom at script:4:7" -- so this number is in the dead-letter reason
// and in the operator's logs, and it is how someone finds the fault in a
// long script.
//
// It matters most for the change that has not happened yet. This package
// runs on a stand-in runtime that is meant to be swapped for pkg/js_runtime.
// If the two wraps disagree on length, that swap moves every line number a
// customer has ever seen, silently, with nothing failing. They agree now,
// and this test and its twin in pkg/js_runtime both go red if either drifts.
func TestCompile_AUsersFirstLineIsLineFour(t *testing.T) {
	rt := newRuntime()
	defer func() { _ = rt.close() }()

	// A one-line script. The throw is on the user's line 1.
	prog, err := rt.compile("script", "throw new Error('boom');")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, runErr := rt.run(t.Context(), prog, nil)
	if runErr == nil {
		t.Fatal("the script was supposed to throw")
	}
	if !strings.Contains(runErr.Error(), "script:4:") {
		t.Errorf("a user's first line should report as line 4, got %v", runErr)
	}

	// Syntax errors count lines too, and from the same origin.
	if _, err := rt.compile("script", "this is not javascript;;;"); err == nil {
		t.Fatal("that was supposed to be a syntax error")
	} else if !strings.Contains(err.Error(), "Line 4:") {
		t.Errorf("a syntax error on the user's first line should report line 4, got %v", err)
	}
}
