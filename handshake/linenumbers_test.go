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

// TestCompile_AUsersFirstLineIsLineOne pins the number a customer sees when
// their handshake script goes wrong.
//
// A script that throws reaches the flow as "handshake ended persistent:
// Error: boom at script:1:...", so this number is in the dead-letter reason
// and in the operator's logs, and it is how someone finds the fault in a
// long script. The wrap around the script must not move it.
func TestCompile_AUsersFirstLineIsLineOne(t *testing.T) {
	// A one-line script. The throw is on the user's line 1.
	prog, _, err := compile("script", "throw new Error('boom');")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, runErr := runProgram(t.Context(), prog, nil)
	if runErr == nil {
		t.Fatal("the script was supposed to throw")
	}
	if !strings.Contains(runErr.Error(), "script:1:") {
		t.Errorf("a user's first line should report as line 1, got %v", runErr)
	}

	// A later line reports as itself too.
	prog, _, err = compile("script", "const a = 1;\nconst b = 2;\nthrow new Error('boom');")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if _, runErr := runProgram(t.Context(), prog, nil); runErr == nil ||
		!strings.Contains(runErr.Error(), "script:3:") {
		t.Errorf("a user's third line should report as line 3, got %v", runErr)
	}

	// Syntax errors count lines too, and from the same origin.
	if _, _, err := compile("script", "this is not javascript;;;"); err == nil {
		t.Fatal("that was supposed to be a syntax error")
	} else if !strings.Contains(err.Error(), "Line 1:") {
		t.Errorf("a syntax error on the user's first line should report line 1, got %v", err)
	}

	// A trailing line comment does not swallow the wrap's closing half.
	if _, _, err := compile("script", "return 1; // the end"); err != nil {
		t.Errorf("a script ending in a line comment must compile, got %v", err)
	}
}
