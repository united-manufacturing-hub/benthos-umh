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

package opcua_plugin

import (
	"testing"
)

// TestSanitizeReplacesInvalidCharacters tests that sanitize() correctly replaces
// non-alphanumeric characters (except hyphens and underscores) with underscores.
func TestSanitizeReplacesInvalidCharacters(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "replaces spaces with underscores",
			input:    "hello world",
			expected: "hello_world",
		},
		{
			name:     "replaces dots with underscores",
			input:    "node.name.path",
			expected: "node_name_path",
		},
		{
			name:     "preserves alphanumeric, hyphens, and underscores",
			input:    "Valid_Name-123",
			expected: "Valid_Name-123",
		},
		{
			name:     "replaces special characters",
			input:    "name!@#$%^&*()",
			expected: "name__________",
		},
		{
			name:     "handles mixed valid and invalid",
			input:    "OPC.UA/Server:Node[0]",
			expected: "OPC_UA_Server_Node_0_",
		},
		{
			name:     "empty string returns empty",
			input:    "",
			expected: "",
		},
		{
			name:     "already clean string unchanged",
			input:    "CleanName123-test_value",
			expected: "CleanName123-test_value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitize(tt.input)
			if result != tt.expected {
				t.Errorf("sanitize(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

// BenchmarkSanitize measures the performance of sanitize() and reveals
// that regexp.Compile() is called on every invocation, causing repeated
// allocations.
//
// Expected results (PROVING THE PROBLEM):
//   - High allocs/op (multiple allocations per call)
//   - High B/op (bytes allocated per operation)
//   - Allocations scale linearly with number of calls
//
// This benchmark proves the performance issue described in ENG-3799:
// During browse operations with 100k nodes, sanitize() is called 200k+ times
// (2x per node), each time compiling the same regex pattern from scratch.
func BenchmarkSanitize(b *testing.B) {
	testCases := []struct {
		name  string
		input string
	}{
		{"short_clean", "ValidName123"},
		{"short_dirty", "Invalid.Name!"},
		{"medium_mixed", "OPC.UA/Server:Node[0]_Temperature"},
		{"long_path", "Enterprise.Site.Area.Line.Machine.Component.Sensor.Value"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = sanitize(tc.input)
			}
		})
	}
}

// BenchmarkSanitizeMemoryPressure specifically measures memory allocations
// to demonstrate the GC pressure from repeated regex compilation.
func BenchmarkSanitizeMemoryPressure(b *testing.B) {
	const input = "OPC.UA/Server:Node[0]"
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = sanitize(input)
	}
}
