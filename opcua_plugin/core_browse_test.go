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
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// TestSanitizeReplacesInvalidCharacters tests that sanitize() correctly replaces
// non-alphanumeric characters (except hyphens and underscores) with underscores.
var _ = Describe("sanitize", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

	DescribeTable("replaces invalid characters",
		func(input, expected string) {
			result := sanitize(input)
			Expect(result).To(Equal(expected))
		},
		Entry("replaces spaces with underscores", "hello world", "hello_world"),
		Entry("replaces dots with underscores", "node.name.path", "node_name_path"),
		Entry("preserves alphanumeric, hyphens, and underscores", "Valid_Name-123", "Valid_Name-123"),
		Entry("replaces special characters", "name!@#$%^&*()", "name__________"),
		Entry("handles mixed valid and invalid", "OPC.UA/Server:Node[0]", "OPC_UA_Server_Node_0_"),
		Entry("empty string returns empty", "", ""),
		Entry("already clean string unchanged", "CleanName123-test_value", "CleanName123-test_value"),
	)
})

// BenchmarkSanitize measures the performance of sanitize() after moving
// regex compilation to package-level (sanitizeRegex), eliminating per-call
// compilation overhead.
//
// Expected results (AFTER FIX):
//   - Low allocs/op (~3-6 allocations per call, down from 15-18)
//   - Low B/op (~48-194 bytes per operation, down from 945-1088)
//   - Significant reduction in memory allocations compared to previous
//     implementation that compiled regex on every call
//
// This benchmark validates the optimization implemented in ENG-3799:
// During browse operations with 100k nodes, sanitize() is called 200k+ times
// (2x per node). Pre-compiling the regex saves ~2.4M allocations and ~171 MiB
// for 200k calls.
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
// to validate reduced GC pressure after precompiling the regex at package level.
func BenchmarkSanitizeMemoryPressure(b *testing.B) {
	const input = "OPC.UA/Server:Node[0]"
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = sanitize(input)
	}
}
