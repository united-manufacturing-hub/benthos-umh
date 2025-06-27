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

package topic

import (
	"strings"
	"testing"
)

// Test suite for UnsTopic validation and parsing

func TestNewUnsTopic_ValidTopics(t *testing.T) {
	testCases := []struct {
		name           string
		topic          string
		expectedLevel0 string
		expectedLevels []string
		expectedDC     string
		expectedVP     *string
		expectedName   string
	}{
		{
			name:           "minimum valid topic",
			topic:          "umh.v1.enterprise._historian.temperature",
			expectedLevel0: "enterprise",
			expectedLevels: []string{},
			expectedDC:     "_historian",
			expectedVP:     nil,
			expectedName:   "temperature",
		},
		{
			name:           "topic with location sublevels",
			topic:          "umh.v1.acme.berlin.assembly._historian.temperature",
			expectedLevel0: "acme",
			expectedLevels: []string{"berlin", "assembly"},
			expectedDC:     "_historian",
			expectedVP:     nil,
			expectedName:   "temperature",
		},
		{
			name:           "topic with virtual path",
			topic:          "umh.v1.factory._raw.motor.diagnostics.temperature",
			expectedLevel0: "factory",
			expectedLevels: []string{},
			expectedDC:     "_raw",
			expectedVP:     strPtr("motor.diagnostics"),
			expectedName:   "temperature",
		},
		{
			name:           "complex topic with all components",
			topic:          "umh.v1.enterprise.site.area.line._historian.axis.x.position",
			expectedLevel0: "enterprise",
			expectedLevels: []string{"site", "area", "line"},
			expectedDC:     "_historian",
			expectedVP:     strPtr("axis.x"),
			expectedName:   "position",
		},
		{
			name:           "topic with name starting with underscore",
			topic:          "umh.v1.enterprise._analytics._internal_state",
			expectedLevel0: "enterprise",
			expectedLevels: []string{},
			expectedDC:     "_analytics",
			expectedVP:     nil,
			expectedName:   "_internal_state",
		},
		{
			name:           "topic with virtual path starting with underscore",
			topic:          "umh.v1.factory._raw._internal.debug.flag",
			expectedLevel0: "factory",
			expectedLevels: []string{},
			expectedDC:     "_raw",
			expectedVP:     strPtr("_internal.debug"),
			expectedName:   "flag",
		},
		{
			name:           "deep location hierarchy",
			topic:          "umh.v1.enterprise.region.country.state.city.plant.area.line.station._historian.temperature",
			expectedLevel0: "enterprise",
			expectedLevels: []string{"region", "country", "state", "city", "plant", "area", "line", "station"},
			expectedDC:     "_historian",
			expectedVP:     nil,
			expectedName:   "temperature",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			topic, err := NewUnsTopic(tc.topic)
			if err != nil {
				t.Fatalf("Expected topic to be valid, got error: %v", err)
			}

			if topic == nil {
				t.Fatal("Expected topic to be non-nil")
			}

			// Test String() method
			if topic.String() != tc.topic {
				t.Errorf("Expected String() = %q, got %q", tc.topic, topic.String())
			}

			// Test AsKafkaKey() method
			if topic.AsKafkaKey() != tc.topic {
				t.Errorf("Expected AsKafkaKey() = %q, got %q", tc.topic, topic.AsKafkaKey())
			}

			// Test Info() method
			info := topic.Info()
			if info == nil {
				t.Fatal("Expected Info() to be non-nil")
			}

			// Validate parsed components
			if info.Level0 != tc.expectedLevel0 {
				t.Errorf("Expected Level0 = %q, got %q", tc.expectedLevel0, info.Level0)
			}

			if !sliceEqual(info.LocationSublevels, tc.expectedLevels) {
				t.Errorf("Expected LocationSublevels = %v, got %v", tc.expectedLevels, info.LocationSublevels)
			}

			if info.DataContract != tc.expectedDC {
				t.Errorf("Expected DataContract = %q, got %q", tc.expectedDC, info.DataContract)
			}

			if !equalStringPtr(info.VirtualPath, tc.expectedVP) {
				t.Errorf("Expected VirtualPath = %v, got %v", ptrStr(tc.expectedVP), ptrStr(info.VirtualPath))
			}

			if info.Name != tc.expectedName {
				t.Errorf("Expected Name = %q, got %q", tc.expectedName, info.Name)
			}

			// Test LocationPath() method
			expectedPath := tc.expectedLevel0
			if len(tc.expectedLevels) > 0 {
				expectedPath += "." + strings.Join(tc.expectedLevels, ".")
			}
			if info.LocationPath() != expectedPath {
				t.Errorf("Expected LocationPath() = %q, got %q", expectedPath, info.LocationPath())
			}

			// Test TotalLocationLevels() method
			expectedTotal := 1 + len(tc.expectedLevels)
			if info.TotalLocationLevels() != expectedTotal {
				t.Errorf("Expected TotalLocationLevels() = %d, got %d", expectedTotal, info.TotalLocationLevels())
			}
		})
	}
}

func TestNewUnsTopic_InvalidTopics(t *testing.T) {
	testCases := []struct {
		name          string
		topic         string
		expectedError string
	}{
		{
			name:          "empty topic",
			topic:         "",
			expectedError: "topic cannot be empty",
		},
		{
			name:          "wrong prefix",
			topic:         "wrong.v1.enterprise._historian.temperature",
			expectedError: "topic must start with umh.v1",
		},
		{
			name:          "no prefix",
			topic:         "enterprise._historian.temperature",
			expectedError: "topic must start with umh.v1",
		},
		{
			name:          "too few parts",
			topic:         "umh.v1.enterprise._historian",
			expectedError: "topic must have at least: umh.v1.level0._contract.name",
		},
		{
			name:          "empty level0",
			topic:         "umh.v1.._historian.temperature",
			expectedError: "level0 (enterprise) cannot be empty",
		},
		{
			name:          "level0 starts with underscore",
			topic:         "umh.v1._enterprise._historian.temperature",
			expectedError: "level0 (enterprise) cannot start with underscore",
		},
		{
			name:          "no data contract",
			topic:         "umh.v1.enterprise.historian.temperature",
			expectedError: "topic must contain a data contract (segment starting with '_') and it cannot be the final segment",
		},
		{
			name:          "data contract just underscore",
			topic:         "umh.v1.enterprise._.temperature",
			expectedError: "data contract cannot be just an underscore",
		},
		{
			name:          "data contract at end",
			topic:         "umh.v1.enterprise.temperature._historian",
			expectedError: "topic must contain a data contract (segment starting with '_') and it cannot be the final segment",
		},
		{
			name:          "empty location sublevel",
			topic:         "umh.v1.enterprise..site._historian.temperature",
			expectedError: "location sublevel at index 0 cannot be empty",
		},

		{
			name:          "empty virtual path segment",
			topic:         "umh.v1.enterprise._historian..debug.temperature",
			expectedError: "virtual path segment at index 0 cannot be empty",
		},
		{
			name:          "empty name",
			topic:         "umh.v1.enterprise._historian.",
			expectedError: "topic name (final segment) cannot be empty",
		},
		{
			name:          "invalid characters",
			topic:         "umh.v1.enterprise._historian.temp@rature",
			expectedError: "topic contained invalid characters",
		},
		{
			name:          "consecutive dots",
			topic:         "umh.v1.enterprise.._historian.temperature",
			expectedError: "location sublevel at index 0 cannot be empty",
		},
		{
			name:          "leading dot",
			topic:         ".umh.v1.enterprise._historian.temperature",
			expectedError: "topic must start with umh.v1",
		},
		{
			name:          "trailing dot",
			topic:         "umh.v1.enterprise._historian.temperature.",
			expectedError: "topic name (final segment) cannot be empty",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			topic, err := NewUnsTopic(tc.topic)
			if err == nil {
				t.Fatalf("Expected topic to be invalid, but got valid topic: %v", topic)
			}

			if !strings.Contains(err.Error(), tc.expectedError) {
				t.Errorf("Expected error to contain %q, got %q", tc.expectedError, err.Error())
			}

			if topic != nil {
				t.Error("Expected topic to be nil when invalid")
			}
		})
	}
}

func TestTopicInfo_LocationPath(t *testing.T) {
	testCases := []struct {
		name         string
		info         *TopicInfo
		expectedPath string
	}{
		{
			name:         "nil info",
			info:         nil,
			expectedPath: "",
		},
		{
			name: "only level0",
			info: &TopicInfo{
				Level0:            "enterprise",
				LocationSublevels: []string{},
			},
			expectedPath: "enterprise",
		},
		{
			name: "level0 with sublevels",
			info: &TopicInfo{
				Level0:            "enterprise",
				LocationSublevels: []string{"site", "area", "line"},
			},
			expectedPath: "enterprise.site.area.line",
		},
		{
			name: "whitespace trimming",
			info: &TopicInfo{
				Level0:            " enterprise ",
				LocationSublevels: []string{" site ", " area "},
			},
			expectedPath: "enterprise.site.area",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.info.LocationPath()
			if result != tc.expectedPath {
				t.Errorf("Expected LocationPath() = %q, got %q", tc.expectedPath, result)
			}
		})
	}
}

func TestTopicInfo_TotalLocationLevels(t *testing.T) {
	testCases := []struct {
		name          string
		info          *TopicInfo
		expectedTotal int
	}{
		{
			name:          "nil info",
			info:          nil,
			expectedTotal: 0,
		},
		{
			name: "only level0",
			info: &TopicInfo{
				Level0:            "enterprise",
				LocationSublevels: []string{},
			},
			expectedTotal: 1,
		},
		{
			name: "level0 with sublevels",
			info: &TopicInfo{
				Level0:            "enterprise",
				LocationSublevels: []string{"site", "area", "line"},
			},
			expectedTotal: 4,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.info.TotalLocationLevels()
			if result != tc.expectedTotal {
				t.Errorf("Expected TotalLocationLevels() = %d, got %d", tc.expectedTotal, result)
			}
		})
	}
}

// Benchmark tests for performance validation

func BenchmarkNewUnsTopic_Simple(b *testing.B) {
	topic := "umh.v1.enterprise._historian.temperature"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewUnsTopic(topic)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewUnsTopic_Complex(b *testing.B) {
	topic := "umh.v1.enterprise.site.area.line.station._historian.motor.axis.x.position"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewUnsTopic(topic)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnsTopic_String(b *testing.B) {
	topic, err := NewUnsTopic("umh.v1.enterprise.site._historian.motor.temperature")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = topic.String()
	}
}

func BenchmarkUnsTopic_Info(b *testing.B) {
	topic, err := NewUnsTopic("umh.v1.enterprise.site._historian.motor.temperature")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = topic.Info()
	}
}

func BenchmarkTopicInfo_LocationPath(b *testing.B) {
	topic, err := NewUnsTopic("umh.v1.enterprise.site.area.line._historian.temperature")
	if err != nil {
		b.Fatal(err)
	}
	info := topic.Info()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = info.LocationPath()
	}
}

// Memory allocation benchmarks

func BenchmarkNewUnsTopic_Allocs(b *testing.B) {
	topic := "umh.v1.enterprise.site.area._historian.motor.temperature"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewUnsTopic(topic)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Test concurrent usage for thread safety

func TestUnsTopic_ConcurrentUsage(t *testing.T) {
	topic, err := NewUnsTopic("umh.v1.enterprise._historian.temperature")
	if err != nil {
		t.Fatal(err)
	}

	const numGoroutines = 100
	const numOperations = 1000

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer func() { done <- true }()
			for j := 0; j < numOperations; j++ {
				// Test all read operations concurrently
				_ = topic.String()
				_ = topic.AsKafkaKey()
				info := topic.Info()
				_ = info.LocationPath()
				_ = info.TotalLocationLevels()
			}
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

// Test edge cases and error conditions

func TestNewUnsTopic_EdgeCases(t *testing.T) {
	testCases := []struct {
		name          string
		topic         string
		shouldBeValid bool
	}{
		{
			name:          "topic with numbers",
			topic:         "umh.v1.factory123.line4._historian.sensor001.temperature",
			shouldBeValid: true,
		},
		{
			name:          "topic with hyphens",
			topic:         "umh.v1.us-east-1.plant-a._historian.motor-1.temperature",
			shouldBeValid: true,
		},
		{
			name:          "topic with mixed valid characters",
			topic:         "umh.v1.enterprise._historian.temp-sensor_001.value",
			shouldBeValid: true,
		},
		{
			name:          "topic with unicode",
			topic:         "umh.v1.enterprise._historian.temperaturé",
			shouldBeValid: false,
		},
		{
			name:          "topic with spaces",
			topic:         "umh.v1.enterprise._historian.temperature value",
			shouldBeValid: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			topic, err := NewUnsTopic(tc.topic)
			if tc.shouldBeValid && err != nil {
				t.Errorf("Expected topic to be valid, got error: %v", err)
			}
			if !tc.shouldBeValid && err == nil {
				t.Errorf("Expected topic to be invalid, but got valid topic: %v", topic)
			}
		})
	}
}

// Helper functions

func strPtr(s string) *string {
	return &s
}

func ptrStr(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStringPtr(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
