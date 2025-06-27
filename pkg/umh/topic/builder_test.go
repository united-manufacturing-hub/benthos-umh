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

// Test suite for Builder

func TestNewBuilder(t *testing.T) {
	builder := NewBuilder()
	if builder == nil {
		t.Fatal("Expected NewBuilder() to return non-nil builder")
	}

	// Test that builder starts with empty fields
	if builder.level0 != "" {
		t.Errorf("Expected empty level0, got %q", builder.level0)
	}
	if len(builder.locationSublevels) != 0 {
		t.Errorf("Expected empty locationSublevels, got %v", builder.locationSublevels)
	}
	if builder.dataContract != "" {
		t.Errorf("Expected empty dataContract, got %q", builder.dataContract)
	}
	if builder.virtualPath != nil {
		t.Errorf("Expected nil virtualPath, got %v", builder.virtualPath)
	}
	if builder.name != "" {
		t.Errorf("Expected empty name, got %q", builder.name)
	}
}

func TestBuilder_SetLevel0(t *testing.T) {
	builder := NewBuilder()
	result := builder.SetLevel0("enterprise")

	// Test chaining
	if result != builder {
		t.Error("Expected SetLevel0 to return the same builder instance")
	}

	// Test value was set
	if builder.level0 != "enterprise" {
		t.Errorf("Expected level0 = 'enterprise', got %q", builder.level0)
	}
}

func TestBuilder_SetLocationPath(t *testing.T) {
	testCases := []struct {
		name                      string
		locationPath              string
		expectedLevel0            string
		expectedLocationSublevels []string
	}{
		{
			name:                      "empty path",
			locationPath:              "",
			expectedLevel0:            "",
			expectedLocationSublevels: []string{},
		},
		{
			name:                      "single level",
			locationPath:              "enterprise",
			expectedLevel0:            "enterprise",
			expectedLocationSublevels: []string{},
		},
		{
			name:                      "two levels",
			locationPath:              "enterprise.site",
			expectedLevel0:            "enterprise",
			expectedLocationSublevels: []string{"site"},
		},
		{
			name:                      "multiple levels",
			locationPath:              "enterprise.site.area.line",
			expectedLevel0:            "enterprise",
			expectedLocationSublevels: []string{"site", "area", "line"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewBuilder()
			result := builder.SetLocationPath(tc.locationPath)

			// Test chaining
			if result != builder {
				t.Error("Expected SetLocationPath to return the same builder instance")
			}

			// Test level0
			if builder.level0 != tc.expectedLevel0 {
				t.Errorf("Expected level0 = %q, got %q", tc.expectedLevel0, builder.level0)
			}

			// Test location sublevels
			if !sliceEqual(builder.locationSublevels, tc.expectedLocationSublevels) {
				t.Errorf("Expected locationSublevels = %v, got %v", tc.expectedLocationSublevels, builder.locationSublevels)
			}
		})
	}
}

func TestBuilder_SetLocationLevels(t *testing.T) {
	testCases := []struct {
		name                      string
		level0                    string
		additionalLevels          []string
		expectedLevel0            string
		expectedLocationSublevels []string
	}{
		{
			name:                      "only level0",
			level0:                    "enterprise",
			additionalLevels:          nil,
			expectedLevel0:            "enterprise",
			expectedLocationSublevels: []string{},
		},
		{
			name:                      "level0 with one additional",
			level0:                    "enterprise",
			additionalLevels:          []string{"site"},
			expectedLevel0:            "enterprise",
			expectedLocationSublevels: []string{"site"},
		},
		{
			name:                      "level0 with multiple additional",
			level0:                    "factory",
			additionalLevels:          []string{"area", "line", "station"},
			expectedLevel0:            "factory",
			expectedLocationSublevels: []string{"area", "line", "station"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewBuilder()
			result := builder.SetLocationLevels(tc.level0, tc.additionalLevels...)

			// Test chaining
			if result != builder {
				t.Error("Expected SetLocationLevels to return the same builder instance")
			}

			// Test level0
			if builder.level0 != tc.expectedLevel0 {
				t.Errorf("Expected level0 = %q, got %q", tc.expectedLevel0, builder.level0)
			}

			// Test location sublevels
			if !sliceEqual(builder.locationSublevels, tc.expectedLocationSublevels) {
				t.Errorf("Expected locationSublevels = %v, got %v", tc.expectedLocationSublevels, builder.locationSublevels)
			}
		})
	}
}

func TestBuilder_AddLocationLevel(t *testing.T) {
	builder := NewBuilder()
	builder.SetLevel0("enterprise")

	// Add first level
	result := builder.AddLocationLevel("site")
	if result != builder {
		t.Error("Expected AddLocationLevel to return the same builder instance")
	}

	expected := []string{"site"}
	if !sliceEqual(builder.locationSublevels, expected) {
		t.Errorf("Expected locationSublevels = %v, got %v", expected, builder.locationSublevels)
	}

	// Add second level
	builder.AddLocationLevel("area")
	expected = []string{"site", "area"}
	if !sliceEqual(builder.locationSublevels, expected) {
		t.Errorf("Expected locationSublevels = %v, got %v", expected, builder.locationSublevels)
	}
}

func TestBuilder_SetDataContract(t *testing.T) {
	builder := NewBuilder()
	result := builder.SetDataContract("_historian")

	// Test chaining
	if result != builder {
		t.Error("Expected SetDataContract to return the same builder instance")
	}

	// Test value was set
	if builder.dataContract != "_historian" {
		t.Errorf("Expected dataContract = '_historian', got %q", builder.dataContract)
	}
}

func TestBuilder_SetVirtualPath(t *testing.T) {
	testCases := []struct {
		name                string
		virtualPath         string
		expectedVirtualPath *string
	}{
		{
			name:                "empty virtual path",
			virtualPath:         "",
			expectedVirtualPath: nil,
		},
		{
			name:                "simple virtual path",
			virtualPath:         "motor",
			expectedVirtualPath: strPtr("motor"),
		},
		{
			name:                "complex virtual path",
			virtualPath:         "motor.diagnostics.vibration",
			expectedVirtualPath: strPtr("motor.diagnostics.vibration"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewBuilder()
			result := builder.SetVirtualPath(tc.virtualPath)

			// Test chaining
			if result != builder {
				t.Error("Expected SetVirtualPath to return the same builder instance")
			}

			// Test value was set
			if !equalStringPtr(builder.virtualPath, tc.expectedVirtualPath) {
				t.Errorf("Expected virtualPath = %v, got %v", ptrStr(tc.expectedVirtualPath), ptrStr(builder.virtualPath))
			}
		})
	}
}

func TestBuilder_SetName(t *testing.T) {
	builder := NewBuilder()
	result := builder.SetName("temperature")

	// Test chaining
	if result != builder {
		t.Error("Expected SetName to return the same builder instance")
	}

	// Test value was set
	if builder.name != "temperature" {
		t.Errorf("Expected name = 'temperature', got %q", builder.name)
	}
}

func TestBuilder_GetLocationPath(t *testing.T) {
	testCases := []struct {
		name         string
		setup        func(*Builder)
		expectedPath string
	}{
		{
			name:         "empty builder",
			setup:        func(b *Builder) {},
			expectedPath: "",
		},
		{
			name: "only level0",
			setup: func(b *Builder) {
				b.SetLevel0("enterprise")
			},
			expectedPath: "enterprise",
		},
		{
			name: "level0 with sublevels",
			setup: func(b *Builder) {
				b.SetLocationLevels("enterprise", "site", "area")
			},
			expectedPath: "enterprise.site.area",
		},
		{
			name: "using SetLocationPath",
			setup: func(b *Builder) {
				b.SetLocationPath("factory.line.station")
			},
			expectedPath: "factory.line.station",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewBuilder()
			tc.setup(builder)

			result := builder.GetLocationPath()
			if result != tc.expectedPath {
				t.Errorf("Expected GetLocationPath() = %q, got %q", tc.expectedPath, result)
			}
		})
	}
}

func TestBuilder_Reset(t *testing.T) {
	builder := NewBuilder()

	// Set up builder with values
	builder.SetLocationLevels("enterprise", "site", "area")
	builder.SetDataContract("_historian")
	builder.SetVirtualPath("motor.diagnostics")
	builder.SetName("temperature")

	// Test that fields are set
	if builder.level0 == "" || len(builder.locationSublevels) == 0 ||
		builder.dataContract == "" || builder.virtualPath == nil || builder.name == "" {
		t.Fatal("Builder fields should be set before reset")
	}

	// Reset and test chaining
	result := builder.Reset()
	if result != builder {
		t.Error("Expected Reset to return the same builder instance")
	}

	// Test that all fields are cleared
	if builder.level0 != "" {
		t.Errorf("Expected level0 to be empty after reset, got %q", builder.level0)
	}
	if len(builder.locationSublevels) != 0 {
		t.Errorf("Expected locationSublevels to be empty after reset, got %v", builder.locationSublevels)
	}
	if builder.dataContract != "" {
		t.Errorf("Expected dataContract to be empty after reset, got %q", builder.dataContract)
	}
	if builder.virtualPath != nil {
		t.Errorf("Expected virtualPath to be nil after reset, got %v", builder.virtualPath)
	}
	if builder.name != "" {
		t.Errorf("Expected name to be empty after reset, got %q", builder.name)
	}
}

func TestBuilder_Build_ValidTopics(t *testing.T) {
	testCases := []struct {
		name          string
		setup         func(*Builder)
		expectedTopic string
	}{
		{
			name: "minimum valid topic",
			setup: func(b *Builder) {
				b.SetLevel0("enterprise").
					SetDataContract("_historian").
					SetName("temperature")
			},
			expectedTopic: "umh.v1.enterprise._historian.temperature",
		},
		{
			name: "topic with location sublevels",
			setup: func(b *Builder) {
				b.SetLocationLevels("enterprise", "site", "area").
					SetDataContract("_historian").
					SetName("temperature")
			},
			expectedTopic: "umh.v1.enterprise.site.area._historian.temperature",
		},
		{
			name: "topic with virtual path",
			setup: func(b *Builder) {
				b.SetLevel0("factory").
					SetDataContract("_raw").
					SetVirtualPath("motor.diagnostics").
					SetName("temperature")
			},
			expectedTopic: "umh.v1.factory._raw.motor.diagnostics.temperature",
		},
		{
			name: "complex topic",
			setup: func(b *Builder) {
				b.SetLocationPath("enterprise.site.area.line").
					SetDataContract("_historian").
					SetVirtualPath("axis.x").
					SetName("position")
			},
			expectedTopic: "umh.v1.enterprise.site.area.line._historian.axis.x.position",
		},
		{
			name: "topic with underscore name",
			setup: func(b *Builder) {
				b.SetLevel0("enterprise").
					SetDataContract("_analytics").
					SetName("_internal_state")
			},
			expectedTopic: "umh.v1.enterprise._analytics._internal_state",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewBuilder()
			tc.setup(builder)

			topic, err := builder.Build()
			if err != nil {
				t.Fatalf("Expected Build() to succeed, got error: %v", err)
			}

			if topic == nil {
				t.Fatal("Expected topic to be non-nil")
			}

			if topic.String() != tc.expectedTopic {
				t.Errorf("Expected topic = %q, got %q", tc.expectedTopic, topic.String())
			}
		})
	}
}

func TestBuilder_Build_InvalidTopics(t *testing.T) {
	testCases := []struct {
		name          string
		setup         func(*Builder)
		expectedError string
	}{
		{
			name: "missing level0",
			setup: func(b *Builder) {
				b.SetDataContract("_historian").SetName("temperature")
			},
			expectedError: "level0 (enterprise) is required",
		},
		{
			name: "missing data contract",
			setup: func(b *Builder) {
				b.SetLevel0("enterprise").SetName("temperature")
			},
			expectedError: "data contract is required",
		},
		{
			name: "missing name",
			setup: func(b *Builder) {
				b.SetLevel0("enterprise").SetDataContract("_historian")
			},
			expectedError: "name is required",
		},
		{
			name: "invalid level0",
			setup: func(b *Builder) {
				b.SetLevel0("_enterprise").SetDataContract("_historian").SetName("temperature")
			},
			expectedError: "level0 (enterprise) cannot start with underscore",
		},
		{
			name: "invalid data contract",
			setup: func(b *Builder) {
				b.SetLevel0("enterprise").SetDataContract("historian").SetName("temperature")
			},
			expectedError: "topic must contain a data contract (segment starting with '_') and it cannot be the final segment",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewBuilder()
			tc.setup(builder)

			topic, err := builder.Build()
			if err == nil {
				t.Fatalf("Expected Build() to fail, but got valid topic: %v", topic)
			}

			if !strings.Contains(err.Error(), tc.expectedError) {
				t.Errorf("Expected error to contain %q, got %q", tc.expectedError, err.Error())
			}

			if topic != nil {
				t.Error("Expected topic to be nil when build fails")
			}
		})
	}
}

func TestBuilder_BuildString(t *testing.T) {
	builder := NewBuilder()
	builder.SetLevel0("enterprise").
		SetDataContract("_historian").
		SetName("temperature")

	topicStr, err := builder.BuildString()
	if err != nil {
		t.Fatalf("Expected BuildString() to succeed, got error: %v", err)
	}

	expectedTopic := "umh.v1.enterprise._historian.temperature"
	if topicStr != expectedTopic {
		t.Errorf("Expected BuildString() = %q, got %q", expectedTopic, topicStr)
	}
}

func TestBuilder_FluentInterface(t *testing.T) {
	// Test that all methods support fluent chaining
	topic, err := NewBuilder().
		SetLevel0("enterprise").
		AddLocationLevel("site").
		AddLocationLevel("area").
		SetDataContract("_historian").
		SetVirtualPath("motor.diagnostics").
		SetName("temperature").
		Build()

	if err != nil {
		t.Fatalf("Expected fluent interface to work, got error: %v", err)
	}

	expectedTopic := "umh.v1.enterprise.site.area._historian.motor.diagnostics.temperature"
	if topic.String() != expectedTopic {
		t.Errorf("Expected topic = %q, got %q", expectedTopic, topic.String())
	}
}

func TestBuilder_ReusePattern(t *testing.T) {
	builder := NewBuilder()

	// Build first topic
	topic1, err := builder.
		SetLocationPath("enterprise.site1").
		SetDataContract("_historian").
		SetName("temperature").
		Build()
	if err != nil {
		t.Fatalf("Expected first build to succeed, got error: %v", err)
	}

	// Reset and build second topic
	topic2, err := builder.Reset().
		SetLocationPath("enterprise.site2").
		SetDataContract("_historian").
		SetName("pressure").
		Build()
	if err != nil {
		t.Fatalf("Expected second build to succeed, got error: %v", err)
	}

	// Verify topics are different
	if topic1.String() == topic2.String() {
		t.Error("Expected different topics after reset")
	}

	expected1 := "umh.v1.enterprise.site1._historian.temperature"
	expected2 := "umh.v1.enterprise.site2._historian.pressure"

	if topic1.String() != expected1 {
		t.Errorf("Expected first topic = %q, got %q", expected1, topic1.String())
	}
	if topic2.String() != expected2 {
		t.Errorf("Expected second topic = %q, got %q", expected2, topic2.String())
	}
}

// Benchmark tests for Builder performance

func BenchmarkBuilder_Build_Simple(b *testing.B) {
	builder := NewBuilder()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.Reset().
			SetLevel0("enterprise").
			SetDataContract("_historian").
			SetName("temperature")
		_, err := builder.Build()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuilder_Build_Complex(b *testing.B) {
	builder := NewBuilder()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.Reset().
			SetLocationLevels("enterprise", "site", "area", "line", "station").
			SetDataContract("_historian").
			SetVirtualPath("motor.axis.x").
			SetName("position")
		_, err := builder.Build()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuilder_GetLocationPath(b *testing.B) {
	builder := NewBuilder()
	builder.SetLocationLevels("enterprise", "site", "area", "line")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = builder.GetLocationPath()
	}
}

func BenchmarkBuilder_Reset(b *testing.B) {
	builder := NewBuilder()
	builder.SetLocationLevels("enterprise", "site", "area").
		SetDataContract("_historian").
		SetVirtualPath("motor.diagnostics").
		SetName("temperature")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.Reset()
		// Set up again for next iteration
		builder.SetLocationLevels("enterprise", "site", "area").
			SetDataContract("_historian").
			SetVirtualPath("motor.diagnostics").
			SetName("temperature")
	}
}

func BenchmarkBuilder_SetLocationPath(b *testing.B) {
	builder := NewBuilder()
	locationPath := "enterprise.site.area.line.station"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.SetLocationPath(locationPath)
	}
}

// Memory allocation benchmarks

func BenchmarkBuilder_Build_Allocs(b *testing.B) {
	builder := NewBuilder()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.Reset().
			SetLocationLevels("enterprise", "site", "area").
			SetDataContract("_historian").
			SetVirtualPath("motor.diagnostics").
			SetName("temperature")
		_, err := builder.Build()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Test concurrent usage for thread safety

func TestBuilder_ConcurrentUsage(t *testing.T) {
	const numGoroutines = 100
	const numOperations = 100

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			// Each goroutine uses its own builder instance
			builder := NewBuilder()

			for j := 0; j < numOperations; j++ {
				topic, err := builder.Reset().
					SetLevel0("enterprise").
					AddLocationLevel("site").
					SetDataContract("_historian").
					SetName("temperature").
					Build()

				if err != nil {
					t.Errorf("Goroutine %d operation %d failed: %v", id, j, err)
					return
				}

				expected := "umh.v1.enterprise.site._historian.temperature"
				if topic.String() != expected {
					t.Errorf("Goroutine %d operation %d got unexpected topic: %s", id, j, topic.String())
					return
				}
			}
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}
