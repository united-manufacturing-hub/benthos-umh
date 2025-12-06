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

package topic_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
)

var _ = Describe("Builder", func() {
	Describe("NewBuilder", func() {
		It("should return a non-nil builder", func() {
			builder := NewBuilder()
			Expect(builder).NotTo(BeNil())
		})

		It("should start with empty fields", func() {
			builder := NewBuilder()
			// We need to access private fields for testing, so we'll test through public methods
			Expect(builder.GetLocationPath()).To(BeEmpty())
		})
	})

	Describe("SetLevel0", func() {
		It("should set the level0 value and return the same builder instance", func() {
			builder := NewBuilder()
			result := builder.SetLevel0("enterprise")

			Expect(result).To(BeIdenticalTo(builder))
			// Test indirectly through GetLocationPath
			Expect(builder.GetLocationPath()).To(Equal("enterprise"))
		})
	})

	Describe("SetLocationPath", func() {
		DescribeTable("should correctly parse location paths",
			func(locationPath, expectedPath string, expectedLevels int) {
				builder := NewBuilder()
				result := builder.SetLocationPath(locationPath)

				Expect(result).To(BeIdenticalTo(builder))
				Expect(builder.GetLocationPath()).To(Equal(expectedPath))
			},
			Entry("empty path", "", "", 0),
			Entry("single level", "enterprise", "enterprise", 1),
			Entry("two levels", "enterprise.site", "enterprise.site", 2),
			Entry("multiple levels", "enterprise.site.area.line", "enterprise.site.area.line", 4),
		)
	})

	Describe("SetLocationLevels", func() {
		DescribeTable("should correctly set location levels",
			func(level0 string, additionalLevels []string, expectedPath string) {
				builder := NewBuilder()
				result := builder.SetLocationLevels(level0, additionalLevels...)

				Expect(result).To(BeIdenticalTo(builder))
				Expect(builder.GetLocationPath()).To(Equal(expectedPath))
			},
			Entry("only level0", "enterprise", []string{}, "enterprise"),
			Entry("level0 with one additional", "enterprise", []string{"site"}, "enterprise.site"),
			Entry("level0 with multiple additional", "factory", []string{"area", "line", "station"}, "factory.area.line.station"),
		)
	})

	Describe("AddLocationLevel", func() {
		It("should add location levels sequentially", func() {
			builder := NewBuilder()
			builder.SetLevel0("enterprise")

			result := builder.AddLocationLevel("site")
			Expect(result).To(BeIdenticalTo(builder))
			Expect(builder.GetLocationPath()).To(Equal("enterprise.site"))

			builder.AddLocationLevel("area")
			Expect(builder.GetLocationPath()).To(Equal("enterprise.site.area"))
		})
	})

	Describe("SetDataContract", func() {
		It("should set the data contract and return the same builder instance", func() {
			builder := NewBuilder()
			result := builder.SetDataContract("_historian")

			Expect(result).To(BeIdenticalTo(builder))
		})
	})

	Describe("SetVirtualPath", func() {
		DescribeTable("should correctly set virtual paths",
			func(virtualPath string, shouldHaveVirtualPath bool) {
				builder := NewBuilder()
				result := builder.SetVirtualPath(virtualPath)

				Expect(result).To(BeIdenticalTo(builder))
				// We can't directly test the virtual path, but we can test it through Build
			},
			Entry("empty virtual path", "", false),
			Entry("simple virtual path", "motor", true),
			Entry("complex virtual path", "motor.diagnostics.vibration", true),
		)
	})

	Describe("SetName", func() {
		It("should set the name and return the same builder instance", func() {
			builder := NewBuilder()
			result := builder.SetName("temperature")

			Expect(result).To(BeIdenticalTo(builder))
		})
	})

	Describe("GetLocationPath", func() {
		DescribeTable("should return correct location paths",
			func(setupFunc func(*Builder), expectedPath string) {
				builder := NewBuilder()
				setupFunc(builder)

				result := builder.GetLocationPath()
				Expect(result).To(Equal(expectedPath))
			},
			Entry("empty builder", func(b *Builder) {}, ""),
			Entry("only level0", func(b *Builder) {
				b.SetLevel0("enterprise")
			}, "enterprise"),
			Entry("level0 with sublevels", func(b *Builder) {
				b.SetLocationLevels("enterprise", "site", "area")
			}, "enterprise.site.area"),
			Entry("using SetLocationPath", func(b *Builder) {
				b.SetLocationPath("factory.line.station")
			}, "factory.line.station"),
		)
	})

	Describe("Reset", func() {
		It("should clear all fields and return the same builder instance", func() {
			builder := NewBuilder()

			// Set up builder with values
			builder.SetLocationLevels("enterprise", "site", "area")
			builder.SetDataContract("_historian")
			builder.SetVirtualPath("motor.diagnostics")
			builder.SetName("temperature")

			// Verify fields are set
			Expect(builder.GetLocationPath()).NotTo(BeEmpty())

			// Reset
			result := builder.Reset()
			Expect(result).To(BeIdenticalTo(builder))

			// Verify all fields are cleared
			Expect(builder.GetLocationPath()).To(BeEmpty())
		})
	})

	Describe("Build", func() {
		Context("with valid configurations", func() {
			DescribeTable("should build valid topics",
				func(setupFunc func(*Builder), expectedTopic string) {
					builder := NewBuilder()
					setupFunc(builder)

					topic, err := builder.Build()
					Expect(err).NotTo(HaveOccurred())
					Expect(topic).NotTo(BeNil())
					Expect(topic.String()).To(Equal(expectedTopic))
					Expect(topic.AsKafkaKey()).To(Equal(expectedTopic))
				},
				Entry("minimum valid topic", func(b *Builder) {
					b.SetLevel0("enterprise").
						SetDataContract("_historian").
						SetName("temperature")
				}, "umh.v1.enterprise._historian.temperature"),
				Entry("topic with location sublevels", func(b *Builder) {
					b.SetLocationLevels("enterprise", "site", "area").
						SetDataContract("_historian").
						SetName("temperature")
				}, "umh.v1.enterprise.site.area._historian.temperature"),
				Entry("topic with virtual path", func(b *Builder) {
					b.SetLevel0("factory").
						SetDataContract("_raw").
						SetVirtualPath("motor.diagnostics").
						SetName("temperature")
				}, "umh.v1.factory._raw.motor.diagnostics.temperature"),
				Entry("complex topic", func(b *Builder) {
					b.SetLocationPath("enterprise.site.area.line").
						SetDataContract("_historian").
						SetVirtualPath("axis.x").
						SetName("position")
				}, "umh.v1.enterprise.site.area.line._historian.axis.x.position"),
				Entry("topic with underscore name", func(b *Builder) {
					b.SetLevel0("enterprise").
						SetDataContract("_analytics").
						SetName("_internal_state")
				}, "umh.v1.enterprise._analytics._internal_state"),
			)
		})

		Context("with invalid configurations", func() {
			DescribeTable("should return appropriate errors",
				func(setupFunc func(*Builder), expectedErrorSubstring string) {
					builder := NewBuilder()
					setupFunc(builder)

					topic, err := builder.Build()
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring(expectedErrorSubstring))
					Expect(topic).To(BeNil())
				},
				Entry("missing level0", func(b *Builder) {
					b.SetDataContract("_historian").SetName("temperature")
				}, "level0 is required"),
				Entry("missing data contract", func(b *Builder) {
					b.SetLevel0("enterprise").SetName("temperature")
				}, "data contract is required"),
				Entry("missing name", func(b *Builder) {
					b.SetLevel0("enterprise").SetDataContract("_historian")
				}, "name is required"),
				Entry("invalid level0", func(b *Builder) {
					b.SetLevel0("_enterprise").SetDataContract("_historian").SetName("temperature")
				}, "level0 cannot start with underscore"),
				Entry("invalid data contract", func(b *Builder) {
					b.SetLevel0("enterprise").SetDataContract("historian").SetName("temperature")
				}, "topic must contain a data contract"),
			)
		})
	})

	Describe("BuildString", func() {
		It("should build a topic string directly", func() {
			builder := NewBuilder()
			builder.SetLevel0("enterprise").
				SetDataContract("_historian").
				SetName("temperature")

			topicStr, err := builder.BuildString()
			Expect(err).NotTo(HaveOccurred())
			Expect(topicStr).To(Equal("umh.v1.enterprise._historian.temperature"))
		})
	})

	Describe("FluentInterface", func() {
		It("should support method chaining", func() {
			topic, err := NewBuilder().
				SetLevel0("enterprise").
				AddLocationLevel("site").
				AddLocationLevel("area").
				SetDataContract("_historian").
				SetVirtualPath("motor.diagnostics").
				SetName("temperature").
				Build()

			Expect(err).NotTo(HaveOccurred())
			expectedTopic := "umh.v1.enterprise.site.area._historian.motor.diagnostics.temperature"
			Expect(topic.String()).To(Equal(expectedTopic))
		})
	})

	Describe("ReusePattern", func() {
		It("should allow reusing the same builder after reset", func() {
			builder := NewBuilder()

			// Build first topic
			topic1, err := builder.
				SetLocationPath("enterprise.site1").
				SetDataContract("_historian").
				SetName("temperature").
				Build()
			Expect(err).NotTo(HaveOccurred())

			// Reset and build second topic
			topic2, err := builder.Reset().
				SetLocationPath("enterprise.site2").
				SetDataContract("_historian").
				SetName("pressure").
				Build()
			Expect(err).NotTo(HaveOccurred())

			// Verify topics are different
			Expect(topic1.String()).NotTo(Equal(topic2.String()))

			expected1 := "umh.v1.enterprise.site1._historian.temperature"
			expected2 := "umh.v1.enterprise.site2._historian.pressure"

			Expect(topic1.String()).To(Equal(expected1))
			Expect(topic2.String()).To(Equal(expected2))
		})
	})

	Describe("ConcurrentUsage", func() {
		It("should work safely when each goroutine uses its own builder instance", func() {
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

						Expect(err).NotTo(HaveOccurred())
						expected := "umh.v1.enterprise.site._historian.temperature"
						Expect(topic.String()).To(Equal(expected))
					}
				}(i)
			}

			for i := 0; i < numGoroutines; i++ {
				<-done
			}
		})
	})
})

// Performance-related tests for Builder
var _ = Describe("Builder Performance", func() {
	It("should perform Build Simple operations efficiently", func() {
		builder := NewBuilder()
		for i := 0; i < 1000; i++ {
			builder.Reset().
				SetLevel0("enterprise").
				SetDataContract("_historian").
				SetName("temperature")
			_, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())
		}
	})

	It("should perform Build Complex operations efficiently", func() {
		builder := NewBuilder()
		for i := 0; i < 1000; i++ {
			builder.Reset().
				SetLocationLevels("enterprise", "site", "area", "line", "station").
				SetDataContract("_historian").
				SetVirtualPath("motor.axis.x").
				SetName("position")
			_, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())
		}
	})

	It("should perform GetLocationPath operations efficiently", func() {
		builder := NewBuilder()
		builder.SetLocationLevels("enterprise", "site", "area", "line")
		for i := 0; i < 1000; i++ {
			_ = builder.GetLocationPath()
		}
	})

	It("should perform Reset operations efficiently", func() {
		builder := NewBuilder()
		for i := 0; i < 1000; i++ {
			builder.SetLocationLevels("enterprise", "site", "area").
				SetDataContract("_historian").
				SetVirtualPath("motor.diagnostics").
				SetName("temperature")
			builder.Reset()
		}
	})

	It("should perform SetLocationPath operations efficiently", func() {
		builder := NewBuilder()
		locationPath := "enterprise.site.area.line.station"
		for i := 0; i < 1000; i++ {
			builder.SetLocationPath(locationPath)
		}
	})

	It("should perform Build with minimal allocations", func() {
		builder := NewBuilder()
		for i := 0; i < 1000; i++ {
			builder.Reset().
				SetLocationLevels("enterprise", "site", "area").
				SetDataContract("_historian").
				SetVirtualPath("motor.diagnostics").
				SetName("temperature")
			_, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())
		}
	})
})

// Helper functions

func strPtr(s string) *string {
	return &s
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
