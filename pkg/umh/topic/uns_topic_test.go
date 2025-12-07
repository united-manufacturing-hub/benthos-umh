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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
)

var _ = Describe("UnsTopic", func() {
	Describe("NewUnsTopic", func() {
		Context("with valid topics", func() {
			DescribeTable("should parse topics correctly",
				func(topic, expectedLevel0 string, expectedLevels []string, expectedDC string, expectedVP *string, expectedName string) {
					unsTopic, err := NewUnsTopic(topic)
					Expect(err).NotTo(HaveOccurred())
					Expect(unsTopic).NotTo(BeNil())

					// Test String() method
					Expect(unsTopic.String()).To(Equal(topic))

					// Test AsKafkaKey() method
					Expect(unsTopic.AsKafkaKey()).To(Equal(topic))

					// Test Info() method
					info := unsTopic.Info()
					Expect(info).NotTo(BeNil())

					// Validate parsed components
					Expect(info.Level0).To(Equal(expectedLevel0))
					Expect(sliceEqual(info.LocationSublevels, expectedLevels)).To(BeTrue())
					Expect(info.DataContract).To(Equal(expectedDC))
					Expect(equalStringPtr(info.VirtualPath, expectedVP)).To(BeTrue())
					Expect(info.Name).To(Equal(expectedName))

					// Test LocationPath() helper
					expectedPath := expectedLevel0
					if len(expectedLevels) > 0 {
						expectedPath += "." + strings.Join(expectedLevels, ".")
					}
					Expect(locationPath(info)).To(Equal(expectedPath))

					// Test TotalLocationLevels() helper
					expectedTotal := 1 + len(expectedLevels)
					Expect(totalLocationLevels(info)).To(Equal(expectedTotal))
				},
				Entry("minimum valid topic",
					"umh.v1.enterprise._historian.temperature",
					"enterprise", []string{}, "_historian", nil, "temperature"),
				Entry("topic with location sublevels",
					"umh.v1.acme.berlin.assembly._historian.temperature",
					"acme", []string{"berlin", "assembly"}, "_historian", nil, "temperature"),
				Entry("topic with virtual path",
					"umh.v1.factory._raw.motor.diagnostics.temperature",
					"factory", []string{}, "_raw", strPtr("motor.diagnostics"), "temperature"),
				Entry("complex topic with all components",
					"umh.v1.enterprise.site.area.line._historian.axis.x.position",
					"enterprise", []string{"site", "area", "line"}, "_historian", strPtr("axis.x"), "position"),
				Entry("topic with name starting with underscore",
					"umh.v1.enterprise._analytics._internal_state",
					"enterprise", []string{}, "_analytics", nil, "_internal_state"),
				Entry("topic with virtual path starting with underscore",
					"umh.v1.factory._raw._internal.debug.flag",
					"factory", []string{}, "_raw", strPtr("_internal.debug"), "flag"),
				Entry("deep location hierarchy",
					"umh.v1.enterprise.region.country.state.city.plant.area.line.station._historian.temperature",
					"enterprise", []string{"region", "country", "state", "city", "plant", "area", "line", "station"}, "_historian", nil, "temperature"),
			)
		})

		Context("with invalid topics", func() {
			DescribeTable("should return appropriate errors",
				func(topic, expectedErrorSubstring string) {
					unsTopic, err := NewUnsTopic(topic)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring(expectedErrorSubstring))
					Expect(unsTopic).To(BeNil())
				},
				Entry("empty topic", "", "topic cannot be empty"),
				Entry("wrong prefix", "wrong.v1.enterprise._historian.temperature", "topic must start with umh.v1"),
				Entry("no prefix", "enterprise._historian.temperature", "topic must start with umh.v1"),
				Entry("too few parts", "umh.v1.enterprise._historian", "topic must have at least"),
				Entry("empty level0", "umh.v1.._historian.temperature", "level0 cannot be empty"),
				Entry("level0 starts with underscore", "umh.v1._enterprise._historian.temperature", "level0 cannot start with underscore"),
				Entry("data contract doesn't start with underscore", "umh.v1.enterprise.historian.temperature", "topic must contain a data contract"),
				Entry("name is empty", "umh.v1.enterprise._historian.", "topic name (final segment) cannot be empty"),
				Entry("location sublevel is empty", "umh.v1.enterprise..area._historian.temperature", "location sublevel at index 1 cannot be empty"),
				Entry("virtual path segment is empty", "umh.v1.enterprise._raw.motor..temperature", "virtual path segment at index 1 cannot be empty"),
				Entry("data contract is final segment", "umh.v1.enterprise._historian", "topic must have at least: umh.v1.level0._contract.name"),
			)
		})
	})

	Describe("TopicInfo helper methods", func() {
		Describe("LocationPath", func() {
			DescribeTable("should return correct location paths",
				func(level0 string, sublevels []string, expectedPath string) {
					info := &proto.TopicInfo{
						Level0:            level0,
						LocationSublevels: sublevels,
					}

					result := locationPath(info)
					Expect(result).To(Equal(expectedPath))
				},
				Entry("only level0", "enterprise", []string{}, "enterprise"),
				Entry("level0 with one sublevel", "enterprise", []string{"site"}, "enterprise.site"),
				Entry("level0 with multiple sublevels", "factory", []string{"area", "line", "station"}, "factory.area.line.station"),
				Entry("empty level0", "", []string{}, ""),
				Entry("empty level0 with sublevels", "", []string{"site", "area"}, ".site.area"),
			)
		})

		Describe("TotalLocationLevels", func() {
			DescribeTable("should count location levels correctly",
				func(level0 string, sublevels []string, expectedTotal int) {
					info := &proto.TopicInfo{
						Level0:            level0,
						LocationSublevels: sublevels,
					}

					result := totalLocationLevels(info)
					Expect(result).To(Equal(expectedTotal))
				},
				Entry("only level0", "enterprise", []string{}, 1),
				Entry("level0 with one sublevel", "enterprise", []string{"site"}, 2),
				Entry("level0 with multiple sublevels", "factory", []string{"area", "line", "station"}, 4),
				Entry("empty level0", "", []string{}, 1),
				Entry("empty level0 with sublevels", "", []string{"site", "area"}, 3),
			)
		})
	})

	Describe("ConcurrentUsage", func() {
		It("should work safely with concurrent access", func() {
			const numGoroutines = 100
			const numOperations = 100

			done := make(chan bool, numGoroutines)

			for i := 0; i < numGoroutines; i++ {
				go func() {
					defer func() { done <- true }()

					for j := 0; j < numOperations; j++ {
						topic := "umh.v1.enterprise.site._historian.temperature"
						unsTopic, err := NewUnsTopic(topic)

						Expect(err).NotTo(HaveOccurred())
						Expect(unsTopic.String()).To(Equal(topic))

						info := unsTopic.Info()
						Expect(info.Level0).To(Equal("enterprise"))
						Expect(info.LocationSublevels).To(Equal([]string{"site"}))
						Expect(info.DataContract).To(Equal("_historian"))
						Expect(info.Name).To(Equal("temperature"))
					}
				}()
			}

			for i := 0; i < numGoroutines; i++ {
				<-done
			}
		})
	})

	Describe("EdgeCases", func() {
		DescribeTable("should handle edge cases correctly",
			func(topic string, shouldSucceed bool, expectedComponents map[string]interface{}) {
				unsTopic, err := NewUnsTopic(topic)

				if shouldSucceed {
					Expect(err).NotTo(HaveOccurred())
					Expect(unsTopic).NotTo(BeNil())

					info := unsTopic.Info()
					if level0, exists := expectedComponents["level0"]; exists {
						Expect(info.Level0).To(Equal(level0))
					}
					if dataContract, exists := expectedComponents["dataContract"]; exists {
						Expect(info.DataContract).To(Equal(dataContract))
					}
					if name, exists := expectedComponents["name"]; exists {
						Expect(info.Name).To(Equal(name))
					}
				} else {
					Expect(err).To(HaveOccurred())
					Expect(unsTopic).To(BeNil())
				}
			},
			Entry("topic with numbers in components",
				"umh.v1.factory123._historian.sensor456",
				true,
				map[string]interface{}{"level0": "factory123", "dataContract": "_historian", "name": "sensor456"}),
			Entry("topic with special characters in name",
				"umh.v1.enterprise._historian.temp-sensor_01",
				true,
				map[string]interface{}{"level0": "enterprise", "dataContract": "_historian", "name": "temp-sensor_01"}),
			Entry("very long topic",
				"umh.v1.enterprise.region.country.state.city.plant.area.line.workstation.cell.machine._historian.axis.x.y.z.sensor.temperature.value.current.measurement",
				true,
				map[string]interface{}{"level0": "enterprise", "dataContract": "_historian", "name": "measurement"}),
			Entry("topic with minimum required segments",
				"umh.v1.a._b.c",
				true,
				map[string]interface{}{"level0": "a", "dataContract": "_b", "name": "c"}),
			Entry("topic with consecutive dots (invalid)",
				"umh.v1.enterprise.._historian.temperature",
				false,
				map[string]interface{}{}),
		)
	})
})

// Performance-related tests for UnsTopic
var _ = Describe("UnsTopic Performance", func() {
	It("should perform NewUnsTopic Simple operations efficiently", func() {
		topic := "umh.v1.enterprise._historian.temperature"
		for i := 0; i < 1000; i++ {
			_, err := NewUnsTopic(topic)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	It("should perform NewUnsTopic Complex operations efficiently", func() {
		topic := "umh.v1.enterprise.site.area.line.station._historian.motor.axis.x.position"
		for i := 0; i < 1000; i++ {
			_, err := NewUnsTopic(topic)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	It("should perform UnsTopic String operations efficiently", func() {
		topic := "umh.v1.enterprise.site.area._historian.motor.diagnostics.temperature"
		unsTopic, _ := NewUnsTopic(topic)
		for i := 0; i < 1000; i++ {
			_ = unsTopic.String()
		}
	})

	It("should perform UnsTopic Info operations efficiently", func() {
		topic := "umh.v1.enterprise.site.area._historian.motor.diagnostics.temperature"
		unsTopic, _ := NewUnsTopic(topic)
		for i := 0; i < 1000; i++ {
			_ = unsTopic.Info()
		}
	})

	It("should perform TopicInfo LocationPath operations efficiently", func() {
		topic := "umh.v1.enterprise.site.area.line._historian.temperature"
		unsTopic, _ := NewUnsTopic(topic)
		info := unsTopic.Info()
		for i := 0; i < 1000; i++ {
			_ = locationPath(info)
		}
	})

	It("should perform NewUnsTopic with minimal allocations", func() {
		topic := "umh.v1.enterprise.site.area._historian.motor.diagnostics.temperature"
		for i := 0; i < 1000; i++ {
			_, err := NewUnsTopic(topic)
			Expect(err).NotTo(HaveOccurred())
		}
	})
})

// Helper functions for UNS topic tests

func locationPath(t *proto.TopicInfo) string {
	if t.Level0 == "" {
		if len(t.LocationSublevels) == 0 {
			return ""
		}
		return "." + strings.Join(t.LocationSublevels, ".")
	}

	if len(t.LocationSublevels) == 0 {
		return t.Level0
	}

	return t.Level0 + "." + strings.Join(t.LocationSublevels, ".")
}

func totalLocationLevels(t *proto.TopicInfo) int {
	return 1 + len(t.LocationSublevels)
}
