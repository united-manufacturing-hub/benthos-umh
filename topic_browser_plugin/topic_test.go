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

package topic_browser_plugin

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("Uns", func() {
	Describe("extractTopicFromMessage", func() {
		It("should extract topic from message meta field", func() {
			msg := service.NewMessage([]byte("test"))
			msg.MetaSet("umh_topic", "test.topic")

			topic, err := extractTopicFromMessage(msg)
			Expect(err).To(BeNil())
			Expect(topic).To(Equal("test.topic"))
		})

		It("should return error when no topic is found", func() {
			msg := service.NewMessage([]byte("test"))

			topic, err := extractTopicFromMessage(msg)
			Expect(err).To(HaveOccurred())
			Expect(topic).To(BeEmpty())
		})
	})

	Describe("topicToUNSInfo", func() {
		It("should parse valid UNS topic with all fields", func() {
			topic := "umh.v1.enterprise.site.area.line.workcell.originid._schema.event.group"

			unsInfo, err := topicToUNSInfo(topic)
			Expect(err).To(BeNil())
			Expect(unsInfo.Level0).To(Equal("enterprise"))
			Expect(unsInfo.LocationSublevels).To(Equal([]string{"site", "area", "line", "workcell", "originid"}))
			Expect(unsInfo.DataContract).To(Equal("_schema"))
			Expect(*unsInfo.VirtualPath).To(Equal("event"))
			Expect(unsInfo.Name).To(Equal("group"))
		})

		It("should parse valid UNS topic with minimal fields", func() {
			topic := "umh.v1.enterprise._schema.temperature"

			unsInfo, err := topicToUNSInfo(topic)
			Expect(err).To(BeNil())
			Expect(unsInfo.Level0).To(Equal("enterprise"))
			Expect(unsInfo.LocationSublevels).To(BeEmpty())
			Expect(unsInfo.DataContract).To(Equal("_schema"))
			Expect(unsInfo.VirtualPath).To(BeNil())
			Expect(unsInfo.Name).To(Equal("temperature"))
		})

		It("should return error for empty topic", func() {
			unsInfo, err := topicToUNSInfo("")
			Expect(err).To(HaveOccurred())
			Expect(unsInfo).To(BeNil())
		})

		It("should return error for invalid prefix", func() {
			unsInfo, err := topicToUNSInfo("invalid.topic")
			Expect(err).To(HaveOccurred())
			Expect(unsInfo).To(BeNil())
		})

		It("should return error for topic with insufficient parts", func() {
			unsInfo, err := topicToUNSInfo("umh.v1")
			Expect(err).To(HaveOccurred())
			Expect(unsInfo).To(BeNil())
		})

		It("should parse topic with location sublevels and virtual path", func() {
			topic := "umh.v1.enterprise.site.area._historian.motor.diagnostics.temperature"

			unsInfo, err := topicToUNSInfo(topic)
			Expect(err).To(BeNil())
			Expect(unsInfo.Level0).To(Equal("enterprise"))
			Expect(unsInfo.LocationSublevels).To(Equal([]string{"site", "area"}))
			Expect(unsInfo.DataContract).To(Equal("_historian"))
			Expect(*unsInfo.VirtualPath).To(Equal("motor.diagnostics"))
			Expect(unsInfo.Name).To(Equal("temperature"))
		})

		Describe("Table-driven tests for different topic structures", func() {
			type testCase struct {
				topic                string
				expectedLevel0       string
				expectedSublevels    []string
				expectedDataContract string
				expectedVirtualPath  *string
				expectedName         string
				shouldHaveError      bool
			}

			// Helper function for string pointers
			stringPtr := func(s string) *string { return &s }

			DescribeTable("should correctly parse topics with various structures",
				func(tc testCase) {
					unsInfo, err := topicToUNSInfo(tc.topic)
					if tc.shouldHaveError {
						Expect(err).To(HaveOccurred())
						Expect(unsInfo).To(BeNil())
						return
					}

					Expect(err).To(BeNil())
					Expect(unsInfo).ToNot(BeNil())
					Expect(unsInfo.Level0).To(Equal(tc.expectedLevel0))
					Expect(unsInfo.LocationSublevels).To(Equal(tc.expectedSublevels))
					Expect(unsInfo.DataContract).To(Equal(tc.expectedDataContract))
					Expect(unsInfo.Name).To(Equal(tc.expectedName))

					if tc.expectedVirtualPath != nil {
						Expect(unsInfo.VirtualPath).ToNot(BeNil())
						Expect(*unsInfo.VirtualPath).To(Equal(*tc.expectedVirtualPath))
					} else {
						Expect(unsInfo.VirtualPath).To(BeNil())
					}
				},
				Entry("minimal: enterprise + schema + name", testCase{
					topic:                "umh.v1.enterprise._schema.temperature",
					expectedLevel0:       "enterprise",
					expectedSublevels:    nil,
					expectedDataContract: "_schema",
					expectedVirtualPath:  nil,
					expectedName:         "temperature",
					shouldHaveError:      false,
				}),
				Entry("enterprise + site + schema + name", testCase{
					topic:                "umh.v1.enterprise.site._schema.pressure",
					expectedLevel0:       "enterprise",
					expectedSublevels:    []string{"site"},
					expectedDataContract: "_schema",
					expectedVirtualPath:  nil,
					expectedName:         "pressure",
					shouldHaveError:      false,
				}),
				Entry("enterprise + site + area + schema + name", testCase{
					topic:                "umh.v1.enterprise.site.area._schema.humidity",
					expectedLevel0:       "enterprise",
					expectedSublevels:    []string{"site", "area"},
					expectedDataContract: "_schema",
					expectedVirtualPath:  nil,
					expectedName:         "humidity",
					shouldHaveError:      false,
				}),
				Entry("enterprise + site + area + line + schema + name", testCase{
					topic:                "umh.v1.enterprise.site.area.line._schema.vibration",
					expectedLevel0:       "enterprise",
					expectedSublevels:    []string{"site", "area", "line"},
					expectedDataContract: "_schema",
					expectedVirtualPath:  nil,
					expectedName:         "vibration",
					shouldHaveError:      false,
				}),
				Entry("enterprise + site + area + line + workcell + schema + name", testCase{
					topic:                "umh.v1.enterprise.site.area.line.workcell._schema.speed",
					expectedLevel0:       "enterprise",
					expectedSublevels:    []string{"site", "area", "line", "workcell"},
					expectedDataContract: "_schema",
					expectedVirtualPath:  nil,
					expectedName:         "speed",
					shouldHaveError:      false,
				}),
				Entry("enterprise + site + area + line + workcell + originid + schema + name", testCase{
					topic:                "umh.v1.enterprise.site.area.line.workcell.originid._schema.power",
					expectedLevel0:       "enterprise",
					expectedSublevels:    []string{"site", "area", "line", "workcell", "originid"},
					expectedDataContract: "_schema",
					expectedVirtualPath:  nil,
					expectedName:         "power",
					shouldHaveError:      false,
				}),
				Entry("with virtual path: single segment", testCase{
					topic:                "umh.v1.enterprise.site.area.line.workcell.originid._schema.event.temperature",
					expectedLevel0:       "enterprise",
					expectedSublevels:    []string{"site", "area", "line", "workcell", "originid"},
					expectedDataContract: "_schema",
					expectedVirtualPath:  stringPtr("event"),
					expectedName:         "temperature",
					shouldHaveError:      false,
				}),
				Entry("with virtual path: multiple segments", testCase{
					topic:                "umh.v1.enterprise.site.area.line.workcell.originid._schema.event.group.subgroup.measurement",
					expectedLevel0:       "enterprise",
					expectedSublevels:    []string{"site", "area", "line", "workcell", "originid"},
					expectedDataContract: "_schema",
					expectedVirtualPath:  stringPtr("event.group.subgroup"),
					expectedName:         "measurement",
					shouldHaveError:      false,
				}),
				Entry("unlimited depth: 10 location levels", testCase{
					topic:                "umh.v1.enterprise.region.site.building.floor.area.line.workcell.station.machine._historian.temperature",
					expectedLevel0:       "enterprise",
					expectedSublevels:    []string{"region", "site", "building", "floor", "area", "line", "workcell", "station", "machine"},
					expectedDataContract: "_historian",
					expectedVirtualPath:  nil,
					expectedName:         "temperature",
					shouldHaveError:      false,
				}),
				Entry("error: missing name segment - minimal case", testCase{
					topic:           "umh.v1.enterprise._schema",
					shouldHaveError: true,
				}),
				Entry("error: missing name segment - with location", testCase{
					topic:           "umh.v1.enterprise.site.area._historian",
					shouldHaveError: true,
				}),
				Entry("error: data contract as final segment", testCase{
					topic:           "umh.v1.enterprise.site.building._analytics",
					shouldHaveError: true,
				}),
			)
		})

	})
})
