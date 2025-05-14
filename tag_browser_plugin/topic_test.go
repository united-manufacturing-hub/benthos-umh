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

package tag_browser_plugin

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var _ = Describe("Uns", func() {
	Describe("extractTopicFromMessage", func() {
		It("should extract topic from message meta field", func() {
			msg := service.NewMessage([]byte("test"))
			msg.MetaSet("topic", "test.topic")

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
			Expect(unsInfo.Enterprise).To(Equal("enterprise"))
			Expect(unsInfo.Site.GetValue()).To(Equal("site"))
			Expect(unsInfo.Area.GetValue()).To(Equal("area"))
			Expect(unsInfo.Line.GetValue()).To(Equal("line"))
			Expect(unsInfo.WorkCell.GetValue()).To(Equal("workcell"))
			Expect(unsInfo.OriginId.GetValue()).To(Equal("originid"))
			Expect(unsInfo.Schema).To(Equal("_schema"))
			Expect(unsInfo.EventGroup.GetValue()).To(Equal("event.group"))
		})

		It("should parse valid UNS topic with minimal fields", func() {
			topic := "umh.v1.enterprise._schema"

			unsInfo, err := topicToUNSInfo(topic)
			Expect(err).To(BeNil())
			Expect(unsInfo.Enterprise).To(Equal("enterprise"))
			Expect(unsInfo.Schema).To(Equal("_schema"))
			Expect(unsInfo.EventGroup.GetValue()).To(BeEmpty())
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

		Describe("Table-driven tests for optional fields", func() {
			type testCase struct {
				topic           string
				expectedInfo    *tagbrowserpluginprotobuf.TopicInfo
				shouldHaveError bool
			}

			DescribeTable("should correctly parse topics with optional fields",
				func(tc testCase) {
					unsInfo, err := topicToUNSInfo(tc.topic)
					if tc.shouldHaveError {
						Expect(err).To(HaveOccurred())
						Expect(unsInfo).To(BeNil())
						return
					}

					Expect(err).To(BeNil())
					Expect(unsInfo).ToNot(BeNil())
					Expect(unsInfo.Enterprise).To(Equal(tc.expectedInfo.Enterprise))
					Expect(unsInfo.Schema).To(Equal(tc.expectedInfo.Schema))

					// Check optional fields
					if tc.expectedInfo.Site != nil {
						Expect(unsInfo.Site.GetValue()).To(Equal(tc.expectedInfo.Site.GetValue()))
					} else {
						Expect(unsInfo.Site).To(BeNil())
					}

					if tc.expectedInfo.Area != nil {
						Expect(unsInfo.Area.GetValue()).To(Equal(tc.expectedInfo.Area.GetValue()))
					} else {
						Expect(unsInfo.Area).To(BeNil())
					}

					if tc.expectedInfo.Line != nil {
						Expect(unsInfo.Line.GetValue()).To(Equal(tc.expectedInfo.Line.GetValue()))
					} else {
						Expect(unsInfo.Line).To(BeNil())
					}

					if tc.expectedInfo.WorkCell != nil {
						Expect(unsInfo.WorkCell.GetValue()).To(Equal(tc.expectedInfo.WorkCell.GetValue()))
					} else {
						Expect(unsInfo.WorkCell).To(BeNil())
					}

					if tc.expectedInfo.OriginId != nil {
						Expect(unsInfo.OriginId.GetValue()).To(Equal(tc.expectedInfo.OriginId.GetValue()))
					} else {
						Expect(unsInfo.OriginId).To(BeNil())
					}

					if tc.expectedInfo.EventGroup != nil {
						Expect(unsInfo.EventGroup.GetValue()).To(Equal(tc.expectedInfo.EventGroup.GetValue()))
					} else {
						Expect(unsInfo.EventGroup).To(BeNil())
					}
				},
				Entry("only enterprise and schema", testCase{
					topic: "umh.v1.enterprise._schema",
					expectedInfo: &tagbrowserpluginprotobuf.TopicInfo{
						Enterprise: "enterprise",
						Schema:     "_schema",
					},
					shouldHaveError: false,
				}),
				Entry("enterprise, site, and schema", testCase{
					topic: "umh.v1.enterprise.site._schema",
					expectedInfo: &tagbrowserpluginprotobuf.TopicInfo{
						Enterprise: "enterprise",
						Site:       wrapperspb.String("site"),
						Schema:     "_schema",
					},
					shouldHaveError: false,
				}),
				Entry("enterprise, site, area, and schema", testCase{
					topic: "umh.v1.enterprise.site.area._schema",
					expectedInfo: &tagbrowserpluginprotobuf.TopicInfo{
						Enterprise: "enterprise",
						Site:       wrapperspb.String("site"),
						Area:       wrapperspb.String("area"),
						Schema:     "_schema",
					},
					shouldHaveError: false,
				}),
				Entry("enterprise, site, area, line, and schema", testCase{
					topic: "umh.v1.enterprise.site.area.line._schema",
					expectedInfo: &tagbrowserpluginprotobuf.TopicInfo{
						Enterprise: "enterprise",
						Site:       wrapperspb.String("site"),
						Area:       wrapperspb.String("area"),
						Line:       wrapperspb.String("line"),
						Schema:     "_schema",
					},
					shouldHaveError: false,
				}),
				Entry("enterprise, site, area, line, workcell, and schema", testCase{
					topic: "umh.v1.enterprise.site.area.line.workcell._schema",
					expectedInfo: &tagbrowserpluginprotobuf.TopicInfo{
						Enterprise: "enterprise",
						Site:       wrapperspb.String("site"),
						Area:       wrapperspb.String("area"),
						Line:       wrapperspb.String("line"),
						WorkCell:   wrapperspb.String("workcell"),
						Schema:     "_schema",
					},
					shouldHaveError: false,
				}),
				Entry("enterprise, site, area, line, workcell, originid, and schema", testCase{
					topic: "umh.v1.enterprise.site.area.line.workcell.originid._schema",
					expectedInfo: &tagbrowserpluginprotobuf.TopicInfo{
						Enterprise: "enterprise",
						Site:       wrapperspb.String("site"),
						Area:       wrapperspb.String("area"),
						Line:       wrapperspb.String("line"),
						WorkCell:   wrapperspb.String("workcell"),
						OriginId:   wrapperspb.String("originid"),
						Schema:     "_schema",
					},
					shouldHaveError: false,
				}),
				Entry("enterprise, site, area, line, workcell, originid, schema, and event group", testCase{
					topic: "umh.v1.enterprise.site.area.line.workcell.originid._schema.event.group",
					expectedInfo: &tagbrowserpluginprotobuf.TopicInfo{
						Enterprise: "enterprise",
						Site:       wrapperspb.String("site"),
						Area:       wrapperspb.String("area"),
						Line:       wrapperspb.String("line"),
						WorkCell:   wrapperspb.String("workcell"),
						OriginId:   wrapperspb.String("originid"),
						Schema:     "_schema",
						EventGroup: wrapperspb.String("event.group"),
					},
					shouldHaveError: false,
				}),
				Entry("enterprise, site, area, line, workcell, originid, schema, and complex event group", testCase{
					topic: "umh.v1.enterprise.site.area.line.workcell.originid._schema.event.group.subgroup",
					expectedInfo: &tagbrowserpluginprotobuf.TopicInfo{
						Enterprise: "enterprise",
						Site:       wrapperspb.String("site"),
						Area:       wrapperspb.String("area"),
						Line:       wrapperspb.String("line"),
						WorkCell:   wrapperspb.String("workcell"),
						OriginId:   wrapperspb.String("originid"),
						Schema:     "_schema",
						EventGroup: wrapperspb.String("event.group.subgroup"),
					},
					shouldHaveError: false,
				}),
			)
		})
	})
})
