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
			Expect(unsInfo.Level1.GetValue()).To(Equal("site"))
			Expect(unsInfo.Level2.GetValue()).To(Equal("area"))
			Expect(unsInfo.Level3.GetValue()).To(Equal("line"))
			Expect(unsInfo.Level4.GetValue()).To(Equal("workcell"))
			Expect(unsInfo.Level5.GetValue()).To(Equal("originid"))
			Expect(unsInfo.Datacontract).To(Equal("_schema"))
			Expect(unsInfo.VirtualPath.GetValue()).To(Equal("event"))
			Expect(unsInfo.EventTag.GetValue()).To(Equal("group"))
		})

		It("should parse valid UNS topic with minimal fields", func() {
			topic := "umh.v1.enterprise._schema"

			unsInfo, err := topicToUNSInfo(topic)
			Expect(err).To(BeNil())
			Expect(unsInfo.Level0).To(Equal("enterprise"))
			Expect(unsInfo.Datacontract).To(Equal("_schema"))
			Expect(unsInfo.VirtualPath.GetValue()).To(BeEmpty())
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
					Expect(unsInfo.Level0).To(Equal(tc.expectedInfo.Level0))
					Expect(unsInfo.Datacontract).To(Equal(tc.expectedInfo.Datacontract))

					// Check optional fields
					if tc.expectedInfo.Level1 != nil {
						Expect(unsInfo.Level1.GetValue()).To(Equal(tc.expectedInfo.Level1.GetValue()))
					} else {
						Expect(unsInfo.Level1).To(BeNil())
					}

					if tc.expectedInfo.Level2 != nil {
						Expect(unsInfo.Level2.GetValue()).To(Equal(tc.expectedInfo.Level2.GetValue()))
					} else {
						Expect(unsInfo.Level2).To(BeNil())
					}

					if tc.expectedInfo.Level3 != nil {
						Expect(unsInfo.Level3.GetValue()).To(Equal(tc.expectedInfo.Level3.GetValue()))
					} else {
						Expect(unsInfo.Level3).To(BeNil())
					}

					if tc.expectedInfo.Level4 != nil {
						Expect(unsInfo.Level4.GetValue()).To(Equal(tc.expectedInfo.Level4.GetValue()))
					} else {
						Expect(unsInfo.Level4).To(BeNil())
					}

					if tc.expectedInfo.Level5 != nil {
						Expect(unsInfo.Level5.GetValue()).To(Equal(tc.expectedInfo.Level5.GetValue()))
					} else {
						Expect(unsInfo.Level5).To(BeNil())
					}

					if tc.expectedInfo.VirtualPath != nil {
						Expect(unsInfo.VirtualPath.GetValue()).To(Equal(tc.expectedInfo.VirtualPath.GetValue()))
						Expect(unsInfo.EventTag.GetValue()).To(Equal(tc.expectedInfo.EventTag.GetValue()))
					} else {
						Expect(unsInfo.VirtualPath).To(BeNil())
						Expect(unsInfo.EventTag).To(BeNil())
					}
				},
				Entry("only enterprise and schema", testCase{
					topic: "umh.v1.enterprise._schema",
					expectedInfo: &tagbrowserpluginprotobuf.TopicInfo{
						Level0:       "enterprise",
						Datacontract: "_schema",
					},
					shouldHaveError: false,
				}),
				Entry("enterprise, site, and schema", testCase{
					topic: "umh.v1.enterprise.site._schema",
					expectedInfo: &tagbrowserpluginprotobuf.TopicInfo{
						Level0:       "enterprise",
						Level1:       wrapperspb.String("site"),
						Datacontract: "_schema",
					},
					shouldHaveError: false,
				}),
				Entry("enterprise, site, area, and schema", testCase{
					topic: "umh.v1.enterprise.site.area._schema",
					expectedInfo: &tagbrowserpluginprotobuf.TopicInfo{
						Level0:       "enterprise",
						Level1:       wrapperspb.String("site"),
						Level2:       wrapperspb.String("area"),
						Datacontract: "_schema",
					},
					shouldHaveError: false,
				}),
				Entry("enterprise, site, area, line, and schema", testCase{
					topic: "umh.v1.enterprise.site.area.line._schema",
					expectedInfo: &tagbrowserpluginprotobuf.TopicInfo{
						Level0:       "enterprise",
						Level1:       wrapperspb.String("site"),
						Level2:       wrapperspb.String("area"),
						Level3:       wrapperspb.String("line"),
						Datacontract: "_schema",
					},
					shouldHaveError: false,
				}),
				Entry("enterprise, site, area, line, workcell, and schema", testCase{
					topic: "umh.v1.enterprise.site.area.line.workcell._schema",
					expectedInfo: &tagbrowserpluginprotobuf.TopicInfo{
						Level0:       "enterprise",
						Level1:       wrapperspb.String("site"),
						Level2:       wrapperspb.String("area"),
						Level3:       wrapperspb.String("line"),
						Level4:       wrapperspb.String("workcell"),
						Datacontract: "_schema",
					},
					shouldHaveError: false,
				}),
				Entry("enterprise, site, area, line, workcell, originid, and schema", testCase{
					topic: "umh.v1.enterprise.site.area.line.workcell.originid._schema",
					expectedInfo: &tagbrowserpluginprotobuf.TopicInfo{
						Level0:       "enterprise",
						Level1:       wrapperspb.String("site"),
						Level2:       wrapperspb.String("area"),
						Level3:       wrapperspb.String("line"),
						Level4:       wrapperspb.String("workcell"),
						Level5:       wrapperspb.String("originid"),
						Datacontract: "_schema",
					},
					shouldHaveError: false,
				}),
				Entry("enterprise, site, area, line, workcell, originid, schema, and event group", testCase{
					topic: "umh.v1.enterprise.site.area.line.workcell.originid._schema.event.group",
					expectedInfo: &tagbrowserpluginprotobuf.TopicInfo{
						Level0:       "enterprise",
						Level1:       wrapperspb.String("site"),
						Level2:       wrapperspb.String("area"),
						Level3:       wrapperspb.String("line"),
						Level4:       wrapperspb.String("workcell"),
						Level5:       wrapperspb.String("originid"),
						Datacontract: "_schema",
						VirtualPath:  wrapperspb.String("event"),
						EventTag:     wrapperspb.String("group"),
					},
					shouldHaveError: false,
				}),
				Entry("enterprise, site, area, line, workcell, originid, schema, and complex event group", testCase{
					topic: "umh.v1.enterprise.site.area.line.workcell.originid._schema.event.group.subgroup",
					expectedInfo: &tagbrowserpluginprotobuf.TopicInfo{
						Level0:       "enterprise",
						Level1:       wrapperspb.String("site"),
						Level2:       wrapperspb.String("area"),
						Level3:       wrapperspb.String("line"),
						Level4:       wrapperspb.String("workcell"),
						Level5:       wrapperspb.String("originid"),
						Datacontract: "_schema",
						VirtualPath:  wrapperspb.String("event.group"),
						EventTag:     wrapperspb.String("subgroup"),
					},
					shouldHaveError: false,
				}),
			)
		})
	})
})
