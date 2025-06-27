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
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
)

var _ = Describe("TopicInfo Helper Methods", func() {
	Describe("LocationPath", func() {
		It("should return just level0 when no sublevels", func() {
			topicInfo := &proto.TopicInfo{
				Level0:            "enterprise",
				LocationSublevels: []string{},
			}

			result := LocationPath(topicInfo)
			Expect(result).To(Equal("enterprise"))
		})

		It("should return level0 when sublevels is nil", func() {
			topicInfo := &proto.TopicInfo{
				Level0:            "enterprise",
				LocationSublevels: nil,
			}

			result := LocationPath(topicInfo)
			Expect(result).To(Equal("enterprise"))
		})

		It("should join level0 with sublevels", func() {
			topicInfo := &proto.TopicInfo{
				Level0:            "enterprise",
				LocationSublevels: []string{"site", "area", "line"},
			}

			result := LocationPath(topicInfo)
			Expect(result).To(Equal("enterprise.site.area.line"))
		})

		It("should handle single sublevel", func() {
			topicInfo := &proto.TopicInfo{
				Level0:            "enterprise",
				LocationSublevels: []string{"site"},
			}

			result := LocationPath(topicInfo)
			Expect(result).To(Equal("enterprise.site"))
		})

		// ✅ NEW: Nil receiver protection tests
		It("should return empty string for nil receiver", func() {
			var topicInfo *proto.TopicInfo = nil

			result := LocationPath(topicInfo)
			Expect(result).To(Equal(""))
		})

		// ✅ NEW: Whitespace handling tests
		It("should trim whitespace from level0", func() {
			topicInfo := &proto.TopicInfo{
				Level0:            "  enterprise  ",
				LocationSublevels: []string{},
			}

			result := LocationPath(topicInfo)
			Expect(result).To(Equal("enterprise"))
		})

		It("should trim whitespace from all sublevels", func() {
			topicInfo := &proto.TopicInfo{
				Level0:            " enterprise ",
				LocationSublevels: []string{" site ", "  area  ", "\tline\t"},
			}

			result := LocationPath(topicInfo)
			Expect(result).To(Equal("enterprise.site.area.line"))
		})

		It("should handle mixed whitespace scenarios", func() {
			topicInfo := &proto.TopicInfo{
				Level0:            "\n enterprise \n",
				LocationSublevels: []string{"\r site \r", "  area  ", " line"},
			}

			result := LocationPath(topicInfo)
			Expect(result).To(Equal("enterprise.site.area.line"))
		})

		It("should handle empty strings after trimming", func() {
			topicInfo := &proto.TopicInfo{
				Level0:            "   ",
				LocationSublevels: []string{"  ", "\t\t", "valid"},
			}

			result := LocationPath(topicInfo)
			Expect(result).To(Equal("...valid"))
		})

		It("should ensure hash equality for equivalent paths with different whitespace", func() {
			topicInfo1 := &proto.TopicInfo{
				Level0:            "enterprise",
				LocationSublevels: []string{"site", "area"},
			}

			topicInfo2 := &proto.TopicInfo{
				Level0:            " enterprise ",
				LocationSublevels: []string{" site ", " area "},
			}

			result1 := LocationPath(topicInfo1)
			result2 := LocationPath(topicInfo2)
			Expect(result1).To(Equal(result2))
			Expect(result1).To(Equal("enterprise.site.area"))
		})
	})
})
