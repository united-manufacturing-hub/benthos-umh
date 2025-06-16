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
)

var _ = Describe("TopicInfo Helper Methods", func() {
	Describe("LocationPath", func() {
		It("should return just level0 when no sublevels", func() {
			topicInfo := &TopicInfo{
				Level0:            "enterprise",
				LocationSublevels: []string{},
			}

			result := topicInfo.LocationPath()
			Expect(result).To(Equal("enterprise"))
		})

		It("should return level0 when sublevels is nil", func() {
			topicInfo := &TopicInfo{
				Level0:            "enterprise",
				LocationSublevels: nil,
			}

			result := topicInfo.LocationPath()
			Expect(result).To(Equal("enterprise"))
		})

		It("should join level0 with sublevels", func() {
			topicInfo := &TopicInfo{
				Level0:            "enterprise",
				LocationSublevels: []string{"site", "area", "line"},
			}

			result := topicInfo.LocationPath()
			Expect(result).To(Equal("enterprise.site.area.line"))
		})

		It("should handle single sublevel", func() {
			topicInfo := &TopicInfo{
				Level0:            "enterprise",
				LocationSublevels: []string{"site"},
			}

			result := topicInfo.LocationPath()
			Expect(result).To(Equal("enterprise.site"))
		})
	})

	Describe("Validate", func() {
		Context("when all fields are valid", func() {
			It("should return nil", func() {
				topicInfo := &TopicInfo{
					Level0:            "enterprise",
					LocationSublevels: []string{"site", "area"},
					DataContract:      "_historian",
					Name:              "temperature",
				}

				err := topicInfo.Validate()
				Expect(err).To(BeNil())
			})

			It("should return nil with virtual path", func() {
				virtualPath := "motor.diagnostics"
				topicInfo := &TopicInfo{
					Level0:            "enterprise",
					LocationSublevels: []string{"site", "area"},
					DataContract:      "_historian",
					VirtualPath:       &virtualPath,
					Name:              "temperature",
				}

				err := topicInfo.Validate()
				Expect(err).To(BeNil())
			})
		})

		Context("when level0 is invalid", func() {
			It("should return error for empty level0", func() {
				topicInfo := &TopicInfo{
					Level0:       "",
					DataContract: "_historian",
					Name:         "temperature",
				}

				err := topicInfo.Validate()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("level0 (enterprise) cannot be empty"))
			})
		})

		Context("when data contract is invalid", func() {
			It("should return error for empty data contract", func() {
				topicInfo := &TopicInfo{
					Level0:       "enterprise",
					DataContract: "",
					Name:         "temperature",
				}

				err := topicInfo.Validate()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("data contract cannot be empty"))
			})

			It("should return error for data contract not starting with underscore", func() {
				topicInfo := &TopicInfo{
					Level0:       "enterprise",
					DataContract: "historian",
					Name:         "temperature",
				}

				err := topicInfo.Validate()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("data contract must start with underscore"))
			})

			It("should return error for data contract that is just underscore", func() {
				topicInfo := &TopicInfo{
					Level0:       "enterprise",
					DataContract: "_",
					Name:         "temperature",
				}

				err := topicInfo.Validate()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("data contract cannot be just an underscore"))
			})
		})

		Context("when name is invalid", func() {
			It("should return error for empty name", func() {
				topicInfo := &TopicInfo{
					Level0:       "enterprise",
					DataContract: "_historian",
					Name:         "",
				}

				err := topicInfo.Validate()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("topic name cannot be empty"))
			})

			It("should return error for name starting with underscore", func() {
				topicInfo := &TopicInfo{
					Level0:       "enterprise",
					DataContract: "_historian",
					Name:         "_temperature",
				}

				err := topicInfo.Validate()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("topic name cannot start with underscore"))
			})
		})

		Context("when location sublevels are invalid", func() {
			It("should return error for empty sublevel", func() {
				topicInfo := &TopicInfo{
					Level0:            "enterprise",
					LocationSublevels: []string{"site", "", "area"},
					DataContract:      "_historian",
					Name:              "temperature",
				}

				err := topicInfo.Validate()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("location sublevel at index 1 cannot be empty"))
			})
		})

		Context("when virtual path is invalid", func() {
			It("should return error for empty segment in virtual path", func() {
				virtualPath := "motor..diagnostics"
				topicInfo := &TopicInfo{
					Level0:       "enterprise",
					DataContract: "_historian",
					VirtualPath:  &virtualPath,
					Name:         "temperature",
				}

				err := topicInfo.Validate()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("virtual path segment at index 1 cannot be empty"))
			})

			It("should allow empty virtual path pointer", func() {
				topicInfo := &TopicInfo{
					Level0:       "enterprise",
					DataContract: "_historian",
					VirtualPath:  nil,
					Name:         "temperature",
				}

				err := topicInfo.Validate()
				Expect(err).To(BeNil())
			})

			It("should allow empty virtual path string", func() {
				virtualPath := ""
				topicInfo := &TopicInfo{
					Level0:       "enterprise",
					DataContract: "_historian",
					VirtualPath:  &virtualPath,
					Name:         "temperature",
				}

				err := topicInfo.Validate()
				Expect(err).To(BeNil())
			})
		})
	})
})
