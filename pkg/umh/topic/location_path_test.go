// Copyright 2026 UMH Systems GmbH
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

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
)

var _ = Describe("TopicInfo.LocationPath", func() {
	It("returns just level0 when no sublevels", func() {
		Expect((&proto.TopicInfo{Level0: "enterprise", LocationSublevels: []string{}}).LocationPath()).
			To(Equal("enterprise"))
	})

	It("returns level0 when sublevels is nil", func() {
		Expect((&proto.TopicInfo{Level0: "enterprise", LocationSublevels: nil}).LocationPath()).
			To(Equal("enterprise"))
	})

	It("joins level0 with sublevels", func() {
		Expect((&proto.TopicInfo{Level0: "enterprise", LocationSublevels: []string{"site", "area", "line"}}).LocationPath()).
			To(Equal("enterprise.site.area.line"))
	})

	It("handles a single sublevel", func() {
		Expect((&proto.TopicInfo{Level0: "enterprise", LocationSublevels: []string{"site"}}).LocationPath()).
			To(Equal("enterprise.site"))
	})

	It("returns empty string for a nil receiver", func() {
		var ti *proto.TopicInfo
		Expect(ti.LocationPath()).To(Equal(""))
	})

	It("trims whitespace from level0 and all sublevels", func() {
		Expect((&proto.TopicInfo{Level0: "\n enterprise \n", LocationSublevels: []string{"\r site \r", "  area  ", " line"}}).LocationPath()).
			To(Equal("enterprise.site.area.line"))
	})

	It("keeps empty segments after trimming (does not drop them)", func() {
		Expect((&proto.TopicInfo{Level0: "   ", LocationSublevels: []string{"  ", "\t\t", "valid"}}).LocationPath()).
			To(Equal("...valid"))
	})

	It("produces equal paths for equivalent inputs differing only in whitespace", func() {
		a := (&proto.TopicInfo{Level0: "enterprise", LocationSublevels: []string{"site", "area"}}).LocationPath()
		b := (&proto.TopicInfo{Level0: " enterprise ", LocationSublevels: []string{" site ", " area "}}).LocationPath()
		Expect(a).To(Equal(b))
		Expect(a).To(Equal("enterprise.site.area"))
	})
})
