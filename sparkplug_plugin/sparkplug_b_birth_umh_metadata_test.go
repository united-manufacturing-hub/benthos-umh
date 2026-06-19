//go:build !integration

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

// NBIRTH/DBIRTH metrics must parse into full UMH metadata, not just a payload value.
//
// The ManagementConsole default Sparkplug B template (ENG-5176) relies on birth
// metrics carrying umh_location_path / umh_tag_name / umh_virtual_path: it lets BIRTH
// through so every metric declared in a birth certificate seeds the Topic Browser
// with its initial value, instead of only the metrics that actively change via DATA.
//
// Existing coverage does not pin this: sparkplug_b_datatype_test.go feeds a BIRTH
// through CreateSplitMessages but asserts only the payload value, and the umh_*
// integration test filters BIRTH out and validates DDATA only. This spec locks in
// that a birth metric produces umh_* metadata so the template fix cannot regress
// silently from the input side.
package sparkplug_plugin_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	sparkplugplugin "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

var _ = Describe("BIRTH metrics parse into UMH metadata (ENG-5176)", func() {
	var wrapper *sparkplugplugin.SparkplugInputTestWrapper

	BeforeEach(func() {
		wrapper = sparkplugplugin.NewSparkplugInputForTesting()
	})

	It("populates umh_* metadata and value for an NBIRTH metric with a hierarchical name", func() {
		topicInfo := &sparkplugplugin.TopicInfo{Group: "Factory", EdgeNode: "Line1"}
		payload := &sparkplugb.Payload{
			Seq: uint64Ptr(0),
			Metrics: []*sparkplugb.Payload_Metric{
				{
					Name:     stringPtr("Refrigeration/Motor1/Amps"),
					Alias:    uint64Ptr(1),
					Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeDouble),
					Value:    &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 12.5},
				},
			},
		}

		wrapper.ProcessBirthMessage(sparkplugplugin.MessageTypeNBIRTH, payload, topicInfo)
		batch := wrapper.CreateSplitMessages(payload, sparkplugplugin.MessageTypeNBIRTH, topicInfo, "spBv1.0/Factory/NBIRTH/Line1")
		Expect(batch).To(HaveLen(1))
		msg := batch[0]

		status, _ := msg.MetaGet("umh_conversion_status")
		Expect(status).To(Equal("success"),
			"a birth metric must convert to UMH metadata; the MC template relies on this to seed the Topic Browser")

		// location_path must be present so the UMH topic can be built. Its exact
		// value (edge-node handling) is governed by ENG-5175; assert presence only
		// so that fix can refine the mapping without breaking this contract.
		locationPath, _ := msg.MetaGet("umh_location_path")
		Expect(locationPath).NotTo(BeEmpty())

		// Birth carries the metric name (DATA carries only aliases), so the topic
		// surfaces with its real name rather than the alias_N fallback.
		tagName, _ := msg.MetaGet("umh_tag_name")
		Expect(tagName).To(Equal("Amps"))

		virtualPath, hasVirtualPath := msg.MetaGet("umh_virtual_path")
		Expect(hasVirtualPath).To(BeTrue())
		Expect(virtualPath).To(Equal("Refrigeration.Motor1"))

		raw, err := msg.AsBytes()
		Expect(err).NotTo(HaveOccurred())
		var body map[string]any
		Expect(json.Unmarshal(raw, &body)).To(Succeed())
		Expect(body["value"]).To(BeNumerically("==", 12.5),
			"birth metrics must seed the topic with their initial value")
	})

	It("populates umh_* metadata and value for a DBIRTH metric without a virtual path", func() {
		topicInfo := &sparkplugplugin.TopicInfo{Group: "Factory", EdgeNode: "Line1", Device: "Pump7"}
		payload := &sparkplugb.Payload{
			Seq: uint64Ptr(0),
			Metrics: []*sparkplugb.Payload_Metric{
				{
					Name:     stringPtr("Configuration"),
					Alias:    uint64Ptr(1),
					Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeString),
					Value:    &sparkplugb.Payload_Metric_StringValue{StringValue: "v1.2"},
				},
			},
		}

		wrapper.ProcessBirthMessage(sparkplugplugin.MessageTypeDBIRTH, payload, topicInfo)
		batch := wrapper.CreateSplitMessages(payload, sparkplugplugin.MessageTypeDBIRTH, topicInfo, "spBv1.0/Factory/DBIRTH/Line1/Pump7")
		Expect(batch).To(HaveLen(1))
		msg := batch[0]

		status, _ := msg.MetaGet("umh_conversion_status")
		Expect(status).To(Equal("success"))

		locationPath, _ := msg.MetaGet("umh_location_path")
		Expect(locationPath).NotTo(BeEmpty())

		tagName, _ := msg.MetaGet("umh_tag_name")
		Expect(tagName).To(Equal("Configuration"))

		// A flat metric name has no virtual path; Benthos cannot store empty
		// strings, so the field must be absent (not present-and-empty).
		_, hasVirtualPath := msg.MetaGet("umh_virtual_path")
		Expect(hasVirtualPath).To(BeFalse())

		raw, err := msg.AsBytes()
		Expect(err).NotTo(HaveOccurred())
		var body map[string]any
		Expect(json.Unmarshal(raw, &body)).To(Succeed())
		Expect(body["value"]).To(Equal("v1.2"))
	})
})
