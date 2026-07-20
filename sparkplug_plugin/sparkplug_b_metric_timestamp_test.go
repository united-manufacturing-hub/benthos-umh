//go:build !integration

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

package sparkplug_plugin_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	sparkplugplugin "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

// ENG-5341: spb_timestamp must reflect each metric's own timestamp (metric.Timestamp,
// proto field 3) when present, falling back to the payload-level timestamp only when a
// metric carries none. Previously every metric in an NDATA/DDATA inherited the single
// payload.Timestamp, losing per-metric sample time and colliding downstream dedup keys.
var _ = Describe("Per-metric timestamp extraction (ENG-5341)", func() {
	var (
		input     *sparkplugplugin.SparkplugInputTestWrapper
		topicInfo *sparkplugplugin.TopicInfo
	)

	BeforeEach(func() {
		input = createMockSparkplugInput()
		topicInfo = &sparkplugplugin.TopicInfo{
			Group:    "test",
			EdgeNode: "edge1",
			Device:   "device1",
		}
	})

	Context("when metrics carry their own timestamps", func() {
		It("uses each metric's timestamp for spb_timestamp", func() {
			payloadTS := uint64(1730986400000)
			tempTS := uint64(1730986400111)
			pressureTS := uint64(1730986400222)
			seq := uint64(42)

			payload := &sparkplugb.Payload{
				Seq:       &seq,
				Timestamp: &payloadTS,
				Metrics: []*sparkplugb.Payload_Metric{
					{Name: stringPtr("temperature"), Timestamp: &tempTS, Datatype: uint32Ptr(SparkplugDataTypeDouble), Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 100.5}},
					{Name: stringPtr("pressure"), Timestamp: &pressureTS, Datatype: uint32Ptr(SparkplugDataTypeDouble), Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 50.2}},
				},
			}

			batch := input.CreateSplitMessages(payload, "NDATA", topicInfo, "spBv1.0/test/NDATA/edge1/device1")
			Expect(batch).To(HaveLen(2))

			ts0, ok := batch[0].MetaGet("spb_timestamp")
			Expect(ok).To(BeTrue())
			Expect(ts0).To(Equal("1730986400111"), "temperature should use its own metric timestamp")

			ts1, ok := batch[1].MetaGet("spb_timestamp")
			Expect(ok).To(BeTrue())
			Expect(ts1).To(Equal("1730986400222"), "pressure should use its own metric timestamp")
		})
	})

	Context("when a metric has no timestamp", func() {
		It("falls back to the payload-level timestamp", func() {
			payloadTS := uint64(1730986400000)
			seq := uint64(43)

			payload := &sparkplugb.Payload{
				Seq:       &seq,
				Timestamp: &payloadTS,
				Metrics: []*sparkplugb.Payload_Metric{
					{Name: stringPtr("temperature"), Datatype: uint32Ptr(SparkplugDataTypeDouble), Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 100.5}},
				},
			}

			batch := input.CreateSplitMessages(payload, "NDATA", topicInfo, "spBv1.0/test/NDATA/edge1/device1")
			Expect(batch).To(HaveLen(1))

			ts, ok := batch[0].MetaGet("spb_timestamp")
			Expect(ok).To(BeTrue())
			Expect(ts).To(Equal("1730986400000"), "should fall back to payload timestamp when metric has none")
		})
	})
})
