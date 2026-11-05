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

package sparkplug_plugin_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	sparkplugplugin "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

var _ = Describe("Sparkplug B datatypes beyond the scalar types", func() {
	var (
		wrapper   *sparkplugplugin.SparkplugInputTestWrapper
		topicInfo *sparkplugplugin.TopicInfo
	)

	BeforeEach(func() {
		wrapper = sparkplugplugin.NewSparkplugInputForTesting()
		topicInfo = &sparkplugplugin.TopicInfo{Group: "Factory", EdgeNode: "Line1"}
	})

	birthMessage := func(metric *sparkplugb.Payload_Metric) *service.Message {
		birthPayload := &sparkplugb.Payload{Seq: uint64Ptr(0), Metrics: []*sparkplugb.Payload_Metric{metric}}
		wrapper.ProcessBirthMessage(sparkplugplugin.MessageTypeNBIRTH, birthPayload, topicInfo)
		batch := wrapper.CreateSplitMessages(birthPayload, sparkplugplugin.MessageTypeNBIRTH, topicInfo, "spBv1.0/Factory/NBIRTH/Line1")
		Expect(batch).To(HaveLen(1))
		return batch[0]
	}

	DescribeTable("names the datatype in spb_datatype",
		func(datatype uint32, expectedName string) {
			msg := birthMessage(&sparkplugb.Payload_Metric{
				Name:     stringPtr("payload"),
				Alias:    uint64Ptr(1),
				Datatype: uint32Ptr(datatype),
			})

			datatypeName, exists := msg.MetaGet("spb_datatype")
			Expect(exists).To(BeTrue())
			Expect(datatypeName).To(Equal(expectedName))
		},
		Entry("UUID", sparkplugplugin.SparkplugDataTypeUUID, "UUID"),
		Entry("DataSet", sparkplugplugin.SparkplugDataTypeDataSet, "DataSet"),
		Entry("Bytes", sparkplugplugin.SparkplugDataTypeBytes, "Bytes"),
		Entry("File", sparkplugplugin.SparkplugDataTypeFile, "File"),
		Entry("Template", sparkplugplugin.SparkplugDataTypeTemplate, "Template"),
	)
})
