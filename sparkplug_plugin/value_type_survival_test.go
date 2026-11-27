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

	sparkplugplugin "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
	"github.com/united-manufacturing-hub/benthos-umh/tag_processor_plugin/tagprocessortest"
)

// negativeInt32 is a variable because Go rejects converting the negative
// constant to uint32; Sparkplug B packs signed integers as two's complement
// into the unsigned IntValue and LongValue fields.
var negativeInt32 int32 = tagprocessortest.NegativeInteger

var _ = Describe("Sparkplug B value type through tag_processor", func() {
	DescribeTable("keeps the type and value of the metric value",
		func(metric *sparkplugb.Payload_Metric, metricValue any) {
			input := sparkplugplugin.NewSparkplugInputForTesting()
			topicInfo := &sparkplugplugin.TopicInfo{Group: "Factory", EdgeNode: "Line1", Device: "Device1"}
			metric.Name = stringPtr("tag")
			payload := &sparkplugb.Payload{Seq: uint64Ptr(0), Metrics: []*sparkplugb.Payload_Metric{metric}}

			batch := input.CreateSplitMessages(payload, sparkplugplugin.MessageTypeDDATA, topicInfo, "spBv1.0/Factory/DDATA/Line1/Device1")
			Expect(batch).To(HaveLen(1))

			tagprocessortest.ExpectTagProcessorKeepsPayloadField(metricValue, batch[0], "value")
		},
		Entry("numeric String", &sparkplugb.Payload_Metric{
			Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeString),
			Value:    &sparkplugb.Payload_Metric_StringValue{StringValue: tagprocessortest.NumericString},
		}, tagprocessortest.NumericString),
		Entry("non-numeric String", &sparkplugb.Payload_Metric{
			Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeString),
			Value:    &sparkplugb.Payload_Metric_StringValue{StringValue: tagprocessortest.NonNumericString},
		}, tagprocessortest.NonNumericString),
		Entry("17-digit numeric String", &sparkplugb.Payload_Metric{
			Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeString),
			Value:    &sparkplugb.Payload_Metric_StringValue{StringValue: tagprocessortest.SeventeenDigitString},
		}, tagprocessortest.SeventeenDigitString),
		Entry("Int32", &sparkplugb.Payload_Metric{
			Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeInt32),
			Value:    &sparkplugb.Payload_Metric_IntValue{IntValue: uint32(negativeInt32)},
		}, tagprocessortest.NegativeInteger),
		Entry("Int64", &sparkplugb.Payload_Metric{
			Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeInt64),
			Value:    &sparkplugb.Payload_Metric_LongValue{LongValue: tagprocessortest.PositiveInteger},
		}, tagprocessortest.PositiveInteger),
		Entry("Float", &sparkplugb.Payload_Metric{
			Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeFloat),
			Value:    &sparkplugb.Payload_Metric_FloatValue{FloatValue: tagprocessortest.FractionalNumber},
		}, tagprocessortest.FractionalNumber),
		Entry("Double", &sparkplugb.Payload_Metric{
			Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeDouble),
			Value:    &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: tagprocessortest.FractionalNumber},
		}, tagprocessortest.FractionalNumber),
		Entry("Boolean", &sparkplugb.Payload_Metric{
			Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeBoolean),
			Value:    &sparkplugb.Payload_Metric_BooleanValue{BooleanValue: true},
		}, true),
	)
})
