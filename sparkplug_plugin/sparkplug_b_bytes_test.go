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
	"encoding/json"

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

	messageBody := func(msg *service.Message) map[string]any {
		raw, err := msg.AsBytes()
		Expect(err).NotTo(HaveOccurred())
		var body map[string]any
		Expect(json.Unmarshal(raw, &body)).To(Succeed())
		return body
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
		Entry("PropertySet", sparkplugplugin.SparkplugDataTypePropertySet, "PropertySet"),
		Entry("PropertySetList", sparkplugplugin.SparkplugDataTypePropertySetList, "PropertySetList"),
		Entry("Int8Array", sparkplugplugin.SparkplugDataTypeInt8Array, "Int8Array"),
		Entry("Int16Array", sparkplugplugin.SparkplugDataTypeInt16Array, "Int16Array"),
		Entry("Int32Array", sparkplugplugin.SparkplugDataTypeInt32Array, "Int32Array"),
		Entry("Int64Array", sparkplugplugin.SparkplugDataTypeInt64Array, "Int64Array"),
		Entry("UInt8Array", sparkplugplugin.SparkplugDataTypeUInt8Array, "UInt8Array"),
		Entry("UInt16Array", sparkplugplugin.SparkplugDataTypeUInt16Array, "UInt16Array"),
		Entry("UInt32Array", sparkplugplugin.SparkplugDataTypeUInt32Array, "UInt32Array"),
		Entry("UInt64Array", sparkplugplugin.SparkplugDataTypeUInt64Array, "UInt64Array"),
		Entry("FloatArray", sparkplugplugin.SparkplugDataTypeFloatArray, "FloatArray"),
		Entry("DoubleArray", sparkplugplugin.SparkplugDataTypeDoubleArray, "DoubleArray"),
		Entry("BooleanArray", sparkplugplugin.SparkplugDataTypeBooleanArray, "BooleanArray"),
		Entry("StringArray", sparkplugplugin.SparkplugDataTypeStringArray, "StringArray"),
		Entry("DateTimeArray", sparkplugplugin.SparkplugDataTypeDateTimeArray, "DateTimeArray"),
	)

	DescribeTable("decodes a bytes_value as a base64 string",
		func(datatype uint32, bytesValue []byte, expectedBase64 string) {
			msg := birthMessage(&sparkplugb.Payload_Metric{
				Name:     stringPtr("payload"),
				Alias:    uint64Ptr(1),
				Datatype: uint32Ptr(datatype),
				Value:    &sparkplugb.Payload_Metric_BytesValue{BytesValue: bytesValue},
			})

			Expect(messageBody(msg)).To(HaveKeyWithValue("value", expectedBase64))
			conversionStatus, _ := msg.MetaGet("umh_conversion_status")
			Expect(conversionStatus).To(Equal("success"))
		},
		Entry("Bytes", sparkplugplugin.SparkplugDataTypeBytes, []byte{0x00, 0x01, 0xff}, "AAH/"),
		Entry("File", sparkplugplugin.SparkplugDataTypeFile, []byte("line one\n"), "bGluZSBvbmUK"),
		Entry("empty Bytes stays an empty string, not null", sparkplugplugin.SparkplugDataTypeBytes, []byte{}, ""),
		Entry("UInt64Array [1] as packed little-endian bytes", sparkplugplugin.SparkplugDataTypeUInt64Array,
			[]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, "AQAAAAAAAAA="),
	)

	It("decodes a Bytes value on an alias-only NDATA using the datatype from NBIRTH", func() {
		birthMessage(&sparkplugb.Payload_Metric{
			Name:     stringPtr("payload"),
			Alias:    uint64Ptr(1),
			Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeBytes),
			Value:    &sparkplugb.Payload_Metric_BytesValue{BytesValue: []byte{0x00}},
		})

		dataPayload := &sparkplugb.Payload{Seq: uint64Ptr(1), Metrics: []*sparkplugb.Payload_Metric{{
			Alias: uint64Ptr(1),
			Value: &sparkplugb.Payload_Metric_BytesValue{BytesValue: []byte{0x00, 0x01, 0xff}},
		}}}
		wrapper.ProcessDataMessage(sparkplugplugin.MessageTypeNDATA, dataPayload, topicInfo)
		batch := wrapper.CreateSplitMessages(dataPayload, sparkplugplugin.MessageTypeNDATA, topicInfo, "spBv1.0/Factory/NDATA/Line1")
		Expect(batch).To(HaveLen(1))

		Expect(messageBody(batch[0])).To(HaveKeyWithValue("value", "AAH/"))
		datatypeName, _ := batch[0].MetaGet("spb_datatype")
		Expect(datatypeName).To(Equal("Bytes"))
	})

	DescribeTable("counts a value the UNS conversion cannot represent as a drop",
		func(datatype uint32, value any) {
			metric := &sparkplugb.Payload_Metric{
				Name:     stringPtr("structured"),
				Alias:    uint64Ptr(1),
				Datatype: uint32Ptr(datatype),
			}
			switch typedValue := value.(type) {
			case *sparkplugb.Payload_Metric_DatasetValue:
				metric.Value = typedValue
			case *sparkplugb.Payload_Metric_TemplateValue:
				metric.Value = typedValue
			default:
				Fail("unsupported value type in test entry")
			}

			firstMessage := birthMessage(metric)
			secondMessage := birthMessage(metric)

			Expect(wrapper.GetUnsupportedValueDropsCount()).To(Equal(int64(2)))
			firstStatus, _ := firstMessage.MetaGet("umh_conversion_status")
			secondStatus, _ := secondMessage.MetaGet("umh_conversion_status")
			Expect(firstStatus).To(Equal("failed_unsupported_type"))
			Expect(secondStatus).To(Equal("failed_unsupported_type"))
		},
		Entry("DataSet", sparkplugplugin.SparkplugDataTypeDataSet,
			&sparkplugb.Payload_Metric_DatasetValue{DatasetValue: &sparkplugb.Payload_DataSet{}}),
		Entry("Template", sparkplugplugin.SparkplugDataTypeTemplate,
			&sparkplugb.Payload_Metric_TemplateValue{TemplateValue: &sparkplugb.Payload_Template{}}),
	)

	It("does not count a metric marked is_null as a drop", func() {
		msg := birthMessage(&sparkplugb.Payload_Metric{
			Name:     stringPtr("payload"),
			Alias:    uint64Ptr(1),
			Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeBytes),
			IsNull:   boolPtr(true),
		})

		Expect(wrapper.GetUnsupportedValueDropsCount()).To(BeZero())
		conversionStatus, _ := msg.MetaGet("umh_conversion_status")
		Expect(conversionStatus).To(Equal("failed_no_value"))
	})
})
