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

// Sparkplug B metric datatype must survive from BIRTH to DATA.
//
// Per the Sparkplug B spec, NBIRTH/DBIRTH carries alias + name + datatype while
// NDATA/DDATA carries only alias + value; the host is required to remember the
// BIRTH definitions. These tests pin two behaviors:
//  1. AliasCache restores Datatype (not just Name) when resolving DATA aliases.
//  2. Signed integer wire values (two's-complement packed into the unsigned
//     int_value/long_value protobuf fields) are reinterpreted using the datatype,
//     so Int32(-12) surfaces as -12, not 4294967284.

package sparkplug_plugin_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	sparkplugplugin "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

var _ = Describe("Datatype persistence across BIRTH and DATA", func() {
	Describe("AliasCache", func() {
		var cache *sparkplugplugin.AliasCache

		BeforeEach(func() {
			cache = sparkplugplugin.NewAliasCache()
		})

		It("restores the cached datatype when resolving an alias-only DATA metric", func() {
			birthMetrics := []*sparkplugb.Payload_Metric{
				{
					Name:     stringPtr("temp"),
					Alias:    uint64Ptr(1),
					Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeInt32),
				},
			}
			Expect(cache.CacheAliases("Factory/Line1", birthMetrics)).To(Equal(1))

			dataMetrics := []*sparkplugb.Payload_Metric{
				{
					Alias: uint64Ptr(1),
					Value: &sparkplugb.Payload_Metric_IntValue{IntValue: 4294967284}, // -12 as Int32
				},
			}
			Expect(cache.ResolveAliases("Factory/Line1", dataMetrics)).To(Equal(1))

			Expect(dataMetrics[0].Name).NotTo(BeNil())
			Expect(*dataMetrics[0].Name).To(Equal("temp"))
			Expect(dataMetrics[0].Datatype).NotTo(BeNil(),
				"datatype from the BIRTH certificate must be restored on DATA metrics")
			Expect(*dataMetrics[0].Datatype).To(Equal(sparkplugplugin.SparkplugDataTypeInt32))
		})

		It("does not overwrite a datatype the DATA metric already carries", func() {
			birthMetrics := []*sparkplugb.Payload_Metric{
				{
					Name:     stringPtr("temp"),
					Alias:    uint64Ptr(1),
					Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeInt32),
				},
			}
			cache.CacheAliases("Factory/Line1", birthMetrics)

			dataMetrics := []*sparkplugb.Payload_Metric{
				{
					Alias:    uint64Ptr(1),
					Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeInt64),
					Value:    &sparkplugb.Payload_Metric_LongValue{LongValue: 7},
				},
			}
			cache.ResolveAliases("Factory/Line1", dataMetrics)

			Expect(*dataMetrics[0].Datatype).To(Equal(sparkplugplugin.SparkplugDataTypeInt64))
		})

		It("resolves the name even when the BIRTH metric had no datatype", func() {
			birthMetrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("legacy"),
					Alias: uint64Ptr(9),
				},
			}
			cache.CacheAliases("Factory/Line1", birthMetrics)

			dataMetrics := []*sparkplugb.Payload_Metric{
				{
					Alias: uint64Ptr(9),
					Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 1.5},
				},
			}
			Expect(cache.ResolveAliases("Factory/Line1", dataMetrics)).To(Equal(1))
			Expect(*dataMetrics[0].Name).To(Equal("legacy"))
			Expect(dataMetrics[0].Datatype).To(BeNil())
		})
	})

	Describe("NBIRTH→NDATA flow through the input", func() {
		var (
			wrapper   *sparkplugplugin.SparkplugInputTestWrapper
			topicInfo *sparkplugplugin.TopicInfo
		)

		// signed wire value helper: Sparkplug packs signed ints as
		// two's-complement into the unsigned protobuf fields.
		wireInt32 := func(v int32) uint32 { return uint32(v) }

		birth := func(metrics ...*sparkplugb.Payload_Metric) *sparkplugb.Payload {
			return &sparkplugb.Payload{Seq: uint64Ptr(0), Metrics: metrics}
		}
		data := func(seq uint64, metrics ...*sparkplugb.Payload_Metric) *sparkplugb.Payload {
			return &sparkplugb.Payload{Seq: uint64Ptr(seq), Metrics: metrics}
		}

		// runNDATA pushes an NBIRTH (full defs) then an NDATA (alias-only) through
		// the same processing path processSparkplugMessage uses and returns the
		// split messages produced for the NDATA.
		runNDATA := func(birthMetric *sparkplugb.Payload_Metric, dataMetric *sparkplugb.Payload_Metric) (string, map[string]any) {
			birthPayload := birth(birthMetric)
			wrapper.ProcessBirthMessage(sparkplugplugin.MessageTypeNBIRTH, birthPayload, topicInfo)

			dataPayload := data(1, dataMetric)
			wrapper.ProcessDataMessage(sparkplugplugin.MessageTypeNDATA, dataPayload, topicInfo)
			batch := wrapper.CreateSplitMessages(dataPayload, sparkplugplugin.MessageTypeNDATA, topicInfo, "spBv1.0/Factory/NDATA/Line1")
			Expect(batch).To(HaveLen(1))

			msg := batch[0]
			datatypeMeta, _ := msg.MetaGet("spb_datatype")

			raw, err := msg.AsBytes()
			Expect(err).NotTo(HaveOccurred())
			var body map[string]any
			Expect(json.Unmarshal(raw, &body)).To(Succeed())

			return datatypeMeta, body
		}

		BeforeEach(func() {
			wrapper = sparkplugplugin.NewSparkplugInputForTesting()
			topicInfo = &sparkplugplugin.TopicInfo{Group: "Factory", EdgeNode: "Line1"}
		})

		It("sets spb_datatype on NDATA messages from the cached BIRTH definition", func() {
			datatypeMeta, body := runNDATA(
				&sparkplugb.Payload_Metric{
					Name:     stringPtr("temp"),
					Alias:    uint64Ptr(1),
					Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeInt32),
					Value:    &sparkplugb.Payload_Metric_IntValue{IntValue: wireInt32(-7)},
				},
				&sparkplugb.Payload_Metric{
					Alias: uint64Ptr(1),
					Value: &sparkplugb.Payload_Metric_IntValue{IntValue: wireInt32(-12)},
				},
			)

			Expect(datatypeMeta).To(Equal("Int32"),
				"spb_datatype must be present on NDATA, sourced from the NBIRTH certificate")
			Expect(body["name"]).To(Equal("temp"))
		})

		// setWireValue assigns an int_value/long_value oneof to a metric. The oneof
		// interface type is unexported in the generated code, so entries pass the
		// exported wrapper structs as any.
		setWireValue := func(m *sparkplugb.Payload_Metric, wire any) {
			switch v := wire.(type) {
			case *sparkplugb.Payload_Metric_IntValue:
				m.Value = v
			case *sparkplugb.Payload_Metric_LongValue:
				m.Value = v
			default:
				Fail("unsupported wire value type in test entry")
			}
		}

		DescribeTable(
			"decodes signed integer wire values using the BIRTH datatype",
			func(datatype uint32, wire any, expected float64) {
				birthMetric := &sparkplugb.Payload_Metric{
					Name:     stringPtr("temp"),
					Alias:    uint64Ptr(1),
					Datatype: uint32Ptr(datatype),
				}
				setWireValue(birthMetric, wire)

				dataMetric := &sparkplugb.Payload_Metric{
					Alias: uint64Ptr(1),
				}
				setWireValue(dataMetric, wire)

				_, body := runNDATA(birthMetric, dataMetric)

				Expect(body["value"]).To(BeNumerically("==", expected))
			},
			Entry("Int8 -7", sparkplugplugin.SparkplugDataTypeInt8,
				&sparkplugb.Payload_Metric_IntValue{IntValue: 249}, float64(-7)),
			Entry("Int16 -7", sparkplugplugin.SparkplugDataTypeInt16,
				&sparkplugb.Payload_Metric_IntValue{IntValue: 65529}, float64(-7)),
			Entry("Int32 -12", sparkplugplugin.SparkplugDataTypeInt32,
				&sparkplugb.Payload_Metric_IntValue{IntValue: 4294967284}, float64(-12)),
			Entry("Int64 -12", sparkplugplugin.SparkplugDataTypeInt64,
				&sparkplugb.Payload_Metric_LongValue{LongValue: 18446744073709551604}, float64(-12)),
			Entry("Int32 positive stays positive", sparkplugplugin.SparkplugDataTypeInt32,
				&sparkplugb.Payload_Metric_IntValue{IntValue: 42}, float64(42)),
			Entry("UInt32 unchanged", sparkplugplugin.SparkplugDataTypeUInt32,
				&sparkplugb.Payload_Metric_IntValue{IntValue: 4294967284}, float64(4294967284)),
			Entry("UInt64 unchanged", sparkplugplugin.SparkplugDataTypeUInt64,
				&sparkplugb.Payload_Metric_LongValue{LongValue: 7}, float64(7)),
		)

		It("decodes signed values correctly on the BIRTH message itself", func() {
			birthPayload := birth(&sparkplugb.Payload_Metric{
				Name:     stringPtr("temp"),
				Alias:    uint64Ptr(1),
				Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeInt32),
				Value:    &sparkplugb.Payload_Metric_IntValue{IntValue: wireInt32(-7)},
			})
			wrapper.ProcessBirthMessage(sparkplugplugin.MessageTypeNBIRTH, birthPayload, topicInfo)
			batch := wrapper.CreateSplitMessages(birthPayload, sparkplugplugin.MessageTypeNBIRTH, topicInfo, "spBv1.0/Factory/NBIRTH/Line1")
			Expect(batch).To(HaveLen(1))

			raw, err := batch[0].AsBytes()
			Expect(err).NotTo(HaveOccurred())
			var body map[string]any
			Expect(json.Unmarshal(raw, &body)).To(Succeed())

			Expect(body["value"]).To(BeNumerically("==", float64(-7)))
		})

		It("preserves float values untouched on NDATA", func() {
			datatypeMeta, body := runNDATA(
				&sparkplugb.Payload_Metric{
					Name:     stringPtr("flow"),
					Alias:    uint64Ptr(2),
					Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeFloat),
					Value:    &sparkplugb.Payload_Metric_FloatValue{FloatValue: 3.5},
				},
				&sparkplugb.Payload_Metric{
					Alias: uint64Ptr(2),
					Value: &sparkplugb.Payload_Metric_FloatValue{FloatValue: 3.5},
				},
			)

			Expect(datatypeMeta).To(Equal("Float"))
			Expect(body["value"]).To(BeNumerically("==", 3.5))
		})

		// DDATA shares processDataMessage/resolveAliases/extractMetricValue with
		// NDATA, so the BIRTH-datatype fix applies to device metrics too; this
		// spec locks that in through the device-level path (aliases are cached
		// per device from DBIRTH).
		It("sets spb_datatype and decodes signed values on DDATA from the DBIRTH definition", func() {
			deviceTopicInfo := &sparkplugplugin.TopicInfo{Group: "Factory", EdgeNode: "Line1", Device: "Device1"}

			birthPayload := birth(&sparkplugb.Payload_Metric{
				Name:     stringPtr("temp"),
				Alias:    uint64Ptr(1),
				Datatype: uint32Ptr(sparkplugplugin.SparkplugDataTypeInt32),
				Value:    &sparkplugb.Payload_Metric_IntValue{IntValue: wireInt32(-7)},
			})
			wrapper.ProcessBirthMessage(sparkplugplugin.MessageTypeDBIRTH, birthPayload, deviceTopicInfo)

			dataPayload := data(1, &sparkplugb.Payload_Metric{
				Alias: uint64Ptr(1),
				Value: &sparkplugb.Payload_Metric_IntValue{IntValue: wireInt32(-12)},
			})
			wrapper.ProcessDataMessage(sparkplugplugin.MessageTypeDDATA, dataPayload, deviceTopicInfo)
			batch := wrapper.CreateSplitMessages(dataPayload, sparkplugplugin.MessageTypeDDATA, deviceTopicInfo, "spBv1.0/Factory/DDATA/Line1/Device1")
			Expect(batch).To(HaveLen(1))

			msg := batch[0]
			datatypeMeta, _ := msg.MetaGet("spb_datatype")
			Expect(datatypeMeta).To(Equal("Int32"),
				"spb_datatype must be present on DDATA, sourced from the DBIRTH certificate")

			raw, err := msg.AsBytes()
			Expect(err).NotTo(HaveOccurred())
			var body map[string]any
			Expect(json.Unmarshal(raw, &body)).To(Succeed())
			Expect(body["name"]).To(Equal("temp"))
			Expect(body["value"]).To(BeNumerically("==", float64(-12)))
		})
	})
})
