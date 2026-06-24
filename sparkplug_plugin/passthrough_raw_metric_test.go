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

// Unit tests for the passthrough_raw_metric flag (ENG-5242).
//
// The flag attaches the whole re-marshaled Payload.Metric as base64 metadata
// (spb_metric_raw) so a downstream decoder (ENG-5243) can read proto2 extension
// fields the plugin's generated types don't declare. The test simulates an
// extension with an unknown field 9 nested inside MetaData (the proto declares
// `extensions 9 to max` at sparkplug_b_official.proto:62). Because that field is
// nested, only re-marshaling the whole metric preserves it; the metric's
// top-level GetUnknown() never sees it.

package sparkplug_plugin_test

import (
	"encoding/base64"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	sparkplugplugin "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

var _ = Describe("Sparkplug B passthrough_raw_metric (ENG-5242)", func() {
	const metaExtFieldNumber = 9 // a MetaData extension field number (proto: extensions 9 to max)
	topicInfo := &sparkplugplugin.TopicInfo{Group: "test", EdgeNode: "edge1", Device: "dev1"}

	// buildMetricWithExtension returns a metric carrying a declared name+value plus a
	// proto2 extension simulated as an unknown field nested inside MetaData, and the
	// raw bytes of that nested extension.
	buildMetricWithExtension := func() (*sparkplugb.Payload_Metric, []byte) {
		ext := protowire.AppendTag(nil, metaExtFieldNumber, protowire.VarintType)
		ext = protowire.AppendVarint(ext, 1730986400123456789)

		metric := &sparkplugb.Payload_Metric{
			Name:     stringPtr("temperature"),
			Datatype: uint32Ptr(SparkplugDataTypeInt32),
			Value:    &sparkplugb.Payload_Metric_IntValue{IntValue: 42},
			Metadata: &sparkplugb.Payload_MetaData{},
		}
		metric.Metadata.ProtoReflect().SetUnknown(protoreflect.RawFields(ext))
		return metric, ext
	}

	newPayload := func(m *sparkplugb.Payload_Metric) *sparkplugb.Payload {
		seq := uint64(1)
		ts := uint64(1730986400000)
		return &sparkplugb.Payload{Seq: &seq, Timestamp: &ts, Metrics: []*sparkplugb.Payload_Metric{m}}
	}

	When("passthrough_raw_metric is enabled", func() {
		It("attaches the whole re-marshaled metric as base64 spb_metric_raw, preserving the nested extension", func() {
			metric, extBytes := buildMetricWithExtension()
			input := createMockSparkplugInput()
			input.SetPassthroughRawMetric(true)

			batch := input.CreateSplitMessages(newPayload(metric), "NDATA", topicInfo, "spBv1.0/test/NDATA/edge1/dev1")
			Expect(batch).To(HaveLen(1))

			raw, ok := batch[0].MetaGet("spb_metric_raw")
			Expect(ok).To(BeTrue(), "spb_metric_raw must be set when passthrough is enabled")

			decodedBytes, err := base64.StdEncoding.DecodeString(raw)
			Expect(err).NotTo(HaveOccurred(), "spb_metric_raw must be valid base64")

			var decoded sparkplugb.Payload_Metric
			Expect(proto.Unmarshal(decodedBytes, &decoded)).To(Succeed())

			Expect(proto.Equal(metric, &decoded)).To(BeTrue(), "round-trip must reproduce the full metric, including unknown fields")
			Expect(decoded.GetMetadata()).NotTo(BeNil(), "MetaData must survive the round-trip")
			Expect([]byte(decoded.GetMetadata().ProtoReflect().GetUnknown())).To(Equal(extBytes),
				"the nested MetaData extension bytes must survive marshal->unmarshal")
		})
	})

	When("passthrough_raw_metric is disabled (default)", func() {
		It("does not attach spb_metric_raw", func() {
			metric, _ := buildMetricWithExtension()
			input := createMockSparkplugInput() // default: passthrough off

			batch := input.CreateSplitMessages(newPayload(metric), "NDATA", topicInfo, "spBv1.0/test/NDATA/edge1/dev1")
			Expect(batch).To(HaveLen(1))

			_, ok := batch[0].MetaGet("spb_metric_raw")
			Expect(ok).To(BeFalse(), "spb_metric_raw must be absent when passthrough is disabled")

			// Spec property 2: the standard decoded output is unchanged when the flag is off.
			name, nameOK := batch[0].MetaGet("spb_metric_name")
			Expect(nameOK).To(BeTrue())
			Expect(name).To(Equal("temperature"), "standard metadata must be untouched")
			datatype, dtOK := batch[0].MetaGet("spb_datatype")
			Expect(dtOK).To(BeTrue(), "standard metadata must be untouched")
			Expect(datatype).NotTo(BeEmpty())
			payloadBytes, err := batch[0].AsBytes()
			Expect(err).NotTo(HaveOccurred())
			Expect(payloadBytes).NotTo(BeEmpty(), "decoded value payload must still be present")
		})
	})
})
