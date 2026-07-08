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

package sparkplug_plugin

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

// metricWithMetaExt builds a Sparkplug metric whose per-metric MetaData carries a proto2
// extension at field 9 (the customer's extra_value), as a device would send on the wire.
func metricWithMetaExt(tsNs int64) *sparkplugb.Payload_Metric {
	var meta []byte
	meta = protowire.AppendTag(meta, 9, protowire.VarintType)
	meta = protowire.AppendVarint(meta, uint64(tsNs))

	var metric []byte
	metric = protowire.AppendTag(metric, 1, protowire.BytesType) // name
	metric = protowire.AppendString(metric, "temp")
	metric = protowire.AppendTag(metric, 8, protowire.BytesType) // metadata
	metric = protowire.AppendBytes(metric, meta)

	var m sparkplugb.Payload_Metric
	Expect(proto.Unmarshal(metric, &m)).To(Succeed())
	return &m
}

// metricWithValueExt builds a metric whose value is a MetricValueExtension carrying a
// message-typed extension (can = CanMessage{id}).
func metricWithValueExt(id uint32) *sparkplugb.Payload_Metric {
	var can []byte
	can = protowire.AppendTag(can, 1, protowire.VarintType)
	can = protowire.AppendVarint(can, uint64(id))
	var extVal []byte
	extVal = protowire.AppendTag(extVal, 1, protowire.BytesType) // the 'can' extension
	extVal = protowire.AppendBytes(extVal, can)

	var metric []byte
	metric = protowire.AppendTag(metric, 1, protowire.BytesType)
	metric = protowire.AppendString(metric, "canmetric")
	metric = protowire.AppendTag(metric, 19, protowire.BytesType) // extension_value
	metric = protowire.AppendBytes(metric, extVal)

	var m sparkplugb.Payload_Metric
	Expect(proto.Unmarshal(metric, &m)).To(Succeed())
	return &m
}

func plainMetric() *sparkplugb.Payload_Metric {
	var metric []byte
	metric = protowire.AppendTag(metric, 1, protowire.BytesType)
	metric = protowire.AppendString(metric, "temp")
	var m sparkplugb.Payload_Metric
	Expect(proto.Unmarshal(metric, &m)).To(Succeed())
	return &m
}

const metaExtSnippet = `package example;
extend org.eclipse.tahu.protobuf.Payload.MetaData {
  optional int64 extra_value = 9;
}
`

const valueExtSnippet = `package example;
message CanMessage { optional uint32 id = 1; }
extend org.eclipse.tahu.protobuf.Payload.MetaData {
  optional int64 extra_value = 9;
}
extend org.eclipse.tahu.protobuf.Payload.MetricValueExtension {
  optional CanMessage can = 1;
}
`

var _ = Describe("Sparkplug extension decode (ENG-5229)", func() {
	Describe("decode", func() {
		It("surfaces a scalar MetaData extension as a flat leaf key and in the JSON blob", func() {
			d, err := newExtensionDecoder(metaExtSnippet)
			Expect(err).NotTo(HaveOccurred())

			flat, decoded, present, err := d.decode(metricWithMetaExt(1719300000123456))
			Expect(err).NotTo(HaveOccurred())
			Expect(present).To(BeTrue())
			Expect(flat).To(HaveKeyWithValue("extra_value", "1719300000123456"))
			// protojson inserts nondeterministic whitespace after the colon, so match with \s*.
			Expect(decoded).To(MatchRegexp(`"\[example\.extra_value\]":\s*"1719300000123456"`))
		})

		It("puts a message-typed extension only in the JSON blob, not in the flat keys", func() {
			d, err := newExtensionDecoder(valueExtSnippet)
			Expect(err).NotTo(HaveOccurred())

			flat, decoded, present, err := d.decode(metricWithValueExt(512))
			Expect(err).NotTo(HaveOccurred())
			Expect(present).To(BeTrue())
			Expect(flat).NotTo(HaveKey("can"))
			// The message-typed extension must carry its decoded field, not just the key.
			Expect(decoded).To(MatchRegexp(`"\[example\.can\]":\s*\{\s*"id":\s*512\s*\}`))
		})

		It("reports not-present for a metric carrying no extension", func() {
			d, err := newExtensionDecoder(metaExtSnippet)
			Expect(err).NotTo(HaveOccurred())

			flat, decoded, present, err := d.decode(plainMetric())
			Expect(err).NotTo(HaveOccurred())
			Expect(present).To(BeFalse())
			Expect(flat).To(BeEmpty())
			Expect(decoded).To(BeEmpty())
		})
	})

	Describe("carriesExtension guard", func() {
		It("is false for a plain metric, so decode skips the re-marshal", func() {
			// The guard's premise: a metric with no extension leaves no unknown bytes on the
			// messages decode walks. If this ever regresses, decode would do needless work.
			Expect(carriesExtension(plainMetric())).To(BeFalse())
		})

		It("is true when MetaData carries extension bytes", func() {
			Expect(carriesExtension(metricWithMetaExt(123))).To(BeTrue())
		})

		It("is true when the value extension carries extension bytes", func() {
			Expect(carriesExtension(metricWithValueExt(512))).To(BeTrue())
		})
	})

	Describe("newExtensionDecoder validation", func() {
		It("rejects a snippet that declares its own syntax", func() {
			_, err := newExtensionDecoder("syntax = \"proto3\";\n" + metaExtSnippet)
			Expect(err).To(MatchError(ContainSubstring("syntax")))
		})

		It("rejects a snippet that imports the Sparkplug schema itself", func() {
			snippet := "package example;\nimport \"" + extSparkplugImportPath + "\";\n" +
				"extend org.eclipse.tahu.protobuf.Payload.MetaData { optional int64 extra_value = 9; }\n"
			_, err := newExtensionDecoder(snippet)
			Expect(err).To(MatchError(ContainSubstring("do not import")))
		})

		It("rejects a snippet that declares no extensions", func() {
			_, err := newExtensionDecoder("package example;\nmessage Foo { optional int32 a = 1; }\n")
			Expect(err).To(MatchError(ContainSubstring("no extensions")))
		})

		It("rejects a schema that only extends an unsupported message", func() {
			// Extends a customer-defined message rather than Payload.MetaData or
			// Payload.MetricValueExtension. It compiles and registers an extension, but decode
			// never walks it, so it could never produce output — reject it at startup.
			snippet := `package example;
message Foo { extensions 1 to max; }
extend Foo {
  optional int64 bar = 1;
}
`
			_, err := newExtensionDecoder(snippet)
			Expect(err).To(MatchError(ContainSubstring("no extensions")))
		})

		It("ignores an unsupported extendee but accepts a supported one alongside it", func() {
			snippet := `package example;
message Foo { extensions 1 to max; }
extend Foo {
  optional int64 bar = 1;
}
extend org.eclipse.tahu.protobuf.Payload.MetaData {
  optional int64 extra_value = 9;
}
`
			_, err := newExtensionDecoder(snippet)
			Expect(err).NotTo(HaveOccurred())
		})

		It("rejects two scalar extensions that collide on the same leaf key", func() {
			// Distinct fully-qualified names (example.extra_value vs example.Holder.extra_value)
			// — so protocompile accepts them — but the same leaf, which our check rejects.
			snippet := `package example;
extend org.eclipse.tahu.protobuf.Payload.MetaData {
  optional int64 extra_value = 9;
}
message Holder {
  extend org.eclipse.tahu.protobuf.Payload.MetricValueExtension {
    optional int64 extra_value = 1;
  }
}
`
			_, err := newExtensionDecoder(snippet)
			Expect(err).To(MatchError(ContainSubstring("spb_ext_extra_value")))
		})

		It("surfaces a parse error against the customer's snippet line", func() {
			// The invalid token is on line 2 of the customer's snippet; the reported location must
			// be remapped to line 2, not the assembled line that includes the injected preamble.
			_, err := newExtensionDecoder("package example;\nthis is not valid proto\n")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("compile extension schema"))
			Expect(err.Error()).To(ContainSubstring("extensions:2:"))
		})
	})
})

// BenchmarkDecodeNoExtension measures the common case: the feature is on but the metric carries
// no extension. The carriesExtension guard should keep this off the re-marshal/decode path.
func BenchmarkDecodeNoExtension(b *testing.B) {
	RegisterTestingT(b)
	d, err := newExtensionDecoder(metaExtSnippet)
	Expect(err).NotTo(HaveOccurred())
	metric := plainMetric()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _ = d.decode(metric)
	}
}

// BenchmarkDecodeWithExtension is the contrast: a metric that does carry an extension still pays
// the full marshal/decode/json cost.
func BenchmarkDecodeWithExtension(b *testing.B) {
	RegisterTestingT(b)
	d, err := newExtensionDecoder(metaExtSnippet)
	Expect(err).NotTo(HaveOccurred())
	metric := metricWithMetaExt(1719300000123456)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _ = d.decode(metric)
	}
}
