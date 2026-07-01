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

package protobuf_test

import (
	"encoding/base64"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	pb "github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin/protobuf"
)

// descriptorSetB64 hand-builds a self-contained proto2 FileDescriptorSet — no protoc.
// Message: testpb.Sample { optional int32 a = 1; extensions 100 to max; }
// Extension: extend Sample { optional int64 ext_field = 100; } (an unregistered
// extension is exactly the shape this feature must surface as a "[pkg.ext]" key).
func descriptorSetB64() string {
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test.proto"),
		Package: proto.String("testpb"),
		Syntax:  proto.String("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Sample"),
				Field: []*descriptorpb.FieldDescriptorProto{{
					Name:   proto.String("a"),
					Number: proto.Int32(1),
					Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
				}},
				ExtensionRange: []*descriptorpb.DescriptorProto_ExtensionRange{{
					Start: proto.Int32(100),
					End:   proto.Int32(536870912), // max extension number + 1
				}},
			},
			{
				// Holder declares a message-NESTED extension of Sample, exercising the
				// recursive extension walk (a top-level extend would not).
				Name: proto.String("Holder"),
				Extension: []*descriptorpb.FieldDescriptorProto{{
					Name:     proto.String("nested_ext"),
					Number:   proto.Int32(101),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					Extendee: proto.String(".testpb.Sample"),
				}},
			},
		},
		Extension: []*descriptorpb.FieldDescriptorProto{{
			Name:     proto.String("ext_field"),
			Number:   proto.Int32(100),
			Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:     descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
			Extendee: proto.String(".testpb.Sample"), // fully qualified, leading dot
		}},
	}
	fds := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{fdp}}
	b, err := proto.Marshal(fds)
	Expect(err).NotTo(HaveOccurred())
	return base64.StdEncoding.EncodeToString(b)
}

var _ = Describe("protobuf decode/encode (ENG-5243)", func() {
	const msgName = "testpb.Sample" // bare FQN, no leading dot
	var desc string

	BeforeEach(func() {
		desc = descriptorSetB64()
	})

	It("round-trips a message including top-level and message-nested proto2 extensions", func() {
		obj := map[string]any{
			"a":                          1,
			"[testpb.ext_field]":         "123",   // top-level extension
			"[testpb.Holder.nested_ext]": "hello", // message-nested extension
		}

		encoded, err := pb.Encode(obj, desc, msgName)
		Expect(err).NotTo(HaveOccurred())
		Expect(encoded).NotTo(BeEmpty())

		decoded, err := pb.Decode(encoded, desc, msgName)
		Expect(err).NotTo(HaveOccurred())
		Expect(decoded).To(HaveKeyWithValue("a", BeNumerically("==", 1)))
		Expect(decoded).To(HaveKeyWithValue("[testpb.ext_field]", "123"),
			"the top-level extension must surface as a bracketed key")
		Expect(decoded).To(HaveKeyWithValue("[testpb.Holder.nested_ext]", "hello"),
			"the message-nested extension must surface too (recursive registration)")
	})

	It("errors when the name resolves to a non-message (not a panic)", func() {
		encoded, err := pb.Encode(map[string]any{"a": 1}, desc, msgName)
		Expect(err).NotTo(HaveOccurred())
		_, err = pb.Decode(encoded, desc, "testpb.ext_field") // an extension field, not a message
		Expect(err).To(MatchError(ContainSubstring("is not a message")))
	})

	It("errors on invalid base64 data", func() {
		_, err := pb.Decode("!!!not base64!!!", desc, msgName)
		Expect(err).To(HaveOccurred())
	})

	It("errors on an unknown message name", func() {
		encoded, err := pb.Encode(map[string]any{"a": 1}, desc, msgName)
		Expect(err).NotTo(HaveOccurred())
		_, err = pb.Decode(encoded, desc, "testpb.DoesNotExist")
		Expect(err).To(HaveOccurred())
	})

	It("errors on a malformed descriptor set", func() {
		_, err := pb.Decode("AAAA", "not a valid descriptor set", msgName)
		Expect(err).To(HaveOccurred())
	})

	It("errors when the encode input is not an object", func() {
		_, err := pb.Encode("not an object", desc, msgName)
		Expect(err).To(MatchError(ContainSubstring("must be an object")))
	})

	It("returns identical results across calls (memoized registry is transparent)", func() {
		encoded, err := pb.Encode(map[string]any{"a": 5, "[testpb.ext_field]": "42"}, desc, msgName)
		Expect(err).NotTo(HaveOccurred())

		d1, err := pb.Decode(encoded, desc, msgName)
		Expect(err).NotTo(HaveOccurred())
		d2, err := pb.Decode(encoded, desc, msgName)
		Expect(err).NotTo(HaveOccurred())
		Expect(d1).To(Equal(d2))
	})
})
