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

package nodered_js_plugin_test

import (
	"context"
	"encoding/base64"

	"github.com/dop251/goja"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin/cache"
	protobufpkg "github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin/protobuf"
)

// minimalDescriptorB64 builds a base64 FileDescriptorSet for testpb.Sample { int32 a = 1 }.
func minimalDescriptorB64() string {
	fds := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name:    proto.String("wiring.proto"),
		Package: proto.String("testpb"),
		Syntax:  proto.String("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Sample"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:   proto.String("a"),
				Number: proto.Int32(1),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
			}},
		}},
	}}}
	b, err := proto.Marshal(fds)
	Expect(err).NotTo(HaveOccurred())
	return base64.StdEncoding.EncodeToString(b)
}

// Proves SetupJSEnvironment registers the `protobuf` namespace (the full decode/encode
// behavior is covered by the protobuf package's own suite). tag_processor calls the same
// SetupJSEnvironment, so this also covers its wiring (ENG-5243).
var _ = Describe("protobuf in the JS environment (ENG-5243)", func() {
	var vm *goja.Runtime

	BeforeEach(func() {
		resources := service.MockResources()
		proc, err := nodered_js_plugin.NewNodeREDJSProcessor("", resources.Logger(), resources.Metrics(), cache.NewMemoryStore(0))
		Expect(err).NotTo(HaveOccurred())

		vm = goja.New()
		Expect(proc.SetupJSEnvironment(context.Background(), vm, map[string]interface{}{})).To(Succeed())
	})

	It("exposes protobuf.decode and protobuf.encode as functions", func() {
		v, err := vm.RunString(`typeof protobuf.decode === "function" && typeof protobuf.encode === "function"`)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.ToBoolean()).To(BeTrue())
	})

	It("throws a catchable error from JS when decode fails", func() {
		v, err := vm.RunString(`(function () {
			try { protobuf.decode("!!not base64!!", "!!not a descriptor!!", "X"); return "no throw"; }
			catch (e) { return "threw"; }
		})()`)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.String()).To(Equal("threw"))
	})

	It("decodes a message through the JS boundary and returns a usable object", func() {
		desc := minimalDescriptorB64()
		encoded, err := protobufpkg.Encode(map[string]any{"a": 7}, desc, "testpb.Sample")
		Expect(err).NotTo(HaveOccurred())

		Expect(vm.Set("DESC", desc)).To(Succeed())
		Expect(vm.Set("ENC", encoded)).To(Succeed())

		v, err := vm.RunString(`protobuf.decode(ENC, DESC, "testpb.Sample").a`)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.ToInteger()).To(Equal(int64(7)))
	})
})
