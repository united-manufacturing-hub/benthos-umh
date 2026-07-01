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

package protobuf

import (
	"encoding/base64"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func descriptorForPackage(pkg string) string {
	fds := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name:    proto.String(pkg + ".proto"),
		Package: proto.String(pkg),
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

var _ = Describe("registry cache bound (ENG-5243)", func() {
	It("never grows past maxCachedRegistries despite many distinct descriptor sets", func() {
		for i := 0; i < maxCachedRegistries+50; i++ {
			_, err := loadRegistry(descriptorForPackage(fmt.Sprintf("testpkg%d", i)))
			Expect(err).NotTo(HaveOccurred())
		}

		registryMu.Lock()
		n := len(registryCache)
		registryMu.Unlock()
		Expect(n).To(BeNumerically("<=", maxCachedRegistries),
			"the cache must stay bounded so an untrusted script can't exhaust memory")
	})
})
