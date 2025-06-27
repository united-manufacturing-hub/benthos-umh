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

package topic_browser_plugin

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var _ = Describe("Protobuf Bundle Operations", func() {
	Context("Bundle serialization and deserialization", func() {
		It("successfully encodes and decodes a bundle with time series data", func() {
			// Create a test bundle
			originalBundle := &proto.UnsBundle{
				UnsMap: &proto.TopicMap{
					Entries: map[string]*proto.TopicInfo{
						"test-topic": {
							Level0:       "enterprise",
							DataContract: "_historian",
						},
					},
				},
				Events: &proto.EventTable{
					Entries: []*proto.EventTableEntry{
						{
							UnsTreeId: "test-topic",
							Payload: &proto.EventTableEntry_Ts{
								Ts: &proto.TimeSeriesPayload{
									ScalarType:  proto.ScalarType_NUMERIC,
									TimestampMs: 1647753600000,
									Value: &proto.TimeSeriesPayload_NumericValue{
										NumericValue: &wrapperspb.DoubleValue{Value: 13.0},
									},
								},
							},
						},
					},
				},
			}

			// Encode the bundle
			protoBytes, err := bundleToProtobuf(originalBundle)
			Expect(err).To(BeNil())
			Expect(protoBytes).NotTo(BeNil())

			// Decode the bundle
			decodedBundle, err := protobufBytesToBundle(protoBytes)
			Expect(err).To(BeNil())
			Expect(decodedBundle).NotTo(BeNil())

			// Verify the decoded bundle matches the original
			Expect(decodedBundle.UnsMap.Entries).To(HaveLen(1))
			Expect(decodedBundle.UnsMap.Entries["test-topic"].Level0).To(Equal(originalBundle.UnsMap.Entries["test-topic"].Level0))
			Expect(decodedBundle.Events.Entries).To(HaveLen(1))
		})

		It("handles empty bundle", func() {
			// Create an empty bundle
			originalBundle := &proto.UnsBundle{
				UnsMap: &proto.TopicMap{
					Entries: make(map[string]*proto.TopicInfo),
				},
				Events: &proto.EventTable{
					Entries: make([]*proto.EventTableEntry, 0),
				},
			}

			// Encode the bundle
			protoBytes, err := bundleToProtobuf(originalBundle)
			Expect(err).To(BeNil())
			Expect(protoBytes).NotTo(BeNil())

			// Decode the bundle
			decodedBundle, err := protobufBytesToBundle(protoBytes)
			Expect(err).To(BeNil())
			Expect(decodedBundle).NotTo(BeNil())

			// Verify the decoded bundle is empty
			Expect(decodedBundle.UnsMap.Entries).To(BeEmpty())
			Expect(decodedBundle.Events.Entries).To(BeEmpty())
		})

		It("handles bundle with multiple entries", func() {
			// Create a bundle with multiple entries
			originalBundle := &proto.UnsBundle{
				UnsMap: &proto.TopicMap{
					Entries: map[string]*proto.TopicInfo{
						"topic1": {
							Level0:       "enterprise",
							DataContract: "_historian",
						},
						"topic2": {
							Level0:       "enterprise",
							DataContract: "_historian",
						},
					},
				},
				Events: &proto.EventTable{
					Entries: []*proto.EventTableEntry{
						{
							UnsTreeId: "topic1",
							Payload: &proto.EventTableEntry_Ts{
								Ts: &proto.TimeSeriesPayload{
									ScalarType:  proto.ScalarType_NUMERIC,
									TimestampMs: 1647753600000,
									Value: &proto.TimeSeriesPayload_NumericValue{
										NumericValue: &wrapperspb.DoubleValue{Value: 13.0},
									},
								},
							},
						},
						{
							UnsTreeId: "topic2",
							Payload: &proto.EventTableEntry_Ts{
								Ts: &proto.TimeSeriesPayload{
									ScalarType:  proto.ScalarType_NUMERIC,
									TimestampMs: 1647753600001,
									Value: &proto.TimeSeriesPayload_NumericValue{
										NumericValue: &wrapperspb.DoubleValue{Value: 13.0},
									},
								},
							},
						},
					},
				},
			}

			// Encode the bundle
			protoBytes, err := bundleToProtobuf(originalBundle)
			Expect(err).To(BeNil())
			Expect(protoBytes).NotTo(BeNil())

			// Decode the bundle
			decodedBundle, err := protobufBytesToBundle(protoBytes)
			Expect(err).To(BeNil())
			Expect(decodedBundle).NotTo(BeNil())

			// Verify the decoded bundle matches the original
			Expect(decodedBundle.UnsMap.Entries).To(HaveLen(2))
			Expect(decodedBundle.Events.Entries).To(HaveLen(2))
		})
	})
})
