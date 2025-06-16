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
	"google.golang.org/protobuf/types/known/anypb"
)

var _ = Describe("Protobuf Functions", func() {
	Describe("bundleToProtobuf and protobufBytesToBundle", func() {
		It("successfully encodes and decodes a bundle with time series data", func() {
			// Create a test bundle
			originalBundle := &UnsBundle{
				UnsMap: &TopicMap{
					Entries: map[string]*TopicInfo{
						"test-topic": {
							Level0:       "enterprise",
							DataContract: "_historian",
						},
					},
				},
				Events: &EventTable{
					Entries: []*EventTableEntry{
						{
							UnsTreeId: "test-topic",
							Payload: &EventTableEntry_Ts{
								Ts: &TimeSeriesPayload{
									ScalarType:  ScalarType_NUMERIC,
									TimestampMs: 1647753600000,
									Value: &anypb.Any{
										TypeUrl: "golang/float64",
										Value:   []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40}, // 13.0 in float64
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
			originalBundle := &UnsBundle{
				UnsMap: &TopicMap{
					Entries: make(map[string]*TopicInfo),
				},
				Events: &EventTable{
					Entries: make([]*EventTableEntry, 0),
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
			originalBundle := &UnsBundle{
				UnsMap: &TopicMap{
					Entries: map[string]*TopicInfo{
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
				Events: &EventTable{
					Entries: []*EventTableEntry{
						{
							UnsTreeId: "topic1",
							Payload: &EventTableEntry_Ts{
								Ts: &TimeSeriesPayload{
									ScalarType:  ScalarType_NUMERIC,
									TimestampMs: 1647753600000,
									Value: &anypb.Any{
										TypeUrl: "golang/float64",
										Value:   []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40}, // 13.0 in float64
									},
								},
							},
						},
						{
							UnsTreeId: "topic2",
							Payload: &EventTableEntry_Ts{
								Ts: &TimeSeriesPayload{
									ScalarType:  ScalarType_NUMERIC,
									TimestampMs: 1647753600001,
									Value: &anypb.Any{
										TypeUrl: "golang/float64",
										Value:   []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40}, // 13.0 in float64
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
			Expect(decodedBundle.UnsMap.Entries["topic1"].Level0).To(Equal(originalBundle.UnsMap.Entries["topic1"].Level0))
			Expect(decodedBundle.UnsMap.Entries["topic2"].Level0).To(Equal(originalBundle.UnsMap.Entries["topic2"].Level0))
			Expect(decodedBundle.Events.Entries).To(HaveLen(2))
		})

		It("handles invalid protobuf bytes", func() {
			// Try to decode invalid bytes
			invalidBytes := []byte{0x00, 0x01, 0x02, 0x03}
			decodedBundle, err := protobufBytesToBundle(invalidBytes)
			Expect(err).NotTo(BeNil())
			Expect(decodedBundle).To(BeNil())
		})
	})
})
