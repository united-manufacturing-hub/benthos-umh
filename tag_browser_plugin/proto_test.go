package tag_browser_plugin

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	tagbrowserpluginprotobuf "github.com/united-manufacturing-hub/benthos-umh/tag_browser_plugin/tag_browser_plugin.protobuf"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var _ = Describe("Protobuf Functions", func() {
	Describe("BundleToProtobuf and ProtobufBytesToBundle", func() {
		It("successfully encodes and decodes a bundle with time series data", func() {
			// Create a test bundle
			originalBundle := &tagbrowserpluginprotobuf.UnsBundle{
				UnsMap: &tagbrowserpluginprotobuf.UnsMap{
					Entries: map[string]*tagbrowserpluginprotobuf.UnsInfo{
						"test-topic": {
							Enterprise: "enterprise",
							Schema:     "_historian",
							EventTag:   wrapperspb.String("temperature"),
						},
					},
				},
				Events: &tagbrowserpluginprotobuf.EventTable{
					Entries: []*tagbrowserpluginprotobuf.EventTableEntry{
						{
							IsTimeseries: true,
							TimestampMs:  wrapperspb.Int64(1647753600000),
							Value: &anypb.Any{
								TypeUrl: "golang/float64",
								Value:   []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40}, // 13.0 in float64
							},
						},
					},
				},
			}

			// Encode the bundle
			protoBytes, err := BundleToProtobuf(originalBundle)
			Expect(err).To(BeNil())
			Expect(protoBytes).NotTo(BeNil())

			// Decode the bundle
			decodedBundle, err := ProtobufBytesToBundle(protoBytes)
			Expect(err).To(BeNil())
			Expect(decodedBundle).NotTo(BeNil())

			// Verify the decoded bundle matches the original
			Expect(decodedBundle.UnsMap.Entries).To(HaveLen(1))
			Expect(decodedBundle.UnsMap.Entries["test-topic"].Enterprise).To(Equal(originalBundle.UnsMap.Entries["test-topic"].Enterprise))
			Expect(decodedBundle.Events.Entries).To(HaveLen(1))
		})

		It("handles empty bundle", func() {
			// Create an empty bundle
			originalBundle := &tagbrowserpluginprotobuf.UnsBundle{
				UnsMap: &tagbrowserpluginprotobuf.UnsMap{
					Entries: make(map[string]*tagbrowserpluginprotobuf.UnsInfo),
				},
				Events: &tagbrowserpluginprotobuf.EventTable{
					Entries: make([]*tagbrowserpluginprotobuf.EventTableEntry, 0),
				},
			}

			// Encode the bundle
			protoBytes, err := BundleToProtobuf(originalBundle)
			Expect(err).To(BeNil())
			Expect(protoBytes).NotTo(BeNil())

			// Decode the bundle
			decodedBundle, err := ProtobufBytesToBundle(protoBytes)
			Expect(err).To(BeNil())
			Expect(decodedBundle).NotTo(BeNil())

			// Verify the decoded bundle is empty
			Expect(decodedBundle.UnsMap.Entries).To(BeEmpty())
			Expect(decodedBundle.Events.Entries).To(BeEmpty())
		})

		It("handles bundle with multiple entries", func() {
			// Create a bundle with multiple entries
			originalBundle := &tagbrowserpluginprotobuf.UnsBundle{
				UnsMap: &tagbrowserpluginprotobuf.UnsMap{
					Entries: map[string]*tagbrowserpluginprotobuf.UnsInfo{
						"topic1": {
							Enterprise: "enterprise",
							Schema:     "_historian",
							EventTag:   wrapperspb.String("temperature"),
						},
						"topic2": {
							Enterprise: "enterprise",
							Schema:     "_historian",
							EventTag:   wrapperspb.String("humidity"),
						},
					},
				},
				Events: &tagbrowserpluginprotobuf.EventTable{
					Entries: []*tagbrowserpluginprotobuf.EventTableEntry{
						{
							IsTimeseries: true,
							TimestampMs:  wrapperspb.Int64(1647753600000),
							Value: &anypb.Any{
								TypeUrl: "golang/float64",
								Value:   []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40}, // 13.0 in float64
							},
						},
						{
							IsTimeseries: true,
							TimestampMs:  wrapperspb.Int64(1647753600001),
							Value: &anypb.Any{
								TypeUrl: "golang/float64",
								Value:   []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40}, // 13.0 in float64
							},
						},
					},
				},
			}

			// Encode the bundle
			protoBytes, err := BundleToProtobuf(originalBundle)
			Expect(err).To(BeNil())
			Expect(protoBytes).NotTo(BeNil())

			// Decode the bundle
			decodedBundle, err := ProtobufBytesToBundle(protoBytes)
			Expect(err).To(BeNil())
			Expect(decodedBundle).NotTo(BeNil())

			// Verify the decoded bundle matches the original
			Expect(decodedBundle.UnsMap.Entries).To(HaveLen(2))
			Expect(decodedBundle.UnsMap.Entries["topic1"].Enterprise).To(Equal(originalBundle.UnsMap.Entries["topic1"].Enterprise))
			Expect(decodedBundle.UnsMap.Entries["topic2"].Enterprise).To(Equal(originalBundle.UnsMap.Entries["topic2"].Enterprise))
			Expect(decodedBundle.UnsMap.Entries["topic2"].EventTag.GetValue()).To(Equal(originalBundle.UnsMap.Entries["topic2"].EventTag.GetValue()))
			Expect(decodedBundle.Events.Entries).To(HaveLen(2))
		})

		It("handles invalid protobuf bytes", func() {
			// Try to decode invalid bytes
			invalidBytes := []byte{0x00, 0x01, 0x02, 0x03}
			decodedBundle, err := ProtobufBytesToBundle(invalidBytes)
			Expect(err).NotTo(BeNil())
			Expect(decodedBundle).To(BeNil())
		})
	})
})
