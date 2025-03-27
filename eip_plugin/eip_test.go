package eip_plugin_test

import (
	"encoding/json"

	"github.com/danomagnum/gologix"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/united-manufacturing-hub/benthos-umh/eip_plugin"
)

var _ = Describe("EthernetIP Unittests", func() {

	BeforeEach(func() {

	})

	DescribeTable("Test for various datatypes", func(item *CIPReadItem, expectedTagType any, rawValue []byte) {
		msg, err := CreateMessageFromValue(rawValue, item)
		Expect(err).NotTo(HaveOccurred())

		message, err := msg.AsStructuredMut()
		Expect(err).NotTo(HaveOccurred())
		Expect(message).To(BeAssignableToTypeOf(expectedTagType))

	},
		Entry("bool - true",
			&CIPReadItem{
				TagName:     "boolean",
				CIPDatatype: gologix.CIPTypeBOOL,
			},
			true,
			[]byte{0xc1, 0x0, 0x1},
		),
		Entry("int16 - 287",
			json.Number("287"),
			[]byte{0xc3, 0x0, 0x1f, 0x1},
		),
		Entry("int32 - 12345",
			json.Number("12345"),
			[]byte{0xc4, 0x0, 0x39, 0x30, 0x0, 0x0},
		),
		Entry("int64 - 12345678",
			json.Number("12345678"),
			[]byte{0xc5, 0x0, 0x4e, 0x61, 0xbc, 0x0, 0x0, 0x0, 0x0, 0x0},
		),
		// NOTE: currently not working with uint
		//	Entry("uint16 - 287",
		//		[]byte{0xc3, 0x0, 0x1f, 0x1},
		//	),
		//	Entry("uint32 - 287",
		//		[]byte{0xc3, 0x0, 0x1f, 0x1},
		//	),
		//	Entry("uint64 - 287",
		//		[]byte{0xc3, 0x0, 0x1f, 0x1},
		//	),
		Entry("float32 - 543.21",
			json.Number("543.21"),
			[]byte{0xca, 0x0, 0x71, 0xcd, 0x7, 0x44},
		),
		Entry("float64 - 123543.21",
			json.Number("123543.21"),
			[]byte{0xcb, 0x0, 0x14, 0xae, 0x47, 0xe1, 0x1a, 0xff, 0xc3, 0x40},
		),
		Entry("string - Hello World",
			[]byte{0xa0, 0x2, 0xce, 0xf, 0xb, 0x0, 0x0, 0x0, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64}),
	)

})
