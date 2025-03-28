package eip_plugin_test

import (
	"reflect"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	. "github.com/united-manufacturing-hub/benthos-umh/eip_plugin"
)

var _ = Describe("EthernetIP Unittests", func() {

	BeforeEach(func() {

	})

	//	DescribeTable("Test for various datatypes", func(item *CIPReadItem, expectedTagType any, rawValue []byte) {
	//		msg, err := CreateMessageFromValue(rawValue, item)
	//		Expect(err).NotTo(HaveOccurred())
	//
	//		message, err := msg.AsStructuredMut()
	//		Expect(err).NotTo(HaveOccurred())
	//		Expect(message).To(BeAssignableToTypeOf(expectedTagType))
	//
	//	},
	//		Entry("bool - true",
	//			&CIPReadItem{
	//				TagName:     "boolean",
	//				CIPDatatype: gologix.CIPTypeBOOL,
	//			},
	//			true,
	//			[]byte{0xc1, 0x0, 0x1},
	//		),
	//		Entry("int16 - 287",
	//			json.Number("287"),
	//			[]byte{0xc3, 0x0, 0x1f, 0x1},
	//		),
	//		Entry("int32 - 12345",
	//			json.Number("12345"),
	//			[]byte{0xc4, 0x0, 0x39, 0x30, 0x0, 0x0},
	//		),
	//		Entry("int64 - 12345678",
	//			json.Number("12345678"),
	//			[]byte{0xc5, 0x0, 0x4e, 0x61, 0xbc, 0x0, 0x0, 0x0, 0x0, 0x0},
	//		),
	//		// NOTE: currently not working with uint
	//		//	Entry("uint16 - 287",
	//		//		[]byte{0xc3, 0x0, 0x1f, 0x1},
	//		//	),
	//		//	Entry("uint32 - 287",
	//		//		[]byte{0xc3, 0x0, 0x1f, 0x1},
	//		//	),
	//		//	Entry("uint64 - 287",
	//		//		[]byte{0xc3, 0x0, 0x1f, 0x1},
	//		//	),
	//		Entry("float32 - 543.21",
	//			json.Number("543.21"),
	//			[]byte{0xca, 0x0, 0x71, 0xcd, 0x7, 0x44},
	//		),
	//		Entry("float64 - 123543.21",
	//			json.Number("123543.21"),
	//			[]byte{0xcb, 0x0, 0x14, 0xae, 0x47, 0xe1, 0x1a, 0xff, 0xc3, 0x40},
	//		),
	//		Entry("string - Hello World",
	//			[]byte{0xa0, 0x2, 0xce, 0xf, 0xb, 0x0, 0x0, 0x0, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64}),
	//	)

	Describe("", func() {
		It("Should correctly parse the input-yaml and create a new EIPInput", func() {
			confYAML := `
endpoint: 127.0.0.1
path: "1,0"
pollRate: 1000
tags:
  - name: bool
    type: bool
  - name: byte
    type: byte
  - name: int8
    type: int8
  - name: int16
    type: int16
  - name: int32
    type: int32
  - name: int64
    type: int64
  - name: uint8
    type: uint8
  - name: uint16
    type: uint16
  - name: uint32
    type: uint32
  - name: uint64
    type: uint64
  - name: float32
    type: float32
  - name: float64
    type: float64
  - name: string
    type: string
  - name: struct
    type: struct
`
			env := service.NewEnvironment()
			parsedConf, err := EthernetIPConfigSpec.ParseYAML(confYAML, env)
			Expect(err).NotTo(HaveOccurred())

			mgr := service.MockResources()

			input, err := NewEthernetIPInput(parsedConf, mgr)
			Expect(err).NotTo(HaveOccurred())

			//	eipInput, ok := input.(*EIPInput)
			//	Expect(ok).To(BeTrue())
			eipInput := GetUnderlyingEIPInputForTest(input)
			Expect(eipInput).NotTo(BeNil())

			Expect(len(eipInput.Items)).To(Equal(14))

		})
	})

})

// GetUnderlyingEIPInputForTest extracts the underlying *EIPInput from a wrapped BatchInput.
// This relies on the fact that AutoRetryNacksBatched wraps your input in a struct that has an "Input" field.
func GetUnderlyingEIPInputForTest(bi service.BatchInput) *EIPInput {
	// Use reflection to try to get a field called "Input"
	v := reflect.ValueOf(bi)
	// If the wrapper is a pointer, get the element
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	field := v.FieldByName("Input")
	if !field.IsValid() {
		return nil
	}
	// Assert that the underlying type is *EIPInput.
	if underlying, ok := field.Interface().(*EIPInput); ok {
		return underlying
	}
	return nil
}
