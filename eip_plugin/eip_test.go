package eip_plugin_test

import (
	"context"
	"reflect"

	"github.com/danomagnum/gologix"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	. "github.com/united-manufacturing-hub/benthos-umh/eip_plugin"
)

var _ = Describe("EthernetIP Unittests", func() {

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
			Skip("skip for now")
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

var _ = Describe("EIP plugin with mock CIP", func() {
	type testCase struct {
		name           string
		cipType        gologix.CIPType
		mockValue      any
		expectedString string
		isAttribute    bool
	}

	DescribeTable("reading single CIP tags from mock",
		func(tc testCase) {
			item := &CIPReadItem{
				IsAttribute: tc.isAttribute,

				TagName:     tc.name,
				CIPDatatype: tc.cipType,
			}

			input := &EIPInput{
				Items:    []*CIPReadItem{item},
				PollRate: 0,
				CIP: &MockCIPReader{
					Tags: map[string]any{
						tc.name: tc.mockValue,
					},
				},
			}

			batch, ackFn, err := input.ReadBatch(context.Background())
			Expect(err).ToNot(HaveOccurred())
			Expect(batch).To(HaveLen(1))
			Expect(ackFn).NotTo(BeNil())

			msg := batch[0]
			raw, err := msg.AsBytes()
			Expect(err).NotTo(HaveOccurred())
			Expect(string(raw)).To(Equal(tc.expectedString))

		},
		Entry("bool = true", testCase{
			name:           "bool",
			cipType:        gologix.CIPTypeBOOL,
			mockValue:      true,
			expectedString: "true",
		}),
		Entry("byte = 0x01", testCase{
			name:           "byte",
			cipType:        gologix.CIPTypeBYTE,
			mockValue:      byte(0x01),
			expectedString: "1",
		}),
		Entry("int8 = 12", testCase{
			name:           "int8",
			cipType:        gologix.CIPTypeSINT,
			mockValue:      int8(12),
			expectedString: "12",
		}),
		Entry("uint8 = 12", testCase{
			name:           "uint8",
			cipType:        gologix.CIPTypeUSINT,
			mockValue:      uint8(12),
			expectedString: "12",
		}),
		Entry("int16 = 123", testCase{
			name:           "int16",
			cipType:        gologix.CIPTypeINT,
			mockValue:      int16(123),
			expectedString: "123",
		}),
		Entry("uint16 = 999", testCase{
			name:           "uint16",
			cipType:        gologix.CIPTypeUINT,
			mockValue:      uint16(999),
			expectedString: "999",
		}),
		Entry("int32 = -555", testCase{
			name:           "int32",
			cipType:        gologix.CIPTypeDINT,
			mockValue:      int32(-555),
			expectedString: "-555",
		}),
		Entry("uint32 = 55566", testCase{
			name:           "uint32",
			cipType:        gologix.CIPTypeUDINT,
			mockValue:      uint32(55566),
			expectedString: "55566",
		}),
		Entry("float32 = 12.34", testCase{
			name:           "float32",
			cipType:        gologix.CIPTypeREAL,
			mockValue:      float32(12.34),
			expectedString: "12.34",
		}),
		Entry("int64 = -55566", testCase{
			name:           "int64",
			cipType:        gologix.CIPTypeLINT,
			mockValue:      int64(-55566),
			expectedString: "-55566",
		}),
		Entry("uint32 = 5556677", testCase{
			name:           "uint64",
			cipType:        gologix.CIPTypeULINT,
			mockValue:      uint64(5556677),
			expectedString: "5556677",
		}),
		Entry("float64 = 1234.567", testCase{
			name:           "float64",
			cipType:        gologix.CIPTypeLREAL,
			mockValue:      float64(1234.567),
			expectedString: "1234.567",
		}),
		Entry("string = Hello", testCase{
			name:           "string",
			cipType:        gologix.CIPTypeSTRING,
			mockValue:      "Hello World",
			expectedString: "Hello World",
		}),
	)

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
	field := v.FieldByName("wrapped")
	if !field.IsValid() {
		return nil
	}
	// Assert that the underlying type is *EIPInput.
	wrapped := field.Interface()
	if eip, ok := wrapped.(*EIPInput); ok {
		return eip
	}
	return nil
}
