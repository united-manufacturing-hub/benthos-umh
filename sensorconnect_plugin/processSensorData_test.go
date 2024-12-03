package sensorconnect_plugin_test

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"math"
	"os"

	"github.com/united-manufacturing-hub/benthos-umh/sensorconnect_plugin"
)

var _ = Describe("ProcessSensorData", func() {

	var (
		s *sensorconnect_plugin.SensorConnectInput
	)

	BeforeEach(func() {
		endpoint := os.Getenv("TEST_DEBUG_IFM_ENDPOINT")

		// Check if environment variables are set
		if endpoint == "" {
			Skip("Skipping test: environment variables not set")
			return
		}

		s = &sensorconnect_plugin.SensorConnectInput{}
	})

	Describe("Bit Conversions", func() {
		It("should correctly convert hex to binary and back with padding", func() {
			longHexString := "0000FC000001FF000000FF000161FF000025FF03"
			length := len(longHexString) * 4
			bitString, err := s.HexToBin(longHexString)
			Expect(err).ToNot(HaveOccurred())
			fmt.Println(bitString)
			bitStringPadded := s.ZeroPadding(bitString, length)
			fmt.Println(bitStringPadded)
			endHexString, err := s.BinToHex(bitStringPadded)
			Expect(err).ToNot(HaveOccurred())
			endHexStringPadded := s.ZeroPadding(endHexString, len(longHexString))
			fmt.Println(endHexStringPadded)
			fmt.Println(longHexString)
			Expect(endHexStringPadded).To(Equal("0000fc000001ff000000ff000161ff000025ff03"), "Problem with BitConversions")
		})
	})

	Describe("BooleanT Conversions", func() {
		tests := []struct {
			binary   string
			expected bool
		}{
			{
				binary:   "1",
				expected: true,
			},
			{
				binary:   "0",
				expected: false,
			},
		}

		for _, tt := range tests {
			tt := tt // Capture range variable
			It(fmt.Sprintf("should convert binary '%s' to bool '%t'", tt.binary, tt.expected), func() {
				result, err := s.ConvertBinaryValue(tt.binary, "BooleanT")
				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(Equal(tt.expected))
			})
		}
	})

	Describe("Float32T Conversions", func() {
		Context("with valid binary strings", func() {
			tests := []struct {
				binary   string
				expected float32
			}{
				{
					binary:   "01000000010000000000000000000000",
					expected: 3.0,
				},
				{
					binary:   "11000000010000000000000000000000",
					expected: -3.0,
				},
				{
					binary:   "00000000000000000000000000000000",
					expected: 0.0,
				},
				{
					binary:   "01111111100000000000000000000000",
					expected: float32(math.Inf(1)),
				},
				{
					binary:   "11111111100000000000000000000000",
					expected: float32(math.Inf(-1)),
				},
			}

			for _, tt := range tests {
				tt := tt // Capture range variable
				It(fmt.Sprintf("should convert binary '%s' to float32 '%f'", tt.binary, tt.expected), func() {
					result, err := s.ConvertBinaryValue(tt.binary, "Float32T")
					Expect(err).ToNot(HaveOccurred())
					Expect(result).To(BeAssignableToTypeOf(float32(0)))
					if math.IsNaN(float64(tt.expected)) {
						Expect(math.IsNaN(float64(result.(float32)))).To(BeTrue())
					} else if math.IsInf(float64(tt.expected), 0) {
						Expect(math.IsInf(float64(result.(float32)), int(math.Copysign(1, float64(tt.expected))))).To(BeTrue())
					} else {
						Expect(result).To(Equal(tt.expected))
					}
				})
			}
		})

		Context("with invalid binary strings", func() {
			It("should return an error for binary strings of incorrect length", func() {
				binary := "01000000" // Only 8 bits instead of 32
				_, err := s.ConvertBinaryValue(binary, "Float32T")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("invalid binary length for Float32T"))
			})

			It("should return an error for empty binary string", func() {
				binary := ""
				_, err := s.ConvertBinaryValue(binary, "Float32T")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("binaryValue is empty"))
			})

			It("should return an error for binary string with invalid characters", func() {
				binary := "01000000ABCD00000000000000000000"
				_, err := s.ConvertBinaryValue(binary, "Float32T")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("invalid syntax"))
			})
		})
	})

	Describe("UIntegerT Conversions", func() {
		tests := []struct {
			binary   string
			expected uint64
		}{
			{
				binary:   "0001101",
				expected: 13,
			},
			{
				binary:   "10110111001011010",
				expected: 93786,
			},
			{
				binary:   "11111111111111111111111111111111",
				expected: math.MaxUint32,
			},
			{
				binary:   "1111111111111111111111111111111111111111111111111111111111111111",
				expected: math.MaxUint64,
			},
		}

		for _, tt := range tests {
			tt := tt // Capture range variable
			It(fmt.Sprintf("should convert binary '%s' to uint64 '%v'", tt.binary, tt.expected), func() {
				result, err := s.ConvertBinaryValue(tt.binary, "UIntegerT")
				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(Equal(tt.expected))
			})
		}
	})

	Describe("IntegerT Conversions", func() {
		tests := []struct {
			binary   string
			expected int
		}{
			{
				binary:   "10000000",
				expected: math.MinInt8,
			},
			{
				binary:   "01111111",
				expected: math.MaxInt8,
			},
			{
				binary:   "1000000000000000",
				expected: math.MinInt16,
			},
			{
				binary:   "10000000000000000000000000000000",
				expected: math.MinInt32,
			},
			{
				binary:   "1000000000000000000000000000000000000000000000000000000000000000",
				expected: math.MinInt64,
			},
		}

		for _, tt := range tests {
			tt := tt // Capture range variable
			It(fmt.Sprintf("should convert binary '%s' to int '%v'", tt.binary, tt.expected), func() {
				result, err := s.ConvertBinaryValue(tt.binary, "IntegerT")
				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(Equal(tt.expected))
			})
		}
	})
})

var _ = Describe("float32T bug (ENG-2010)", func() {

	var (
		s *sensorconnect_plugin.SensorConnectInput
	)

	BeforeEach(func() {
		endpoint := os.Getenv("TEST_DEBUG_IFM_ENDPOINT")

		// Check if environment variables are set
		if endpoint == "" {
			Skip("Skipping test: environment variables not set")
			return
		}

		s = &sensorconnect_plugin.SensorConnectInput{}
	})

	It("should return 32 for Float32T when bitLength is not specified", func() {
		datatype := "Float32T"
		bitLength := uint(0)   // bitLength not specified
		fixedLength := uint(0) // fixedLength not specified

		valueBitLength := s.DetermineValueBitLength(datatype, bitLength, fixedLength)

		Expect(valueBitLength).To(Equal(uint(32)), fmt.Sprintf("Expected 32 bits for %s with no bitLength, got %d", datatype, valueBitLength))
	})

	It("should correctly process Float32T SimpleDatatype with no bitLength specified", func() {
		// Simulate raw sensor output binary padded to 96 bits
		rawSensorOutputBinaryPadded := "01000000010000000000000000000000" + // 32 bits for Float32T (e.g., 3.0)
			"0000000000000000" + // 16 bits placeholder
			"0000000000000000" + // 16 bits placeholder
			"0000" + // 4 bits placeholder
			"00" + // 2 bits placeholder
			"00" // 2 bits placeholder to reach 96 bits total

		simpleDatatype := sensorconnect_plugin.SimpleDatatype{
			Type: "Float32T",
			// BitLength is not specified
		}

		bitOffset := 64 // As in your XML
		outputBitLength := 96
		nameTextId := "TestFloat32TValue"
		primLangExternalTextCollection := []sensorconnect_plugin.Text{
			{Id: "TestFloat32TValue", Value: "FloatValue"},
		}

		payload, err := s.ProcessSimpleDatatype(
			simpleDatatype,
			outputBitLength,
			rawSensorOutputBinaryPadded,
			bitOffset,
			nameTextId,
			primLangExternalTextCollection)

		Expect(err).ToNot(HaveOccurred())
		Expect(payload).To(HaveKey("FloatValue"))
		Expect(payload["FloatValue"]).To(Equal(float32(3.0)))
	})
})
