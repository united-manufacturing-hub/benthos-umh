package sensorconnect_plugin_test

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"math"
	"os"

	"github.com/united-manufacturing-hub/benthos-umh/v2/sensorconnect_plugin"
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
		}

		for _, tt := range tests {
			tt := tt // Capture range variable
			It(fmt.Sprintf("should convert binary '%s' to float32 '%f'", tt.binary, tt.expected), func() {
				result, err := s.ConvertBinaryValue(tt.binary, "Float32T")
				Expect(err).ToNot(HaveOccurred())
				Expect(result).To(Equal(tt.expected))
			})
		}
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
