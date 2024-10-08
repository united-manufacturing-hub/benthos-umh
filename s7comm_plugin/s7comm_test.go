package s7comm_plugin_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/united-manufacturing-hub/benthos-umh/v2/s7comm_plugin"
)

var _ = Describe("S7Comm Plugin Unittests", FlakeAttempts(5), func() {
	BeforeEach(func() {
		testActive := os.Getenv("TEST_S7COMM_UNITTEST")

		// Check if environment variables are set
		if testActive == "" {
			Skip("Skipping test: environment variables are not set")
			return
		}
	})

	Describe("Parsing Addresses", func() {
		type testCase struct {
			address            string
			inputBytesHex      string
			expectedConversion interface{}
		}

		It("correctly parses addresses and converts input bytes", func() {
			tests := []testCase{
				{"DB2.W0", "0000", uint16(0)},
				{"DB2.W1", "0001", uint16(1)},
			}

			for _, tc := range tests {
				By("Testing address "+tc.address+" with bytes "+tc.inputBytesHex, func() {
					addresses := []string{tc.address}
					batchMaxSize := 1

					batches, err := s7comm_plugin.ParseAddresses(addresses, batchMaxSize)
					Expect(err).NotTo(HaveOccurred())
					Expect(batches).To(HaveLen(1))
					Expect(batches[0]).To(HaveLen(1))

					converterFunc := batches[0][0].ConverterFunc

					inputBytes, err := hex.DecodeString(tc.inputBytesHex)
					Expect(err).NotTo(HaveOccurred())

					actualConversionResult := converterFunc(inputBytes)
					Expect(actualConversionResult).To(Equal(tc.expectedConversion))
				})
			}
		})
	})
})

var _ = Describe("S7Comm Test Against Local PLC", func() {

	Describe("Communication with a Remote S7 Instance", func() {
		var (
			endpoint string
			rack     int
			slot     int
			ctx      context.Context
			input    *s7comm_plugin.S7CommInput
			cancel   context.CancelFunc
		)

		BeforeEach(func() {
			endpoint = os.Getenv("TEST_S7_TCPDEVICE")
			rackStr := os.Getenv("TEST_S7_RACK")
			slotStr := os.Getenv("TEST_S7_SLOT")

			if endpoint == "" || rackStr == "" || slotStr == "" {
				Skip("Skipping test: environment variables not set")
			}

			var err error
			rack, err = strconv.Atoi(rackStr)
			Expect(err).NotTo(HaveOccurred())

			slot, err = strconv.Atoi(slotStr)
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)

			addresses := []string{"DB2.W0"}
			batchMaxSize := 480 // default

			batches, err := s7comm_plugin.ParseAddresses(addresses, batchMaxSize)
			Expect(err).NotTo(HaveOccurred())

			input = &s7comm_plugin.S7CommInput{
				TcpDevice:    endpoint,
				Rack:         rack,
				Slot:         slot,
				BatchMaxSize: batchMaxSize,
				Batches:      batches,
			}
		})

		AfterEach(func() {
			if input != nil && ctx != nil {
				input.Close(ctx)
			}

			if ctx != nil {
				cancel()
			}
		})

		It("connects and reads data successfully", func() {

			By("Connecting to the remote instance", func() {
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())
			})

			By("Reading data", func() {
				messageBatch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(messageBatch).To(HaveLen(1))

				for _, message := range messageBatch {
					messageStructuredMut, err := message.AsStructuredMut()
					Expect(err).NotTo(HaveOccurred())
					Expect(messageStructuredMut).To(BeAssignableToTypeOf(json.Number("22.565684")))

					s7Address, wasFound := message.MetaGet("s7_address")
					Expect(wasFound).To(BeTrue())
					Expect(s7Address).To(Equal("DB2.W0"))
				}
			})
		})
	})
})
