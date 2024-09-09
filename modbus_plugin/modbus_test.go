package modbus_plugin_test

import (
	"context"
	"encoding/json"
	"github.com/grid-x/modbus"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/united-manufacturing-hub/benthos-umh/v2/modbus_plugin"
)

var _ = Describe("Test Against Docker Modbus Simulator", func() {

	BeforeEach(func() {
		testModbusSimulator := os.Getenv("TEST_MODBUS_SIMULATOR")

		// Check if environment variables are set
		if testModbusSimulator == "" {
			Skip("Skipping test: environment variables not set")
			return
		}

	})

	It("should connect and read discrete register", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		input := &ModbusInput{
			SlaveIDs:    []byte{1},
			BusyRetries: 1,
			Addresses: []ModbusDataItemWithAddress{
				{
					Name:     "firstFlagOfDiscreteInput",
					Register: "discrete",
					Address:  1,
					Type:     "BIT",
					Output:   "BOOL",
				},
				{
					Name:     "secondFlagOfDiscreteInput",
					Register: "discrete",
					Address:  2,
					Type:     "BIT",
					Output:   "BOOL",
				},
				{
					Name:     "fourthFlagOfDiscreteInput",
					Register: "discrete",
					Address:  4,
					Type:     "BIT",
					Output:   "BOOL",
				},
			},
			Handler: modbus.NewTCPClientHandler("127.0.0.1:50502"),
		}
		input.Client = modbus.NewClient(input.Handler)

		// Parse the addresses into batches
		var err error
		input.RequestSet, err = input.CreateBatchesFromAddresses(input.Addresses)
		Expect(err).NotTo(HaveOccurred())

		// Attempt to connect
		err = input.Connect(ctx)
		Expect(err).NotTo(HaveOccurred())

		messageBatch, _, err := input.ReadBatch(ctx)
		Expect(err).NotTo(HaveOccurred())

		Expect(messageBatch).To(HaveLen(3))

		for _, message := range messageBatch {
			messageStruct, err := message.AsStructuredMut()
			Expect(err).NotTo(HaveOccurred())

			tagName, exists := message.MetaGet("modbus_tag_name")
			Expect(exists).To(BeTrue())
			Expect(err).NotTo(HaveOccurred())

			Expect(messageStruct).To(BeAssignableToTypeOf(true))
			switch tagName {
			case "firstFlagOfDiscreteInput":
				Expect(messageStruct).To(Equal(true))
			case "secondFlagOfDiscreteInput":
				Expect(messageStruct).To(Equal(false))
			case "fourthFlagOfDiscreteInput":
				Expect(messageStruct).To(Equal(false))
			}

			GinkgoWriter.Printf("Received message: %+v\n", messageStruct)
		}

		// Close connection
		err = input.Close(ctx)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should connect and read input register", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		input := &ModbusInput{
			SlaveIDs:    []byte{1},
			BusyRetries: 1,
			Addresses: []ModbusDataItemWithAddress{
				{
					Name:     "zeroElementOfInputRegister",
					Register: "input",
					Address:  0,
					Type:     "UINT16",
				},
				{
					Name:     "firstElementOfInputRegister",
					Register: "input",
					Address:  1,
					Type:     "UINT16",
				},
				{
					Name:     "eighthElementOfInputRegister",
					Register: "input",
					Address:  8,
					Type:     "UINT16",
				},
			},
			Handler: modbus.NewTCPClientHandler("127.0.0.1:50502"),
		}
		input.Client = modbus.NewClient(input.Handler)

		// Parse the addresses into batches
		var err error
		input.RequestSet, err = input.CreateBatchesFromAddresses(input.Addresses)
		Expect(err).NotTo(HaveOccurred())

		// Attempt to connect
		err = input.Connect(ctx)
		Expect(err).NotTo(HaveOccurred())

		messageBatch, _, err := input.ReadBatch(ctx)
		Expect(err).NotTo(HaveOccurred())

		Expect(messageBatch).To(HaveLen(3))

		for _, message := range messageBatch {
			messageStruct, err := message.AsStructuredMut()
			Expect(err).NotTo(HaveOccurred())

			tagName, exists := message.MetaGet("modbus_tag_name")
			Expect(exists).To(BeTrue())
			Expect(err).NotTo(HaveOccurred())

			var zeroElement json.Number = "34009"
			var firstElement json.Number = "16877"
			var eighthElement json.Number = "116"
			Expect(messageStruct).To(BeAssignableToTypeOf(zeroElement))

			switch tagName {
			case "zeroElementOfInputRegister":

				Expect(messageStruct).To(Equal(zeroElement))
			case "firstElementOfInputRegister":
				Expect(messageStruct).To(Equal(firstElement))
			case "eighthElementOfInputRegister":
				Expect(messageStruct).To(Equal(eighthElement))
			}

			GinkgoWriter.Printf("Received message: %+v\n", messageStruct)
		}

		// Close connection
		err = input.Close(ctx)
		Expect(err).NotTo(HaveOccurred())
	})
})
