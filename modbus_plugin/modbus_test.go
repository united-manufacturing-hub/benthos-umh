package modbus_plugin_test

import (
	"context"
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

	It("should connect successfully", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		input := &ModbusInput{
			SlaveID: 1,
			Addresses: []ModbusDataItemWithAddress{
				{
					Name:     "firstFlagOfDiscreteInput",
					Register: "discrete",
					Address:  1,
					Type:     "BIT",
					Output:   "BOOL",
					Bit:      1,
				},
			},
			Handler: modbus.NewTCPClientHandler("127.0.0.1:50502"),
		}
		input.Client = modbus.NewClient(input.Handler)

		// Attempt to connect
		err := input.Connect(ctx)
		Expect(err).NotTo(HaveOccurred())

		messageBatch, _, err := input.ReadBatch(ctx)
		Expect(err).NotTo(HaveOccurred())

		Expect(messageBatch).To(HaveLen(1))

		for _, message := range messageBatch {
			message, err := message.AsStructuredMut()
			Expect(err).NotTo(HaveOccurred())

			Expect(message).To(BeAssignableToTypeOf(true))
			GinkgoWriter.Printf("Received message: %+v\n", message)
		}

		// Close connection
		err = input.Close(ctx)
		Expect(err).NotTo(HaveOccurred())
	})
})
