package sensorconnect_plugin_test

import (
	"context"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/united-manufacturing-hub/benthos-umh/v2/sensorconnect_plugin"
	"os"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("Sensorconnnect", func() {

	var endpoint string

	BeforeEach(func() {
		endpoint = os.Getenv("TEST_DEBUG_IFM_ENDPOINT")

		// Check if environment variables are set
		if endpoint == "" {
			Skip("Skipping test: environment variables not set")
			return
		}

	})

	When("ReadBatch", func() {
		It("should receive data from the AL1350", func() {

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			input := &sensorconnect_plugin.SensorConnectInput{
				DeviceAddress: endpoint,
				CurrentCid:    0,
			}

			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Read a batch of messages
			msgs, _, err := input.ReadBatch(ctx)
			Expect(err).NotTo(HaveOccurred())
			fmt.Printf("Received %d messages\n", len(msgs))
			Expect(len(msgs)).To(BeNumerically(">", 0))

			for _, message := range msgs {
				messageBytes, err := message.AsBytes()
				Expect(err).NotTo(HaveOccurred())

				//var exampleNumber json.Number = "22.565684"
				//Expect(message).To(BeAssignableToTypeOf(exampleNumber))
				fmt.Printf("Received messageBytes: %+v\n", messageBytes)
			}

			// Close the connection
			err = input.Close(ctx)
		})
	})

	When("using a yaml and stream builder", func() {

		It("should subscribe to all nodes and receive data", func() {
			Skip("not relevant for now")

			// Create a new stream builder
			builder := service.NewStreamBuilder()

			// Create a new stream
			err := builder.AddInputYAML(fmt.Sprintf(`
sensorconnect:
  device_address: "%s"
`, endpoint))

			Expect(err).NotTo(HaveOccurred())

			err = builder.SetLoggerYAML(`level: off`)
			Expect(err).NotTo(HaveOccurred())

			err = builder.SetTracerYAML(`type: none`)
			Expect(err).NotTo(HaveOccurred())

			// Add a total message count consumer
			var count int64
			err = builder.AddConsumerFunc(func(c context.Context, m *service.Message) error {
				atomic.AddInt64(&count, 1)
				return err
			})

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			timeout := time.Second * 45

			// Run the stream
			ctx, cncl := context.WithTimeout(context.Background(), timeout)
			go func() {
				_ = stream.Run(ctx)
			}()

			// Check if we received any messages continuously
			Eventually(
				func() int64 {
					return atomic.LoadInt64(&count)
				}, timeout).Should(BeNumerically(">", int64(0)))

			cncl()

		})
	})
})
