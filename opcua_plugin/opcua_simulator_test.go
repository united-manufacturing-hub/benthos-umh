package opcua_plugin_test

// This package is used to test the OPC UA plugin against a Prosys OPC UA simulator and a Microsoft OPC UA simulator.

import (
	"context"
	"encoding/json"
	"os"
	"time"

	. "github.com/united-manufacturing-hub/benthos-umh/v2/opcua_plugin"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Test Against Prosys Simulator", func() {

	Describe("Insecure Connect", func() {

		var endpoint string

		BeforeEach(func() {
			endpoint = os.Getenv("TEST_PROSYS_ENDPOINT_URI")

			// Check if environment variables are set
			if endpoint == "" {
				Skip("Skipping test: environment variables not set")
				return
			}

		})
		It("should read data correctly", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var nodeIDStrings = []string{"ns=3;i=1003"}
			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				Endpoint: endpoint,
				Username: "",
				Password: "",
				NodeIDs:  parsedNodeIDs,
				Insecure: true,
			}

			// Attempt to connect
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			messageBatch, _, err := input.ReadBatch(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(messageBatch).To(HaveLen(1))

			for _, message := range messageBatch {
				message, err := message.AsStructuredMut()
				Expect(err).NotTo(HaveOccurred())

				var exampleNumber json.Number = "22.565684"
				Expect(message).To(BeAssignableToTypeOf(exampleNumber))
				GinkgoWriter.Printf("Received message: %+v\n", message)
			}

			// Close connection
			if input.Client != nil {
				err = input.Client.Close(ctx)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

})

var _ = Describe("Test Against Microsoft OPC UA simulator", func() {

	BeforeEach(func() {
		endpoint := os.Getenv("TEST_WAGO_ENDPOINT_URI")
		username := os.Getenv("TEST_WAGO_USERNAME")
		password := os.Getenv("TEST_WAGO_PASSWORD")

		// Check if environment variables are set
		if endpoint != "" || username != "" || password != "" {
			Skip("Skipping test: environment variables are set --> the wago test is running and we are not running a test against the simulator")
			return
		}
	})

	Describe("ConnectAnonymousSecure", func() {
		It("should connect securely and anonymously", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var nodeIDStrings []string = []string{"ns=3;s=Basic"}

			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				Endpoint:         "opc.tcp://localhost:50000",
				Username:         "",
				Password:         "",
				NodeIDs:          parsedNodeIDs,
				Insecure:         false,
				SubscribeEnabled: false,
			}

			// Attempt to connect
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			messageBatch, _, err := input.ReadBatch(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(messageBatch).To(HaveLen(4))

			// Close connection
			if input.Client != nil {
				input.Client.Close(ctx)
			}
		})
	})
})
