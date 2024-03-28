package opcua_plugin_test

import (
	"context"
	"os"
	"time"

	. "github.com/united-manufacturing-hub/benthos-umh/v2/opcua_plugin"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Test Against Siemens S7", func() {

	var endpoint string

	BeforeEach(func() {
		endpoint = os.Getenv("TEST_S7_ENDPOINT_URI")

		// Check if environment variables are set
		if endpoint == "" {
			Skip("Skipping test: environment variables are not set")
			return
		}
	})

	Describe("Connect", func() {
		It("should connect", func() {
			Skip("This currently fails")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			nodeIDStrings := []string{"ns=4;i=1"}

			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				Endpoint:         endpoint,
				Username:         "",
				Password:         "",
				NodeIDs:          parsedNodeIDs,
				SubscribeEnabled: false,
			}

			// Attempt to connect
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			messageBatch, _, err := input.ReadBatch(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(messageBatch).To(HaveLen(3))

			// Close connection
			if input.Client != nil {
				input.Client.Close(ctx)
			}
		})
	})
})

var _ = Describe("Test Against WAGO PLC", func() {

	var endpoint string
	var username string
	var password string

	BeforeEach(func() {
		// These information can be found in Bitwarden under WAGO PLC
		endpoint = os.Getenv("TEST_WAGO_ENDPOINT_URI")
		username = os.Getenv("TEST_WAGO_USERNAME")
		password = os.Getenv("TEST_WAGO_PASSWORD")

		// Check if environment variables are set
		if endpoint == "" || username == "" || password == "" {
			Skip("Skipping test: environment variables not set")
			return
		}
	})

	Describe("Connect Anonymous", func() {
		It("should connect in default mode", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			input := &OPCUAInput{
				Endpoint: endpoint,
				Username: "",
				Password: "",
				NodeIDs:  nil,
			}

			// Attempt to connect
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Close connection
			if input.Client != nil {
				input.Client.Close(ctx)
			}
		})

		It("should connect in no security mode", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			input := &OPCUAInput{
				Endpoint:       endpoint,
				Username:       "",
				Password:       "",
				NodeIDs:        nil,
				SecurityMode:   "None",
				SecurityPolicy: "None",
			}

			// Attempt to connect
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Close connection
			if input.Client != nil {
				input.Client.Close(ctx)
			}
		})
	})
})
