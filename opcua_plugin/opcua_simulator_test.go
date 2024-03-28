package opcua_plugin_test

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

	Describe("Insecure (None/None) Connect", func() {

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
				Endpoint:       endpoint,
				Username:       "",
				Password:       "",
				NodeIDs:        parsedNodeIDs,
				SecurityMode:   "None",
				SecurityPolicy: "None",
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

	Describe("Secure (SignAndEncrypt/Basic256Sha256) Connect", func() {

		var endpoint string

		BeforeEach(func() {
			Skip("Skipping test: prosys will reject all unknown certificates")

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
				Endpoint:       endpoint,
				Username:       "",
				Password:       "",
				NodeIDs:        parsedNodeIDs,
				SecurityMode:   "SignAndEncrypt",
				SecurityPolicy: "Basic256Sha256",
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

	Describe("Connect Anonymous", func() {
		It("should connect anonymously", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var nodeIDStrings []string = []string{"ns=3;s=Basic"}

			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				Endpoint:         "opc.tcp://localhost:50000",
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

			Expect(messageBatch).To(HaveLen(4))

			// Close connection
			if input.Client != nil {
				input.Client.Close(ctx)
			}
		})
	})

	Describe("Connect with Username and Password", func() {
		Context("when using invalid credentials", func() {
			It("should fail to connect", func() {
				Skip("Skipping due to OPC-UA simulator limitations on logging in multiple times")
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				input := &OPCUAInput{
					Endpoint: "opc.tcp://localhost:50000",
					Username: "sysadmin_bad", // Incorrect username and password
					Password: "demo",
					NodeIDs:  nil,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).To(HaveOccurred())

				// Close connection
				if input.Client != nil {
					err := input.Client.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		Context("when using valid credentials", func() {
			It("should successfully connect", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				input := &OPCUAInput{
					Endpoint: "opc.tcp://localhost:50000",
					Username: "sysadmin", // Correct username and password
					Password: "demo",
					NodeIDs:  nil,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				// Close connection
				if input.Client != nil {
					err := input.Client.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})
	})

	Describe("Subscribe", func() {
		Context("when connecting to subscribe to Fast data changes", func() {
			It("should connect and receive data changes", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeIDStrings := []string{"ns=3;s=Fast"}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: true,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				messageBatch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())

				Expect(len(messageBatch)).To(BeNumerically(">=", 6))

				for _, message := range messageBatch {
					message, err := message.AsStructuredMut()
					Expect(err).NotTo(HaveOccurred())
					Expect(message).To(BeAssignableToTypeOf(json.Number("22.565684")))
					GinkgoT().Log("Received message: ", message)
				}

				// Close connection
				if input.Client != nil {
					err := input.Client.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		Context("when connecting to subscribe to Boolean with Properties", func() {
			It("should connect and confirm properties are not browsed by default", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeIDStrings := []string{"ns=6;s=DataAccess_AnalogType_Byte"}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: true,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				messageBatch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(messageBatch)).To(Equal(1))

				for _, message := range messageBatch {
					message, err := message.AsStructuredMut()
					Expect(err).NotTo(HaveOccurred())
					Expect(message).To(Equal(json.Number("0")))
					GinkgoT().Log("Received message: ", message)
				}

				// Close connection
				if input.Client != nil {
					err := input.Client.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})
	})

	// TODO: Move more tests over from TestAgainstSimulator in opcua_test.go

})

var _ = FDescribe("Test Against Siemens S7", func() {

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
