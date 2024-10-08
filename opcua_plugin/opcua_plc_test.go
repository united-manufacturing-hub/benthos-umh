package opcua_plugin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	. "github.com/united-manufacturing-hub/benthos-umh/v2/opcua_plugin"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Test Against Siemens S7", FlakeAttempts(5), Serial, func() {

	var endpoint string
	var input *OPCUAInput
	var ctx context.Context
	var cancel context.CancelFunc

	BeforeEach(func() {
		endpoint = os.Getenv("TEST_S7_ENDPOINT_URI")

		// Check if environment variables are set
		if endpoint == "" {
			Skip("Skipping test: environment variables are not set")
			return
		}

		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	})

	AfterEach(func() {
		if input != nil {
			if input.Client != nil {
				err := input.Client.Close(context.Background())
				Expect(err).NotTo(HaveOccurred())
			}
		}

		if cancel != nil {
			cancel()
		}
	})

	Describe("Connect", func() {
		It("should connect", func() {

			nodeIDStrings := []string{"ns=4;i=2"}

			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input = &OPCUAInput{
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

			Expect(messageBatch).To(HaveLen(1))
		})

		It("should connect with no security", func() {
			nodeIDStrings := []string{"ns=4;i=2"}

			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input = &OPCUAInput{
				Endpoint:         endpoint,
				Username:         "",
				Password:         "",
				NodeIDs:          parsedNodeIDs,
				SubscribeEnabled: false,
				SecurityMode:     "None",
				SecurityPolicy:   "None",
			}

			// Attempt to connect
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			messageBatch, _, err := input.ReadBatch(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(messageBatch).To(HaveLen(1))
		})
	})

	When("Subscribing to a struct", func() {
		It("should return data changes", func() {
			var err error

			var nodeIDStrings = []string{"ns=4;i=6"}

			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input = &OPCUAInput{
				Endpoint:         endpoint,
				Username:         "",
				Password:         "",
				NodeIDs:          parsedNodeIDs,
				SubscribeEnabled: false,
			}
			// Attempt to connect
			err = input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			messageBatch, _, err := input.ReadBatch(ctx)
			Expect(err).NotTo(HaveOccurred())

			// expect 2 messages for both nodes
			Expect(len(messageBatch)).To(Equal(2))
		})
	})
})

var _ = Describe("Test Against WAGO PLC", Serial, func() {

	var endpoint string
	var username string
	var password string

	var input *OPCUAInput
	var ctx context.Context
	var cancel context.CancelFunc

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

		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	})

	AfterEach(func() {
		if input != nil {
			if input.Client != nil {
				err := input.Client.Close(context.Background())
				Expect(err).NotTo(HaveOccurred())
				fmt.Println("Closed connection")
			}
		}
		if cancel != nil {
			cancel()
		}
	})

	Describe("Connect Anonymous", func() {
		It("should connect in default mode", func() {
			input = &OPCUAInput{
				Endpoint: endpoint,
				Username: "",
				Password: "",
				NodeIDs:  nil,
			}

			// Attempt to connect
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should connect in no security mode", func() {
			input = &OPCUAInput{
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
		})
	})

	Describe("Connect with Username and Password", func() {
		Context("when using incorrect credentials", func() {
			It("should fail to connect", func() {

				input = &OPCUAInput{
					Endpoint:       endpoint,
					Username:       "123", // Incorrect username and password
					Password:       "123",
					NodeIDs:        nil,
					SessionTimeout: 1000,
				}
				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when using correct credentials", func() {
			It("should successfully connect", func() {

				input = &OPCUAInput{
					Endpoint: endpoint,
					Username: username,
					Password: password,
					NodeIDs:  nil,
				}
				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())
			})
		})
	})

	Describe("Parse Nodes", func() {

		It("should parse node IDs", func() {

			var err error

			var nodeIDStrings = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL"}

			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input = &OPCUAInput{
				Endpoint: endpoint,
				Username: "",
				Password: "",
				NodeIDs:  parsedNodeIDs,
			}

			// Attempt to connect
			err = input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	When("Reading a batch", func() {
		It("should return a batch of messages", func() {

			var err error

			var nodeIDStrings = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL"}

			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input = &OPCUAInput{
				Endpoint: endpoint,
				Username: "",
				Password: "",
				NodeIDs:  parsedNodeIDs,
			}
			// Attempt to connect
			err = input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			messageBatch, _, err := input.ReadBatch(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(len(messageBatch)).To(Equal(1))

			for _, message := range messageBatch {
				message, err := message.AsStructuredMut()
				Expect(err).NotTo(HaveOccurred())
				var exampleNumber json.Number = "22.565684"

				Expect(message).To(BeAssignableToTypeOf(exampleNumber)) // it should be a number
			}
		})
	})

	When("Subscribing", func() {
		It("should return data changes", func() {

			var err error

			var nodeIDStrings = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL", "ns=4;s=|vprop|WAGO 750-8101 PFC100 CS 2ETH.Application.RevisionCounter"}

			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input = &OPCUAInput{
				Endpoint:         endpoint,
				Username:         "",
				Password:         "",
				NodeIDs:          parsedNodeIDs,
				SubscribeEnabled: true,
			}
			// Attempt to connect
			err = input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			messageBatch, _, err := input.ReadBatch(ctx)
			Expect(err).NotTo(HaveOccurred())

			// expect 2 messages for both nodes
			Expect(len(messageBatch)).To(Equal(2))

			for _, message := range messageBatch {
				message, err := message.AsStructuredMut()
				Expect(err).NotTo(HaveOccurred())
				var exampleNumber json.Number = "22.565684"

				Expect(message).To(BeAssignableToTypeOf(exampleNumber)) // it should be a number
			}

			messageBatch2, _, err := input.ReadBatch(ctx)
			Expect(err).NotTo(HaveOccurred())

			// expect 1 message only as RevisionCounter will not change
			Expect(len(messageBatch2)).To(Equal(1))

			for _, message := range messageBatch2 {
				message, err := message.AsStructuredMut()
				Expect(err).NotTo(HaveOccurred())
				var exampleNumber json.Number = "22.565684"

				Expect(message).To(BeAssignableToTypeOf(exampleNumber)) // it should be a number
			}

		})
	})

	When("Selecting a custom SecurityPolicy", func() {
		It("should connect", func() {
			Skip("This currently fails")
			input = &OPCUAInput{
				Endpoint:       endpoint,
				Username:       "",
				Password:       "",
				NodeIDs:        nil,
				SecurityMode:   "SignAndEncrypt",
				SecurityPolicy: "Basic128Rsa15",
			}

			// Attempt to connect
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Direct Connect", func() {
		When("connecting anonymously", func() {
			It("should connect successful", func() {

				input = &OPCUAInput{
					Endpoint:      endpoint,
					Username:      "",
					Password:      "",
					DirectConnect: true,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())
			})
		})

		When("connecting with username password", func() {
			It("should connect successful", func() {

				input = &OPCUAInput{
					Endpoint:      endpoint,
					Username:      username,
					Password:      password,
					DirectConnect: true,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())
			})
		})
	})
})
