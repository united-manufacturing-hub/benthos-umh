package opcua_plugin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"

	"github.com/redpanda-data/benthos/v4/public/service"
	. "github.com/united-manufacturing-hub/benthos-umh/opcua_plugin"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Getting Nodes for a OPC Ua server in a tree datastructure", func() {
	var ctx context.Context
	var cancel context.CancelFunc
	var endpoint string
	var username string
	var password string

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 60*time.Second)
		endpoint = os.Getenv("TEST_WAGO_ENDPOINT_URI")
		username = os.Getenv("TEST_WAGO_USERNAME")
		password = os.Getenv("TEST_WAGO_PASSWORD")
	})

	AfterEach(func() {
		cancel()
	})

	Context("When GetNodeTree is called for a PLC", func() {
		It("should return a node tree", func() {
			if endpoint == "" {
				Skip("Skipping test: environment variables not set")
				return
			}
			opc := &OPCUAInput{
				Endpoint: endpoint,
				Username: username,
				Password: password,
			}
			msgCh := make(chan string, 100000)
			parentNode := &Node{
				NodeId:   ua.NewNumericNodeID(0, id.RootFolder),
				Name:     "Root",
				Children: make([]*Node, 0),
			}
			_, err := opc.GetNodeTree(ctx, msgCh, parentNode)
			Expect(err).ShouldNot(HaveOccurred())
			go func() {
				for {
					select {
					// Do nothing from the messages from msgCh
					case <-msgCh:
					case <-ctx.Done():
						return
					}
				}
			}()

		})
	})
})

var _ = Describe("Test Against Siemens S7", Serial, func() {

	var endpoint string
	var fingerprint string
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

		ctx, cancel = context.WithTimeout(context.Background(), 45*time.Second)
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
				Endpoint:                   endpoint,
				Username:                   "",
				Password:                   "",
				NodeIDs:                    parsedNodeIDs,
				SubscribeEnabled:           false,
				AutoReconnect:              true,
				ReconnectIntervalInSeconds: 5,
			}

			// Attempt to connect
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() (int, error) {
				messageBatch, _, err := input.ReadBatch(ctx)
				return len(messageBatch), err
			}, 10*time.Second, 100*time.Millisecond).WithContext(ctx).Should(Equal(1))
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

			Eventually(func() (int, error) {
				messageBatch, _, err := input.ReadBatch(ctx)
				return len(messageBatch), err
			}, 30*time.Second, 100*time.Millisecond).WithContext(ctx).Should(Equal(1))
		})
	})

	Describe("Connect to trusted server", func() {
		It("should connect successfully", func() {
			fingerprint = os.Getenv("TEST_S7_FINGERPRINT")

			// we only want to skip the fingerprint test here
			if fingerprint == "" {
				Skip("Skipping test: environment variable not set")
			}

			input = &OPCUAInput{
				Endpoint:                     endpoint,
				NodeIDs:                      nil,
				ServerCertificateFingerprint: fingerprint, // correct certificate fingerprint
			}

			// Attempt to connect with matching fingerprints
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should fail due to fingerprint-mismatch", func() {
			input = &OPCUAInput{
				Endpoint:                     endpoint,
				NodeIDs:                      nil,
				ServerCertificateFingerprint: "test123", // incorrect certificate fingerprint
			}

			// Attempt to connect and fail due to fingerprint mismatch
			err := input.Connect(ctx)
			Expect(err).To(HaveOccurred())
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

			// expect 2 messages for both nodes
			Eventually(func() (int, error) {
				messageBatch, _, err := input.ReadBatch(ctx)
				return len(messageBatch), err
			}, 30*time.Second, 100*time.Millisecond).WithContext(ctx).Should(Equal(2))
		})
	})
})

var _ = Describe("Test Against WAGO PLC", Serial, func() {

	var endpoint string
	var username string
	var password string
	var fingerprint string

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

	Describe("Connect to trusted server", func() {
		It("should connect successfully", func() {
			fingerprint = os.Getenv("TEST_WAGO_FINGERPRINT")

			// we only want to skip the fingerprint test here
			if fingerprint == "" {
				Skip("Skipping test: environment variable not set")
			}

			input = &OPCUAInput{
				Endpoint:                     endpoint,
				NodeIDs:                      nil,
				ServerCertificateFingerprint: fingerprint, // correct certificate fingerprint
			}

			// Attempt to connect with matching fingerprints
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should fail due to fingerprint-mismatch", func() {
			input = &OPCUAInput{
				Endpoint:                     endpoint,
				NodeIDs:                      nil,
				ServerCertificateFingerprint: "test123", // incorrect certificate fingerprint
			}

			// Attempt to connect and fail due to fingerprint mismatch
			err := input.Connect(ctx)
			Expect(err).To(HaveOccurred())
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
		It("should return a batch of messages", FlakeAttempts(3), func() {

			var err error

			var nodeIDStrings = []string{"ns=4;s=|var|WAGO 750-8101 PFC100 CS 2ETH.Application.GVL"}

			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input = &OPCUAInput{
				Endpoint:                   endpoint,
				Username:                   "",
				Password:                   "",
				NodeIDs:                    parsedNodeIDs,
				AutoReconnect:              true,
				ReconnectIntervalInSeconds: 5,
			}
			// Attempt to connect
			err = input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			var messageBatch service.MessageBatch

			Eventually(func() (int, error) {
				messageBatch, _, err = input.ReadBatch(ctx)
				return len(messageBatch), err
			}, 30*time.Second, 100*time.Millisecond).WithContext(ctx).Should(Equal(1))

			for _, message := range messageBatch {
				message, err := message.AsStructuredMut()
				Expect(err).NotTo(HaveOccurred())
				var exampleNumber json.Number = "22.565684"

				Expect(message).To(BeAssignableToTypeOf(exampleNumber)) // it should be a number
			}
		})
	})

	When("Subscribing", func() {
		It("should return data changes", FlakeAttempts(3), func() {

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
			ctx := context.Background()
			err = input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			var messageBatch service.MessageBatch
			// expect 2 messages for both nodes
			Eventually(func() (int, error) {
				messageBatch, _, err := input.ReadBatch(ctx)
				return len(messageBatch), err
			}, 30*time.Second, 100*time.Millisecond).WithContext(ctx).Should(Equal(2))

			for _, message := range messageBatch {
				message, err := message.AsStructuredMut()
				Expect(err).NotTo(HaveOccurred())
				var exampleNumber json.Number = "22.565684"

				Expect(message).To(BeAssignableToTypeOf(exampleNumber)) // it should be a number
			}

			var messageBatch2 service.MessageBatch
			// expect 2 message only as RevisionCounter will not change
			Eventually(func() (int, error) {
				messageBatch2, _, err = input.ReadBatch(ctx)
				return len(messageBatch2), err
			}, 30*time.Second, 100*time.Millisecond).WithContext(ctx).Should(Equal(1))

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
