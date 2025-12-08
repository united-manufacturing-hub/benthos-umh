// Copyright 2025 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package opcua_plugin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"

	. "github.com/united-manufacturing-hub/benthos-umh/opcua_plugin"
)

// other simulators are not running when starting up the devcontainer and are a rather manual process to start
// this test is to ensure that the opcua plugin works with other simulators
var _ = Describe("Test Against Softing OPC DataFeed", Serial, func() {
	When("Subscribing to server without discovery urls", func() {
		It("does successfully connects", func() {
			Skip("not implemented in CI pipeline")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			nodeIDStrings := []string{"ns=3;s=Siemens_1"}
			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				OPCUAConnection: &OPCUAConnection{
					Endpoint: "opc.tcp://10.13.37.125:4998",
					Username: "",
					Password: "",
				},
				NodeIDs:          parsedNodeIDs,
				SubscribeEnabled: false,
			}

			// Attempt to connect
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			messageBatch, _, err := input.ReadBatch(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(messageBatch).To(HaveLen(1))

			// Close connection
			if input.Client != nil {
				err = input.Close(ctx)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})
	When("Subscribing to softing and manually adjusting local item", func() {
		It("does successfully reports a data change", func() {
			// This requires manual intervention and a manual change of localitem during this test
			Skip("not implemented in CI pipeline")
			ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel1()

			nodeIDStrings := []string{"ns=3;s=Local Items.test"}
			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				OPCUAConnection: &OPCUAConnection{
					Endpoint: "opc.tcp://10.13.37.125:4998",
					Username: "",
					Password: "",
				},
				NodeIDs:          parsedNodeIDs,
				SubscribeEnabled: true,
			}

			// Attempt to connect
			err := input.Connect(ctx1)
			Expect(err).NotTo(HaveOccurred())

			messageBatch, _, err := input.ReadBatch(ctx1)
			Expect(err).NotTo(HaveOccurred())
			Expect(messageBatch).To(HaveLen(1))

			ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel2()

			messageBatch, _, err = input.ReadBatch(ctx2)
			Expect(err).To(Equal(context.DeadlineExceeded)) // there should be no data change
			Expect(messageBatch).To(BeEmpty())

			ctx3, cancel3 := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel3()

			messageBatch, _, err = input.ReadBatch(ctx3)
			Expect(err).NotTo(HaveOccurred())
			Expect(messageBatch).To(HaveLen(1))

			// Close connection
			if input.Client != nil {
				ctxEnd, cancelEnd := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancelEnd()
				err = input.Close(ctxEnd)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})
})

var _ = Describe("Debugging test", func() {
	var endpoint string

	BeforeEach(func() {
		endpoint = os.Getenv("TEST_DEBUG_ENDPOINT_URI")

		// Check if environment variables are set
		if endpoint == "" {
			Skip("Skipping test: environment variables not set")
			return
		}
	})

	When("using a yaml and stream builder", func() {
		It("should subscribe to all nodes and receive data", func() {
			// Create a new stream builder
			builder := service.NewStreamBuilder()

			// Create a new stream
			err := builder.AddInputYAML(fmt.Sprintf(`
opcua:
  endpoint: "%s"
  username: ""
  password: ""
  subscribeEnabled: true
  useHeartbeat: true
  nodeIDs:
    - i=84
`, endpoint))

			Expect(err).NotTo(HaveOccurred())

			err = builder.SetLoggerYAML(`level: off`)
			Expect(err).NotTo(HaveOccurred())

			err = builder.SetTracerYAML(`type: none`)
			Expect(err).NotTo(HaveOccurred())

			// Add a total message count consumer
			var count int64
			err = builder.AddConsumerFunc(func(_ context.Context, _ *service.Message) error {
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

var _ = Describe("Test Against Prosys Simulator", func() {
	var endpoint string

	BeforeEach(func() {
		endpoint = os.Getenv("TEST_PROSYS_ENDPOINT_URI")

		// Check if environment variables are set
		if endpoint == "" {
			Skip("Skipping test: environment variables not set")
			return
		}
	})

	Describe("OPC UA Server Information", func() {
		It("should connect to the server and retrieve server information", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			nodeIDStrings := []string{"ns=3;i=1003"}
			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				OPCUAConnection: &OPCUAConnection{
					Endpoint:       endpoint,
					Username:       "",
					Password:       "",
					SecurityMode:   "None",
					SecurityPolicy: "None",
				},
				NodeIDs: parsedNodeIDs,
			}

			// Attempt to connect
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			serverInformation, err := input.GetOPCUAServerInformation(ctx)
			Expect(err).NotTo(HaveOccurred())

			GinkgoWriter.Printf("Server Information: \n")
			GinkgoWriter.Printf("ManufacturerName: %s\n", serverInformation.ManufacturerName)
			GinkgoWriter.Printf("ProductName: %s\n", serverInformation.ProductName)
			GinkgoWriter.Printf("SoftwareVersion: %s\n", serverInformation.SoftwareVersion)

			// Close connection
			if input.Client != nil {
				err = input.Close(ctx)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

	Describe("YAML Configuration", func() {
		When("using a yaml and stream builder", func() {
			It("should subscribe to all nodes and receive data", func() {
				// Create a new stream builder
				builder := service.NewStreamBuilder()

				// Create a new stream
				err := builder.AddInputYAML(fmt.Sprintf(`
opcua:
  endpoint: "%s"
  username: ""
  password: ""
  subscribeEnabled: true
  useHeartbeat: true
  nodeIDs:
    - i=84
`, endpoint))

				Expect(err).NotTo(HaveOccurred())

				err = builder.SetLoggerYAML(`level: off`)
				Expect(err).NotTo(HaveOccurred())

				err = builder.SetTracerYAML(`type: none`)
				Expect(err).NotTo(HaveOccurred())

				// Add a total message count consumer
				var count int64
				err = builder.AddConsumerFunc(func(_ context.Context, _ *service.Message) error {
					atomic.AddInt64(&count, 1)
					return err
				})

				stream, err := builder.Build()
				Expect(err).NotTo(HaveOccurred())

				timeout := time.Second * 120

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

	Describe("Insecure (None/None) Connect", func() {
		var endpoint string

		It("should read data correctly", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			nodeIDStrings := []string{"ns=3;i=1003"}
			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				OPCUAConnection: &OPCUAConnection{
					Endpoint:       endpoint,
					Username:       "",
					Password:       "",
					SecurityMode:   "None",
					SecurityPolicy: "None",
				},
				NodeIDs: parsedNodeIDs,
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
				err = input.Close(ctx)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

	Describe("Secure (SignAndEncrypt/Basic256Sha256) Connect", func() {
		It("should read data correctly", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			nodeIDStrings := []string{"ns=3;i=1003"}
			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				OPCUAConnection: &OPCUAConnection{
					Endpoint:       endpoint,
					Username:       "",
					Password:       "",
					SecurityMode:   "SignAndEncrypt",
					SecurityPolicy: "Basic256Sha256",
				},
				NodeIDs: parsedNodeIDs,
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
				err = input.Close(ctx)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})
})
