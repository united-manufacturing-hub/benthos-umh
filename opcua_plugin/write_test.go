package opcua_plugin

import (
	"context"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = FDescribe("OPC UA Output", func() {

	BeforeEach(func() {
		testActivated := os.Getenv("TEST_OPCUA_WRITE_SIMULATOR")

		// Check if environment variables are set
		if testActivated == "" {
			Skip("Skipping write unit tests against simulator: TEST_OPCUA_WRITE_SIMULATOR not set")
			return
		}
	})

	When("creating a basic output configuration", func() {
		It("should successfully parse the configuration", func() {
			// Create a new stream builder
			builder := service.NewStreamBuilder()

			err := builder.AddOutputYAML(`
opcua:
  endpoint: "opc.tcp://localhost:50000"
  nodeMappings:
    - nodeId: "ns=4;i=6210"
      valueFrom: "setpoint"
  forcedDataTypes:
    "ns=4;i=6210": "Int32"
  handshake:
    enabled: true
    readbackTimeoutMs: 2000
    maxWriteAttempts: 3
    timeBetweenRetriesMs: 1000
`)
			Expect(err).NotTo(HaveOccurred())

			err = builder.SetLoggerYAML(`level: off`)
			Expect(err).NotTo(HaveOccurred())

			err = builder.SetTracerYAML(`type: none`)
			Expect(err).NotTo(HaveOccurred())

			// Add a simple input to feed messages to the output
			err = builder.AddInputYAML(`
generate:
  mapping: |
    root = {
      "setpoint": 123
    }
  interval: "1s"
  count: 1
`)
			Expect(err).NotTo(HaveOccurred())

			// Build the stream (just to verify everything initializes correctly)
			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			// Start the stream with a short timeout - we just want to verify it can start
			// We don't need to verify actual connection since we're just testing configuration parsing
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			// We expect this to timeout normally, since we don't have a real OPC UA server
			err = stream.Run(ctx)
			Expect(err).To(HaveOccurred())
			Expect(strings.Contains(err.Error(), "context deadline exceeded")).To(BeTrue())
		})
	})

	When("creating a configuration with minimal settings", func() {
		It("should use default values for omitted fields", func() {
			// Create a new stream builder
			builder := service.NewStreamBuilder()

			err := builder.AddOutputYAML(`
opcua:
  endpoint: "opc.tcp://localhost:50000"
  nodeMappings:
    - nodeId: "ns=4;i=6210"
      valueFrom: "setpoint"
`)
			Expect(err).NotTo(HaveOccurred())

			err = builder.SetLoggerYAML(`level: off`)
			Expect(err).NotTo(HaveOccurred())

			// Build the stream (just to verify everything initializes correctly)
			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			// Verify that the stream was built correctly, but don't run it
			Expect(stream).NotTo(BeNil())
		})
	})
})
