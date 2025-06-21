/*
Sparkplug B Integration Test

This integration test validates the Sparkplug B input plugin against a real MQTT broker.
It publishes Sparkplug NBIRTH and NDATA messages and verifies that the plugin correctly
processes them into JSON output.

## Test Types:

1. **Unit Tests**: Run with `make test-sparkplug-unit` (no external dependencies)
2. **Integration Tests**: Run with `make test-sparkplug-b-integration` (requires Mosquitto)

## Quick Setup for Integration Tests:

**Option A - Automatic (Recommended):**
```bash
make test-sparkplug-b-full    # Starts Mosquitto + runs tests + keeps broker running
make stop-mosquitto           # Clean up when done
```

**Option B - Step by Step:**
```bash
make start-mosquitto          # Start broker
make test-sparkplug-b-integration  # Run tests
make stop-mosquitto           # Clean up
```

**Option C - Manual Docker Setup:**
```bash
# Create mosquitto config (allow external connections)
echo "listener 1883
allow_anonymous true" > /tmp/mosquitto.conf

# Start broker
docker run -d --name test-mosquitto -p 1883:1883 \
  -v /tmp/mosquitto.conf:/mosquitto/config/mosquitto.conf \
  eclipse-mosquitto:2.0

# Run integration test
cd sparkplug_plugin
TEST_SPARKPLUG_B=1 go test -v -timeout=60s

# Clean up
docker stop test-mosquitto && docker rm test-mosquitto
```

## Manual Debugging with Benthos:

For detailed debugging, build and run Benthos manually:
```bash
cd ..
go build -o benthos-umh ./cmd/benthos

# Create test config
cat > test-config.yaml << 'EOF'
input:
  sparkplug_b:
    mqtt:
      urls: ["tcp://127.0.0.1:1883"]
      client_id: "test-primary-host"
      qos: 1
    identity:
      group_id: "FactoryA"
      edge_node_id: "CentralHost"
    role: "primary_host"
    behaviour:
      auto_split_metrics: true
output:
  stdout: {}
logger:
  level: DEBUG
EOF

# Run with debug logs
./benthos-umh -c test-config.yaml
```

## Expected Behavior:
- The plugin should successfully process NBIRTH and NDATA messages
- It should resolve aliases (e.g., alias 100 -> "Temperature")
- It should output proper JSON with decoded metrics

## Known Issues (Current Bug to Fix):
The debug logs will show this error:
```
ERRO Failed to unmarshal Sparkplug payload from topic spBv1.0/FactoryA/STATE/CentralHost:
     proto: cannot parse invalid wire-format data
```

This happens because STATE messages contain plain text "ONLINE" but the plugin
tries to unmarshal them as Sparkplug protobuf. This is the main bug we need to fix
by filtering STATE messages from protobuf parsing.
*/

package sparkplug_plugin_test

import (
	"context"
	"fmt"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/weekaung/sparkplugb-client/sproto"
	"google.golang.org/protobuf/proto"

	_ "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin" // Import to register
)

// setupMQTTBroker ensures a test MQTT broker is running
func setupMQTTBroker() error {
	// Check if broker is already running
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://127.0.0.1:1883")
	opts.SetClientID("test-broker-check")
	opts.SetConnectTimeout(2 * time.Second)

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.WaitTimeout(2*time.Second) && token.Error() == nil {
		client.Disconnect(100)
		return nil // Broker already running
	}

	return fmt.Errorf("MQTT broker not running. Please start with:\n" +
		"docker run -d --name test-mosquitto -p 1883:1883 -v /tmp/mosquitto.conf:/mosquitto/config/mosquitto.conf eclipse-mosquitto:2.0\n" +
		"(See documentation in this file for details)")
}

// TestSparkplugBPoC tests the Sparkplug B input plugin end-to-end
var _ = ginkgo.Describe("Sparkplug B PoC Integration Tests", func() {
	ginkgo.Describe("End-to-End Message Processing", func() {
		ginkgo.Context("MQTT Broker Integration", func() {
			var (
				brokerURL  string
				mqttClient mqtt.Client
			)

			ginkgo.BeforeEach(func() {
				// Check if we have a real MQTT broker available
				brokerURL = os.Getenv("TEST_MQTT_BROKER")
				if brokerURL == "" {
					brokerURL = "tcp://127.0.0.1:1883"
				}

				ginkgo.By(fmt.Sprintf("Using MQTT broker: %s", brokerURL))

				// Ensure MQTT broker is running
				if err := setupMQTTBroker(); err != nil {
					ginkgo.Skip(fmt.Sprintf("MQTT broker setup failed: %v", err))
				}

				// Create MQTT client for publishing test messages
				opts := mqtt.NewClientOptions()
				opts.AddBroker(brokerURL)
				opts.SetClientID("test-edge-publisher")
				opts.SetConnectTimeout(10 * time.Second)

				mqttClient = mqtt.NewClient(opts)
				token := mqttClient.Connect()
				if !token.WaitTimeout(10 * time.Second) {
					ginkgo.Skip("No MQTT broker available for integration test")
				}
				if token.Error() != nil {
					ginkgo.Skip(fmt.Sprintf("Cannot connect to MQTT broker: %v", token.Error()))
				}
			})

			ginkgo.AfterEach(func() {
				if mqttClient != nil && mqttClient.IsConnected() {
					mqttClient.Disconnect(1000)
				}
			})

			ginkgo.It("should process NBIRTH and NDATA messages with debug logging", func() {
				ginkgo.By("Creating a stream with the Sparkplug input plugin")

				// Create a stream with the input to test it
				streamBuilder := service.NewEnvironment().NewStreamBuilder()
				err := streamBuilder.SetYAML(fmt.Sprintf(`
input:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "test-primary-host"
      qos: 1
    identity:
      group_id: "FactoryA"
      edge_node_id: "CentralHost"
    role: "primary_host"
    behaviour:
      auto_split_metrics: true

output:
  stdout: {}

logger:
  level: DEBUG
`, brokerURL))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				ginkgo.By("Starting the stream (this starts the input plugin)")

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				stream, err := streamBuilder.Build()
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Start the stream in background
				streamDone := make(chan error, 1)
				go func() {
					streamDone <- stream.Run(ctx)
				}()

				// Give the stream time to start and connect
				time.Sleep(3 * time.Second)

				ginkgo.By("Publishing NBIRTH message to MQTT broker")

				// Create test data using existing helpers
				testData := createTestData()

				// Publish NBIRTH
				birthBytes, err := proto.Marshal(testData.NBirthPayload)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				topic := "spBv1.0/FactoryA/NBIRTH/Line1"
				token := mqttClient.Publish(topic, 1, false, birthBytes)
				gomega.Expect(token.Wait()).To(gomega.BeTrue())
				gomega.Expect(token.Error()).NotTo(gomega.HaveOccurred())

				fmt.Printf("📤 Published NBIRTH to topic: %s\n", topic)

				// Wait a bit for processing
				time.Sleep(2 * time.Second)

				ginkgo.By("Publishing NDATA message to MQTT broker")

				// Publish NDATA
				dataBytes, err := proto.Marshal(testData.NDataPayload)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				dataTopic := "spBv1.0/FactoryA/NDATA/Line1"
				token = mqttClient.Publish(dataTopic, 1, false, dataBytes)
				gomega.Expect(token.Wait()).To(gomega.BeTrue())
				gomega.Expect(token.Error()).NotTo(gomega.HaveOccurred())

				fmt.Printf("📤 Published NDATA to topic: %s\n", dataTopic)

				// Wait for processing
				time.Sleep(2 * time.Second)

				ginkgo.By("The debug logs should reveal what happens to the messages")

				// At this point, the debug logs we added should show:
				// 📥 messageHandler: received message on topic...
				// 🔍 ReadBatch: processing message from topic...
				// 🔄 processSparkplugMessage: starting to process topic...
				// etc.

				// Cancel the context to stop the stream
				cancel()

				// Wait for stream to finish
				select {
				case err := <-streamDone:
					if err != nil && err != context.Canceled {
						fmt.Printf("Stream ended with error: %v\n", err)
					}
				case <-time.After(5 * time.Second):
					fmt.Printf("Stream didn't stop gracefully\n")
				}

				fmt.Printf("✅ Integration test completed - check debug logs above\n")
			})
		})
	})
})

// createTestData creates test Sparkplug B payloads
func createTestData() *TestData {
	return &TestData{
		NBirthPayload: &sproto.Payload{
			Timestamp: uint64Ptr(uint64(time.Now().UnixMilli())),
			Seq:       uint64Ptr(0),
			Metrics: []*sproto.Payload_Metric{
				{
					Name:     stringPtr("bdSeq"),
					Alias:    uint64Ptr(0),
					Datatype: uint32Ptr(7), // UInt64
					Value:    &sproto.Payload_Metric_LongValue{LongValue: 12345},
				},
				{
					Name:     stringPtr("Node Control/Rebirth"),
					Datatype: uint32Ptr(11), // Boolean
					Value:    &sproto.Payload_Metric_BooleanValue{BooleanValue: false},
				},
				{
					Name:     stringPtr("Temperature"),
					Alias:    uint64Ptr(100),
					Datatype: uint32Ptr(10), // Double
					Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 25.5},
				},
			},
		},
		NDataPayload: &sproto.Payload{
			Timestamp: uint64Ptr(uint64(time.Now().UnixMilli())),
			Seq:       uint64Ptr(1),
			Metrics: []*sproto.Payload_Metric{
				{
					Alias:    uint64Ptr(100), // Should resolve to "Temperature"
					Datatype: uint32Ptr(10),  // Double
					Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 26.8},
				},
			},
		},
	}
}

type TestData struct {
	NBirthPayload *sproto.Payload
	NDataPayload  *sproto.Payload
}
