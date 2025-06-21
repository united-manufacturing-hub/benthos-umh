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
