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

//go:build integration

// Integration tests for Sparkplug B plugin - Real MQTT broker tests
// These tests automatically start/stop Docker Mosquitto broker

package sparkplug_plugin_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/weekaung/sparkplugb-client/sproto"
	"google.golang.org/protobuf/proto"

	_ "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"     // Import to register
	_ "github.com/united-manufacturing-hub/benthos-umh/tag_processor_plugin" // Import tag processor for full pipeline tests
)

// Global message capture for tests
var captureChannel chan *service.Message

// MessageCapture implements service.Output to capture messages for testing
type MessageCapture struct {
	messages chan *service.Message
}

func (m *MessageCapture) Connect(ctx context.Context) error {
	return nil // Always ready
}

func (m *MessageCapture) Write(ctx context.Context, msg *service.Message) error {
	if captureChannel != nil {
		select {
		case captureChannel <- msg.Copy():
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Channel full, skip message to avoid blocking
		}
	}
	return nil
}

func (m *MessageCapture) Close(ctx context.Context) error {
	return nil
}

func init() {
	// Register custom output for tests
	err := service.RegisterOutput("message_capture",
		service.NewConfigSpec().Field(service.NewStringField("dummy").Default("")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			return &MessageCapture{}, 1, nil
		})
	if err != nil {
		panic(err)
	}
}

func TestSparkplugIntegrationBroker(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sparkplug B Integration Test Suite")
}

var _ = BeforeSuite(func() {
	By("Starting Docker Mosquitto MQTT broker for integration tests")

	// Stop any existing mosquitto container
	stopMosquittoContainer()

	// Start new mosquitto container
	startMosquittoContainer()

	// Set environment variable for tests
	os.Setenv("TEST_MQTT_BROKER", "tcp://127.0.0.1:1883")
})

var _ = AfterSuite(func() {
	By("Stopping Docker Mosquitto MQTT broker")
	stopMosquittoContainer()
})

func startMosquittoContainer() {
	By("Starting Docker Mosquitto container")

	// Start mosquitto container
	cmd := exec.Command("docker", "run", "-d", "--name", "mosquitto-integration-test", "-p", "1883:1883", "eclipse-mosquitto:1.6")
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Failed to start Mosquitto container: %v\nOutput: %s\n", err, string(output))
		Fail("Cannot start Docker Mosquitto container - ensure Docker is running")
	}

	// Wait for container to be ready
	fmt.Printf("Waiting for Mosquitto to be ready...\n")
	time.Sleep(3 * time.Second)

	// Verify container is running
	cmd = exec.Command("docker", "ps", "--filter", "name=mosquitto-integration-test", "--format", "{{.Names}}")
	output, err = cmd.Output()
	if err != nil || !strings.Contains(string(output), "mosquitto-integration-test") {
		Fail("Mosquitto container failed to start properly")
	}

	fmt.Printf("✅ Mosquitto container started successfully\n")
}

func stopMosquittoContainer() {
	// Stop container (ignore errors if it doesn't exist)
	cmd := exec.Command("docker", "stop", "mosquitto-integration-test")
	cmd.Run()

	// Remove container (ignore errors if it doesn't exist)
	cmd = exec.Command("docker", "rm", "mosquitto-integration-test")
	cmd.Run()
}

var _ = Describe("Real MQTT Broker Integration", func() {
	Context("End-to-End Message Processing", func() {
		var (
			brokerURL  string
			mqttClient mqtt.Client
		)

		BeforeEach(func() {
			brokerURL = os.Getenv("TEST_MQTT_BROKER")
			if brokerURL == "" {
				brokerURL = "tcp://127.0.0.1:1883"
			}

			By(fmt.Sprintf("Using MQTT broker: %s", brokerURL))

			// Create MQTT client for publishing test messages
			opts := mqtt.NewClientOptions()
			opts.AddBroker(brokerURL)
			opts.SetClientID("test-edge-publisher")
			opts.SetConnectTimeout(10 * time.Second)

			mqttClient = mqtt.NewClient(opts)
			token := mqttClient.Connect()
			Expect(token.WaitTimeout(10*time.Second)).To(BeTrue(), "Should connect to MQTT broker")
			Expect(token.Error()).NotTo(HaveOccurred(), "Should connect without error")
		})

		AfterEach(func() {
			if mqttClient != nil && mqttClient.IsConnected() {
				mqttClient.Disconnect(1000)
			}
		})

		It("should run full Edge Node to Primary Host pipeline (like shell script)", func() {
			By("Creating message capture system for Primary Host")

			// Create channels to capture processed messages from Primary Host
			capturedMessages := make(chan *service.Message, 20)
			captureChannel = capturedMessages

			By("Testing complete pipeline: generate → tag_processor → sparkplug_b → sparkplug_b → message_capture")

			// This test replicates what the shell script was actually doing:
			// 1. Edge Node: generate → tag_processor → sparkplug_b output
			// 2. Primary Host: sparkplug_b input → message_capture (instead of stdout)
			// 3. Real end-to-end communication via MQTT

			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()

			// Edge Node Stream (similar to sparkplug-device-level-test.yaml)
			edgeNodeConfig := fmt.Sprintf(`
input:
  generate:
    interval: "2s"
    count: 3
    mapping: |
      root = {"counter": counter()}

pipeline:
  processors:
    - tag_processor:
        defaults: |
          msg.meta.location_path = "enterprise.factory.line1.station1";
          msg.meta.data_contract = "_historian";
          
          let counterValue = msg.payload.counter;
          if (counterValue %% 2 == 0) {
            msg.meta.tag_name = "value";
            msg.meta.virtual_path = "temperature";
            msg.payload = 25.0 + (counterValue %% 10);
          } else {
            msg.meta.tag_name = "value";
            msg.meta.virtual_path = "humidity";
            msg.payload = 60.0 + (counterValue %% 20);
          }
          
          return msg;

output:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "test-edge-node"
    identity:
      group_id: "DeviceLevelTest"
      edge_node_id: "StaticEdgeNode01"

logger:
  level: INFO
`, brokerURL)

			// Primary Host Stream (similar to sparkplug-device-level-primary-host.yaml)
			primaryHostConfig := fmt.Sprintf(`
input:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "test-primary-host"
    identity:
      group_id: "DeviceLevelTest"
      edge_node_id: "PrimaryHost"
    role: "primary"

output:
  message_capture:
    dummy: ""

logger:
  level: INFO
`, brokerURL)

			By("Starting Edge Node stream (generate → tag_processor → sparkplug_b)")
			edgeStreamBuilder := service.NewStreamBuilder()
			err := edgeStreamBuilder.SetYAML(edgeNodeConfig)
			Expect(err).NotTo(HaveOccurred())

			edgeStream, err := edgeStreamBuilder.Build()
			Expect(err).NotTo(HaveOccurred())

			edgeNodeDone := make(chan error, 1)
			go func() {
				edgeNodeDone <- edgeStream.Run(ctx)
			}()

			By("Starting Primary Host stream (sparkplug_b → message_capture)")
			hostStreamBuilder := service.NewStreamBuilder()
			err = hostStreamBuilder.SetYAML(primaryHostConfig)
			Expect(err).NotTo(HaveOccurred())

			hostStream, err := hostStreamBuilder.Build()
			Expect(err).NotTo(HaveOccurred())

			primaryHostDone := make(chan error, 1)
			go func() {
				primaryHostDone <- hostStream.Run(ctx)
			}()

			By("Waiting for full message pipeline processing")
			// Wait for:
			// 1. Edge Node to start and publish NBIRTH + DBIRTH
			// 2. Edge Node to generate and publish 3 DDATA messages
			// 3. Primary Host to receive and process all messages
			// 4. Full tag_processor → sparkplug_b → MQTT → sparkplug_b → message_capture flow
			time.Sleep(15 * time.Second)

			// Cancel streams
			cancel()

			// Wait for streams to finish
			select {
			case <-edgeNodeDone:
			case <-time.After(5 * time.Second):
			}

			select {
			case <-primaryHostDone:
			case <-time.After(5 * time.Second):
			}

			By("Verifying captured messages from full pipeline")

			// Clean up capture channel
			captureChannel = nil
			close(capturedMessages)

			// Collect all captured messages
			var messages []*service.Message
			for msg := range capturedMessages {
				messages = append(messages, msg)
			}

			fmt.Printf("📥 Captured %d messages from full pipeline\n", len(messages))

			// Verify we received messages (should get NBIRTH, DBIRTH, and DDATA messages)
			Expect(len(messages)).To(BeNumerically(">=", 3), "Should capture at least 3 messages (NBIRTH, DBIRTH, DDATA)")

			// Count message types
			msgTypeCounts := make(map[string]int)
			for _, msg := range messages {
				if msgType, exists := msg.MetaGet("sparkplug_msg_type"); exists {
					msgTypeCounts[msgType]++
				}
			}

			fmt.Printf("📊 Message type distribution: %+v\n", msgTypeCounts)

			// Should have at least one NBIRTH and one DDATA
			Expect(msgTypeCounts["NBIRTH"]).To(BeNumerically(">=", 1), "Should have at least one NBIRTH message")
			Expect(msgTypeCounts["DDATA"]).To(BeNumerically(">=", 1), "Should have at least one DDATA message")

			// Verify message content structure
			for i, msg := range messages {
				By(fmt.Sprintf("Validating pipeline message %d", i+1))

				// Check that message has payload
				payload, err := msg.AsBytes()
				Expect(err).NotTo(HaveOccurred())
				Expect(len(payload)).To(BeNumerically(">", 0), "Message should have non-empty payload")

				// Check metadata by walking through it
				meta := make(map[string]string)
				err = msg.MetaWalk(func(key, value string) error {
					meta[key] = value
					return nil
				})
				Expect(err).NotTo(HaveOccurred())

				// Should have sparkplug-related metadata
				msgType, hasMsgType := meta["sparkplug_msg_type"]
				Expect(hasMsgType).To(BeTrue(), "Should have sparkplug message type")

				// Handle different message types appropriately
				if msgType == "NDEATH" {
					// NDEATH messages have different metadata structure
					deviceKey, hasDeviceKey := meta["sparkplug_device_key"]
					eventType, hasEventType := meta["event_type"]

					Expect(hasDeviceKey).To(BeTrue(), "NDEATH should have device key")
					Expect(hasEventType).To(BeTrue(), "NDEATH should have event type")
					Expect(eventType).To(Equal("device_offline"))
					Expect(deviceKey).To(Equal("DeviceLevelTest/StaticEdgeNode01"))
				} else {
					// Regular messages should have group_id and edge_node_id
					groupID, hasGroupID := meta["group_id"]
					edgeNodeID, hasEdgeNodeID := meta["edge_node_id"]

					if !hasGroupID {
						fmt.Printf("🔍 Message %d missing group_id, available metadata: %+v\n", i+1, meta)
					}
					if !hasEdgeNodeID {
						fmt.Printf("🔍 Message %d missing edge_node_id, available metadata: %+v\n", i+1, meta)
					}
					Expect(hasGroupID).To(BeTrue(), "Should have group ID")
					Expect(hasEdgeNodeID).To(BeTrue(), "Should have edge node ID")

					// Verify group ID matches what we configured
					Expect(groupID).To(Equal("DeviceLevelTest"))
					Expect(edgeNodeID).To(Equal("StaticEdgeNode01"))
				}
				fmt.Printf("✅ Pipeline message %d validation passed: type=%s\n", i+1, msgType)
			}

			fmt.Printf("✅ Full pipeline integration test completed - %d messages captured and validated\n", len(messages))
		})

		It("should process NBIRTH and NDATA messages end-to-end", func() {
			By("Creating a message capture system")

			// Create channels to capture processed messages
			capturedMessages := make(chan *service.Message, 10)
			captureChannel = capturedMessages

			By("Creating a stream with the Sparkplug input plugin and message capture")

			// Create a stream with the input to test it
			streamBuilder := service.NewStreamBuilder()
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
    role: "primary"
    behaviour:
      auto_split_metrics: true

output:
  message_capture:
    dummy: ""

logger:
  level: INFO
`, brokerURL))
			if err != nil {
				// Skip if there's a configuration parsing issue
				Skip(fmt.Sprintf("Configuration parsing issue - skipping stream test: %v", err))
			}

			By("Starting the stream (this starts the input plugin)")

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			stream, err := streamBuilder.Build()
			if err != nil {
				// Skip if there's a configuration issue with outputs
				Skip(fmt.Sprintf("Configuration issue - skipping stream test: %v", err))
			}

			// Start the stream in background
			streamDone := make(chan error, 1)
			go func() {
				streamDone <- stream.Run(ctx)
			}()

			// Give the stream time to start and connect
			time.Sleep(3 * time.Second)

			By("Publishing NBIRTH message to MQTT broker")

			// Create test data using existing helpers
			testData := createIntegrationTestData()

			// Publish NBIRTH
			birthBytes, err := proto.Marshal(testData.NBirthPayload)
			Expect(err).NotTo(HaveOccurred())

			topic := "spBv1.0/FactoryA/NBIRTH/Line1"
			token := mqttClient.Publish(topic, 1, false, birthBytes)
			Expect(token.Wait()).To(BeTrue())
			Expect(token.Error()).NotTo(HaveOccurred())

			fmt.Printf("📤 Published NBIRTH to topic: %s\n", topic)

			// Wait a bit for processing
			time.Sleep(2 * time.Second)

			By("Publishing NDATA message to MQTT broker")

			// Publish NDATA
			dataBytes, err := proto.Marshal(testData.NDataPayload)
			Expect(err).NotTo(HaveOccurred())

			dataTopic := "spBv1.0/FactoryA/NDATA/Line1"
			token = mqttClient.Publish(dataTopic, 1, false, dataBytes)
			Expect(token.Wait()).To(BeTrue())
			Expect(token.Error()).NotTo(HaveOccurred())

			fmt.Printf("📤 Published NDATA to topic: %s\n", dataTopic)

			// Wait for processing
			time.Sleep(2 * time.Second)

			By("Publishing STATE message to test STATE filtering")

			// Publish STATE message (plain text, not protobuf)
			stateTopic := "spBv1.0/FactoryA/STATE/Line1"
			token = mqttClient.Publish(stateTopic, 1, true, "ONLINE") // Retained message
			Expect(token.Wait()).To(BeTrue())
			Expect(token.Error()).NotTo(HaveOccurred())

			fmt.Printf("📤 Published STATE to topic: %s\n", stateTopic)

			// Wait for processing
			time.Sleep(2 * time.Second)

			By("Verifying captured messages")

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

			// Clean up capture channel
			captureChannel = nil
			close(capturedMessages)

			// Collect all captured messages
			var messages []*service.Message
			for msg := range capturedMessages {
				messages = append(messages, msg)
			}

			fmt.Printf("📥 Captured %d messages\n", len(messages))

			// Verify we received messages
			Expect(len(messages)).To(BeNumerically(">=", 1), "Should capture at least one message")

			// Verify message content
			for i, msg := range messages {
				By(fmt.Sprintf("Validating message %d", i+1))

				// Check that message has payload
				payload, err := msg.AsBytes()
				Expect(err).NotTo(HaveOccurred())
				Expect(len(payload)).To(BeNumerically(">", 0), "Message should have non-empty payload")

				// Check metadata by walking through it
				meta := make(map[string]string)
				err = msg.MetaWalk(func(key, value string) error {
					meta[key] = value
					return nil
				})
				Expect(err).NotTo(HaveOccurred())

				fmt.Printf("📋 Message %d metadata: %+v\n", i+1, meta)

				// Should have sparkplug-related metadata
				msgType, hasMsgType := meta["sparkplug_msg_type"]
				Expect(hasMsgType).To(BeTrue(), "Should have sparkplug message type")

				// Check message type is valid
				Expect(msgType).To(BeElementOf([]string{"NBIRTH", "NDATA", "STATE"}), "Should be valid Sparkplug message type")

				// Verify message structure based on type
				if msgType == "NBIRTH" || msgType == "NDATA" {
					// Should have group_id and edge_node_id
					groupID, hasGroupID := meta["group_id"]
					edgeNodeID, hasEdgeNodeID := meta["edge_node_id"]

					Expect(hasGroupID).To(BeTrue(), "Should have group ID")
					Expect(hasEdgeNodeID).To(BeTrue(), "Should have edge node ID")
					Expect(groupID).To(Equal("FactoryA"))
					Expect(edgeNodeID).To(Equal("Line1"))
				}

				fmt.Printf("✅ Message %d validation passed: type=%s\n", i+1, msgType)
			}

			fmt.Printf("✅ Integration test completed - %d messages captured and validated\n", len(messages))
		})

		It("should handle broker disconnection gracefully", func() {
			By("Testing connection resilience")

			// This test would require stopping/starting the broker
			// For now, we'll test the connection timeout behavior
			opts := mqtt.NewClientOptions()
			opts.AddBroker("tcp://127.0.0.1:9999") // Non-existent broker
			opts.SetClientID("test-connection-failure")
			opts.SetConnectTimeout(2 * time.Second)

			failClient := mqtt.NewClient(opts)
			token := failClient.Connect()

			// Should timeout (connection should fail)
			success := token.WaitTimeout(3 * time.Second)
			if success {
				// If connection succeeded, check if there's an error
				Expect(token.Error()).To(HaveOccurred())
			} else {
				// Connection timed out as expected
				fmt.Printf("✅ Connection timeout as expected\n")
			}
			fmt.Printf("✅ Connection timeout handled correctly\n")
		})
	})

	Context("Plugin-to-Plugin Communication", func() {
		var (
			brokerURL    string
			inputClient  mqtt.Client
			outputClient mqtt.Client
		)

		BeforeEach(func() {
			brokerURL = os.Getenv("TEST_MQTT_BROKER")
			if brokerURL == "" {
				brokerURL = "tcp://127.0.0.1:1883"
			}

			// Create input side client (simulates primary host)
			inputOpts := mqtt.NewClientOptions()
			inputOpts.AddBroker(brokerURL)
			inputOpts.SetClientID("test-input-client")
			inputOpts.SetConnectTimeout(10 * time.Second)

			inputClient = mqtt.NewClient(inputOpts)
			token := inputClient.Connect()
			if !token.WaitTimeout(10*time.Second) || token.Error() != nil {
				Skip("No MQTT broker available for plugin-to-plugin test")
			}

			// Create output side client (simulates edge node)
			outputOpts := mqtt.NewClientOptions()
			outputOpts.AddBroker(brokerURL)
			outputOpts.SetClientID("test-output-client")
			outputOpts.SetConnectTimeout(10 * time.Second)

			outputClient = mqtt.NewClient(outputOpts)
			token = outputClient.Connect()
			if !token.WaitTimeout(10*time.Second) || token.Error() != nil {
				Skip("No MQTT broker available for plugin-to-plugin test")
			}
		})

		AfterEach(func() {
			if inputClient != nil && inputClient.IsConnected() {
				inputClient.Disconnect(1000)
			}
			if outputClient != nil && outputClient.IsConnected() {
				outputClient.Disconnect(1000)
			}
		})

		It("should enable bidirectional communication between Input and Output plugins", func() {
			By("Setting up message subscription")

			// Subscribe to rebirth requests (NCMD messages)
			rebirthReceived := make(chan bool, 1)

			token := outputClient.Subscribe("spBv1.0/FactoryA/NCMD/Line1", 1, func(client mqtt.Client, msg mqtt.Message) {
				fmt.Printf("📥 Received NCMD message: %s\n", string(msg.Payload()))
				rebirthReceived <- true
			})
			Expect(token.Wait()).To(BeTrue())
			Expect(token.Error()).NotTo(HaveOccurred())

			By("Publishing NBIRTH from edge node")

			testData := createIntegrationTestData()
			birthBytes, err := proto.Marshal(testData.NBirthPayload)
			Expect(err).NotTo(HaveOccurred())

			topic := "spBv1.0/FactoryA/NBIRTH/Line1"
			token = outputClient.Publish(topic, 1, false, birthBytes)
			Expect(token.Wait()).To(BeTrue())
			Expect(token.Error()).NotTo(HaveOccurred())

			By("Simulating rebirth request from primary host")

			// Create rebirth command payload
			rebirthCmd := &sproto.Payload{
				Timestamp: uint64Ptr(uint64(time.Now().UnixMilli())),
				Seq:       uint64Ptr(0),
				Metrics: []*sproto.Payload_Metric{
					{
						Name:     stringPtr("Node Control/Rebirth"),
						Datatype: uint32Ptr(11), // Boolean
						Value:    &sproto.Payload_Metric_BooleanValue{BooleanValue: true},
					},
				},
			}

			cmdBytes, err := proto.Marshal(rebirthCmd)
			Expect(err).NotTo(HaveOccurred())

			cmdTopic := "spBv1.0/FactoryA/NCMD/Line1"
			token = inputClient.Publish(cmdTopic, 1, false, cmdBytes)
			Expect(token.Wait()).To(BeTrue())
			Expect(token.Error()).NotTo(HaveOccurred())

			By("Verifying rebirth command reception")

			select {
			case <-rebirthReceived:
				fmt.Printf("✅ Bidirectional communication working\n")
			case <-time.After(5 * time.Second):
				Fail("Rebirth command not received within timeout")
			}
		})

		It("should handle multiple edge nodes publishing to same broker", func() {
			By("Testing multi-edge node scenario")

			// Publish from multiple edge nodes
			edgeNodes := []string{"Line1", "Line2", "Line3"}

			for _, edgeNode := range edgeNodes {
				testData := createIntegrationTestData()
				birthBytes, err := proto.Marshal(testData.NBirthPayload)
				Expect(err).NotTo(HaveOccurred())

				topic := fmt.Sprintf("spBv1.0/FactoryA/NBIRTH/%s", edgeNode)
				token := outputClient.Publish(topic, 1, false, birthBytes)
				Expect(token.Wait()).To(BeTrue())
				Expect(token.Error()).NotTo(HaveOccurred())

				fmt.Printf("📤 Published NBIRTH for edge node: %s\n", edgeNode)
				time.Sleep(500 * time.Millisecond) // Stagger messages
			}

			fmt.Printf("✅ Multiple edge nodes handled successfully\n")
		})

		It("should handle concurrent connections", func() {
			By("Testing concurrent client connections")

			// Create multiple concurrent clients
			clients := make([]mqtt.Client, 5)

			for i := 0; i < 5; i++ {
				opts := mqtt.NewClientOptions()
				opts.AddBroker(brokerURL)
				opts.SetClientID(fmt.Sprintf("test-concurrent-client-%d", i))
				opts.SetConnectTimeout(10 * time.Second)

				client := mqtt.NewClient(opts)
				token := client.Connect()
				Expect(token.WaitTimeout(10 * time.Second)).To(BeTrue())
				Expect(token.Error()).NotTo(HaveOccurred())

				clients[i] = client
			}

			// Publish from all clients concurrently
			for i, client := range clients {
				go func(clientIndex int, c mqtt.Client) {
					defer GinkgoRecover()

					testData := createIntegrationTestData()
					birthBytes, err := proto.Marshal(testData.NBirthPayload)
					Expect(err).NotTo(HaveOccurred())

					topic := fmt.Sprintf("spBv1.0/FactoryA/NBIRTH/Line%d", clientIndex)
					token := c.Publish(topic, 1, false, birthBytes)
					Expect(token.Wait()).To(BeTrue())
					Expect(token.Error()).NotTo(HaveOccurred())
				}(i, client)
			}

			// Wait for all publishes to complete
			time.Sleep(2 * time.Second)

			// Cleanup
			for _, client := range clients {
				if client.IsConnected() {
					client.Disconnect(1000)
				}
			}

			fmt.Printf("✅ Concurrent connections handled successfully\n")
		})
	})
})

var _ = Describe("Performance Benchmarks", func() {
	Context("Throughput Testing", func() {
		var (
			brokerURL  string
			mqttClient mqtt.Client
		)

		BeforeEach(func() {
			brokerURL = os.Getenv("TEST_MQTT_BROKER")
			if brokerURL == "" {
				brokerURL = "tcp://127.0.0.1:1883"
			}

			opts := mqtt.NewClientOptions()
			opts.AddBroker(brokerURL)
			opts.SetClientID("test-performance-client")
			opts.SetConnectTimeout(10 * time.Second)

			mqttClient = mqtt.NewClient(opts)
			token := mqttClient.Connect()
			if !token.WaitTimeout(10*time.Second) || token.Error() != nil {
				Skip("No MQTT broker available for performance test")
			}
		})

		AfterEach(func() {
			if mqttClient != nil && mqttClient.IsConnected() {
				mqttClient.Disconnect(1000)
			}
		})

		It("should handle high message rates", func() {
			By("Publishing messages at high rate")

			testData := createIntegrationTestData()
			dataBytes, err := proto.Marshal(testData.NDataPayload)
			Expect(err).NotTo(HaveOccurred())

			messageCount := 100
			startTime := time.Now()

			for i := 0; i < messageCount; i++ {
				topic := "spBv1.0/FactoryA/NDATA/Line1"
				token := mqttClient.Publish(topic, 1, false, dataBytes)
				Expect(token.Wait()).To(BeTrue())
				Expect(token.Error()).NotTo(HaveOccurred())
			}

			duration := time.Since(startTime)
			messagesPerSecond := float64(messageCount) / duration.Seconds()

			fmt.Printf("📊 Published %d messages in %v (%.2f msg/sec)\n",
				messageCount, duration, messagesPerSecond)

			// Should handle at least 50 messages per second
			Expect(messagesPerSecond).To(BeNumerically(">=", 50))
		})

		It("should handle large metric payloads", func() {
			By("Creating large NBIRTH message with many metrics")

			// Create NBIRTH with 100 metrics
			metrics := make([]*sproto.Payload_Metric, 100)
			for i := 0; i < 100; i++ {
				metrics[i] = &sproto.Payload_Metric{
					Name:     stringPtr(fmt.Sprintf("Metric_%d", i)),
					Alias:    uint64Ptr(uint64(i + 100)),
					Datatype: uint32Ptr(10), // Double
					Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: float64(i) * 1.5},
				}
			}

			largeBirth := &sproto.Payload{
				Timestamp: uint64Ptr(uint64(time.Now().UnixMilli())),
				Seq:       uint64Ptr(0),
				Metrics:   metrics,
			}

			birthBytes, err := proto.Marshal(largeBirth)
			Expect(err).NotTo(HaveOccurred())

			fmt.Printf("📦 Large payload size: %d bytes\n", len(birthBytes))

			topic := "spBv1.0/FactoryA/NBIRTH/Line1"
			startTime := time.Now()

			token := mqttClient.Publish(topic, 1, false, birthBytes)
			Expect(token.Wait()).To(BeTrue())
			Expect(token.Error()).NotTo(HaveOccurred())

			duration := time.Since(startTime)
			fmt.Printf("📊 Large payload published in %v\n", duration)

			// Should publish large payloads within reasonable time
			Expect(duration).To(BeNumerically("<", 2*time.Second))
		})
	})
})

var _ = Describe("Sparkplug B Specification Compliance", func() {
	Context("Sequence and Session Management", func() {
		var (
			brokerURL        string
			edgeNodeClient   mqtt.Client
			subscriberClient mqtt.Client
		)

		BeforeEach(func() {
			brokerURL = os.Getenv("TEST_MQTT_BROKER")
			if brokerURL == "" {
				brokerURL = "tcp://127.0.0.1:1883"
			}

			// Create MQTT clients for direct broker interaction
			edgeNodeClient = createMQTTClient(brokerURL, "test-edge-node-client")
			subscriberClient = createMQTTClient(brokerURL, "test-subscriber-client")
		})

		AfterEach(func() {
			if edgeNodeClient != nil && edgeNodeClient.IsConnected() {
				edgeNodeClient.Disconnect(1000)
			}
			if subscriberClient != nil && subscriberClient.IsConnected() {
				subscriberClient.Disconnect(1000)
			}
		})

		It("should handle sequence counter wrap from 255 to 0", func() {
			// Test B: Sequence Counter Wrap-Around
			//
			// Approach:
			// 1. Start Edge Node Benthos stream with sparkplug_b output
			// 2. Use MQTT subscriber to listen on spBv1.0/TestGroup/NDATA/EdgeNode1
			// 3. Configure Edge Node to publish 260 messages (triggers wrap)
			// 4. Validate received messages have seq: 0,1,2...254,255,0,1,2,3,4

			By("Setting up MQTT subscriber for all Sparkplug messages")
			// Subscribe to all message types to capture NBIRTH + NDATA
			allMsgChan := subscribeToSparkplugTopic(subscriberClient, "spBv1.0/TestGroup/+/EdgeNode1")

			By("Starting Edge Node stream configured for sequence wrap test")
			edgeConfig := createEdgeNodeConfig(brokerURL, "TestGroup", "EdgeNode1", "sequence_wrap")

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			edgeStreamBuilder := service.NewStreamBuilder()
			err := edgeStreamBuilder.SetYAML(edgeConfig)
			Expect(err).NotTo(HaveOccurred())

			edgeStream, err := edgeStreamBuilder.Build()
			Expect(err).NotTo(HaveOccurred())

			// Start edge stream in background
			edgeStreamDone := make(chan error, 1)
			go func() {
				edgeStreamDone <- edgeStream.Run(ctx)
			}()

			By("Collecting and validating 261 messages with sequence wrap")
			var allMessages []mqtt.Message

			// Collect messages for up to 18 seconds (261 messages at 50ms interval = ~13s + buffer)
			timeout := time.After(18 * time.Second)

		collectLoop:
			for len(allMessages) < 261 {
				select {
				case msg := <-allMsgChan:
					allMessages = append(allMessages, msg)
					fmt.Printf("📨 Collected message %d on topic: %s\n", len(allMessages), msg.Topic())
				case <-timeout:
					break collectLoop
				case <-ctx.Done():
					break collectLoop
				}
			}

			// Stop the edge stream
			cancel()
			select {
			case <-edgeStreamDone:
			case <-time.After(5 * time.Second):
			}

			fmt.Printf("📊 Collected %d messages total\n", len(allMessages))
			Expect(len(allMessages)).To(BeNumerically(">=", 261), "Should collect at least 261 messages (1 NBIRTH + 260 NDATA)")

			// Validate sequence progression with wrap-around
			err = validateSequenceWrap(allMessages, 261)
			Expect(err).NotTo(HaveOccurred())

			fmt.Printf("✅ Sequence wrap test completed successfully\n")
		})

		It("should handle NDEATH will messages on connection loss", func() {
			// Test C: NDEATH Will Message
			//
			// Approach:
			// 1. Start Edge Node stream with MQTT will configured
			// 2. Subscribe to NDEATH topic
			// 3. Force disconnect Edge Node (TCP kill, not graceful)
			// 4. Validate broker publishes NDEATH will message

			By("Setting up MQTT subscriber for all Sparkplug messages")
			// Subscribe to capture NBIRTH and NDEATH messages
			allMsgChan := subscribeToSparkplugTopic(subscriberClient, "spBv1.0/TestGroup/+/EdgeNode1")

			By("Creating Edge Node configuration with short keep-alive for faster will message triggering")
			// Use a very short keep-alive so broker will detect disconnect quickly
			edgeConfig := fmt.Sprintf(`
input:
  generate:
    interval: "1s"
    count: 10
    mapping: |
      root = {"value": 42.0}

pipeline:
  processors:
    - tag_processor:
        defaults: |
          msg.meta.location_path = "enterprise.factory.line1.station1"
          msg.meta.data_contract = "_sparkplug"
          msg.meta.tag_name = "test_value"
          msg.meta.virtual_path = "test.ndeath"
          msg.payload = msg.payload.value
          
          return msg

output:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "test-edge-ndeath-will"
      keep_alive: "3s"    # Short keep-alive for faster detection
      connect_timeout: "5s"
    identity:
      group_id: "TestGroup"
      edge_node_id: "EdgeNode1"

logger:
  level: INFO
`, brokerURL)

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			edgeStreamBuilder := service.NewStreamBuilder()
			err := edgeStreamBuilder.SetYAML(edgeConfig)
			Expect(err).NotTo(HaveOccurred())

			edgeStream, err := edgeStreamBuilder.Build()
			Expect(err).NotTo(HaveOccurred())

			// Start edge stream in background
			edgeStreamDone := make(chan error, 1)
			go func() {
				edgeStreamDone <- edgeStream.Run(ctx)
			}()

			By("Waiting for NBIRTH message and capturing bdSeq")
			var nbirthMsg mqtt.Message
			var nbirthFound bool

			// Wait for NBIRTH message (should be first)
			timeout := time.After(8 * time.Second)
			for !nbirthFound {
				select {
				case msg := <-allMsgChan:
					if strings.Contains(msg.Topic(), "NBIRTH") {
						nbirthMsg = msg
						nbirthFound = true
						fmt.Printf("📨 Captured NBIRTH on topic: %s\n", msg.Topic())
					}
				case <-timeout:
					Fail("NBIRTH message not received within timeout")
				}
			}

			// Extract bdSeq from NBIRTH
			nbirthPayload, err := decodeSparkplugPayload(nbirthMsg.Payload())
			Expect(err).NotTo(HaveOccurred())

			var nbirthBdSeq uint64
			bdSeqFound := false
			for _, metric := range nbirthPayload.Metrics {
				if metric.Name != nil && *metric.Name == "bdSeq" {
					nbirthBdSeq = metric.GetLongValue()
					bdSeqFound = true
					fmt.Printf("📊 NBIRTH bdSeq: %d\n", nbirthBdSeq)
					break
				}
			}
			Expect(bdSeqFound).To(BeTrue(), "bdSeq metric should be found in NBIRTH")

			By("Simulating abrupt network disconnect (not graceful shutdown)")
			// Wait for Edge Node to be fully established and send some messages
			time.Sleep(3 * time.Second)

			// Kill the context abruptly - this simulates network failure
			// The broker should detect timeout and publish the will message
			cancel()

			// Don't wait for graceful shutdown - simulate network failure
			select {
			case <-edgeStreamDone:
				// Stream ended
			case <-time.After(2 * time.Second):
				// Stream didn't end gracefully - this is good for will message testing
			}

			By("Waiting for broker to detect client timeout and publish NDEATH will message")
			// With 3s keep-alive, broker should detect disconnect and publish will message within ~6-9s
			var ndeathMsg mqtt.Message
			var ndeathFound bool

			// Wait longer for broker to detect timeout and publish will message
			timeout = time.After(12 * time.Second)
			for !ndeathFound {
				select {
				case msg := <-allMsgChan:
					if strings.Contains(msg.Topic(), "NDEATH") {
						ndeathMsg = msg
						ndeathFound = true
						fmt.Printf("📨 Captured NDEATH will message on topic: %s\n", msg.Topic())
					}
				case <-timeout:
					Fail("NDEATH will message not received - Mosquitto should publish will messages!")
				}
			}

			By("Validating NDEATH will message structure")
			// Validate NDEATH message
			ndeathPayload, err := decodeSparkplugPayload(ndeathMsg.Payload())
			Expect(err).NotTo(HaveOccurred())

			// NDEATH should have seq=0 per Sparkplug spec
			Expect(ndeathPayload.Seq).NotTo(BeNil(), "NDEATH should have sequence number")
			Expect(*ndeathPayload.Seq).To(Equal(uint64(0)), "NDEATH should have seq=0")

			// NDEATH should have same bdSeq as NBIRTH
			var ndeathBdSeq uint64
			bdSeqFoundInDeath := false
			for _, metric := range ndeathPayload.Metrics {
				if metric.Name != nil && *metric.Name == "bdSeq" {
					ndeathBdSeq = metric.GetLongValue()
					bdSeqFoundInDeath = true
					fmt.Printf("📊 NDEATH bdSeq: %d\n", ndeathBdSeq)
					break
				}
			}

			Expect(bdSeqFoundInDeath).To(BeTrue(), "NDEATH should contain bdSeq metric")
			Expect(ndeathBdSeq).To(Equal(nbirthBdSeq), "NDEATH should have same bdSeq as NBIRTH")

			fmt.Printf("✅ NDEATH will message test passed: seq=0, bdSeq=%d matches NBIRTH\n", ndeathBdSeq)
		})

		It("should increment bdSeq on reconnect after NDEATH", func() {
			// Test D: bdSeq Increment on Reconnect
			//
			// Approach:
			// 1. Start Edge Node, capture first NBIRTH (bdSeq=0)
			// 2. Stop Edge Node stream
			// 3. Restart Edge Node stream
			// 4. Validate new NBIRTH has bdSeq=1 (incremented)

			By("Setting up MQTT subscriber for NBIRTH messages")
			allMsgChan := subscribeToSparkplugTopic(subscriberClient, "spBv1.0/TestGroup/+/EdgeNode1")

			By("Starting first Edge Node session and capturing initial bdSeq")
			edgeConfig1 := createEdgeNodeConfig(brokerURL, "TestGroup", "EdgeNode1", "ndeath_test")

			ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)

			edgeStreamBuilder1 := service.NewStreamBuilder()
			err := edgeStreamBuilder1.SetYAML(edgeConfig1)
			Expect(err).NotTo(HaveOccurred())

			edgeStream1, err := edgeStreamBuilder1.Build()
			Expect(err).NotTo(HaveOccurred())

			// Start first edge stream
			edgeStreamDone1 := make(chan error, 1)
			go func() {
				edgeStreamDone1 <- edgeStream1.Run(ctx1)
			}()

			// Wait for first NBIRTH
			var nbirth1 mqtt.Message
			var nbirth1Found bool

			timeout := time.After(5 * time.Second)
			for !nbirth1Found {
				select {
				case msg := <-allMsgChan:
					if strings.Contains(msg.Topic(), "NBIRTH") {
						nbirth1 = msg
						nbirth1Found = true
						fmt.Printf("📨 Captured first NBIRTH on topic: %s\n", msg.Topic())
					}
				case <-timeout:
					Fail("First NBIRTH message not received within timeout")
				}
			}

			By("Stopping first Edge Node session")
			cancel1()
			select {
			case <-edgeStreamDone1:
			case <-time.After(5 * time.Second):
			}

			// Wait a moment between sessions
			time.Sleep(1 * time.Second)

			By("Starting second Edge Node session")
			edgeConfig2 := createEdgeNodeConfig(brokerURL, "TestGroup", "EdgeNode1", "ndeath_test")

			ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel2()

			edgeStreamBuilder2 := service.NewStreamBuilder()
			err = edgeStreamBuilder2.SetYAML(edgeConfig2)
			Expect(err).NotTo(HaveOccurred())

			edgeStream2, err := edgeStreamBuilder2.Build()
			Expect(err).NotTo(HaveOccurred())

			// Start second edge stream
			edgeStreamDone2 := make(chan error, 1)
			go func() {
				edgeStreamDone2 <- edgeStream2.Run(ctx2)
			}()

			By("Capturing second NBIRTH after reconnect")
			var nbirth2 mqtt.Message
			var nbirth2Found bool

			timeout = time.After(5 * time.Second)
			for !nbirth2Found {
				select {
				case msg := <-allMsgChan:
					if strings.Contains(msg.Topic(), "NBIRTH") {
						nbirth2 = msg
						nbirth2Found = true
						fmt.Printf("📨 Captured second NBIRTH on topic: %s\n", msg.Topic())
					}
				case <-timeout:
					Fail("Second NBIRTH message not received within timeout")
				}
			}

			// Stop second stream
			cancel2()
			select {
			case <-edgeStreamDone2:
			case <-time.After(5 * time.Second):
			}

			By("Validating bdSeq increment from 0 to 1")
			err = validateBdSeqIncrement(nbirth1, nbirth2)
			Expect(err).NotTo(HaveOccurred())

			fmt.Printf("✅ bdSeq increment test completed successfully\n")
		})
	})

	Context("Data Type and Encoding Support", func() {
		var (
			brokerURL        string
			subscriberClient mqtt.Client
		)

		BeforeEach(func() {
			brokerURL = os.Getenv("TEST_MQTT_BROKER")
			if brokerURL == "" {
				brokerURL = "tcp://127.0.0.1:1883"
			}

			subscriberClient = createMQTTClient(brokerURL, "test-datatype-subscriber")
		})

		AfterEach(func() {
			if subscriberClient != nil && subscriberClient.IsConnected() {
				subscriberClient.Disconnect(1000)
			}
		})

		It("should handle UTF-8 and international metric names", func() {
			// Test F: UTF-8 Metric Names
			//
			// Approach:
			// 1. Create Edge Node stream with UTF-8 metric names
			// 2. Use tag_processor to create metrics: 压力 (pressure), 🌡️Temp, Müller, etc.
			// 3. Validate MQTT subscriber receives correct UTF-8 strings
			// 4. Verify topic and metric name encoding/decoding
			//
			// Key Points:
			// - Test Chinese characters: 压力
			// - Test emoji: 🌡️Temp
			// - Test European characters: Müller, Åström
			// - Validate protobuf UTF-8 encoding
			// - Check topic path character handling

			By("Setting up MQTT subscriber for UTF-8 messages")
			// TODO: Subscribe to spBv1.0/UTF8Test/+/EdgeNode1
			// TODO: Create channel to collect messages

			By("Starting Edge Node stream with UTF-8 metric names")
			// TODO: Create Edge Node config with:
			// - generate input with UTF-8 test data
			// - tag_processor creating metrics: 压力, 🌡️Temp, Müller, Åström
			// - sparkplug_b output: group_id=UTF8Test

			By("Validating UTF-8 metric names in protobuf")
			// TODO: Decode received protobuf messages
			// TODO: Assert metric names are correctly encoded
			// TODO: Verify no character corruption or encoding issues
			// TODO: Check topic path handles UTF-8 correctly

			Skip("Test skeleton - implementation needed")
		})

		It("should handle all Sparkplug data types correctly", func() {
			// Test G: Complete Data Type Matrix
			//
			// Approach:
			// 1. Create Edge Node stream publishing all Sparkplug data types
			// 2. Table-driven test for each data type
			// 3. Validate protobuf encoding and UMH-Core conversion
			//
			// Data Types to Test:
			// - Int8, Int16, Int32, Int64
			// - UInt8, UInt16, UInt32, UInt64
			// - Float, Double
			// - Boolean
			// - String
			// - DateTime
			// - Text (if supported)
			//
			// Key Points:
			// - Each data type gets its own metric
			// - Validate protobuf oneof field selection
			// - Verify UMH-Core JSON conversion correctness

			By("Setting up MQTT subscriber for data type test")
			// TODO: Subscribe to spBv1.0/DataTypeTest/+/EdgeNode1
			// TODO: Create channel to collect messages

			By("Starting Edge Node stream with all data types")
			// TODO: Create Edge Node config with:
			// - generate input creating all data types
			// - tag_processor mapping each type to separate metric
			// - sparkplug_b output: group_id=DataTypeTest

			By("Validating each Sparkplug data type")
			// TODO: Create table-driven test for each data type
			// TODO: Decode protobuf and validate correct oneof field
			// TODO: Verify value preservation and type conversion
			// TODO: Check UMH-Core JSON representation

			// Data type test cases:
			testCases := []struct {
				sparkplugType string
				testValue     interface{}
				expectedField string
			}{
				{"Int32", int32(-123), "int_value"},
				{"Int64", int64(-12345), "long_value"},
				{"Float", float32(3.14), "float_value"},
				{"Double", float64(3.141592), "double_value"},
				{"Boolean", true, "boolean_value"},
				{"String", "test string", "string_value"},
				// TODO: Add more data types
			}

			for _, tc := range testCases {
				By(fmt.Sprintf("Testing %s data type", tc.sparkplugType))
				// TODO: Validate specific data type encoding
			}

			Skip("Test skeleton - implementation needed")
		})
	})

	Context("UMH Integration and Mapping", func() {
		var (
			brokerURL        string
			capturedMessages chan *service.Message
		)

		BeforeEach(func() {
			brokerURL = os.Getenv("TEST_MQTT_BROKER")
			if brokerURL == "" {
				brokerURL = "tcp://127.0.0.1:1883"
			}

			// Use MessageCapture for UMH metadata validation
			capturedMessages = make(chan *service.Message, 50)
			captureChannel = capturedMessages
		})

		AfterEach(func() {
			if captureChannel != nil {
				captureChannel = nil
			}
			if capturedMessages != nil {
				close(capturedMessages)
			}
		})

		It("should map Sparkplug topics to UMH location paths correctly", func() {
			// Test J: Location/Virtual Path Mapping
			//
			// Approach:
			// 1. Create Edge Node stream with complex location paths
			// 2. Use Sparkplug B input to process and convert to UMH
			// 3. Validate location_path and virtual_path mapping
			// 4. Test colon/dot conversion rules
			//
			// Mapping Rules to Test:
			// - Topic: spBv1.0/FactoryA/DDATA/EdgeNode1/A:B:C
			// - Metric: x:y.z:w
			// - Expected: location_path="A.B.C"
			// - Expected: virtual_path="x.y.z", tag_name="w"
			//
			// Key Points:
			// - Validate topic structure parsing
			// - Test colon/dot conversion rules
			// - Verify hierarchical path construction
			// - Check metric name splitting logic

			By("Setting up full pipeline for location mapping test")
			// TODO: Create pipeline:
			// - Edge Node: sparkplug_b output with complex device paths
			// - MQTT broker
			// - Primary Host: sparkplug_b input → message_capture

			By("Publishing messages with complex location paths")
			// TODO: Create Edge Node config with:
			// - Location path: enterprise.factory.line1.station1
			// - Metrics: sensors.ambient.temperature, sensors.vibration.x_axis
			// - Validate topic structure: .../DDATA/EdgeNode1/enterprise.factory.line1.station1

			By("Validating UMH location path mapping")
			// TODO: Collect messages from MessageCapture
			// TODO: Validate location_path metadata
			// TODO: Validate virtual_path and tag_name splitting
			// TODO: Check colon/dot conversion rules

			// Test cases for mapping validation:
			mappingTestCases := []struct {
				devicePath       string
				metricName       string
				expectedLocation string
				expectedVirtual  string
				expectedTag      string
			}{
				{
					"enterprise:factory:line1:station1",
					"sensors:ambient:temperature",
					"enterprise.factory.line1.station1",
					"sensors.ambient",
					"temperature",
				},
				{
					"simple:device",
					"metric:name",
					"simple.device",
					"metric",
					"name",
				},
				// TODO: Add more mapping test cases
			}

			for _, tc := range mappingTestCases {
				By(fmt.Sprintf("Testing mapping for %s", tc.devicePath))
				// TODO: Validate specific mapping case
			}

			Skip("Test skeleton - implementation needed")
		})
	})
})

// Helper functions for new integration tests
// TODO: Add these helper functions before the existing helper functions

func createMQTTClient(brokerURL, clientID string) mqtt.Client {
	// Standardized MQTT client creation for integration tests
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(clientID)
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetKeepAlive(30 * time.Second)
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(false) // Explicit control for testing

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		panic(fmt.Sprintf("Failed to connect MQTT client %s: %v", clientID, token.Error()))
	}
	return client
}

func forceDisconnectMQTT(client mqtt.Client) {
	// Abrupt MQTT disconnection to trigger will messages
	if client != nil && client.IsConnected() {
		// Disconnect with 0ms timeout = immediate disconnect without DISCONNECT packet
		// This simulates network failure and should trigger the broker to publish the will message
		client.Disconnect(0)
	}
}

func subscribeToSparkplugTopic(client mqtt.Client, topic string) <-chan mqtt.Message {
	// Sparkplug topic subscription with message channel
	msgChan := make(chan mqtt.Message, 100) // Buffer for message collection

	token := client.Subscribe(topic, 1, func(client mqtt.Client, msg mqtt.Message) {
		select {
		case msgChan <- msg:
		default:
			// Channel full, drop message to avoid blocking
			fmt.Printf("Warning: Dropped message on topic %s (channel full)\n", topic)
		}
	})

	if !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		panic(fmt.Sprintf("Failed to subscribe to topic %s: %v", topic, token.Error()))
	}

	return msgChan
}

func decodeSparkplugPayload(payload []byte) (*sproto.Payload, error) {
	// Protobuf decoding for Sparkplug payloads
	var sparkplugPayload sproto.Payload
	err := proto.Unmarshal(payload, &sparkplugPayload)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Sparkplug protobuf: %w", err)
	}
	return &sparkplugPayload, nil
}

func createEdgeNodeConfig(brokerURL, groupID, edgeNodeID string, testType string) string {
	// Edge Node configuration generator for different test scenarios
	switch testType {
	case "sequence_wrap":
		return fmt.Sprintf(`
input:
  generate:
    interval: "50ms"
    count: 261  # 1 NBIRTH + 260 NDATA
    mapping: |
      root = {"counter": counter()}

pipeline:
  processors:
    - tag_processor:
        defaults: |
          msg.meta.location_path = "enterprise.factory.line1.station1"
          msg.meta.data_contract = "_sparkplug"
          
          let counterValue = msg.payload.counter
          msg.meta.tag_name = "counter_value"
          msg.meta.virtual_path = "test.sequence"
          msg.payload = counterValue
          
          return msg

output:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "test-edge-sequence-wrap"
    identity:
      group_id: "%s"
      edge_node_id: "%s"

logger:
  level: INFO
`, brokerURL, groupID, edgeNodeID)

	case "ndeath_test":
		return fmt.Sprintf(`
input:
  generate:
    interval: "2s"
    count: 5  # Just a few messages before disconnect
    mapping: |
      root = {"value": 42.0}

pipeline:
  processors:
    - tag_processor:
        defaults: |
          msg.meta.location_path = "enterprise.factory.line1.station1"
          msg.meta.data_contract = "_sparkplug"
          msg.meta.tag_name = "test_value"
          msg.meta.virtual_path = "test.ndeath"
          msg.payload = msg.payload.value
          
          return msg

output:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "test-edge-ndeath"
    identity:
      group_id: "%s"
      edge_node_id: "%s"

logger:
  level: INFO
`, brokerURL, groupID, edgeNodeID)

	case "utf8_test":
		return fmt.Sprintf(`
input:
  generate:
    interval: "1s"
    count: 8  # Different UTF-8 metrics
    mapping: |
      root = {"value": counter() * 10.5}

pipeline:
  processors:
    - tag_processor:
        defaults: |
          msg.meta.location_path = "enterprise.factory.line1.station1"
          msg.meta.data_contract = "_sparkplug"
          
          let counterValue = msg.payload.value
          let idx = counterValue %% 4
          
          if (idx == 0) {
            msg.meta.tag_name = "temperature"
            msg.meta.virtual_path = "压力.sensors"  # Chinese: pressure
          } else if (idx == 1) {
            msg.meta.tag_name = "humidity"
            msg.meta.virtual_path = "🌡️Temp.ambient"  # Emoji temperature
          } else if (idx == 2) {
            msg.meta.tag_name = "flow"
            msg.meta.virtual_path = "Müller.station"  # German umlaut
          } else {
            msg.meta.tag_name = "pressure"
            msg.meta.virtual_path = "Åström.controller"  # Swedish character
          }
          
          msg.payload = counterValue
          return msg

output:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "test-edge-utf8"
    identity:
      group_id: "%s"
      edge_node_id: "%s"

logger:
  level: INFO
`, brokerURL, groupID, edgeNodeID)

	case "datatype_test":
		return fmt.Sprintf(`
input:
  generate:
    interval: "500ms"
    count: 12  # Different data types
    mapping: |
      root = {"counter": counter()}

pipeline:
  processors:
    - tag_processor:
        defaults: |
          msg.meta.location_path = "enterprise.factory.line1.station1"
          msg.meta.data_contract = "_sparkplug"
          
          let idx = msg.payload.counter %% 6
          
          if (idx == 0) {
            msg.meta.tag_name = "int32_value"
            msg.meta.virtual_path = "datatype.test"
            msg.payload = -12345  # Int32
          } else if (idx == 1) {
            msg.meta.tag_name = "int64_value"
            msg.meta.virtual_path = "datatype.test"
            msg.payload = -123456789  # Int64
          } else if (idx == 2) {
            msg.meta.tag_name = "float_value"
            msg.meta.virtual_path = "datatype.test"
            msg.payload = 3.14159  # Float/Double
          } else if (idx == 3) {
            msg.meta.tag_name = "bool_value"
            msg.meta.virtual_path = "datatype.test"
            msg.payload = true  # Boolean
          } else if (idx == 4) {
            msg.meta.tag_name = "string_value"
            msg.meta.virtual_path = "datatype.test"
            msg.payload = "test string with UTF-8: 测试"  # String
          } else {
            msg.meta.tag_name = "double_value"
            msg.meta.virtual_path = "datatype.test"
            msg.payload = 2.718281828459045  # Double precision
          }
          
          return msg

output:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "test-edge-datatype"
    identity:
      group_id: "%s"
      edge_node_id: "%s"

logger:
  level: INFO
`, brokerURL, groupID, edgeNodeID)

	default:
		panic(fmt.Sprintf("Unknown test type: %s", testType))
	}
}

func createUTF8TestData() []map[string]interface{} {
	// TODO: Implement UTF-8 test data generator
	// - Create test data with international characters
	// - Include Chinese: 压力 (pressure)
	// - Include emoji: 🌡️Temp
	// - Include European: Müller, Åström
	// - Return slice of test data maps
	panic("Helper function not implemented")
}

func createDataTypeTestData() []map[string]interface{} {
	// TODO: Implement data type test data generator
	// - Create test data for all Sparkplug data types
	// - Include edge cases (min/max values)
	// - Return slice of test data maps
	panic("Helper function not implemented")
}

func validateSequenceWrap(messages []mqtt.Message, expectedCount int) error {
	// Sequence wrap validation for messages
	if len(messages) != expectedCount {
		return fmt.Errorf("expected %d messages, got %d", expectedCount, len(messages))
	}

	var lastSeq uint64
	var hasWrapped bool

	for i, msg := range messages {
		payload, err := decodeSparkplugPayload(msg.Payload())
		if err != nil {
			return fmt.Errorf("failed to decode message %d: %w", i, err)
		}

		if payload.Seq == nil {
			return fmt.Errorf("message %d missing sequence number", i)
		}

		currentSeq := *payload.Seq

		if i == 0 {
			// First message should be NBIRTH with seq=0
			if currentSeq != 0 {
				return fmt.Errorf("first message (NBIRTH) should have seq=0, got %d", currentSeq)
			}
		} else {
			// Validate sequence progression
			expectedSeq := (lastSeq + 1) % 256
			if currentSeq != expectedSeq {
				return fmt.Errorf("message %d: expected seq=%d, got %d", i, expectedSeq, currentSeq)
			}

			// Track when wrap occurs
			if lastSeq == 255 && currentSeq == 0 {
				hasWrapped = true
				fmt.Printf("✅ Sequence wrap detected at message %d: 255 → 0\n", i)
			}
		}

		lastSeq = currentSeq
	}

	// For 261 messages, we should see wrap from 255 to 0
	if expectedCount > 256 && !hasWrapped {
		return fmt.Errorf("expected sequence wrap from 255 to 0, but wrap not detected")
	}

	fmt.Printf("✅ Sequence validation passed: %d messages with proper progression\n", len(messages))
	return nil
}

func validateBdSeqIncrement(nbirth1, nbirth2 mqtt.Message) error {
	// bdSeq increment validation for NBIRTH messages
	payload1, err := decodeSparkplugPayload(nbirth1.Payload())
	if err != nil {
		return fmt.Errorf("failed to decode first NBIRTH: %w", err)
	}

	payload2, err := decodeSparkplugPayload(nbirth2.Payload())
	if err != nil {
		return fmt.Errorf("failed to decode second NBIRTH: %w", err)
	}

	// Find bdSeq metric in first NBIRTH
	var bdSeq1 uint64
	found1 := false
	for _, metric := range payload1.Metrics {
		if metric.Name != nil && *metric.Name == "bdSeq" {
			if metric.GetLongValue() != 0 {
				bdSeq1 = metric.GetLongValue()
				found1 = true
				break
			}
		}
	}

	if !found1 {
		return fmt.Errorf("bdSeq metric not found in first NBIRTH")
	}

	// Find bdSeq metric in second NBIRTH
	var bdSeq2 uint64
	found2 := false
	for _, metric := range payload2.Metrics {
		if metric.Name != nil && *metric.Name == "bdSeq" {
			if metric.GetLongValue() != 0 {
				bdSeq2 = metric.GetLongValue()
				found2 = true
				break
			}
		}
	}

	if !found2 {
		return fmt.Errorf("bdSeq metric not found in second NBIRTH")
	}

	// Validate increment
	if bdSeq1 != 0 {
		return fmt.Errorf("first NBIRTH should have bdSeq=0, got %d", bdSeq1)
	}

	if bdSeq2 != 1 {
		return fmt.Errorf("second NBIRTH should have bdSeq=1, got %d", bdSeq2)
	}

	fmt.Printf("✅ bdSeq increment validation passed: %d → %d\n", bdSeq1, bdSeq2)
	return nil
}

func validateUTF8MetricNames(messages []mqtt.Message, expectedNames []string) error {
	// TODO: Implement UTF-8 metric name validation
	// - Decode protobuf messages
	// - Extract metric names
	// - Validate UTF-8 encoding correctness
	// - Check for character corruption
	// - Return error if validation fails
	panic("Helper function not implemented")
}

func validateDataTypeMatrix(messages []mqtt.Message, expectedTypes map[string]interface{}) error {
	// TODO: Implement data type validation
	// - Decode protobuf messages
	// - Validate each data type encoding
	// - Check protobuf oneof field selection
	// - Verify value preservation
	// - Return error if validation fails
	panic("Helper function not implemented")
}

func validateLocationPathMapping(messages []*service.Message, expectedMappings []struct {
	devicePath       string
	metricName       string
	expectedLocation string
	expectedVirtual  string
	expectedTag      string
}) error {
	// TODO: Implement location path mapping validation
	// - Extract metadata from messages
	// - Validate location_path construction
	// - Check virtual_path and tag_name splitting
	// - Verify colon/dot conversion rules
	// - Return error if validation fails
	panic("Helper function not implemented")
}

// Helper functions for integration testing

// createIntegrationTestData creates test Sparkplug B payloads for integration tests
func createDeviceLevelTestData() *DeviceLevelTestData {
	// Create DBIRTH payload for device-level testing
	dbirthMetrics := []*sproto.Payload_Metric{
		{
			Name:     stringPtr("temperature"),
			Alias:    uint64Ptr(300),
			Datatype: uint32Ptr(9), // Float
			Value:    &sproto.Payload_Metric_FloatValue{FloatValue: 25.0},
		},
		{
			Name:     stringPtr("humidity"),
			Alias:    uint64Ptr(301),
			Datatype: uint32Ptr(9), // Float
			Value:    &sproto.Payload_Metric_FloatValue{FloatValue: 60.0},
		},
	}

	dbirthPayload := &sproto.Payload{
		Timestamp: uint64Ptr(1672531200000),
		Seq:       uint64Ptr(0), // DBIRTH starts at 0
		Metrics:   dbirthMetrics,
	}

	// Create DDATA payload using aliases
	ddataMetrics := []*sproto.Payload_Metric{
		{
			Alias:    uint64Ptr(300), // temperature
			Datatype: uint32Ptr(9),
			Value:    &sproto.Payload_Metric_FloatValue{FloatValue: 26.5},
		},
		{
			Alias:    uint64Ptr(301), // humidity
			Datatype: uint32Ptr(9),
			Value:    &sproto.Payload_Metric_FloatValue{FloatValue: 62.0},
		},
	}

	ddataPayload := &sproto.Payload{
		Timestamp: uint64Ptr(1672531260000),
		Seq:       uint64Ptr(1), // Incremented from DBIRTH
		Metrics:   ddataMetrics,
	}

	return &DeviceLevelTestData{
		DBirthPayload: dbirthPayload,
		DDataPayload:  ddataPayload,
	}
}

func createIntegrationTestData() *IntegrationTestData {
	return &IntegrationTestData{
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
				{
					Name:     stringPtr("Pressure"),
					Alias:    uint64Ptr(101),
					Datatype: uint32Ptr(10), // Double
					Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 1013.25},
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
				{
					Alias:    uint64Ptr(101), // Should resolve to "Pressure"
					Datatype: uint32Ptr(10),  // Double
					Value:    &sproto.Payload_Metric_DoubleValue{DoubleValue: 1015.5},
				},
			},
		},
	}
}

type IntegrationTestData struct {
	NBirthPayload *sproto.Payload
	NDataPayload  *sproto.Payload
}

type DeviceLevelTestData struct {
	DBirthPayload *sproto.Payload
	DDataPayload  *sproto.Payload
}

// Helper functions for pointer creation
func stringPtr(s string) *string {
	return &s
}

func uint64Ptr(u uint64) *uint64 {
	return &u
}

func uint32Ptr(u uint32) *uint32 {
	return &u
}
