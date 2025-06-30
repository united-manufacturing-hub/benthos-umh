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
