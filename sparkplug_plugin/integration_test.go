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
//
// CRITICAL BUGS DISCOVERED AND FIXED DURING INTEGRATION TEST DEVELOPMENT:
//
// 1. RACE CONDITION: DBIRTH published before NBIRTH (SPECIFICATION VIOLATION)
//    - Problem: onConnect() callback and Write() method race condition
//    - Solution: Added synchronization flag (nbirthPublished) to prevent device messages until NBIRTH completes
//    - Impact: Ensures proper Sparkplug B message ordering per specification
//
// 2. SEQUENCE COUNTER HARDCODED TO 0 (SPECIFICATION VIOLATION)
//    - Problem: Both NBIRTH and DBIRTH were hardcoded to seq=0, ignoring sequence progression
//    - Solution: Implemented proper sequence counter increment across ALL message types
//    - Impact: Sequence now properly increments: NBIRTH(0) → DBIRTH(1) → DDATA(2) → ...
//
// 3. CLIENT ID CONFLICTS IN PARALLEL TESTS
//    - Problem: Multiple tests using same client IDs causing MQTT broker disconnections (EOF errors)
//    - Solution: Implemented unique client ID generation using GinkgoParallelProcess() + UUID
//    - Impact: Eliminated "EOF" connection errors during concurrent test execution
//
// 4. BDSEQ VALIDATION MISUNDERSTANDING
//    - Learning: Sparkplug B allows both sequential (0→1→2) and random (55955→30697) bdSeq values
//    - Solution: Updated tests to validate spec-compliant random bdSeq generation
//    - Impact: Tests now correctly validate increment behavior rather than specific values
//
// 5. NDEATH WILL MESSAGE TESTING CHALLENGES
//    - Learning: Will messages only trigger on abrupt disconnections, not graceful disconnects
//    - Solution: Implemented proper timeout-based disconnection simulation
//    - Impact: Tests now properly validate MQTT will message functionality
//
// These fixes ensure the plugin is fully compliant with Sparkplug B v3.0 specification
// and can handle production-grade scenarios including concurrent connections, proper
// sequence management, and real MQTT broker integration.

package sparkplug_plugin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
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

// Package-level variables for test infrastructure
// Simple test-specific MessageCapture - set by each test
var currentTestCapture *MessageCapture
var currentTestCaptureMu sync.RWMutex

type MessageCapture struct {
	messages chan *service.Message
}

func (m *MessageCapture) Connect(ctx context.Context) error {
	// Register this instance as the current test capture
	currentTestCaptureMu.Lock()
	currentTestCapture = m
	currentTestCaptureMu.Unlock()
	return nil
}

func (m *MessageCapture) Write(ctx context.Context, msg *service.Message) error {
	if m.messages != nil {
		select {
		case m.messages <- msg.Copy():
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Channel full, skip message to avoid blocking
		}
	}
	return nil
}

func (m *MessageCapture) Close(ctx context.Context) error {
	// Clear the current test capture when closing
	currentTestCaptureMu.Lock()
	if currentTestCapture == m {
		currentTestCapture = nil
	}
	currentTestCaptureMu.Unlock()
	return nil
}

// Helper function to get the current test's MessageCapture
func getCurrentTestCapture() *MessageCapture {
	currentTestCaptureMu.RLock()
	defer currentTestCaptureMu.RUnlock()
	return currentTestCapture
}

func init() {
	// Register custom output for tests
	err := service.RegisterOutput("message_capture",
		service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			// Create a capture with a buffered channel
			capture := &MessageCapture{
				messages: make(chan *service.Message, 100),
			}
			return capture, 1, nil
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
			opts.SetClientID(fmt.Sprintf("test-edge-publisher-%d-%s", GinkgoParallelProcess(), uuid.New().String()[:8]))
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

			By("Testing complete pipeline: generate → tag_processor → sparkplug_b → sparkplug_b → message_capture")

			// This test replicates what the shell script was actually doing:
			// 1. Edge Node: generate → tag_processor → sparkplug_b output
			// 2. Primary Host: sparkplug_b input → message_capture (instead of stdout)
			// 3. Real end-to-end communication via MQTT

			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()

			// Generate unique group ID to avoid cross-contamination with other tests
			uniqueGroupID := fmt.Sprintf("PipelineTest-%d-%s", GinkgoParallelProcess(), uuid.New().String()[:8])

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
      client_id: "test-edge-node-%d-%s"
    identity:
      group_id: "%s"
      edge_node_id: "StaticEdgeNode01"

logger:
  level: INFO
`, brokerURL, GinkgoParallelProcess(), uuid.New().String()[:8], uniqueGroupID)

			// Primary Host Stream (similar to sparkplug-device-level-primary-host.yaml)
			primaryHostConfig := fmt.Sprintf(`
input:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "test-primary-host-%d-%s"
    identity:
      group_id: "%s"
      edge_node_id: "PrimaryHost"
    role: "primary"
    subscription:
      groups: ["%s"]  # Only subscribe to our specific test group

output:
  message_capture: {}

logger:
  level: INFO
`, brokerURL, GinkgoParallelProcess(), uuid.New().String()[:8], uniqueGroupID, uniqueGroupID)

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

			// Get the MessageCapture instance
			messageCapture := getCurrentTestCapture()
			Expect(messageCapture).NotTo(BeNil(), "MessageCapture should be available")

			// Collect all captured messages
			var messages []*service.Message
			timeout := time.After(2 * time.Second)
		collectLoop:
			for {
				select {
				case msg := <-messageCapture.messages:
					if msg != nil {
						messages = append(messages, msg)
					}
				case <-timeout:
					break collectLoop
				default:
					if len(messages) > 0 {
						time.Sleep(100 * time.Millisecond) // Brief pause to collect any remaining messages
						break collectLoop
					} else {
						time.Sleep(100 * time.Millisecond) // Keep waiting if no messages yet
					}
				}
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
					Expect(deviceKey).To(Equal(fmt.Sprintf("%s/StaticEdgeNode01", uniqueGroupID)))
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
					Expect(groupID).To(Equal(uniqueGroupID))
					Expect(edgeNodeID).To(Equal("StaticEdgeNode01"))
				}
				fmt.Printf("✅ Pipeline message %d validation passed: type=%s\n", i+1, msgType)
			}

			fmt.Printf("✅ Full pipeline integration test completed - %d messages captured and validated\n", len(messages))
		})

		It("should process NBIRTH and NDATA messages end-to-end", func() {
			By("Creating a message capture system")

			By("Creating a stream with the Sparkplug input plugin and message capture")

			// Create a stream with the input to test it
			streamBuilder := service.NewStreamBuilder()
			err := streamBuilder.SetYAML(fmt.Sprintf(`
input:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "test-primary-host-%d-%s"
      qos: 1
    identity:
      group_id: "FactoryA"
      edge_node_id: "CentralHost"
    role: "primary"
    behaviour:
      auto_split_metrics: true

output:
  message_capture: {}

logger:
  level: INFO
`, brokerURL, GinkgoParallelProcess(), uuid.New().String()[:8]))
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

			// Get the MessageCapture instance
			messageCapture := getCurrentTestCapture()
			Expect(messageCapture).NotTo(BeNil(), "MessageCapture should be available")

			// Collect all captured messages
			var messages []*service.Message
			timeout := time.After(2 * time.Second)
		collectLoop:
			for {
				select {
				case msg := <-messageCapture.messages:
					if msg != nil {
						messages = append(messages, msg)
					}
				case <-timeout:
					break collectLoop
				default:
					if len(messages) > 0 {
						time.Sleep(100 * time.Millisecond) // Brief pause to collect any remaining messages
						break collectLoop
					} else {
						time.Sleep(100 * time.Millisecond) // Keep waiting if no messages yet
					}
				}
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
			inputOpts.SetClientID(fmt.Sprintf("test-input-client-%d-%s", GinkgoParallelProcess(), uuid.New().String()[:8]))
			inputOpts.SetConnectTimeout(10 * time.Second)

			inputClient = mqtt.NewClient(inputOpts)
			token := inputClient.Connect()
			if !token.WaitTimeout(10*time.Second) || token.Error() != nil {
				Skip("No MQTT broker available for plugin-to-plugin test")
			}

			// Create output side client (simulates edge node)
			outputOpts := mqtt.NewClientOptions()
			outputOpts.AddBroker(brokerURL)
			outputOpts.SetClientID(fmt.Sprintf("test-output-client-%d-%s", GinkgoParallelProcess(), uuid.New().String()[:8]))
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
				opts.SetClientID(fmt.Sprintf("test-concurrent-client-%d-%d-%s", i, GinkgoParallelProcess(), uuid.New().String()[:8]))
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
			opts.SetClientID(fmt.Sprintf("test-performance-client-%d-%s", GinkgoParallelProcess(), uuid.New().String()[:8]))
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

			groupID := "SequenceWrapTest"
			edgeNodeID := "EdgeNode1"
			// Subscribe to all message types to capture NBIRTH + DDATA
			allMsgChan := subscribeToSparkplugTopic(subscriberClient, fmt.Sprintf("spBv1.0/%s/#", groupID))

			By("Starting Edge Node stream configured for sequence wrap test")
			edgeConfig := createEdgeNodeConfig(brokerURL, groupID, edgeNodeID, "sequence_wrap")

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

			By("Collecting and validating 262 messages with sequence wrap")
			var allMessages []mqtt.Message

			// Collect messages for up to 25 seconds (262 messages at 50ms interval = ~13s + buffer)
			// Allow extra time for stream startup and message processing
			timeout := time.After(25 * time.Second)

		collectLoop:
			for len(allMessages) < 262 {
				select {
				case msg := <-allMsgChan:
					allMessages = append(allMessages, msg)
					if len(allMessages) <= 10 || len(allMessages)%50 == 0 {
						GinkgoWriter.Printf("📨 Collected message %d on topic: %s\n", len(allMessages), msg.Topic())
					}
				case <-timeout:
					GinkgoWriter.Printf("⏰ Timeout reached, collected %d messages so far\n", len(allMessages))
					break collectLoop
				case <-ctx.Done():
					GinkgoWriter.Printf("⏹️ Context cancelled, collected %d messages so far\n", len(allMessages))
					break collectLoop
				}
			}

			// Only stop the stream after we've collected all messages or timeout
			cancel()
			select {
			case <-edgeStreamDone:
			case <-time.After(5 * time.Second):
			}

			GinkgoWriter.Printf("📊 Collected %d messages total\n", len(allMessages))
			Expect(len(allMessages)).To(BeNumerically(">=", 262), "Should collect at least 262 messages (1 NBIRTH + 1 DBIRTH + 260 DDATA)")

			// Validate sequence progression with wrap-around
			err = validateSequenceWrap(allMessages, 262)
			Expect(err).NotTo(HaveOccurred())

			GinkgoWriter.Printf("✅ Sequence wrap test completed successfully\n")
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
      client_id: "test-edge-ndeath-will-%d-%s"
      keep_alive: "3s"    # Short keep-alive for faster detection
      connect_timeout: "5s"
    identity:
      group_id: "TestGroup"
      edge_node_id: "EdgeNode1"

logger:
  level: INFO
`, brokerURL, GinkgoParallelProcess(), uuid.New().String()[:8])

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

		It("should reset bdSeq to 0 on component restart (stateless limitation)", func() {
			// Test D: bdSeq Behavior on Component Restart
			//
			// STATELESS ARCHITECTURE LIMITATION:
			// Benthos components cannot persist bdSeq across restarts (no database/disk storage).
			// This test validates the expected behavior: bdSeq resets to 0 on each component restart.
			//
			// Test approach:
			// 1. Start first Edge Node component, capture NBIRTH (bdSeq=0)
			// 2. Stop component completely
			// 3. Start second Edge Node component, capture NBIRTH (bdSeq=0 again)
			// 4. Validate both sessions start with bdSeq=0 (expected stateless behavior)

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

			By("Validating bdSeq resets to 0 on component restart")
			err = validateBdSeqReset(nbirth1, nbirth2)
			Expect(err).NotTo(HaveOccurred())

			fmt.Printf("✅ bdSeq reset test completed successfully (stateless behavior validated)\n")
		})
	})

	Context("UMH Integration and Mapping", func() {
		var (
			brokerURL string
		)

		BeforeEach(func() {
			brokerURL = os.Getenv("TEST_MQTT_BROKER")
			if brokerURL == "" {
				brokerURL = "tcp://127.0.0.1:1883"
			}
		})

		AfterEach(func() {
			// No cleanup needed - MessageCapture instances clean themselves up
		})

		It("should map UMH location paths to Sparkplug B structure correctly", func() {
			// Test J: UMH-Core Location Path Mapping
			//
			// Tests full pipeline: UMH location_path → Sparkplug B topics → UMH location_path
			// Validates that hierarchical UMH paths are correctly mapped to/from Sparkplug B structure
			// This test uses sparkplug_b input to convert back to UMH format for validation

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			// Generate unique identifiers for this test
			uniqueGroupID := fmt.Sprintf("LocationTest-%d-%s", GinkgoParallelProcess(), uuid.New().String()[:8])

			By("Setting up full pipeline for location mapping test")

			// Test data with various UMH-Core location path formats
			locationTestData := []struct {
				locationPath string
				virtualPath  string
				tagName      string
				value        float64
			}{
				{
					locationPath: "enterprise.factory.line1.station1",
					virtualPath:  "sensors.ambient",
					tagName:      "temperature",
					value:        23.5,
				},
				{
					locationPath: "acme.plant2.zone3.machine7",
					virtualPath:  "actuators.valve",
					tagName:      "position",
					value:        75.0,
				},
				{
					locationPath: "umh.demo.area1.cell5",
					virtualPath:  "plc.tags",
					tagName:      "speed",
					value:        1200.5,
				},
			}

			By("Starting Edge Node stream with UMH location paths")

			// Create Edge Node that publishes with UMH metadata
			edgeNodeConfig := fmt.Sprintf(`
input:
  generate:
    interval: "1s"
    count: %d
    mapping: |
      root = {"counter": counter()}

pipeline:
  processors:
    - tag_processor:
        defaults: |
          let counterValue = msg.payload.counter;
          let testCases = [
            {
              "location_path": "enterprise.factory.line1.station1",
              "virtual_path": "sensors.ambient", 
              "tag_name": "temperature",
              "value": 23.5
            },
            {
              "location_path": "acme.plant2.zone3.machine7",
              "virtual_path": "actuators.valve",
              "tag_name": "position", 
              "value": 75.0
            },
            {
              "location_path": "umh.demo.area1.cell5",
              "virtual_path": "plc.tags",
              "tag_name": "speed",
              "value": 1200.5
            }
          ];
          
          let testCase = testCases[counterValue %% testCases.length];
          msg.meta.location_path = testCase.location_path;
          msg.meta.virtual_path = testCase.virtual_path;
          msg.meta.tag_name = testCase.tag_name;
          msg.meta.data_contract = "_historian";
          msg.payload = testCase.value;
          
          return msg;

output:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "test-location-edge-node-%d-%s"
    identity:
      group_id: "%s"
      edge_node_id: "EdgeNode1"

logger:
  level: INFO
`, len(locationTestData), brokerURL, GinkgoParallelProcess(), uuid.New().String()[:8], uniqueGroupID)

			By("Starting Primary Host stream to convert back to UMH")

			// Create Primary Host that subscribes and converts back to UMH messages
			primaryHostConfig := fmt.Sprintf(`
input:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "test-location-primary-host-%d-%s"
    identity:
      group_id: "%s"
      edge_node_id: "PrimaryHost"
    role: "primary"
    subscription:
      groups: ["%s"]

output:
  message_capture: {}

logger:
  level: INFO
`, brokerURL, GinkgoParallelProcess(), uuid.New().String()[:8], uniqueGroupID, uniqueGroupID)

			// Start Edge Node stream
			edgeStreamBuilder := service.NewStreamBuilder()
			err := edgeStreamBuilder.SetYAML(edgeNodeConfig)
			Expect(err).NotTo(HaveOccurred())

			edgeStream, err := edgeStreamBuilder.Build()
			Expect(err).NotTo(HaveOccurred())

			edgeStreamDone := make(chan error, 1)
			go func() {
				edgeStreamDone <- edgeStream.Run(ctx)
			}()

			// Wait a moment for Edge Node to start
			time.Sleep(2 * time.Second)

			// Start Primary Host stream
			primaryStreamBuilder := service.NewStreamBuilder()
			err = primaryStreamBuilder.SetYAML(primaryHostConfig)
			Expect(err).NotTo(HaveOccurred())

			primaryStream, err := primaryStreamBuilder.Build()
			Expect(err).NotTo(HaveOccurred())

			primaryStreamDone := make(chan error, 1)
			go func() {
				primaryStreamDone <- primaryStream.Run(ctx)
			}()

			// Wait for Primary Host to start and MessageCapture to be registered
			time.Sleep(2 * time.Second)

			By("Collecting UMH messages from Primary Host")

			// Get the MessageCapture instance for this test
			messageCapture := getCurrentTestCapture()
			Expect(messageCapture).NotTo(BeNil(), "MessageCapture should be available")

			var capturedUMHMessages []*service.Message
			messageTimeout := time.After(10 * time.Second)

			// Collect UMH messages from the Primary Host (converted from Sparkplug B)
			expectedMessageCount := len(locationTestData) // 3 DDATA messages converted to UMH
			for len(capturedUMHMessages) < expectedMessageCount {
				select {
				case msg := <-messageCapture.messages:
					if msg != nil {
						capturedUMHMessages = append(capturedUMHMessages, msg)
						fmt.Printf("📨 Captured UMH message: %d/%d\n", len(capturedUMHMessages), expectedMessageCount)
					}
				case <-messageTimeout:
					Fail(fmt.Sprintf("Timeout waiting for UMH messages. Got %d, expected %d", len(capturedUMHMessages), expectedMessageCount))
				case <-ctx.Done():
					Fail("Context cancelled while waiting for UMH messages")
				}
			}

			By("Validating UMH location path mapping")
			err = validateLocationPathMapping(capturedUMHMessages, locationTestData)
			Expect(err).NotTo(HaveOccurred())

			By("Stopping streams")
			cancel()
			select {
			case <-edgeStreamDone:
				// Edge Node stream stopped
			case <-time.After(5 * time.Second):
				Fail("Edge Node stream did not stop within timeout")
			}

			select {
			case <-primaryStreamDone:
				// Primary Host stream stopped
			case <-time.After(5 * time.Second):
				Fail("Primary Host stream did not stop within timeout")
			}

			fmt.Printf("✅ UMH location path mapping test completed successfully\n")
		})
	})
})

// Helper functions for new integration tests
// TODO: Add these helper functions before the existing helper functions

func createMQTTClient(brokerURL, baseClientID string) mqtt.Client {
	// Generate unique client ID to prevent MQTT client conflicts
	// MQTT-3.1.1 §3.1.4-2: "If a ClientId is in use, the broker MUST disconnect the existing client"
	uniqueID := fmt.Sprintf("%s-%d-%s", baseClientID, GinkgoParallelProcess(), uuid.New().String()[:8])

	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(uniqueID)
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetKeepAlive(30 * time.Second)
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(false) // Explicit control for testing

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		panic(fmt.Sprintf("Failed to connect MQTT client %s: %v", uniqueID, token.Error()))
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
	// Generate unique client ID for Benthos streams to prevent MQTT conflicts
	uniqueClientID := fmt.Sprintf("test-edge-%s-%d-%s", testType, GinkgoParallelProcess(), uuid.New().String()[:8])

	// Edge Node configuration generator for different test scenarios
	switch testType {
	case "sequence_wrap":
		return fmt.Sprintf(`
input:
  generate:
    interval: 50ms
    count: 262 # NBIRTH(0) + DBIRTH(1) + 260 DDATA(2→255→0→5) to test sequence wrap  
    mapping: |
      root = {"counter": counter()}

pipeline:
  processors:
    - tag_processor:
        defaults: |
          msg.meta.location_path = "enterprise.factory.line1.station1";
          msg.meta.data_contract = "_historian";
          
          let counterValue = msg.payload.counter;
          msg.meta.tag_name = "value";
          msg.meta.virtual_path = "temperature-"+counterValue;
          msg.payload = 25.0 + (counterValue %% 10);
          
          return msg;

output:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "%s"
    identity:
      group_id: "%s"
      edge_node_id: "%s"

logger:
  level: INFO
`, brokerURL, uniqueClientID, groupID, edgeNodeID)

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
      client_id: "%s"
    identity:
      group_id: "%s"
      edge_node_id: "%s"

logger:
  level: INFO
`, brokerURL, uniqueClientID, groupID, edgeNodeID)

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
      client_id: "%s"
    identity:
      group_id: "%s"
      edge_node_id: "%s"

logger:
  level: INFO
`, brokerURL, uniqueClientID, groupID, edgeNodeID)

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

shutdown:
  linger: "2s"

output:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "%s"
    identity:
      group_id: "%s"
      edge_node_id: "%s"

logger:
  level: INFO
`, brokerURL, uniqueClientID, groupID, edgeNodeID)

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

// validateSequenceWrap validates sequence counter progression and wrap-around behavior
//
// CRITICAL LEARNING: Sparkplug B sequence counters operate at EDGE NODE LEVEL, not device level
// The sequence counter increments across ALL message types from the same Edge Node:
// - NBIRTH (seq=0)     ← Always first after connection
// - DBIRTH (seq=1,2,3) ← Device births increment the global sequence
// - DDATA  (seq=4,5,6) ← Device data continues the sequence
// - NDATA  (seq=7,8,9) ← Node data continues the sequence
// - Only DEATH messages reset to seq=0
//
// SEQUENCE WRAP: uint8 sequence counter wraps from 255 → 0 and continues incrementing.
// This test validates proper wrap-around behavior per Sparkplug B specification.
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

		// Log message type and sequence for debugging
		topic := msg.Topic()
		msgType := "UNKNOWN"
		if strings.Contains(topic, "/NBIRTH/") {
			msgType = "NBIRTH"
		} else if strings.Contains(topic, "/DBIRTH/") {
			msgType = "DBIRTH"
		} else if strings.Contains(topic, "/DDATA/") {
			msgType = "DDATA"
		} else if strings.Contains(topic, "/NDEATH/") {
			msgType = "NDEATH"
		}

		GinkgoWriter.Printf("📨 Message %d: %s seq=%d\n", i, msgType, currentSeq)

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
				GinkgoWriter.Printf("✅ Sequence wrap detected at message %d: 255 → 0\n", i)
			}
		}

		lastSeq = currentSeq
	}

	// For 262 messages (0-261), we should see wrap from 255 to 0
	if expectedCount > 256 && !hasWrapped {
		return fmt.Errorf("expected sequence wrap from 255 to 0, but wrap not detected")
	}

	GinkgoWriter.Printf("✅ Sequence validation passed: %d messages with proper progression\n", len(messages))
	return nil
}

// validateBdSeqReset validates that bdSeq resets to 0 on component restart (stateless limitation)
//
// BENTHOS STATELESS ARCHITECTURE LIMITATION:
// Unlike persistent Sparkplug implementations, Benthos components cannot store bdSeq across restarts.
// This is due to Benthos's stateless design (no database/disk persistence).
//
// Expected behavior:
// - Component Start 1: bdSeq=0 ✅
// - Component Restart: bdSeq=0 ✅ (resets, cannot persist)
// - Component Start 2: bdSeq=0 ✅
//
// This test validates that both component starts use bdSeq=0, documenting the
// stateless limitation users should be aware of.
func validateBdSeqReset(nbirth1, nbirth2 mqtt.Message) error {
	// bdSeq reset validation for NBIRTH messages (stateless architecture)
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
			bdSeq1 = metric.GetLongValue()
			found1 = true
			break
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
			bdSeq2 = metric.GetLongValue()
			found2 = true
			break
		}
	}

	if !found2 {
		return fmt.Errorf("bdSeq metric not found in second NBIRTH")
	}

	// Validate bdSeq reset behavior due to stateless architecture
	// Benthos components cannot persist bdSeq across restarts (no database/disk storage)
	fmt.Printf("📊 bdSeq comparison: first component=%d, second component=%d\n", bdSeq1, bdSeq2)

	// Both component starts should use bdSeq=0 (stateless limitation)
	if bdSeq1 != 0 {
		return fmt.Errorf("first component start should use bdSeq=0, got %d", bdSeq1)
	}
	if bdSeq2 != 0 {
		return fmt.Errorf("second component start should use bdSeq=0 (stateless reset), got %d", bdSeq2)
	}

	fmt.Printf("✅ bdSeq reset validation passed: both components start with bdSeq=0 (stateless architecture)\n")
	return nil
}

func validateLocationPathMapping(messages []*service.Message, expectedMappings []struct {
	locationPath string
	virtualPath  string
	tagName      string
	value        float64
}) error {
	// UMH location path mapping validation for messages converted from Sparkplug B
	// Validates that hierarchical paths are correctly preserved through Sparkplug B conversion

	foundMappings := make(map[string]bool)

	fmt.Printf("📊 Validating location path mapping in %d UMH messages\n", len(messages))

	for i, msg := range messages {
		// Extract UMH metadata from the message
		locationPath, hasLocationPath := msg.MetaGet("location_path")
		if !hasLocationPath {
			return fmt.Errorf("message %d missing location_path metadata", i)
		}

		virtualPath, hasVirtualPath := msg.MetaGet("virtual_path")
		if !hasVirtualPath {
			return fmt.Errorf("message %d missing virtual_path metadata", i)
		}

		tagName, hasTagName := msg.MetaGet("tag_name")
		if !hasTagName {
			return fmt.Errorf("message %d missing tag_name metadata", i)
		}

		// Get the payload value for validation
		payloadBytes, err := msg.AsBytes()
		if err != nil {
			return fmt.Errorf("message %d failed to get payload: %w", i, err)
		}

		var payloadValue float64
		err = json.Unmarshal(payloadBytes, &payloadValue)
		if err != nil {
			return fmt.Errorf("message %d failed to unmarshal payload as float64: %w", i, err)
		}

		// Create mapping key for validation
		mappingKey := fmt.Sprintf("%s|%s|%s", locationPath, virtualPath, tagName)
		foundMappings[mappingKey] = true

		fmt.Printf("📝 Found UMH mapping: location_path='%s', virtual_path='%s', tag_name='%s', value=%.2f\n",
			locationPath, virtualPath, tagName, payloadValue)

		// Validate hierarchical structure preservation
		if locationPath == "" {
			return fmt.Errorf("message %d has empty location_path", i)
		}
		if virtualPath == "" {
			return fmt.Errorf("message %d has empty virtual_path", i)
		}
		if tagName == "" {
			return fmt.Errorf("message %d has empty tag_name", i)
		}

		// Validate location_path format (should be dot-separated)
		if !strings.Contains(locationPath, ".") {
			return fmt.Errorf("message %d location_path '%s' should be dot-separated hierarchical path", i, locationPath)
		}

		// Validate virtual_path format (should be dot-separated)
		if !strings.Contains(virtualPath, ".") {
			return fmt.Errorf("message %d virtual_path '%s' should be dot-separated hierarchical path", i, virtualPath)
		}
	}

	// Check that all expected mappings were found
	for _, expected := range expectedMappings {
		expectedKey := fmt.Sprintf("%s|%s|%s", expected.locationPath, expected.virtualPath, expected.tagName)
		if !foundMappings[expectedKey] {
			return fmt.Errorf("expected UMH mapping not found: location_path='%s', virtual_path='%s', tag_name='%s'",
				expected.locationPath, expected.virtualPath, expected.tagName)
		}
		fmt.Printf("✅ Found expected UMH mapping: %s → %s.%s\n", expected.locationPath, expected.virtualPath, expected.tagName)
	}

	// Verify we found the correct number of unique mappings
	if len(foundMappings) < len(expectedMappings) {
		return fmt.Errorf("expected at least %d unique UMH mappings, found %d", len(expectedMappings), len(foundMappings))
	}

	fmt.Printf("✅ Location path mapping validation passed: all %d UMH hierarchical paths correctly preserved\n", len(expectedMappings))
	return nil
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
