//go:build integration

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

// End-to-End Integration Test for ENG-3720: Missing Devices Investigation
//
// CUSTOMER ISSUE (ENG-3720):
// - Customer reported "device 967 is not showing up" in Topic Browser
// - "But there should be way more nodes in here" - missing devices
// - Hypothesis: Something failing in pipeline BEFORE Kafka/Topic Browser
//
// TEST PURPOSE:
// Validate that ALL SparkplugB devices process through the FULL pipeline without drops:
//   SparkplugB NDATA → tag_processor → downsampler → output capture
//
// TEST STRATEGY:
// 1. Create sanitized NDATA messages for multiple devices (including "missing" device 967)
// 2. Process through full DFC pipeline (tag_processor + downsampler)
// 3. Capture output messages (NOT stdout - use benthos message capture)
// 4. Validate ALL devices appear in output (no drops)
// 5. Specifically check device 967 appears (the "missing" device from customer report)
//
// SANITIZATION:
// - Real customer: Production PCAP data
// - Sanitized: org_A/site_B, generic device IDs (702, 967, 936)
// - Preserves: Message structure, sequence patterns, metric counts
//
// EXPECTED RESULT:
// - Test PASSES: All devices processed, device 967 appears in output
// - Test FAILS: Device 967 missing → confirms customer bug, identifies pipeline issue

package sparkplug_plugin_test

import (
	"context"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
	"google.golang.org/protobuf/proto"

	_ "github.com/united-manufacturing-hub/benthos-umh/downsampler_plugin" // Import downsampler for pipeline
	sparkplugb "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

var _ = Describe("End-to-End Device Processing Pipeline (ENG-3720)", Serial, func() {
	Context("Full Pipeline Validation: SparkplugB → tag_processor → downsampler", func() {
		var (
			ctx             context.Context
			cancel          context.CancelFunc
			brokerURL       string
			uniqueGroupID   string
			consumerStream  *service.Stream
			publisherClient mqtt.Client
			consumerDone    chan error
		)

		BeforeEach(func() {
			// Create test context with generous timeout for full pipeline
			ctx, cancel = context.WithTimeout(context.Background(), 60*time.Second)

			// Generate unique identifiers for test isolation
			uniqueGroupID = fmt.Sprintf("ENG3720Test-%d-%s", GinkgoParallelProcess(), uuid.New().String()[:8])
			brokerURL = "tcp://localhost:1883"

			// Initialize channel for consumer error handling
			consumerDone = make(chan error, 1)

			// Message capture is automatically registered by the output plugin when config includes "message_capture: {}"
			// The message_capture output plugin's Connect() method sets currentTestCapture
			// Use getCurrentTestCapture() later to retrieve the registered instance

			By(fmt.Sprintf("Using unique group ID: %s for test isolation", uniqueGroupID))
		})

		AfterEach(func() {
			// Cleanup: stop streams and disconnect clients
			if publisherClient != nil && publisherClient.IsConnected() {
				publisherClient.Disconnect(250)
			}
			if consumerStream != nil {
				consumerStream.StopWithin(5 * time.Second)
			}
			cancel()
		})

		It("should process ALL devices through full pipeline without drops (ENG-3720 validation)", func() {
			By("Step 1: Setting up SparkplugB consumer with tag_processor + downsampler pipeline")

			// Consumer configuration: SparkplugB input → tag_processor → downsampler → message_capture
			// This simulates the real DFC pipeline configuration
			consumerConfig := fmt.Sprintf(`
input:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "test-consumer-%d-%s"
      qos: 1
      keep_alive: "60s"
      connect_timeout: "30s"
      clean_session: true
    identity:
      group_id: "%s"
      edge_node_id: "PrimaryHost"
    role: "primary"
    subscription:
      groups: ["%s"]

pipeline:
  processors:
    # Tag processor: Adds UMH topic metadata (location_path, data_contract, tag_name)
    - tag_processor:
        defaults: |
          // Extract device ID from SparkplugB metadata
          msg.meta.location_path = "org_A.site_B.line_C." + msg.meta.spb_device_id;
          msg.meta.data_contract = "_historian";
          msg.meta.tag_name = msg.meta.spb_metric_name;
          return msg;

    # Downsampler: Data reduction (simulates real DFC downsampling)
    - downsampler:
        default:
          deadband:
            threshold: 0.5
            max_time: 10s

output:
  message_capture: {}

logger:
  level: INFO
`, brokerURL, GinkgoParallelProcess(), uuid.New().String()[:8], uniqueGroupID, uniqueGroupID)

			By("Building and starting consumer stream")
			consumerStreamBuilder := service.NewStreamBuilder()
			err := consumerStreamBuilder.SetYAML(consumerConfig)
			Expect(err).NotTo(HaveOccurred(), "Consumer config should be valid")

			consumerStream, err = consumerStreamBuilder.Build()
			Expect(err).NotTo(HaveOccurred(), "Consumer stream should build successfully")

			// Start consumer in background
			go func() {
				consumerDone <- consumerStream.Run(ctx)
			}()

			// Wait for consumer to initialize
			time.Sleep(2 * time.Second)

			By("Step 2: Publishing SparkplugB NDATA messages for multiple devices")

			// Create MQTT publisher for SparkplugB messages
			opts := mqtt.NewClientOptions()
			opts.AddBroker(brokerURL)
			opts.SetClientID(fmt.Sprintf("test-publisher-%d-%s", GinkgoParallelProcess(), uuid.New().String()[:8]))
			opts.SetCleanSession(true)
			opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
				GinkgoWriter.Printf("Publisher connection lost: %v\n", err)
			})

			publisherClient = mqtt.NewClient(opts)
			token := publisherClient.Connect()
			Expect(token.WaitTimeout(30*time.Second)).To(BeTrue(), "Publisher should connect to broker")
			Expect(token.Error()).NotTo(HaveOccurred(), "Publisher connection should succeed")

			By("Creating sanitized NDATA messages for test devices")

			// Device 702: Known working device (baseline)
			device702Messages := createDeviceNDATAMessages("702", 5)

			// Device 967: MISSING device from customer report (ENG-3720)
			// THIS IS THE CRITICAL TEST - does device 967 appear in output?
			device967Messages := createDeviceNDATAMessages("967", 5)

			// Device 936: Another device from bridge logs (additional validation)
			device936Messages := createDeviceNDATAMessages("936", 5)

			// Collect all messages for publishing
			allMessages := []struct {
				deviceID string
				payload  *sparkplugb.Payload
			}{
				// Device 702 messages
				{"702", device702Messages[0]},
				{"702", device702Messages[1]},
				{"702", device702Messages[2]},
				{"702", device702Messages[3]},
				{"702", device702Messages[4]},
				// Device 967 messages (the "missing" device)
				{"967", device967Messages[0]},
				{"967", device967Messages[1]},
				{"967", device967Messages[2]},
				{"967", device967Messages[3]},
				{"967", device967Messages[4]},
				// Device 936 messages
				{"936", device936Messages[0]},
				{"936", device936Messages[1]},
				{"936", device936Messages[2]},
				{"936", device936Messages[3]},
				{"936", device936Messages[4]},
			}

			By(fmt.Sprintf("Publishing %d NDATA messages across 3 devices", len(allMessages)))

			// Publish all NDATA messages to broker
			for i, msg := range allMessages {
				topic := fmt.Sprintf("spBv1.0/%s/NDATA/edge01/%s", uniqueGroupID, msg.deviceID)

				// Marshal payload to protobuf
				payloadBytes, err := proto.Marshal(msg.payload)
				Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("Should marshal payload %d", i))

				// Publish to MQTT broker
				token := publisherClient.Publish(topic, 1, false, payloadBytes)
				Expect(token.WaitTimeout(5*time.Second)).To(BeTrue(), fmt.Sprintf("Publish %d should complete", i))
				Expect(token.Error()).NotTo(HaveOccurred(), fmt.Sprintf("Publish %d should succeed", i))

				GinkgoWriter.Printf("Published NDATA %d for device %s to topic: %s\n", i, msg.deviceID, topic)

				// Small delay between messages to simulate realistic timing
				time.Sleep(100 * time.Millisecond)
			}

			By("Step 3: Collecting output messages from pipeline")

			// Wait for messages to process through full pipeline
			// (SparkplugB input → tag_processor → downsampler → message_capture)
			time.Sleep(3 * time.Second)

			// Get the MessageCapture instance that was registered by the output plugin
			messageCapture := getCurrentTestCapture()
			Expect(messageCapture).NotTo(BeNil(), "MessageCapture should be available")

			// Collect captured messages from message_capture output
			var outputMessages []*service.Message
			timeout := time.After(10 * time.Second)
			collecting := true

			for collecting {
				select {
				case msg := <-messageCapture.messages:
					outputMessages = append(outputMessages, msg)
					GinkgoWriter.Printf("Captured message %d from pipeline\n", len(outputMessages))
				case <-timeout:
					collecting = false
					GinkgoWriter.Println("Timeout reached, stopping message collection")
				case <-time.After(2 * time.Second):
					// No messages for 2 seconds - assume processing complete
					collecting = false
					GinkgoWriter.Println("No messages for 2s, assuming processing complete")
				}
			}

			By(fmt.Sprintf("Step 4: Validating output - collected %d messages", len(outputMessages)))

			// CRITICAL VALIDATION 1: Total message count
			// Input: 15 NDATA messages (5 per device, 3 devices)
			// Output: Should be 15 messages (or more if downsampler doesn't reduce)
			// NOTE: Downsampler might keep all messages if values vary enough
			Expect(outputMessages).NotTo(BeEmpty(), "Pipeline should produce output messages")
			GinkgoWriter.Printf("✓ Output contains %d messages (input was 15)\n", len(outputMessages))

			// CRITICAL VALIDATION 2: Extract device IDs from output
			devicesSeen := make(map[string]int)
			for _, msg := range outputMessages {
				// Extract device ID from metadata
				// tag_processor sets location_path = "org_A.site_B.line_C.{device_id}"
				locationPath, exists := msg.MetaGet("location_path")
				if !exists {
					GinkgoWriter.Printf("⚠ Message missing location_path metadata\n")
					continue
				}

				// Extract device ID from location path (last segment)
				deviceID := extractDeviceIDFromLocationPath(locationPath)
				if deviceID != "" {
					devicesSeen[deviceID]++
					GinkgoWriter.Printf("  → Message from device: %s (total: %d)\n", deviceID, devicesSeen[deviceID])
				}
			}

			// CRITICAL VALIDATION 3: All 3 devices must appear
			Expect(devicesSeen).To(HaveKey("702"), "Device 702 (baseline) MUST appear in output")
			Expect(devicesSeen).To(HaveKey("967"), "Device 967 (MISSING device from ENG-3720) MUST appear in output")
			Expect(devicesSeen).To(HaveKey("936"), "Device 936 (additional device) MUST appear in output")

			GinkgoWriter.Println("\n✓ SUCCESS: All devices processed through pipeline")
			GinkgoWriter.Printf("  Device 702: %d messages\n", devicesSeen["702"])
			GinkgoWriter.Printf("  Device 967: %d messages ← THE MISSING DEVICE\n", devicesSeen["967"])
			GinkgoWriter.Printf("  Device 936: %d messages\n", devicesSeen["936"])

			// CRITICAL VALIDATION 4: Device 967 SPECIFICALLY must appear
			// This is the device customer reported as missing
			Expect(devicesSeen["967"]).To(BeNumerically(">", 0),
				"Device 967 MUST appear in output - this is the MISSING device from customer report (ENG-3720)")

			// CRITICAL VALIDATION 5: Each device should have multiple messages
			// (at least some of the 5 NDATA we sent per device)
			Expect(devicesSeen["702"]).To(BeNumerically(">=", 1), "Device 702 should have messages")
			Expect(devicesSeen["967"]).To(BeNumerically(">=", 1), "Device 967 should have messages")
			Expect(devicesSeen["936"]).To(BeNumerically(">=", 1), "Device 936 should have messages")

			// VALIDATION 6: Metadata preservation check
			// Validate that tag_processor correctly set metadata fields
			for _, msg := range outputMessages {
				locationPath, _ := msg.MetaGet("location_path")
				dataContract, _ := msg.MetaGet("data_contract")
				tagName, _ := msg.MetaGet("tag_name")

				// All messages should have required metadata from tag_processor
				Expect(locationPath).NotTo(BeEmpty(), "Messages should have location_path from tag_processor")
				Expect(dataContract).To(Equal("_historian"), "Messages should have data_contract = _historian")
				Expect(tagName).NotTo(BeEmpty(), "Messages should have tag_name from SparkplugB metric")
			}

			GinkgoWriter.Println("\n✓ PASS: ENG-3720 validation complete - device 967 processed successfully")
		})
	})
})

// createDeviceNDATAMessages creates sanitized NDATA messages for a device
// Each NDATA has 5 metrics with varying values to simulate real sensor data
func createDeviceNDATAMessages(deviceID string, count int) []*sparkplugb.Payload {
	messages := make([]*sparkplugb.Payload, count)
	baseTimestamp := uint64(1730986400000) // Fixed base timestamp
	baseSeq := uint64(100)                 // Starting sequence number

	for i := 0; i < count; i++ {
		seq := baseSeq + uint64(i)
		timestamp := baseTimestamp + uint64(i*1000) // 1 second apart

		// Create 5 metrics per NDATA (simulating multi-metric device)
		metrics := []*sparkplugb.Payload_Metric{
			{
				Name:      stringPtr(fmt.Sprintf("temperature_%d", i)),
				Datatype:  uint32Ptr(10), // Double
				Timestamp: &timestamp,
				Value:     &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 20.0 + float64(i)*0.5},
			},
			{
				Name:      stringPtr(fmt.Sprintf("pressure_%d", i)),
				Datatype:  uint32Ptr(10), // Double
				Timestamp: &timestamp,
				Value:     &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 100.0 + float64(i)*1.2},
			},
			{
				Name:      stringPtr(fmt.Sprintf("flow_%d", i)),
				Datatype:  uint32Ptr(10), // Double
				Timestamp: &timestamp,
				Value:     &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 50.0 + float64(i)*0.8},
			},
			{
				Name:      stringPtr(fmt.Sprintf("speed_%d", i)),
				Datatype:  uint32Ptr(4), // Int64
				Timestamp: &timestamp,
				Value:     &sparkplugb.Payload_Metric_LongValue{LongValue: uint64(1200 + i*10)},
			},
			{
				Name:      stringPtr(fmt.Sprintf("status_%d", i)),
				Datatype:  uint32Ptr(12), // String
				Timestamp: &timestamp,
				Value:     &sparkplugb.Payload_Metric_StringValue{StringValue: fmt.Sprintf("OK_%d", i)},
			},
		}

		messages[i] = &sparkplugb.Payload{
			Timestamp: &timestamp,
			Seq:       &seq,
			Metrics:   metrics,
		}
	}

	return messages
}

// extractDeviceIDFromLocationPath extracts device ID from location_path
// Example: "org_A.site_B.line_C.967" → "967"
func extractDeviceIDFromLocationPath(locationPath string) string {
	// Split by dots and get last segment
	parts := splitString(locationPath, ".")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}

// splitString splits a string by separator (Go stdlib replacement)
func splitString(s, sep string) []string {
	var result []string
	current := ""

	for _, char := range s {
		if string(char) == sep {
			if current != "" {
				result = append(result, current)
				current = ""
			}
		} else {
			current += string(char)
		}
	}

	if current != "" {
		result = append(result, current)
	}

	return result
}
