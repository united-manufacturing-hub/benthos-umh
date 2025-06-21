/*
Sparkplug B Bidirectional Communication Integration Test

This integration test validates that the Sparkplug B input and output plugins
can communicate together flawlessly, implementing the complete edge node ↔ primary host
communication flow as outlined in P2.5 of the plan.

Test Scenarios:
1. Basic Flow Validation: STATE→NBIRTH→NDATA→NDEATH lifecycle
2. Alias Resolution: NBIRTH establishes aliases, NDATA uses them
3. Sequence Validation: Gap detection triggers rebirth requests
4. Error Recovery: Network disconnection/reconnection handling
5. Performance: Sustained throughput with latency measurement
6. Stability: Memory usage monitoring during extended operation

Usage:
  cd sparkplug_plugin
  make start-mosquitto
  TEST_SPARKPLUG_B=1 go test -v -run "Bidirectional" -timeout=10m
  make stop-mosquitto
*/

package sparkplug_plugin_test

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin" // Import to register
)

var _ = ginkgo.Describe("Sparkplug B Bidirectional Communication", func() {
	ginkgo.Context("P2.5 - Complete Edge Node ↔ Primary Host Integration", func() {

		var (
			brokerURL         string
			edgeNodeStream    *service.Stream
			primaryHostStream *service.Stream
			ctx               context.Context
			cancel            context.CancelFunc
			messagesReceived  atomic.Int64
			startTime         time.Time
		)

		ginkgo.BeforeEach(func() {
			// Skip if not running integration tests
			if !isIntegrationTestEnabled() {
				ginkgo.Skip("Integration test disabled. Set TEST_SPARKPLUG_B=1 to enable")
			}

			// Setup broker
			brokerURL = getBrokerURL()
			if err := setupMQTTBroker(); err != nil {
				ginkgo.Skip(fmt.Sprintf("MQTT broker setup failed: %v", err))
			}

			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Minute)
			messagesReceived.Store(0)
			startTime = time.Now()
		})

		ginkgo.AfterEach(func() {
			if cancel != nil {
				cancel()
			}

			// Stop streams gracefully
			if edgeNodeStream != nil {
				_ = edgeNodeStream.Stop(context.Background())
			}
			if primaryHostStream != nil {
				_ = primaryHostStream.Stop(context.Background())
			}

			// Allow cleanup time
			time.Sleep(2 * time.Second)
		})

		ginkgo.It("should handle complete edge node lifecycle with primary host monitoring", func() {
			ginkgo.By("Setting up Edge Node (Output Plugin)")

			edgeNodeConfig := fmt.Sprintf(`
input:
  generate:
    interval: "1s"
    count: 10
    mapping: |
      root = {
        "temperature": 20.0 + (count %% 10) * 2.5,
        "pressure": 1.0 + (count %% 5) * 0.2,
        "vibration": (count %% 100) / 20.0,
        "motor_enabled": count %% 3 != 0,
        "timestamp": timestamp_unix(),
        "count": count
      }

output:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "test-edge-node"
      qos: 1
      keep_alive: "30s"
      connect_timeout: "10s"
      clean_session: true
    
    identity:
      group_id: "Factory"
      edge_node_id: "Line1"
      device_id: "TestDevice"
    
    role: "edge_node"
    
    behaviour:
      auto_extract_tag_name: true
      retain_last_values: true
    
    metrics:
      - name: "Temperature"
        alias: 100
        type: "float"
        value_from: "temperature"
      - name: "Pressure"
        alias: 101
        type: "float"
        value_from: "pressure"
      - name: "Vibration"
        alias: 102
        type: "float"
        value_from: "vibration"
      - name: "Motor_Enabled"
        alias: 103
        type: "boolean"
        value_from: "motor_enabled"

logger:
  level: DEBUG
`, brokerURL)

			edgeBuilder := service.NewEnvironment().NewStreamBuilder()
			err := edgeBuilder.SetYAML(edgeNodeConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			edgeNodeStream, err = edgeBuilder.Build()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Setting up Primary Host (Input Plugin)")

			// Capture received messages
			var receivedMessages []*service.Message
			var messagesMutex sync.Mutex

			primaryHostConfig := fmt.Sprintf(`
input:
  sparkplug_b:
    mqtt:
      urls: ["%s"]
      client_id: "test-primary-host"
      qos: 1
      keep_alive: "30s"
      connect_timeout: "10s"
      clean_session: true
    
    identity:
      group_id: "SCADA"
      edge_node_id: "PrimaryHost"
    
    role: "primary_host"
    
    subscription:
      groups: ["Factory"]
    
    behaviour:
      auto_split_metrics: true
      data_messages_only: false
      enable_rebirth_req: true
      drop_birth_messages: false
      strict_topic_validation: false
      auto_extract_values: true

output:
  drop: {}

logger:
  level: DEBUG
`, brokerURL)

			primaryBuilder := service.NewEnvironment().NewStreamBuilder()
			err = primaryBuilder.SetYAML(primaryHostConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Intercept messages from primary host
			primaryBuilder.AddBatchConsumerFunc(func(ctx context.Context, batch service.MessageBatch) error {
				messagesMutex.Lock()
				defer messagesMutex.Unlock()

				for _, msg := range batch {
					clonedMsg := msg.Copy()
					receivedMessages = append(receivedMessages, clonedMsg)
					messagesReceived.Add(1)

					msgType, _ := msg.MetaGet("sparkplug_msg_type")
					deviceKey, _ := msg.MetaGet("sparkplug_device_key")
					tagName, _ := msg.MetaGet("tag_name")

					fmt.Printf("📥 Received: msgType=%s, deviceKey=%s, tagName=%s\n",
						msgType, deviceKey, tagName)
				}
				return nil
			})

			primaryHostStream, err = primaryBuilder.Build()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Starting both streams concurrently")

			// Start Primary Host first (needs to be listening)
			primaryHostDone := make(chan error, 1)
			go func() {
				primaryHostDone <- primaryHostStream.Run(ctx)
			}()

			// Give primary host time to connect and subscribe
			time.Sleep(3 * time.Second)

			// Start Edge Node
			edgeNodeDone := make(chan error, 1)
			go func() {
				edgeNodeDone <- edgeNodeStream.Run(ctx)
			}()

			ginkgo.By("Waiting for message flow and validating")

			// Wait for edge node to complete (it will stop after 10 messages)
			select {
			case err := <-edgeNodeDone:
				if err != nil && err != context.Canceled {
					ginkgo.Fail(fmt.Sprintf("Edge node stream failed: %v", err))
				}
				fmt.Printf("✅ Edge node completed successfully\n")
			case <-time.After(30 * time.Second):
				ginkgo.Fail("Edge node did not complete within timeout")
			}

			// Give primary host extra time to process all messages
			time.Sleep(5 * time.Second)

			ginkgo.By("Validating received messages")

			messagesMutex.Lock()
			defer messagesMutex.Unlock()

			totalReceived := len(receivedMessages)
			fmt.Printf("📊 Total messages received: %d\n", totalReceived)

			gomega.Expect(totalReceived).To(gomega.BeNumerically(">=", 15),
				"Should receive at least 15 messages (STATE, NBIRTH split, NDATA stream, NDEATH)")

			// Categorize messages by type
			messageTypes := make(map[string]int)
			aliasResolutionCount := 0

			for _, msg := range receivedMessages {
				msgType, _ := msg.MetaGet("sparkplug_msg_type")
				messageTypes[msgType]++

				// Check alias resolution for NDATA messages
				if msgType == "NDATA" {
					tagName, hasTag := msg.MetaGet("tag_name")
					if hasTag && tagName != "" {
						aliasResolutionCount++
					}
				}
			}

			fmt.Printf("📊 Message type breakdown: %+v\n", messageTypes)
			fmt.Printf("📊 NDATA messages with resolved aliases: %d\n", aliasResolutionCount)

			// Validate message types
			gomega.Expect(messageTypes["STATE"]).To(gomega.BeNumerically(">=", 1),
				"Should receive STATE messages")
			gomega.Expect(messageTypes["NBIRTH"]).To(gomega.BeNumerically(">=", 1),
				"Should receive NBIRTH messages")
			gomega.Expect(messageTypes["NDATA"]).To(gomega.BeNumerically(">=", 8),
				"Should receive multiple NDATA messages")

			// Validate alias resolution
			gomega.Expect(aliasResolutionCount).To(gomega.BeNumerically(">=", 5),
				"Should resolve aliases in NDATA messages")

			ginkgo.By("Validating performance metrics")

			duration := time.Since(startTime)
			throughput := float64(totalReceived) / duration.Seconds()

			fmt.Printf("📊 Test duration: %v\n", duration)
			fmt.Printf("📊 Throughput: %.2f msg/sec\n", throughput)

			gomega.Expect(throughput).To(gomega.BeNumerically(">=", 1.0),
				"Throughput should be at least 1 msg/sec")

			ginkgo.By("Checking memory stability")

			var memStats runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&memStats)

			memUsageMB := float64(memStats.Alloc) / 1024 / 1024
			fmt.Printf("📊 Memory usage: %.2f MB\n", memUsageMB)

			gomega.Expect(memUsageMB).To(gomega.BeNumerically("<", 100),
				"Memory usage should be under 100MB")

			fmt.Printf("✅ Bidirectional communication test completed successfully!\n")
		})

		ginkgo.It("should handle sequence gap detection and rebirth requests", func() {
			ginkgo.Skip("Sequence gap testing requires more complex setup - will be implemented in Phase 2")
		})

		ginkgo.It("should recover from network disconnection gracefully", func() {
			ginkgo.Skip("Network failure testing requires broker control - will be implemented in Phase 3")
		})

		ginkgo.It("should maintain performance under sustained load", func() {
			ginkgo.Skip("Long-running stability test - will be implemented in Phase 3")
		})
	})
})

// Helper functions

func isIntegrationTestEnabled() bool {
	return getEnvWithDefault("TEST_SPARKPLUG_B", "") == "1"
}

func getBrokerURL() string {
	return getEnvWithDefault("TEST_MQTT_BROKER", "tcp://127.0.0.1:1883")
}

func getEnvWithDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
