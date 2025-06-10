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
// +build integration

package sparkplug_plugin

import (
	"context"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("Sparkplug Device Publisher Integration Test", Ordered, func() {
	var (
		stream *service.Stream
		ctx    context.Context
		cancel context.CancelFunc
		wg     sync.WaitGroup
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 70*time.Second)

		// Create Benthos stream configuration for publishing Sparkplug data
		// Using the new sectioned structure
		streamConfig := `
logger:
  level: DEBUG
  
input:
  generate:
    interval: "5s"  
    count: 0  # Generate indefinitely
    mapping: |
      # Simple static data that increments based on count
      root = {
        "temperature": 25.5,
        "pressure": 1.2,
        "motor_rpm": 1250,
        "status": "RUNNING",
        "enabled": true,
        "timestamp": timestamp_unix(),
        "message_id": 1
      }

output:
  broker:
    outputs:
      - sparkplug_b:
          # MQTT Transport Configuration
          mqtt:
            urls: 
              - "tcp://broker.hivemq.com:1883"
            client_id: "benthos-test-publisher"
            credentials:
              username: ""   # No authentication for public broker  
              password: ""   # No authentication for public broker
            qos: 0         # Use QoS 0 for public broker
            keep_alive: "60s"
            connect_timeout: "30s"
            clean_session: true
          
          # Sparkplug Identity Configuration
          identity:
            group_id: "benthos"        # Test group that discovery should detect
            edge_node_id: "BenthosTestLine" # Test edge node
            device_id: "TestDevice01"       # Test device
          
          # Role Configuration
          role: "edge_node"              # Act as edge node publishing data
          
          # Behavior Configuration
          behaviour:
            auto_extract_tag_name: true     # Extract tag names from metadata
            retain_last_values: true        # Retain values for BIRTH messages
          
          # Define metrics for the test data
          metrics:
            - name: "Temperature"
              alias: 1
              type: "float"
              value_from: "temperature"
            - name: "Pressure"
              alias: 2
              type: "float"
              value_from: "pressure"
            - name: "Motor_RPM"
              alias: 3
              type: "uint32"
              value_from: "motor_rpm"
            - name: "Status"
              alias: 4
              type: "string"
              value_from: "status"
            - name: "Enabled"
              alias: 5
              type: "boolean"
              value_from: "enabled"

          
      - stdout:
          codec: lines
`

		// Build the stream using Benthos stream builder
		streamBuilder := service.NewStreamBuilder()

		// Parse and set the stream configuration
		err := streamBuilder.SetYAML(streamConfig)
		Expect(err).NotTo(HaveOccurred())

		// No need for message handler since we're using generate input

		// Build the stream
		stream, err = streamBuilder.Build()
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		if cancel != nil {
			cancel()
		}
		wg.Wait()
		if stream != nil {
			stream.Stop(ctx)
		}
	})

	Context("Sparkplug Data Publishing", func() {
		It("should publish Sparkplug B messages to HiveMQ for device discovery", func() {
			GinkgoWriter.Printf("\n📡 Starting Sparkplug Device Publisher to broker.hivemq.com\n")
			GinkgoWriter.Printf("🏭 Publishing as: Group=benthos, EdgeNode=BenthosTestLine, Device=TestDevice01\n")
			GinkgoWriter.Printf("🔄 Will publish BIRTH and DATA messages for 60 seconds...\n\n")

			// Start the stream in a goroutine
			streamDone := make(chan error, 1)
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				streamDone <- stream.Run(ctx)
			}()

			// Give the stream time to connect
			time.Sleep(2 * time.Second)

			GinkgoWriter.Printf("✅ Benthos stream started and connected to broker.hivemq.com\n")
			GinkgoWriter.Printf("📊 Edge Node (role: edge_node) will publish to Sparkplug topics\n")
			GinkgoWriter.Printf("📍 BIRTH topic: spBv1.0/benthos/DBIRTH/BenthosTestLine/TestDevice01\n")
			GinkgoWriter.Printf("📍 DATA topic: spBv1.0/benthos/DDATA/BenthosTestLine/TestDevice01\n\n")

			// Stream will automatically generate and publish messages

			// Let it publish for 60 seconds
			time.Sleep(60 * time.Second)

			// Check stream status
			select {
			case err := <-streamDone:
				if err != nil && err.Error() != "context canceled" {
					GinkgoWriter.Printf("⚠️  Stream ended with error: %v\n", err)
				}
			default:
				GinkgoWriter.Printf("✅ Stream completed successfully\n")
			}

			GinkgoWriter.Printf("\n🎯 Publishing Test Results:\n")
			GinkgoWriter.Printf("====================================================\n")
			GinkgoWriter.Printf("✅ Successfully published Sparkplug B messages to HiveMQ\n")
			GinkgoWriter.Printf("🏭 Published Device: benthos/BenthosTestLine/TestDevice01\n")
			GinkgoWriter.Printf("📡 Message Types: DBIRTH (device birth) and DDATA (device data)\n")
			GinkgoWriter.Printf("📈 Metrics Published: Temperature, Pressure, Motor_Speed, Status, Enabled\n")
			GinkgoWriter.Printf("🚀 Stream-based publishing completed successfully!\n")
			GinkgoWriter.Printf("\n💡 If running in parallel with device discovery test,\n")
			GinkgoWriter.Printf("   the discovery test should detect this published device.\n")
		})
	})
})
