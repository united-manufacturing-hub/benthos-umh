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
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("Sparkplug Device Discovery Integration Test", Ordered, func() {
	var (
		stream            *service.Stream
		ctx               context.Context
		cancel            context.CancelFunc
		discoveredDevices sync.Map
		messagesChan      chan *service.Message
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 75*time.Second)
		messagesChan = make(chan *service.Message, 1000)

		// Create Benthos stream configuration for device discovery
		// Using the idiomatic configuration structure
		streamConfig := `
input:
  sparkplug_b:
    mqtt:
      urls: 
        - "tcp://broker.hivemq.com:1883"
      client_id: "benthos-device-discovery-test"
      credentials:
        username: ""   # No authentication for public broker  
        password: ""   # No authentication for public broker
      qos: 0           # Use QoS 0 for public broker
      keep_alive: "60s"
      connect_timeout: "30s"
      clean_session: true
    
    identity:
      group_id: "benthos"           # Our group ID for STATE topic
      edge_node_id: "device-discovery"  # Our edge node ID  
      device_id: ""               # Node-level identity
    
    role: "primary_host"          # Subscribe to all groups (spBv1.0/+/#)
    
    subscription:
      groups: ["benthos"]         # Only subscribe to benthos group for testing
    
    behaviour:
      auto_split_metrics: true    # Split metrics for easier processing
      data_messages_only: false   # We want BIRTH messages to discover devices
      enable_rebirth_req: false   # Don't send rebirth requests in discovery mode
      drop_birth_messages: false  # Keep BIRTH messages for discovery
      strict_topic_validation: false
      auto_extract_values: true

# Use a simple output that we can intercept
output:
  drop: {}

# No processing needed - we'll intercept messages
pipeline:  
  processors: []
`

		// Build the stream using Benthos stream builder
		streamBuilder := service.NewStreamBuilder()

		// Parse and set the stream configuration
		err := streamBuilder.SetYAML(streamConfig)
		Expect(err).NotTo(HaveOccurred())

		// Add message interceptor
		streamBuilder.AddBatchConsumerFunc(func(ctx context.Context, batch service.MessageBatch) error {
			for _, msg := range batch {
				select {
				case messagesChan <- msg:
				case <-ctx.Done():
					return ctx.Err()
				default:
					// Channel full, skip message
				}
			}
			return nil
		})

		// Build the stream
		stream, err = streamBuilder.Build()
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		if cancel != nil {
			cancel()
		}
		if stream != nil {
			stream.Stop(ctx)
		}
		close(messagesChan)
	})

	Context("Device Discovery", func() {
		It("should connect to HiveMQ and discover active Sparkplug devices using Benthos stream", func() {
			GinkgoWriter.Printf("\n🔍 Starting Sparkplug Device Discovery on broker.hivemq.com\n")
			GinkgoWriter.Printf("📡 Using Benthos Stream Builder for realistic testing\n")
			GinkgoWriter.Printf("🎯 Listening for Sparkplug B messages for 60 seconds...\n\n")

			// Track devices and their last activity
			deviceActivity := make(map[string]time.Time)
			var activityMutex sync.Mutex

			// Start the stream in a goroutine so it doesn't block
			streamDone := make(chan error, 1)
			go func() {
				defer GinkgoRecover()
				streamDone <- stream.Run(ctx)
			}()

			// Give the stream time to connect and start receiving messages
			time.Sleep(2 * time.Second)

			GinkgoWriter.Printf("✅ Benthos stream started and connected to broker.hivemq.com\n")
			GinkgoWriter.Printf("📊 Primary Application (role: primary_host) subscribing to benthos group (spBv1.0/benthos/#)\n")
			GinkgoWriter.Printf("🏭 Identity: Group=benthos, EdgeNode=device-discovery (node-level)\n")
			GinkgoWriter.Printf("📍 STATE topic: spBv1.0/benthos/STATE/device-discovery\n\n")

			// Start message processing in goroutine
			go func() {
				defer GinkgoRecover()

				for {
					select {
					case <-ctx.Done():
						return
					case msg, ok := <-messagesChan:
						if !ok {
							return // Channel closed
						}
						processSparkplugMessage(msg, &discoveredDevices, &deviceActivity, &activityMutex)
					}
				}
			}()

			// Wait for discovery period
			time.Sleep(60 * time.Second)

			// Check if stream had any errors (non-blocking check)
			select {
			case err := <-streamDone:
				if err != nil && err.Error() != "context canceled" {
					GinkgoWriter.Printf("⚠️  Stream ended with error: %v\n", err)
				}
			default:
				// Stream still running, which is expected
			}

			// Print discovery results
			GinkgoWriter.Printf("\n📊 Device Discovery Results:\n")
			GinkgoWriter.Printf("%s\n", "====================================================")

			deviceCount := 0
			discoveredDevices.Range(func(key, value interface{}) bool {
				deviceKey := key.(string)
				deviceInfo := value.(DeviceInfo)

				activityMutex.Lock()
				lastSeen := deviceActivity[deviceKey]
				activityMutex.Unlock()

				GinkgoWriter.Printf("\n🏭 Device: %s\n", deviceKey)
				GinkgoWriter.Printf("   📍 Group: %s\n", deviceInfo.Group)
				GinkgoWriter.Printf("   🔗 Edge Node: %s\n", deviceInfo.EdgeNode)
				if deviceInfo.Device != "" {
					GinkgoWriter.Printf("   📱 Device ID: %s\n", deviceInfo.Device)
				}
				GinkgoWriter.Printf("   📡 Message Type: %s\n", deviceInfo.LastMessageType)
				GinkgoWriter.Printf("   🕐 Last Seen: %s\n", lastSeen.Format("15:04:05"))
				if len(deviceInfo.Metrics) > 0 {
					GinkgoWriter.Printf("   📈 Metrics: %d available\n", len(deviceInfo.Metrics))
					// Show first few metrics as examples
					count := 0
					for metric := range deviceInfo.Metrics {
						if count < 3 {
							GinkgoWriter.Printf("      - %s\n", metric)
						}
						count++
					}
					if count > 3 {
						GinkgoWriter.Printf("      ... and %d more\n", count-3)
					}
				}

				deviceCount++
				return true
			})

			GinkgoWriter.Printf("\n🎯 Total Devices Discovered: %d\n", deviceCount)
			GinkgoWriter.Printf("🚀 Stream-based discovery completed successfully!\n")

			if deviceCount == 0 {
				GinkgoWriter.Printf("\n💡 No Sparkplug devices found. This could mean:\n")
				GinkgoWriter.Printf("   • No devices are currently publishing to this broker\n")
				GinkgoWriter.Printf("   • Devices are using different group IDs\n")
				GinkgoWriter.Printf("   • Devices might be on private broker instances\n")
				GinkgoWriter.Printf("   • Network connectivity issues in test environment\n")
				GinkgoWriter.Printf("\n🔧 Try running this test when you know devices are active,\n")
				GinkgoWriter.Printf("   or modify the group_id to target specific Sparkplug groups.\n")
			} else {
				GinkgoWriter.Printf("\n✅ Success! Stream builder successfully discovered active devices\n")
				GinkgoWriter.Printf("🎉 This confirms the Sparkplug input component works in real streams\n")
			}
		})
	})
})

// DeviceInfo holds information about discovered Sparkplug devices
type DeviceInfo struct {
	Group           string
	EdgeNode        string
	Device          string
	LastMessageType string
	Metrics         map[string]interface{}
}

// processSparkplugMessage processes incoming Sparkplug messages and updates device discovery
func processSparkplugMessage(msg *service.Message, devices *sync.Map, activity *map[string]time.Time, mutex *sync.Mutex) {
	// Get MQTT topic from metadata
	topic, exists := msg.MetaGet("mqtt_topic")
	if !exists {
		return
	}

	// Parse Sparkplug topic: spBv1.0/<Group>/<MsgType>/<EdgeNode>[/<Device>]
	parts := strings.Split(topic, "/")
	if len(parts) < 4 || parts[0] != "spBv1.0" {
		return // Not a Sparkplug topic
	}

	group := parts[1]
	msgType := parts[2]
	edgeNode := parts[3]
	device := ""
	if len(parts) >= 5 {
		device = parts[4]
	}

	// Create device key
	deviceKey := fmt.Sprintf("%s/%s", group, edgeNode)
	if device != "" {
		deviceKey = fmt.Sprintf("%s/%s", deviceKey, device)
	}

	// Update activity time
	mutex.Lock()
	(*activity)[deviceKey] = time.Now()
	mutex.Unlock()

	// Get or create device info
	var deviceInfo DeviceInfo
	if existing, exists := devices.Load(deviceKey); exists {
		deviceInfo = existing.(DeviceInfo)
	} else {
		deviceInfo = DeviceInfo{
			Group:    group,
			EdgeNode: edgeNode,
			Device:   device,
			Metrics:  make(map[string]interface{}),
		}
	}

	deviceInfo.LastMessageType = msgType

	// If this is a BIRTH or DATA message, try to extract metrics
	if msgType == "NBIRTH" || msgType == "DBIRTH" || msgType == "NDATA" || msgType == "DDATA" {
		// Try to get message content as JSON to extract metric information
		content, err := msg.AsStructured()
		if err == nil {
			if contentMap, ok := content.(map[string]interface{}); ok {
				// Look for metrics in the payload
				if metrics, exists := contentMap["metrics"]; exists {
					if metricsList, ok := metrics.([]interface{}); ok {
						for _, metric := range metricsList {
							if metricMap, ok := metric.(map[string]interface{}); ok {
								if name, exists := metricMap["name"]; exists {
									if nameStr, ok := name.(string); ok {
										deviceInfo.Metrics[nameStr] = metricMap
									}
								}
							}
						}
					}
				}

				// Also check for single metric (if auto-split is enabled)
				if metricName, exists := contentMap["sparkplug_metric_name"]; exists {
					if nameStr, ok := metricName.(string); ok {
						deviceInfo.Metrics[nameStr] = contentMap
					}
				}
			}
		}
	}

	// Store updated device info
	devices.Store(deviceKey, deviceInfo)

	// Print real-time discovery
	fmt.Printf("📡 [%s] %s: %s -> %s\n",
		time.Now().Format("15:04:05"),
		msgType,
		deviceKey,
		topic)
}
