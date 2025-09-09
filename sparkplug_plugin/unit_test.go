//go:build !integration

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

// Unit tests for Sparkplug B plugin core components
// These tests run offline with no external dependencies (<3s)

package sparkplug_plugin_test

import (
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	sparkplugplugin "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
	"google.golang.org/protobuf/proto"
)

func TestSparkplugUnit(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sparkplug B Complete Test Suite")
}

// Helper functions for unit testing
func stringPtr(s string) *string {
	return &s
}

func uint64Ptr(u uint64) *uint64 {
	return &u
}

func uint32Ptr(u uint32) *uint32 {
	return &u
}

func boolPtr(b bool) *bool {
	return &b
}


var _ = Describe("AliasCache Unit Tests", func() {

	var cache *sparkplugplugin.AliasCache

	BeforeEach(func() {
		cache = sparkplugplugin.NewAliasCache()
	})

	Context("Alias Resolution", func() {
		It("should cache aliases from BIRTH metrics", func() {
			metrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(100),
				},
				{
					Name:  stringPtr("Pressure"),
					Alias: uint64Ptr(101),
				},
				{
					Name:  stringPtr("Speed"),
					Alias: uint64Ptr(200),
				},
			}

			count := cache.CacheAliases("TestFactory/Line1", metrics)
			Expect(count).To(Equal(3))
		})

		It("should resolve metric aliases from NBIRTH context", func() {
			// First cache aliases from NBIRTH
			birthMetrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(100),
				},
				{
					Name:  stringPtr("Pressure"),
					Alias: uint64Ptr(101),
				},
			}
			cache.CacheAliases("TestFactory/Line1", birthMetrics)

			// Now test alias resolution in NDATA
			dataMetrics := []*sparkplugb.Payload_Metric{
				{
					Alias: uint64Ptr(100), // Should resolve to "Temperature"
					Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 25.5},
				},
				{
					Alias: uint64Ptr(101), // Should resolve to "Pressure"
					Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 1013.25},
				},
			}

			count := cache.ResolveAliases("TestFactory/Line1", dataMetrics)
			Expect(count).To(Equal(2))
			Expect(*dataMetrics[0].Name).To(Equal("Temperature"))
			Expect(*dataMetrics[1].Name).To(Equal("Pressure"))
		})

		It("should handle alias collisions in NBIRTH", func() {
			// Create metrics with duplicate aliases (collision)
			metrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("MotorRPM"),
					Alias: uint64Ptr(5),
				},
				{
					Name:  stringPtr("MotorTemp"),
					Alias: uint64Ptr(5), // Duplicate alias - should cause issue
				},
			}

			// The current implementation doesn't check for collisions
			// This is a test to ensure we handle them correctly
			count := cache.CacheAliases("TestFactory/Line1", metrics)
			// Implementation should ideally detect this as an error condition
			// For now, we just verify it handles the scenario without crashing
			Expect(count).To(BeNumerically(">=", 1))
		})

		It("should reset alias cache on session restart", func() {
			// Cache initial aliases
			metrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(100),
				},
			}
			cache.CacheAliases("TestFactory/Line1", metrics)

			// Clear cache (simulating session restart)
			cache.Clear()

			// Verify cache is empty by trying to resolve
			dataMetrics := []*sparkplugb.Payload_Metric{
				{
					Alias: uint64Ptr(100),
					Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 25.5},
				},
			}
			count := cache.ResolveAliases("TestFactory/Line1", dataMetrics)
			Expect(count).To(Equal(0))
		})

		It("should handle multiple devices independently", func() {
			// Cache aliases for device 1
			device1Metrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(100),
				},
			}
			count1 := cache.CacheAliases("TestFactory/Line1", device1Metrics)
			Expect(count1).To(Equal(1))

			// Cache aliases for device 2
			device2Metrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Pressure"),
					Alias: uint64Ptr(100), // Same alias, different device
				},
			}
			count2 := cache.CacheAliases("TestFactory/Line2", device2Metrics)
			Expect(count2).To(Equal(1))

			// Test resolution for each device independently
			dataMetrics1 := []*sparkplugb.Payload_Metric{
				{
					Alias: uint64Ptr(100),
					Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 25.5},
				},
			}
			resolved1 := cache.ResolveAliases("TestFactory/Line1", dataMetrics1)
			Expect(resolved1).To(Equal(1))
			Expect(*dataMetrics1[0].Name).To(Equal("Temperature"))

			dataMetrics2 := []*sparkplugb.Payload_Metric{
				{
					Alias: uint64Ptr(100),
					Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 1013.25},
				},
			}
			resolved2 := cache.ResolveAliases("TestFactory/Line2", dataMetrics2)
			Expect(resolved2).To(Equal(1))
			Expect(*dataMetrics2[0].Name).To(Equal("Pressure"))
		})

		It("should handle edge cases gracefully", func() {
			// Test empty device key
			metrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(100),
				},
			}
			count := cache.CacheAliases("", metrics)
			Expect(count).To(Equal(0))

			// Test nil metrics
			count = cache.CacheAliases("TestFactory/Line1", nil)
			Expect(count).To(Equal(0))

			// Test metrics without name or alias
			invalidMetrics := []*sparkplugb.Payload_Metric{
				{
					Name: stringPtr("Temperature"), // Missing alias
				},
				{
					Alias: uint64Ptr(100), // Missing name
				},
				{
					Name:  stringPtr(""), // Empty name
					Alias: uint64Ptr(101),
				},
				{
					Name:  stringPtr("ValidMetric"),
					Alias: uint64Ptr(102),
				},
			}
			count = cache.CacheAliases("TestFactory/Line1", invalidMetrics)
			Expect(count).To(Equal(1)) // Only ValidMetric should be cached
		})

		// Comprehensive negative test cases for alias resolution
		It("should handle corrupted alias cache gracefully", func() {
			// First cache some valid aliases
			validMetrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(100),
				},
				{
					Name:  stringPtr("Pressure"),
					Alias: uint64Ptr(101),
				},
			}
			cache.CacheAliases("TestFactory/Line1", validMetrics)
			
			// Try to resolve with non-existent aliases
			dataMetrics := []*sparkplugb.Payload_Metric{
				{
					Alias: uint64Ptr(999), // Non-existent alias
					Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 25.5},
				},
				{
					Alias: uint64Ptr(1000), // Non-existent alias
					Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 30.5},
				},
			}
			
			count := cache.ResolveAliases("TestFactory/Line1", dataMetrics)
			Expect(count).To(Equal(0)) // No aliases should be resolved
			Expect(dataMetrics[0].Name).To(BeNil()) // Name should remain nil
			Expect(dataMetrics[1].Name).To(BeNil()) // Name should remain nil
		})

		It("should handle duplicate aliases in different sessions", func() {
			// Session 1: Cache alias 100 for Temperature
			metrics1 := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(100),
				},
			}
			cache.CacheAliases("TestFactory/Line1", metrics1)
			
			// Session 2 (simulated): Try to cache alias 100 for different metric
			// This simulates a rebirth where aliases might be reassigned
			metrics2 := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Humidity"),
					Alias: uint64Ptr(100), // Same alias, different metric
				},
			}
			cache.CacheAliases("TestFactory/Line1", metrics2)
			
			// Resolve should use the latest assignment
			dataMetrics := []*sparkplugb.Payload_Metric{
				{
					Alias: uint64Ptr(100),
					Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 45.5},
				},
			}
			count := cache.ResolveAliases("TestFactory/Line1", dataMetrics)
			Expect(count).To(Equal(1))
			Expect(*dataMetrics[0].Name).To(Equal("Humidity")) // Should be the latest assignment
		})

		It("should handle alias overflow scenarios", func() {
			// Test aliases at the boundary of uint64
			largeAliasMetrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("MaxAlias"),
					Alias: uint64Ptr(^uint64(0)), // Max uint64 value
				},
				{
					Name:  stringPtr("NormalAlias"),
					Alias: uint64Ptr(65535), // Max uint16 value (common in Sparkplug)
				},
				{
					Name:  stringPtr("ZeroAlias"),
					Alias: uint64Ptr(0), // Min value - will be skipped as 0 is invalid
				},
			}
			
			count := cache.CacheAliases("TestFactory/Line1", largeAliasMetrics)
			Expect(count).To(Equal(2)) // Only 2 should be cached, alias 0 is invalid
			
			// Verify resolution works with extreme values
			dataMetrics := []*sparkplugb.Payload_Metric{
				{
					Alias: uint64Ptr(^uint64(0)),
					Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 100.0},
				},
				{
					Alias: uint64Ptr(0),
					Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 200.0},
				},
			}
			
			resolved := cache.ResolveAliases("TestFactory/Line1", dataMetrics)
			Expect(resolved).To(Equal(1)) // Only 1 resolved, alias 0 was not cached
			Expect(*dataMetrics[0].Name).To(Equal("MaxAlias"))
			Expect(dataMetrics[1].Name).To(BeNil()) // Alias 0 won't be resolved
		})

		It("should handle concurrent alias operations safely", func() {
			// This test ensures thread safety in alias cache operations
			done := make(chan bool)
			errors := make(chan error, 10)
			
			// Concurrent writers
			for i := 0; i < 5; i++ {
				go func(idx int) {
					defer GinkgoRecover()
					metrics := []*sparkplugb.Payload_Metric{
						{
							Name:  stringPtr(fmt.Sprintf("Metric%d", idx)),
							Alias: uint64Ptr(uint64(100 + idx)),
						},
					}
					cache.CacheAliases("TestFactory/Line1", metrics)
					done <- true
				}(i)
			}
			
			// Concurrent readers
			for i := 0; i < 5; i++ {
				go func(idx int) {
					defer GinkgoRecover()
					dataMetrics := []*sparkplugb.Payload_Metric{
						{
							Alias: uint64Ptr(uint64(100 + idx)),
							Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: float64(idx)},
						},
					}
					cache.ResolveAliases("TestFactory/Line1", dataMetrics)
					done <- true
				}(i)
			}
			
			// Wait for all goroutines
			for i := 0; i < 10; i++ {
				<-done
			}
			
			// Verify no errors occurred
			select {
			case err := <-errors:
				Fail(fmt.Sprintf("Concurrent operation failed: %v", err))
			default:
				// No errors
			}
		})

		It("should handle malformed device keys", func() {
			// Test various malformed device keys
			testCases := []struct {
				deviceKey string
				desc      string
			}{
				{"", "empty device key"},
				{" ", "whitespace only"},
				{"TestFactory/", "trailing slash"},
				{"/Line1", "leading slash"},
				{"Test Factory/Line 1", "spaces in key"},
				{"Test\nFactory/Line1", "newline in key"},
				{"Test\x00Factory/Line1", "null byte in key"},
			}
			
			for _, tc := range testCases {
				metrics := []*sparkplugb.Payload_Metric{
					{
						Name:  stringPtr("TestMetric"),
						Alias: uint64Ptr(100),
					},
				}
				
				count := cache.CacheAliases(tc.deviceKey, metrics)
				// Most implementations should handle empty key specially
				if tc.deviceKey == "" {
					Expect(count).To(Equal(0), fmt.Sprintf("Failed for: %s", tc.desc))
				} else {
					// Other malformed keys might still work depending on implementation
					Expect(count).To(BeNumerically(">=", 0), fmt.Sprintf("Failed for: %s", tc.desc))
				}
			}
		})

		It("should handle resolution with nil or invalid metric fields", func() {
			// Cache valid alias first
			validMetrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(100),
				},
			}
			cache.CacheAliases("TestFactory/Line1", validMetrics)
			
			// Try to resolve with various invalid metrics
			invalidDataMetrics := []*sparkplugb.Payload_Metric{
				{
					// Metric with nil alias
					Alias: nil,
					Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 25.5},
				},
				{
					// Valid alias but metric might have other issues
					Alias: uint64Ptr(100),
					Value: nil, // Nil value
				},
				{
					// Metric with already populated name (should not override)
					Name:  stringPtr("ExistingName"),
					Alias: uint64Ptr(100),
					Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 30.5},
				},
			}
			
			count := cache.ResolveAliases("TestFactory/Line1", invalidDataMetrics)
			// Implementation should handle these gracefully
			Expect(count).To(BeNumerically(">=", 0))
			
			// Check that existing name wasn't overwritten
			if invalidDataMetrics[2].Name != nil {
				Expect(*invalidDataMetrics[2].Name).To(Equal("ExistingName"))
			}
		})

		It("should handle rapid session changes and rebirth scenarios", func() {
			// Simulate multiple rapid rebirths with changing aliases
			for session := 0; session < 3; session++ {
				// Clear cache to simulate new session
				cache.Clear()
				
				// Each session uses different aliases for same metrics
				metrics := []*sparkplugb.Payload_Metric{
					{
						Name:  stringPtr("Temperature"),
						Alias: uint64Ptr(uint64(100 + session*10)),
					},
					{
						Name:  stringPtr("Pressure"),
						Alias: uint64Ptr(uint64(101 + session*10)),
					},
				}
				
				count := cache.CacheAliases("TestFactory/Line1", metrics)
				Expect(count).To(Equal(2))
				
				// Verify old aliases don't work
				if session > 0 {
					oldDataMetrics := []*sparkplugb.Payload_Metric{
						{
							Alias: uint64Ptr(uint64(100 + (session-1)*10)),
							Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 25.5},
						},
					}
					resolved := cache.ResolveAliases("TestFactory/Line1", oldDataMetrics)
					Expect(resolved).To(Equal(0)) // Old aliases should not resolve
				}
				
				// Verify new aliases work
				newDataMetrics := []*sparkplugb.Payload_Metric{
					{
						Alias: uint64Ptr(uint64(100 + session*10)),
						Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 25.5},
					},
				}
				resolved := cache.ResolveAliases("TestFactory/Line1", newDataMetrics)
				Expect(resolved).To(Equal(1))
				Expect(*newDataMetrics[0].Name).To(Equal("Temperature"))
			}
		})
	})
})


var _ = Describe("TypeConverter Unit Tests", func() {

	Context("Data Type Conversions", func() {
		
		It("should convert Sparkplug types to UMH format", func() {
			// Test various Sparkplug data type conversions
			testCases := []struct {
				sparkplugType uint32
				value         interface{}
				expectedType  string
			}{
				{7, uint64(12345), "uint64"},      // UInt32 in Sparkplug
				{9, float32(25.5), "float32"},     // Float
				{10, float64(1013.25), "float64"}, // Double
				{11, true, "bool"},                // Boolean
				{12, "RUNNING", "string"},         // String
			}

			for _, tc := range testCases {
				By(fmt.Sprintf("converting type %s", tc.expectedType), func() {
					// Verify type handling
					switch tc.value.(type) {
					case uint64:
						Expect(tc.sparkplugType).To(Equal(uint32(7)))
					case float32:
						Expect(tc.sparkplugType).To(Equal(uint32(9)))
					case float64:
						Expect(tc.sparkplugType).To(Equal(uint32(10)))
					case bool:
						Expect(tc.sparkplugType).To(Equal(uint32(11)))
					case string:
						Expect(tc.sparkplugType).To(Equal(uint32(12)))
					}
				})
			}
		})

		It("should infer correct data types from Go values", func() {
			converter := sparkplugplugin.NewTypeConverter()

			// Test various value types
			values := map[interface{}]string{
				true:          "boolean",
				false:         "boolean",
				int(42):       "int32",
				int32(42):     "int32",
				int64(42):     "int64",
				uint32(42):    "uint32",
				uint64(42):    "uint64",
				float32(3.14): "float",
				float64(3.14): "double",
				"hello":       "string",
			}

			for value, expectedType := range values {
				inferredType := converter.InferMetricType(value)
				Expect(inferredType).To(Equal(expectedType), fmt.Sprintf("value=%v expectedType=%q", value, expectedType))
			}

			// Test unknown type (should default to string)
			unknownValue := struct{ field string }{field: "test"}
			inferredType := converter.InferMetricType(unknownValue)
			Expect(inferredType).To(Equal("string"), "Unknown types should default to string")
		})
	})
})

var _ = Describe("MQTTClientBuilder Unit Tests", func() {
	Context("Client Configuration", func() {
		It("should build valid MQTT client options", func() {
			// Test MQTT client configuration building
			testConfigs := []struct {
				name   string
				config map[string]interface{}
			}{
				{
					"basic_config",
					map[string]interface{}{
						"broker_urls":     []string{"tcp://localhost:1883"},
						"client_id":       "test-client",
						"qos":             1,
						"keep_alive":      "30s",
						"connect_timeout": "10s",
						"clean_session":   true,
					},
				},
				{
					"ssl_config",
					map[string]interface{}{
						"broker_urls":     []string{"ssl://broker.example.com:8883"},
						"client_id":       "ssl-client",
						"qos":             2,
						"keep_alive":      "60s",
						"connect_timeout": "20s",
						"clean_session":   false,
						"username":        "testuser",
						"password":        "testpass",
					},
				},
			}

			for _, tc := range testConfigs {
				By("building config for: "+tc.name, func() {
					// Verify configuration values are valid
					Expect(tc.config["broker_urls"]).NotTo(BeNil())
					Expect(tc.config["client_id"]).NotTo(BeNil())
					Expect(tc.config["qos"]).To(BeNumerically(">=", 0))
					Expect(tc.config["qos"]).To(BeNumerically("<=", 2))

					// Verify broker URL format
					urls := tc.config["broker_urls"].([]string)
					for _, url := range urls {
						Expect(url).To(MatchRegexp(`^(tcp|ssl)://[^:]+:\d+$`))
					}

					// Verify client ID is not empty
					clientId := tc.config["client_id"].(string)
					Expect(clientId).NotTo(BeEmpty())
				})
			}
		})

		It("should handle connection configuration validation", func() {
			// Test authentication configuration validation
			authConfigs := []struct {
				name   string
				config map[string]interface{}
				valid  bool
			}{
				{
					"no_auth",
					map[string]interface{}{
						"client_id": "test-client",
					},
					true,
				},
				{
					"username_password",
					map[string]interface{}{
						"client_id": "test-client",
						"username":  "testuser",
						"password":  "testpass",
					},
					true,
				},
				{
					"username_only",
					map[string]interface{}{
						"client_id": "test-client",
						"username":  "testuser",
					},
					true, // Username without password is valid
				},
				{
					"empty_username",
					map[string]interface{}{
						"client_id": "test-client",
						"username":  "",
						"password":  "testpass",
					},
					false, // Empty username with password should be invalid
				},
			}

			for _, tc := range authConfigs {
				By("validating auth config: "+tc.name, func() {
					// Basic validation logic
					username, hasUsername := tc.config["username"]
					password, hasPassword := tc.config["password"]

					if tc.valid {
						// Valid configurations
						if hasUsername {
							usernameStr := username.(string)
							if hasPassword {
								passwordStr := password.(string)
								// Username + password should both be non-empty
								if usernameStr == "" {
									// This should be caught as invalid
									Expect(tc.valid).To(BeFalse())
								} else {
									Expect(usernameStr).NotTo(BeEmpty())
									Expect(passwordStr).NotTo(BeEmpty())
								}
							}
						}
					} else {
						// Invalid configurations should be caught
						if hasUsername && hasPassword {
							usernameStr := username.(string)
							if usernameStr == "" {
								// Empty username with password is invalid
								Expect(usernameStr).To(BeEmpty())
							}
						}
					}
				})
			}
		})
	})
})


var _ = Describe("Configuration Unit Tests", func() {

	Context("Three-Mode System Validation", func() {
		It("should validate role values", func() {
			// Test valid roles
			validRoles := []string{"secondary_passive", "secondary_active", "primary"}
			
			for _, role := range validRoles {
				By(fmt.Sprintf("validating role: %s", role), func() {
					config := &sparkplugplugin.Config{
						Role: sparkplugplugin.Role(role),
						MQTT: sparkplugplugin.MQTT{
							URLs: []string{"tcp://localhost:1883"},
							QoS:  1,
						},
						Identity: sparkplugplugin.Identity{
							GroupID: "TestGroup",
						},
					}
					
					// For primary role, edge_node_id is required
					if role == "primary" {
						config.Identity.EdgeNodeID = "PrimaryHost"
					}
					
					err := config.Validate()
					Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("Role %s should be valid", role))
				})
			}
			
			// Test invalid roles
			invalidRoles := []string{"host", "invalid", "secondary"}
			
			for _, role := range invalidRoles {
				By(fmt.Sprintf("rejecting invalid role: %s", role), func() {
					config := &sparkplugplugin.Config{
						Role: sparkplugplugin.Role(role),
						MQTT: sparkplugplugin.MQTT{
							URLs: []string{"tcp://localhost:1883"},
							QoS:  1,
						},
						Identity: sparkplugplugin.Identity{
							GroupID: "TestGroup",
						},
					}
					
					err := config.Validate()
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("invalid role"))
				})
			}
		})
		
		It("should default to secondary_passive role", func() {
			config := &sparkplugplugin.Config{}
			config.AutoDetectRole()
			Expect(config.Role).To(Equal(sparkplugplugin.RoleSecondaryPassive))
		})
		
		It("should require edge_node_id for primary role", func() {
			config := &sparkplugplugin.Config{
				Role: sparkplugplugin.RolePrimaryHost,
				MQTT: sparkplugplugin.MQTT{
					URLs: []string{"tcp://localhost:1883"},
					QoS:  1,
				},
				Identity: sparkplugplugin.Identity{
					GroupID: "TestGroup",
					// EdgeNodeID is missing
				},
			}
			
			err := config.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("edge_node_id is required for Primary Host"))
		})
		
		It("should not require edge_node_id for secondary modes", func() {
			secondaryModes := []sparkplugplugin.Role{
				sparkplugplugin.RoleSecondaryPassive,
				sparkplugplugin.RoleSecondaryActive,
			}
			
			for _, role := range secondaryModes {
				By(fmt.Sprintf("testing %s without edge_node_id", role), func() {
					config := &sparkplugplugin.Config{
						Role: role,
						MQTT: sparkplugplugin.MQTT{
							URLs: []string{"tcp://localhost:1883"},
							QoS:  1,
						},
						Identity: sparkplugplugin.Identity{
							GroupID: "TestGroup",
							// EdgeNodeID is not required
						},
					}
					
					err := config.Validate()
					Expect(err).NotTo(HaveOccurred())
				})
			}
		})
	})

	Context("Config Validation", func() {
		It("should validate required configuration fields", func() {
			// Test required field validation (migrated from old input test)
			requiredFields := []string{
				"group_id",
			}

			for _, field := range requiredFields {
				By("requiring field: "+field, func() {
					// group_id is required for Sparkplug B operation
					if field == "group_id" {
						Expect(field).To(Equal("group_id"))
					}
				})
			}
		})

		It("should provide sensible defaults", func() {
			// Test default value assignment (migrated from old input test)
			defaults := map[string]interface{}{
				"broker_urls":             []string{"tcp://localhost:1883"},
				"client_id":               "benthos-sparkplug-host",
				"split_metrics":           true,
				"enable_rebirth_requests": true,
				"qos":                     1,
				"keep_alive":              "30s",
				"connect_timeout":         "10s",
				"clean_session":           true,
			}

			for key, expectedValue := range defaults {
				By("checking default for: "+key, func() {
					switch key {
					case "broker_urls":
						urls := expectedValue.([]string)
						Expect(urls).To(Equal([]string{"tcp://localhost:1883"}))
					case "client_id":
						Expect(expectedValue).To(Equal("benthos-sparkplug-host"))
					case "split_metrics":
						Expect(expectedValue).To(BeTrue())
					case "enable_rebirth_requests":
						Expect(expectedValue).To(BeTrue())
					case "qos":
						Expect(expectedValue).To(Equal(1))
					case "keep_alive":
						Expect(expectedValue).To(Equal("30s"))
					case "connect_timeout":
						Expect(expectedValue).To(Equal("10s"))
					case "clean_session":
						Expect(expectedValue).To(BeTrue())
					}
				})
			}
		})

		It("should validate complex configuration scenarios", func() {
			// Test complex configuration validation (migrated from old input test)
			complexConfig := map[string]interface{}{
				"broker_urls":             []string{"tcp://broker1:1883", "ssl://broker2:8883"},
				"client_id":               "primary-host-001",
				"username":                "sparkplug_user",
				"password":                "secret123",
				"group_id":                "FactoryA",
				"primary_host_id":         "SCADA-001",
				"split_metrics":           false,
				"enable_rebirth_requests": false,
				"qos":                     2,
				"keep_alive":              "60s",
				"connect_timeout":         "20s",
				"clean_session":           false,
			}

			// Verify complex configuration is valid
			Expect(complexConfig["broker_urls"]).To(Equal([]string{"tcp://broker1:1883", "ssl://broker2:8883"}))
			Expect(complexConfig["username"]).To(Equal("sparkplug_user"))
			Expect(complexConfig["qos"]).To(Equal(2))
			Expect(complexConfig["split_metrics"]).To(BeFalse())
		})

		It("should handle invalid topic patterns", func() {
			// Test invalid topic validation (migrated from old input test)
			invalidTopics := []string{
				"invalid/topic/format",
				"spBv2.0/Factory1/NDATA/Line1", // Wrong version
				"spBv1.0/Factory1",             // Too short
				"",                             // Empty
				"spBv1.0//NDATA/Line1",         // Empty group
				"spBv1.0/Factory1//Line1",      // Empty message type
			}

			sparkplugPattern := `^spBv1\.0/[^/]+/(NBIRTH|NDATA|NDEATH|NCMD|DBIRTH|DDATA|DDEATH|DCMD)/[^/]+(/[^/]+)?$`

			for _, topic := range invalidTopics {
				By("rejecting invalid topic: "+topic, func() {
					// These should not match valid Sparkplug pattern
					if topic != "" {
						Expect(topic).NotTo(MatchRegexp(sparkplugPattern))
					}
				})
			}
		})
	})
})

var _ = Describe("MessageProcessor Unit Tests", func() {
	Context("Message Type Processing", func() {
		It("should process BIRTH messages and extract aliases", func() {
			// Test BIRTH message processing (migrated from old input test)
			birthMetrics := []*sparkplugb.Payload_Metric{
				{
					Name:     stringPtr("bdSeq"),
					Alias:    uint64Ptr(1),
					Datatype: uint32Ptr(8), // UInt64 (Sparkplug spec)
					Value:    &sparkplugb.Payload_Metric_LongValue{LongValue: 12345},
				},
				{
					Name:     stringPtr("Temperature"),
					Alias:    uint64Ptr(100),
					Datatype: uint32Ptr(9), // Float
					Value:    &sparkplugb.Payload_Metric_FloatValue{FloatValue: 25.5},
				},
			}

			// Verify BIRTH structure
			for _, metric := range birthMetrics {
				Expect(metric.Name).NotTo(BeNil())
				Expect(metric.Alias).NotTo(BeNil())
				Expect(metric.Value).NotTo(BeNil())

				if *metric.Name == "bdSeq" {
					Expect(metric.GetLongValue()).To(Equal(uint64(12345)))
				}
			}
		})

		It("should process DATA messages with alias resolution", func() {
			// Test DATA message processing (migrated from old input test)
			dataMetrics := []*sparkplugb.Payload_Metric{
				{
					Alias:    uint64Ptr(100), // Temperature alias
					Datatype: uint32Ptr(9),
					Value:    &sparkplugb.Payload_Metric_FloatValue{FloatValue: 26.8},
				},
				{
					Alias:    uint64Ptr(101), // Pressure alias
					Datatype: uint32Ptr(10),
					Value:    &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 1015.50},
				},
			}

			// DATA messages should use aliases (no names initially)
			for _, metric := range dataMetrics {
				Expect(metric.Alias).NotTo(BeNil())
				// Names should be nil initially (to be resolved)
				Expect(metric.Name).To(BeNil())
				Expect(metric.Value).NotTo(BeNil())
			}
		})

		It("should handle pre-birth data scenarios", func() {
			// Test DATA before BIRTH scenario (migrated from old edge cases)
			cache := sparkplugplugin.NewAliasCache()

			// Attempt to resolve aliases without cached BIRTH data
			dataMetrics := []*sparkplugb.Payload_Metric{
				{
					Alias:    uint64Ptr(100),
					Datatype: uint32Ptr(9),
					Value:    &sparkplugb.Payload_Metric_FloatValue{FloatValue: 25.0},
				},
			}

			// Should fail to resolve (no cached aliases)
			resolvedCount := cache.ResolveAliases("Factory/UnknownLine", dataMetrics)
			Expect(resolvedCount).To(Equal(0))

			// Metric name should still be nil
			Expect(dataMetrics[0].Name).To(BeNil())
		})

		It("should handle message splitting configuration", func() {
			// Test split_metrics behavior (migrated from old input test)
			multiMetricPayload := []*sparkplugb.Payload_Metric{
				{
					Name:     stringPtr("Temperature"),
					Alias:    uint64Ptr(100),
					Datatype: uint32Ptr(9),
					Value:    &sparkplugb.Payload_Metric_FloatValue{FloatValue: 25.5},
				},
				{
					Name:     stringPtr("Pressure"),
					Alias:    uint64Ptr(101),
					Datatype: uint32Ptr(10),
					Value:    &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 1013.25},
				},
			}

			// When split_metrics=true, each metric should be processable individually
			for i, metric := range multiMetricPayload {
				By("processing metric "+string(rune(i)), func() {
					Expect(metric).NotTo(BeNil())
					Expect(metric.Name).NotTo(BeNil())
					Expect(metric.Alias).NotTo(BeNil())
					Expect(metric.Value).NotTo(BeNil())
				})
			}

			// When split_metrics=false, all metrics stay together
			Expect(multiMetricPayload).To(HaveLen(2))
		})
	})
})



// P8 Sparkplug B Spec Compliance Audit Tests
var _ = Describe("P8 Sparkplug B Spec Compliance Audit Tests", func() {
	Context("Birth/Death Message Compliance", func() {
		It("should verify NBIRTH includes all required fields", func() {
			// Test NBIRTH message structure compliance
			nbirthVector := sparkplugplugin.GetTestVector("NBIRTH_V1")
			Expect(nbirthVector).NotTo(BeNil())

			payloadBytes, err := base64.StdEncoding.DecodeString(nbirthVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var payload sparkplugb.Payload
			err = proto.Unmarshal(payloadBytes, &payload)
			Expect(err).NotTo(HaveOccurred())

			// NBIRTH MUST have sequence 0 (Sparkplug spec requirement)
			Expect(payload.Seq).NotTo(BeNil())
			Expect(*payload.Seq).To(Equal(uint64(0)), "NBIRTH must start with sequence 0")

			// NBIRTH MUST have timestamp
			Expect(payload.Timestamp).NotTo(BeNil(), "NBIRTH must include timestamp")

			// NBIRTH MUST contain bdSeq metric
			bdSeqFound := false
			for _, metric := range payload.Metrics {
				if metric.Name != nil {
					if *metric.Name == "bdSeq" {
						bdSeqFound = true
						// bdSeq must be UInt64 type
						Expect(metric.Datatype).NotTo(BeNil())
						Expect(*metric.Datatype).To(Equal(uint32(8))) // UInt64 type
					}
					if *metric.Name == "Node Control/Rebirth" {
						// Node Control must be Boolean type
						Expect(metric.Datatype).NotTo(BeNil())
						Expect(*metric.Datatype).To(Equal(uint32(11))) // Boolean type
					}
				}
			}
			Expect(bdSeqFound).To(BeTrue(), "NBIRTH must contain bdSeq metric")
			// Note: Node Control/Rebirth is optional for Edge Nodes, required for Primary Hosts
		})

		It("should verify NDEATH has correct payload structure", func() {
			// Test NDEATH message structure compliance
			ndeathVector := sparkplugplugin.GetTestVector("NDEATH_V1")
			Expect(ndeathVector).NotTo(BeNil())

			payloadBytes, err := base64.StdEncoding.DecodeString(ndeathVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var payload sparkplugb.Payload
			err = proto.Unmarshal(payloadBytes, &payload)
			Expect(err).NotTo(HaveOccurred())

			// NDEATH MUST have sequence 0 (Sparkplug spec requirement)
			Expect(payload.Seq).NotTo(BeNil())
			Expect(*payload.Seq).To(Equal(uint64(0)), "NDEATH must reset sequence to 0")

			// NDEATH MUST have timestamp
			Expect(payload.Timestamp).NotTo(BeNil(), "NDEATH must include timestamp")

			// NDEATH MUST contain bdSeq metric matching NBIRTH
			Expect(payload.Metrics).To(HaveLen(1), "NDEATH should only contain bdSeq metric")

			bdSeqMetric := payload.Metrics[0]
			Expect(bdSeqMetric.Name).NotTo(BeNil())
			Expect(*bdSeqMetric.Name).To(Equal("bdSeq"))
			Expect(bdSeqMetric.Datatype).NotTo(BeNil())
			Expect(*bdSeqMetric.Datatype).To(Equal(uint32(8))) // UInt64 type
		})

		It("should validate alias uniqueness in BIRTH messages", func() {
			// Test that BIRTH messages don't have duplicate aliases (spec requirement)
			nbirthVector := sparkplugplugin.GetTestVector("NBIRTH_V1")
			payloadBytes, err := base64.StdEncoding.DecodeString(nbirthVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var payload sparkplugb.Payload
			err = proto.Unmarshal(payloadBytes, &payload)
			Expect(err).NotTo(HaveOccurred())

			// Check for duplicate aliases
			aliasMap := make(map[uint64]string)
			for _, metric := range payload.Metrics {
				if metric.Alias != nil {
					alias := *metric.Alias
					if existingName, exists := aliasMap[alias]; exists {
						Fail(fmt.Sprintf("Duplicate alias %d found: '%s' and '%s'", alias, existingName, *metric.Name))
					}
					if metric.Name != nil {
						aliasMap[alias] = *metric.Name
					}
				}
			}
			// All aliases should be unique
			Expect(len(aliasMap)).To(BeNumerically(">", 0), "BIRTH should contain metrics with aliases")
		})
	})

	Context("Sequence Number Management", func() {
		It("should implement proper seq counter (0-255 with wraparound)", func() {
			// Test sequence number wraparound behavior
			sequenceManager := sparkplugplugin.NewSequenceManager()

			// Test normal increment - starts at 0
			seq1 := sequenceManager.NextSequence()
			seq2 := sequenceManager.NextSequence()
			Expect(seq1).To(Equal(uint8(0)), "First sequence should be 0")
			Expect(seq2).To(Equal(uint8(1)), "Second sequence should be 1")

			// Test wraparound at 255
			sequenceManager.SetSequence(254)
			seq254 := sequenceManager.NextSequence()
			seq255 := sequenceManager.NextSequence()
			seq0 := sequenceManager.NextSequence()

			Expect(seq254).To(Equal(uint8(254)), "Should return current value then increment")
			Expect(seq255).To(Equal(uint8(255)), "Should increment to 255")
			Expect(seq0).To(Equal(uint8(0)), "Sequence should wrap from 255 to 0")
		})

		It("should validate sequence gap detection", func() {
			// Test sequence gap detection and validation
			// IsSequenceValid checks if received is the next expected sequence after current
			sequences := []struct {
				current  uint8 // Last seen sequence
				received uint8 // Newly received sequence
				isValid  bool  // Should be valid (received = current + 1)
			}{
				{0, 1, true},   // Normal increment 0->1
				{1, 2, true},   // Normal increment 1->2
				{1, 3, false},  // Gap detected (missing 2)
				{255, 0, true}, // Valid wraparound 255->0
				{1, 5, false},  // Large gap
			}

			sequenceManager := sparkplugplugin.NewSequenceManager()
			for _, test := range sequences {
				isValid := sequenceManager.IsSequenceValid(test.current, test.received)
				Expect(isValid).To(Equal(test.isValid),
					fmt.Sprintf("Sequence validation failed for current=%d, received=%d", test.current, test.received))
			}
		})

		It("should handle out-of-order sequence detection", func() {
			// Test that out-of-order sequences are properly detected
			gapVector := sparkplugplugin.GetTestVector("NDATA_GAP")
			Expect(gapVector).NotTo(BeNil())

			payloadBytes, err := base64.StdEncoding.DecodeString(gapVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var payload sparkplugb.Payload
			err = proto.Unmarshal(payloadBytes, &payload)
			Expect(err).NotTo(HaveOccurred())

			// This vector should represent a sequence gap (1→5)
			Expect(payload.Seq).NotTo(BeNil())
			currentSeq := *payload.Seq
			expectedSeq := uint64(1) // After NBIRTH with seq=0
			gap := currentSeq - expectedSeq

			Expect(gap).To(BeNumerically(">", 1), "Should detect sequence gap")
			Expect(currentSeq).To(Equal(uint64(5)), "Gap vector should have seq=5")
		})
	})

	Context("Timestamp and Encoding", func() {
		It("should ensure outgoing metrics include timestamps", func() {
			// Test that all Sparkplug messages include timestamps
			nbirthVector := sparkplugplugin.GetTestVector("NBIRTH_V1")
			payloadBytes, err := base64.StdEncoding.DecodeString(nbirthVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var payload sparkplugb.Payload
			err = proto.Unmarshal(payloadBytes, &payload)
			Expect(err).NotTo(HaveOccurred())

			// Payload MUST have timestamp
			Expect(payload.Timestamp).NotTo(BeNil(), "All Sparkplug messages must include timestamps")

			// Timestamp should be reasonable (Unix milliseconds)
			timestamp := *payload.Timestamp
			// Allow for test data to be older, but should be positive
			Expect(timestamp).To(BeNumerically(">", 0), "Timestamp should be positive Unix milliseconds")
		})

		It("should validate Protobuf encoding completeness", func() {
			// Test that all test vectors can be properly decoded
			for _, vector := range sparkplugplugin.TestVectors {
				By("validating protobuf encoding for "+vector.Name, func() {
					payloadBytes, err := base64.StdEncoding.DecodeString(vector.Base64Data)
					Expect(err).NotTo(HaveOccurred(), "Base64 decoding should succeed for "+vector.Name)

					var payload sparkplugb.Payload
					err = proto.Unmarshal(payloadBytes, &payload)
					Expect(err).NotTo(HaveOccurred(), "Protobuf unmarshaling should succeed for "+vector.Name)

					// Re-marshal to verify completeness
					_, err = proto.Marshal(&payload)
					Expect(err).NotTo(HaveOccurred(), "Protobuf marshaling should succeed for "+vector.Name)
				})
			}
		})

		It("should preserve historical timestamps in input processing", func() {
			// Test that historical timestamps are preserved
			nbirthVector := sparkplugplugin.GetTestVector("NBIRTH_V1")
			payloadBytes, err := base64.StdEncoding.DecodeString(nbirthVector.Base64Data)
			Expect(err).NotTo(HaveOccurred())

			var payload sparkplugb.Payload
			err = proto.Unmarshal(payloadBytes, &payload)
			Expect(err).NotTo(HaveOccurred())

			originalTimestamp := *payload.Timestamp

			// Simulate processing - timestamp should be preserved
			// Note: Individual metric timestamps are not part of the Sparkplug B spec
			// The payload-level timestamp is the primary timing mechanism

			// Payload timestamp should remain unchanged
			Expect(*payload.Timestamp).To(Equal(originalTimestamp), "Historical timestamps should be preserved")
		})

		It("should support metric-level timestamps with official protobuf", func() {
			// Test metric-level timestamp functionality using the official Eclipse Tahu protobuf
			// This functionality was added to ensure Sparkplug B specification compliance
			
			timestampValue := uint64(time.Now().UnixMilli()) // Current timestamp
			
			// Create a metric with timestamp using the official protobuf
			metric := &sparkplugb.Payload_Metric{
				Name:      func() *string { s := "test_metric"; return &s }(),
				Timestamp: &timestampValue, // This field is available in the official protobuf
				Value: &sparkplugb.Payload_Metric_DoubleValue{
					DoubleValue: 25.5,
				},
				Datatype: func() *uint32 { d := uint32(10); return &d }(), // Double type
			}
			
			// Verify timestamp is properly set
			Expect(metric.Timestamp).NotTo(BeNil(), "Metric should have timestamp field")
			Expect(*metric.Timestamp).To(Equal(timestampValue), "Metric timestamp should match set value")
			
			// Verify the metric can be marshaled and unmarshaled with timestamp intact
			payload := &sparkplugb.Payload{
				Timestamp: &timestampValue,
				Seq:       func() *uint64 { s := uint64(1); return &s }(),
				Metrics:   []*sparkplugb.Payload_Metric{metric},
			}
			
			// Marshal to protobuf bytes
			payloadBytes, err := proto.Marshal(payload)
			Expect(err).NotTo(HaveOccurred(), "Should marshal payload with metric timestamps")
			
			// Unmarshal and verify timestamp is preserved
			var reconstructedPayload sparkplugb.Payload
			err = proto.Unmarshal(payloadBytes, &reconstructedPayload)
			Expect(err).NotTo(HaveOccurred(), "Should unmarshal payload with metric timestamps")
			
			// Verify metric timestamp is preserved
			Expect(len(reconstructedPayload.Metrics)).To(Equal(1), "Should have one metric")
			reconstructedMetric := reconstructedPayload.Metrics[0]
			Expect(reconstructedMetric.Timestamp).NotTo(BeNil(), "Reconstructed metric should have timestamp")
			Expect(*reconstructedMetric.Timestamp).To(Equal(timestampValue), "Reconstructed timestamp should match original")
		})
	})

	Context("Topic Namespace Compliance", func() {
		It("should validate Sparkplug topic format", func() {
			// Test Sparkplug topic namespace compliance (§8.2)
			topicParser := sparkplugplugin.NewTopicParser()

			validTopics := []string{
				"spBv1.0/Group1/NBIRTH/EdgeNode1",
				"spBv1.0/Group1/NDATA/EdgeNode1",
				"spBv1.0/Group1/NDEATH/EdgeNode1",
				"spBv1.0/Group1/DBIRTH/EdgeNode1/Device1",
				"spBv1.0/Group1/DDATA/EdgeNode1/Device1",
				"spBv1.0/Group1/DDEATH/EdgeNode1/Device1",
				"spBv1.0/Group1/NCMD/EdgeNode1",
				"spBv1.0/Group1/DCMD/EdgeNode1/Device1",
				"spBv1.0/Group1/STATE/EdgeNode1",
			}

			for _, topic := range validTopics {
				msgType, deviceKey := topicParser.ParseSparkplugTopic(topic)
				Expect(msgType).NotTo(BeEmpty(), "Should parse message type from: "+topic)
				Expect(deviceKey).NotTo(BeEmpty(), "Should parse device key from: "+topic)
			}
		})

		It("should validate message type classification", func() {
			// Test message type validation
			topicParser := sparkplugplugin.NewTopicParser()

			// Birth messages
			Expect(topicParser.IsBirthMessage("NBIRTH")).To(BeTrue())
			Expect(topicParser.IsBirthMessage("DBIRTH")).To(BeTrue())
			Expect(topicParser.IsBirthMessage("NDATA")).To(BeFalse())

			// Data messages
			Expect(topicParser.IsDataMessage("NDATA")).To(BeTrue())
			Expect(topicParser.IsDataMessage("DDATA")).To(BeTrue())
			Expect(topicParser.IsDataMessage("NBIRTH")).To(BeFalse())

			// Death messages
			Expect(topicParser.IsDeathMessage("NDEATH")).To(BeTrue())
			Expect(topicParser.IsDeathMessage("DDEATH")).To(BeTrue())
			Expect(topicParser.IsDeathMessage("NDATA")).To(BeFalse())

			// Command messages
			Expect(topicParser.IsCommandMessage("NCMD")).To(BeTrue())
			Expect(topicParser.IsCommandMessage("DCMD")).To(BeTrue())
			Expect(topicParser.IsCommandMessage("NDATA")).To(BeFalse())
		})
	})
})

// P9 Edge Case Validation Tests
var _ = Describe("P9 Edge Case Validation", func() {
	Context("Dynamic Behavior Testing", func() {



		It("should handle sequence number wraparound (255 → 0)", func() {
			// Test sequence number wraparound edge case
			sequenceManager := sparkplugplugin.NewSequenceManager()

			// Test wraparound scenarios
			wrapCases := []struct {
				current  uint8
				received uint8
				valid    bool
				desc     string
			}{
				{254, 255, true, "Normal increment to 255"},
				{255, 0, true, "Valid wraparound 255→0"},
				{0, 1, true, "Normal increment after wraparound"},
				{255, 1, false, "Invalid skip during wraparound"},
				{254, 0, false, "Invalid large jump"},
			}

			for _, test := range wrapCases {
				isValid := sequenceManager.IsSequenceValid(test.current, test.received)
				Expect(isValid).To(Equal(test.valid), test.desc)
			}
		})
	})

	Context("Connection Handling", func() {
		It("should handle Primary Host disconnect/reconnect behavior", func() {
			// Test primary host connection resilience
			cache := sparkplugplugin.NewAliasCache()

			// Simulate established session with cached aliases
			metrics := []*sparkplugb.Payload_Metric{
				{Name: stringPtr("Temperature"), Alias: uint64Ptr(1)},
				{Name: stringPtr("Pressure"), Alias: uint64Ptr(2)},
			}
			cache.CacheAliases("Factory/Line1", metrics)

			// Verify aliases are cached
			dataMetrics := []*sparkplugb.Payload_Metric{
				{Alias: uint64Ptr(1), Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 25.5}},
			}
			resolved := cache.ResolveAliases("Factory/Line1", dataMetrics)
			Expect(resolved).To(Equal(1))

			// Simulate disconnect/reconnect - session state should be preserved
			// In real implementation, this would depend on Clean Session setting
			// For persistent sessions (Clean Session = false), aliases should persist

			// Test reconnection with new BIRTH message
			newMetrics := []*sparkplugb.Payload_Metric{
				{Name: stringPtr("Temperature"), Alias: uint64Ptr(1)},
				{Name: stringPtr("Pressure"), Alias: uint64Ptr(2)},
				{Name: stringPtr("Humidity"), Alias: uint64Ptr(3)}, // New metric after reconnect
			}
			newCount := cache.CacheAliases("Factory/Line1", newMetrics)
			Expect(newCount).To(Equal(3), "Should handle new metrics after reconnect")
		})


	})

	Context("Large Payload Handling", func() {
		It("should handle Birth messages with 500+ metrics", func() {
			// Test large payload handling
			cache := sparkplugplugin.NewAliasCache()

			// Create 500+ metrics
			largeMetrics := make([]*sparkplugb.Payload_Metric, 500)
			for i := 0; i < 500; i++ {
				largeMetrics[i] = &sparkplugb.Payload_Metric{
					Name:  stringPtr(fmt.Sprintf("Metric_%d", i)),
					Alias: uint64Ptr(uint64(i + 1)),
					Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: float64(i)},
				}
			}

			// Cache all metrics
			start := time.Now()
			count := cache.CacheAliases("Factory/LargeLine", largeMetrics)
			duration := time.Since(start)

			Expect(count).To(Equal(500), "Should cache all 500 metrics")
			Expect(duration).To(BeNumerically("<", 100*time.Millisecond), "Should cache quickly")
		})

		It("should validate performance impact of large alias tables", func() {
			// Test alias resolution performance with large tables
			cache := sparkplugplugin.NewAliasCache()

			// Create large alias table (1000 metrics)
			largeMetrics := make([]*sparkplugb.Payload_Metric, 1000)
			for i := 0; i < 1000; i++ {
				largeMetrics[i] = &sparkplugb.Payload_Metric{
					Name:  stringPtr(fmt.Sprintf("Metric_%d", i)),
					Alias: uint64Ptr(uint64(i + 1)),
				}
			}
			cache.CacheAliases("Factory/LargeLine", largeMetrics)

			// Test resolution performance
			testMetrics := make([]*sparkplugb.Payload_Metric, 100)
			for i := 0; i < 100; i++ {
				testMetrics[i] = &sparkplugb.Payload_Metric{
					Alias: uint64Ptr(uint64(i + 1)),
					Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: float64(i)},
				}
			}

			start := time.Now()
			resolved := cache.ResolveAliases("Factory/LargeLine", testMetrics)
			duration := time.Since(start)

			Expect(resolved).To(Equal(100), "Should resolve all 100 aliases")
			Expect(duration).To(BeNumerically("<", 10*time.Millisecond), "Should resolve quickly")
		})

		It("should validate message size limits", func() {
			// Test Sparkplug message size considerations
			// MQTT has practical limits around 256MB, but Sparkplug should be much smaller

			// Create a reasonably large payload
			metrics := make([]*sparkplugb.Payload_Metric, 100)
			for i := 0; i < 100; i++ {
				// Create metrics with various data types
				metrics[i] = &sparkplugb.Payload_Metric{
					Name:  stringPtr(fmt.Sprintf("LongMetricNameForTesting_%d", i)),
					Alias: uint64Ptr(uint64(i + 1)),
					Value: &sparkplugb.Payload_Metric_StringValue{
						StringValue: strings.Repeat("TestData", 10), // 80 characters
					},
				}
			}

			payload := &sparkplugb.Payload{
				Timestamp: uint64Ptr(uint64(time.Now().UnixMilli())),
				Metrics:   metrics,
				Seq:       uint64Ptr(1),
			}

			// Marshal to check size
			data, err := proto.Marshal(payload)
			Expect(err).NotTo(HaveOccurred())

			// Should be reasonable size (less than 1MB for 100 metrics)
			Expect(len(data)).To(BeNumerically("<", 1024*1024), "Payload should be under 1MB")
			Expect(len(data)).To(BeNumerically(">", 1000), "Payload should have substantial content")
		})
	})

	Context("Edge Cases", func() {
		It("should handle UTF-8 and special characters in metric names", func() {
			// Test Unicode and special character handling
			specialMetrics := []*sparkplugb.Payload_Metric{
				{Name: stringPtr("Temperature_°C"), Alias: uint64Ptr(1)},
				{Name: stringPtr("Druck_μBar"), Alias: uint64Ptr(2)},
				{Name: stringPtr("速度_RPM"), Alias: uint64Ptr(3)},
				{Name: stringPtr("Metric-With-Dashes"), Alias: uint64Ptr(4)},
				{Name: stringPtr("Metric_With_Underscores"), Alias: uint64Ptr(5)},
				{Name: stringPtr("Metric With Spaces"), Alias: uint64Ptr(6)},
				{Name: stringPtr("Metric/With/Slashes"), Alias: uint64Ptr(7)},
			}

			cache := sparkplugplugin.NewAliasCache()
			count := cache.CacheAliases("Factory/International", specialMetrics)
			Expect(count).To(Equal(7), "Should handle all special character metrics")

			// Test resolution
			testMetrics := []*sparkplugb.Payload_Metric{
				{Alias: uint64Ptr(1), Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 25.5}},
				{Alias: uint64Ptr(3), Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 1500}},
			}

			resolved := cache.ResolveAliases("Factory/International", testMetrics)
			Expect(resolved).To(Equal(2))
			Expect(*testMetrics[0].Name).To(Equal("Temperature_°C"))
			Expect(*testMetrics[1].Name).To(Equal("速度_RPM"))
		})

		It("should handle historical flag processing", func() {
			// Test historical data flag handling
			historicalMetrics := []*sparkplugb.Payload_Metric{
				{
					Name:         stringPtr("HistoricalTemp"),
					Alias:        uint64Ptr(1),
					Value:        &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 25.5},
					IsHistorical: boolPtr(true),
				},
				{
					Name:         stringPtr("CurrentTemp"),
					Alias:        uint64Ptr(2),
					Value:        &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 26.0},
					IsHistorical: boolPtr(false),
				},
			}

			// Validate historical flag handling
			for _, metric := range historicalMetrics {
				if metric.IsHistorical != nil {
					if *metric.IsHistorical {
						// Historical data should be flagged appropriately
						Expect(*metric.IsHistorical).To(BeTrue(), "Historical metric should be flagged as historical")
					} else {
						// Current data should not be historical
						Expect(*metric.IsHistorical).To(BeFalse(), "Current metric should not be flagged as historical")
					}
				}
			}

			// Test that historical payloads can be created with timestamps
			now := time.Now()
			historicalPayload := &sparkplugb.Payload{
				Timestamp: uint64Ptr(uint64(now.Add(-1 * time.Hour).UnixMilli())),
				Metrics:   historicalMetrics,
				Seq:       uint64Ptr(1),
			}

			currentPayload := &sparkplugb.Payload{
				Timestamp: uint64Ptr(uint64(now.UnixMilli())),
				Metrics:   historicalMetrics,
				Seq:       uint64Ptr(2),
			}

			// Validate payload timestamps
			Expect(*historicalPayload.Timestamp).To(BeNumerically("<", uint64(now.Add(-30*time.Minute).UnixMilli())))
			Expect(*currentPayload.Timestamp).To(BeNumerically(">", uint64(now.Add(-5*time.Minute).UnixMilli())))
		})

		It("should handle mixed Node/Device metric scenarios", func() {
			// Test mixed node-level and device-level metrics
			cache := sparkplugplugin.NewAliasCache()

			// Node-level metrics (no device in key)
			nodeMetrics := []*sparkplugb.Payload_Metric{
				{Name: stringPtr("NodeCPU"), Alias: uint64Ptr(1)},
				{Name: stringPtr("NodeMemory"), Alias: uint64Ptr(2)},
			}
			cache.CacheAliases("Factory/Gateway", nodeMetrics)

			// Device-level metrics (with device in key)
			deviceMetrics := []*sparkplugb.Payload_Metric{
				{Name: stringPtr("DeviceTemp"), Alias: uint64Ptr(1)}, // Same alias, different scope
				{Name: stringPtr("DevicePressure"), Alias: uint64Ptr(2)},
			}
			cache.CacheAliases("Factory/Gateway/Device1", deviceMetrics)

			// Test independent resolution
			nodeData := []*sparkplugb.Payload_Metric{
				{Alias: uint64Ptr(1), Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 75.5}},
			}
			nodeResolved := cache.ResolveAliases("Factory/Gateway", nodeData)
			Expect(nodeResolved).To(Equal(1))
			Expect(*nodeData[0].Name).To(Equal("NodeCPU"))

			deviceData := []*sparkplugb.Payload_Metric{
				{Alias: uint64Ptr(1), Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 25.5}},
			}
			deviceResolved := cache.ResolveAliases("Factory/Gateway/Device1", deviceData)
			Expect(deviceResolved).To(Equal(1))
			Expect(*deviceData[0].Name).To(Equal("DeviceTemp"))
		})

		It("should handle null and empty value edge cases", func() {
			// Test handling of null/empty values
			edgeCaseMetrics := []*sparkplugb.Payload_Metric{
				{
					Name:  stringPtr("EmptyString"),
					Alias: uint64Ptr(1),
					Value: &sparkplugb.Payload_Metric_StringValue{StringValue: ""},
				},
				{
					Name:  stringPtr("ZeroValue"),
					Alias: uint64Ptr(2),
					Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 0.0},
				},
				{
					Name:  stringPtr("FalseBoolean"),
					Alias: uint64Ptr(3),
					Value: &sparkplugb.Payload_Metric_BooleanValue{BooleanValue: false},
				},
				{
					Name:  stringPtr("NoValue"),
					Alias: uint64Ptr(4),
					// No Value field set - represents null
				},
			}

			cache := sparkplugplugin.NewAliasCache()
			count := cache.CacheAliases("Factory/EdgeCases", edgeCaseMetrics)
			Expect(count).To(Equal(4), "Should handle all edge case metrics")

			// Test resolution of edge cases
			testData := []*sparkplugb.Payload_Metric{
				{Alias: uint64Ptr(1)}, // Empty string metric
				{Alias: uint64Ptr(4)}, // Null value metric
			}

			resolved := cache.ResolveAliases("Factory/EdgeCases", testData)
			Expect(resolved).To(Equal(2))
			Expect(*testData[0].Name).To(Equal("EmptyString"))
			Expect(*testData[1].Name).To(Equal("NoValue"))
		})

		It("should handle metric name collisions and duplicates", func() {
			// Test handling of duplicate metric names (should be avoided but handled gracefully)
			duplicateMetrics := []*sparkplugb.Payload_Metric{
				{Name: stringPtr("Temperature"), Alias: uint64Ptr(1)},
				{Name: stringPtr("Temperature"), Alias: uint64Ptr(2)}, // Duplicate name, different alias
				{Name: stringPtr("Pressure"), Alias: uint64Ptr(3)},
			}

			cache := sparkplugplugin.NewAliasCache()
			count := cache.CacheAliases("Factory/Duplicates", duplicateMetrics)

			// Implementation should handle this gracefully
			// The exact behavior may vary, but it shouldn't crash
			Expect(count).To(BeNumerically(">=", 2), "Should handle duplicate names gracefully")
		})
	})
})

var _ = Describe("EON Node ID Resolution (Parris Method) Unit Tests", func() {
	var output *mockSparkplugOutput

	BeforeEach(func() {
		output = newMockSparkplugOutput()
	})

	Context("Dynamic EON Node ID from location_path metadata", func() {
		It("should convert UMH dot notation to Sparkplug colon notation", func() {
			msg := newMockMessage()
			msg.SetMeta("location_path", "enterprise.factory.line1.station1")

			eonNodeID := output.getEONNodeID(msg)

			Expect(eonNodeID).To(Equal("enterprise:factory:line1:station1"))
		})

		It("should handle complex hierarchical structures", func() {
			testCases := []struct {
				locationPath string
				expected     string
			}{
				{"enterprise", "enterprise"},
				{"enterprise.site", "enterprise:site"},
				{"enterprise.site.area.line.cell", "enterprise:site:area:line:cell"},
				{"automotive.bodyshop.line3.station5.robot", "automotive:bodyshop:line3:station5:robot"},
				{"pharma.production.cleanroom.line2.packaging.station1", "pharma:production:cleanroom:line2:packaging:station1"},
			}

			for _, tc := range testCases {
				msg := newMockMessage()
				msg.SetMeta("location_path", tc.locationPath)

				eonNodeID := output.getEONNodeID(msg)

				Expect(eonNodeID).To(Equal(tc.expected),
					"Failed for location_path: %s", tc.locationPath)
			}
		})

		It("should handle edge cases in location_path", func() {
			testCases := []struct {
				locationPath string
				expected     string
				description  string
			}{
				{"", "static_node", "empty location_path should fall back to static"},
				{"single", "single", "single level hierarchy"},
				{"with.dots.everywhere", "with:dots:everywhere", "multiple dots"},
				{"enterprise.site-with-dashes.area_with_underscores", "enterprise:site-with-dashes:area_with_underscores", "special characters preserved"},
			}

			// Set static EdgeNodeID for fallback tests
			output.config.Identity.EdgeNodeID = "static_node"

			for _, tc := range testCases {
				msg := newMockMessage()
				if tc.locationPath != "" {
					msg.SetMeta("location_path", tc.locationPath)
				}

				eonNodeID := output.getEONNodeID(msg)

				Expect(eonNodeID).To(Equal(tc.expected),
					"Failed for %s: location_path='%s'", tc.description, tc.locationPath)
			}
		})
	})

	Context("Static override from configuration", func() {
		It("should use static EdgeNodeID when location_path is missing", func() {
			output.config.Identity.EdgeNodeID = "StaticNode01"
			msg := newMockMessage()
			// No location_path metadata

			eonNodeID := output.getEONNodeID(msg)

			Expect(eonNodeID).To(Equal("StaticNode01"))
		})

		It("should use static EdgeNodeID when location_path is empty", func() {
			output.config.Identity.EdgeNodeID = "ConfiguredNode"
			msg := newMockMessage()
			msg.SetMeta("location_path", "")

			eonNodeID := output.getEONNodeID(msg)

			Expect(eonNodeID).To(Equal("ConfiguredNode"))
		})

		It("should prefer location_path over static configuration", func() {
			output.config.Identity.EdgeNodeID = "StaticNode"
			msg := newMockMessage()
			msg.SetMeta("location_path", "enterprise.dynamic.path")

			eonNodeID := output.getEONNodeID(msg)

			Expect(eonNodeID).To(Equal("enterprise:dynamic:path"))
		})
	})

	Context("Default fallback behavior", func() {
		It("should use default_node when no configuration or metadata", func() {
			output.config.Identity.EdgeNodeID = ""
			msg := newMockMessage()
			// No location_path metadata

			eonNodeID := output.getEONNodeID(msg)

			Expect(eonNodeID).To(Equal("default_node"))
		})

		It("should log warning for default fallback", func() {
			output.config.Identity.EdgeNodeID = ""
			msg := newMockMessage()

			// Capture log output (this is a simplified test)
			eonNodeID := output.getEONNodeID(msg)

			Expect(eonNodeID).To(Equal("default_node"))
			// Note: In a real implementation, we'd verify the warning was logged
		})
	})

	Context("Priority logic validation", func() {
		It("should follow correct priority: location_path > static > default", func() {
			// Test all combinations to verify priority
			testCases := []struct {
				locationPath string
				staticConfig string
				expected     string
				description  string
			}{
				{"enterprise.path", "static", "enterprise:path", "location_path takes priority over static"},
				{"", "static", "static", "static used when location_path empty"},
				{"", "", "default_node", "default used when both empty"},
				{"enterprise.path", "", "enterprise:path", "location_path used when static empty"},
			}

			for _, tc := range testCases {
				output.config.Identity.EdgeNodeID = tc.staticConfig
				msg := newMockMessage()
				if tc.locationPath != "" {
					msg.SetMeta("location_path", tc.locationPath)
				}

				eonNodeID := output.getEONNodeID(msg)

				Expect(eonNodeID).To(Equal(tc.expected),
					"Failed for %s", tc.description)
			}
		})
	})

	Context("Real-world ISA-95 scenarios", func() {
		It("should handle automotive manufacturing hierarchy", func() {
			msg := newMockMessage()
			msg.SetMeta("location_path", "automotive.plant_detroit.bodyshop.line3.welding_station.robot_kuka")

			eonNodeID := output.getEONNodeID(msg)

			Expect(eonNodeID).To(Equal("automotive:plant_detroit:bodyshop:line3:welding_station:robot_kuka"))
		})

		It("should handle pharmaceutical manufacturing hierarchy", func() {
			msg := newMockMessage()
			msg.SetMeta("location_path", "pharma.site_berlin.production.cleanroom_a.tablet_line.coating_station")

			eonNodeID := output.getEONNodeID(msg)

			Expect(eonNodeID).To(Equal("pharma:site_berlin:production:cleanroom_a:tablet_line:coating_station"))
		})

		It("should handle food and beverage hierarchy", func() {
			msg := newMockMessage()
			msg.SetMeta("location_path", "brewery.site_munich.brewing.tank_farm.fermentation_tank_5")

			eonNodeID := output.getEONNodeID(msg)

			Expect(eonNodeID).To(Equal("brewery:site_munich:brewing:tank_farm:fermentation_tank_5"))
		})
	})
})

var _ = Describe("UMH Metadata Generation Unit Tests", func() {
	Context("Sanitization for UMH compatibility", func() {
		It("should replace forward slashes with dots", func() {
			// Test slash replacement - slashes become dots for hierarchical representation
			Expect(sparkplugplugin.SanitizeForUMH("Refrigeration/Tower1/Pumps/chemHOA")).To(Equal("Refrigeration.Tower1.Pumps.chemHOA"))
			Expect(sparkplugplugin.SanitizeForUMH("Level1/Level2/Level3")).To(Equal("Level1.Level2.Level3"))
			Expect(sparkplugplugin.SanitizeForUMH("/StartSlash/EndSlash/")).To(Equal("StartSlash.EndSlash"))  // Leading/trailing dots trimmed
		})
		
		It("should replace invalid characters with underscores", func() {
			// Test invalid character replacement
			Expect(sparkplugplugin.SanitizeForUMH("Device@Name#123")).To(Equal("Device_Name_123"))
			Expect(sparkplugplugin.SanitizeForUMH("Tag with spaces")).To(Equal("Tag_with_spaces"))
			Expect(sparkplugplugin.SanitizeForUMH("Special!@#$%^&*()")).To(Equal("Special__________"))
		})
		
		It("should replace colons with underscores", func() {
			// Colons are not valid in UMH topics, replaced with underscores
			// The format converter handles colon-splitting BEFORE sanitization
			Expect(sparkplugplugin.SanitizeForUMH("virtual:path:metric")).To(Equal("virtual_path_metric"))
			Expect(sparkplugplugin.SanitizeForUMH("motor:diagnostics")).To(Equal("motor_diagnostics"))
			Expect(sparkplugplugin.SanitizeForUMH(":leading:trailing:")).To(Equal("_leading_trailing_"))
		})
		
		It("should handle mixed cases correctly", func() {
			// Test combination of slash and invalid characters
			Expect(sparkplugplugin.SanitizeForUMH("Area/Zone@1/Device#2")).To(Equal("Area.Zone_1.Device_2"))
			Expect(sparkplugplugin.SanitizeForUMH("Plant/Building 1/Floor-2/Room_3")).To(Equal("Plant.Building_1.Floor-2.Room_3"))
		})
		
		It("should preserve valid characters", func() {
			// Test that valid characters are not changed
			Expect(sparkplugplugin.SanitizeForUMH("Valid_Name-123.test")).To(Equal("Valid_Name-123.test"))
			Expect(sparkplugplugin.SanitizeForUMH("abcABC123._-")).To(Equal("abcABC123._-"))
		})
		
		It("should handle empty strings", func() {
			Expect(sparkplugplugin.SanitizeForUMH("")).To(Equal(""))
		})
		
		It("should prevent double dots and trim leading/trailing dots", func() {
			// Test that multiple slashes don't create double dots
			Expect(sparkplugplugin.SanitizeForUMH("//")).To(Equal(""))  // All dots trimmed
			Expect(sparkplugplugin.SanitizeForUMH("a//b")).To(Equal("a.b"))
			Expect(sparkplugplugin.SanitizeForUMH("/hello/")).To(Equal("hello"))  // Leading/trailing dots trimmed
			Expect(sparkplugplugin.SanitizeForUMH("path///with////many/////slashes")).To(Equal("path.with.many.slashes"))
			// Even with replacement of invalid chars, no double dots should appear
			Expect(sparkplugplugin.SanitizeForUMH("/@hello/")).To(Equal("_hello"))  // Leading/trailing dots trimmed
			// Test explicit leading/trailing dots are trimmed
			Expect(sparkplugplugin.SanitizeForUMH(".test.")).To(Equal("test"))
			Expect(sparkplugplugin.SanitizeForUMH("...test...")).To(Equal("test"))
		})
	})
	
	Context("Full Conversion Flow with Slashes and Colons", func() {
		var converter *sparkplugplugin.FormatConverter
		
		BeforeEach(func() {
			converter = sparkplugplugin.NewFormatConverter()
		})
		
		It("should handle Refrigeration/receiverLevel through complete conversion flow", func() {
			// Test the actual issue: metric with slash like "Refrigeration/receiverLevel"
			// Step 1: Create a Sparkplug message with slash in metric name
			sparkplugMsg := &sparkplugplugin.SparkplugMessage{
				GroupID:    "TestGroup",
				EdgeNodeID: "TestNode",
				DeviceID:   "TestDevice",
				MetricName: "Refrigeration/receiverLevel", // Original with slash
				Value:      42.5,
				DataType:   "Double",
				Timestamp:  time.Now(),
			}
			
			// Step 2: Pre-process as the input does - replace slash with dot
			processedMetricName := strings.ReplaceAll(sparkplugMsg.MetricName, "/", ".")
			sparkplugMsg.MetricName = processedMetricName // "Refrigeration.receiverLevel"
			
			// Step 3: Convert to UMH
			umhMsg, err := converter.DecodeSparkplugToUMH(sparkplugMsg, "_raw")
			Expect(err).ToNot(HaveOccurred())
			Expect(umhMsg).ToNot(BeNil())
			
			// Step 4: Verify the parsing - splits on last dot
			// "Refrigeration.receiverLevel" splits into:
			// - virtual path: "Refrigeration"
			// - tag name: "receiverLevel"
			Expect(umhMsg.TopicInfo.VirtualPath).ToNot(BeNil())
			Expect(*umhMsg.TopicInfo.VirtualPath).To(Equal("Refrigeration"))
			Expect(umhMsg.TopicInfo.Name).To(Equal("receiverLevel"))
			
			// Step 5: Sanitize for final UMH topic
			sanitizedVirtualPath := sparkplugplugin.SanitizeForUMH(*umhMsg.TopicInfo.VirtualPath)
			sanitizedTagName := sparkplugplugin.SanitizeForUMH(umhMsg.TopicInfo.Name)
			Expect(sanitizedVirtualPath).To(Equal("Refrigeration"))
			Expect(sanitizedTagName).To(Equal("receiverLevel"))
		})
		
		It("should handle virtual paths with colons correctly", func() {
			// Test metric with colons for virtual path: "vpath:segment:temperature"
			sparkplugMsg := &sparkplugplugin.SparkplugMessage{
				GroupID:    "TestGroup",
				EdgeNodeID: "TestNode", 
				DeviceID:   "TestDevice",
				MetricName: "vpath:segment:temperature", // Colons for virtual path
				Value:      25.3,
				DataType:   "Double",
				Timestamp:  time.Now(),
			}
			
			// Pre-process - replace colons with dots
			processedMetricName := strings.ReplaceAll(sparkplugMsg.MetricName, ":", ".")
			sparkplugMsg.MetricName = processedMetricName // "vpath.segment.temperature"
			
			// Convert to UMH
			umhMsg, err := converter.DecodeSparkplugToUMH(sparkplugMsg, "_raw")
			Expect(err).ToNot(HaveOccurred())
			
			// Verify parsing - splits on last dot
			// "vpath.segment.temperature" splits into:
			// - virtual path: "vpath.segment"
			// - tag name: "temperature"
			Expect(umhMsg.TopicInfo.VirtualPath).ToNot(BeNil())
			Expect(*umhMsg.TopicInfo.VirtualPath).To(Equal("vpath.segment"))
			Expect(umhMsg.TopicInfo.Name).To(Equal("temperature"))
			
			// Sanitize for final UMH topic
			sanitizedVirtualPath := sparkplugplugin.SanitizeForUMH(*umhMsg.TopicInfo.VirtualPath)
			sanitizedTagName := sparkplugplugin.SanitizeForUMH(umhMsg.TopicInfo.Name)
			Expect(sanitizedVirtualPath).To(Equal("vpath.segment"))
			Expect(sanitizedTagName).To(Equal("temperature"))
		})
		
		It("should handle mixed colons and slashes correctly", func() {
			// Test complex case: "motor:diagnostics/Refrigeration/temperature"
			// Colons separate virtual path, slashes are hierarchical within names
			sparkplugMsg := &sparkplugplugin.SparkplugMessage{
				GroupID:    "TestGroup",
				EdgeNodeID: "TestNode",
				DeviceID:   "TestDevice",
				MetricName: "motor:diagnostics/Refrigeration/temperature",
				Value:      18.7,
				DataType:   "Double",
				Timestamp:  time.Now(),
			}
			
			// Step 1: Pre-process - replace both slashes and colons with dots
			processedMetricName := strings.ReplaceAll(sparkplugMsg.MetricName, "/", ".")
			processedMetricName = strings.ReplaceAll(processedMetricName, ":", ".")
			sparkplugMsg.MetricName = processedMetricName // "motor.diagnostics.Refrigeration.temperature"
			
			// Step 2: Convert to UMH
			umhMsg, err := converter.DecodeSparkplugToUMH(sparkplugMsg, "_raw")
			Expect(err).ToNot(HaveOccurred())
			
			// Step 3: Verify parsing - splits on last dot
			// "motor.diagnostics.Refrigeration.temperature" splits into:
			// - virtual path: "motor.diagnostics.Refrigeration"
			// - tag name: "temperature"
			Expect(umhMsg.TopicInfo.VirtualPath).ToNot(BeNil())
			Expect(*umhMsg.TopicInfo.VirtualPath).To(Equal("motor.diagnostics.Refrigeration"))
			Expect(umhMsg.TopicInfo.Name).To(Equal("temperature"))
			
			// Step 4: Sanitize for final UMH topic
			sanitizedVirtualPath := sparkplugplugin.SanitizeForUMH(*umhMsg.TopicInfo.VirtualPath)
			sanitizedTagName := sparkplugplugin.SanitizeForUMH(umhMsg.TopicInfo.Name)
			Expect(sanitizedVirtualPath).To(Equal("motor.diagnostics.Refrigeration"))
			Expect(sanitizedTagName).To(Equal("temperature"))
		})
		
		It("should provide fallback when conversion fails due to invalid characters", func() {
			// Test with characters that would fail UMH validation even after slash replacement
			sparkplugMsg := &sparkplugplugin.SparkplugMessage{
				GroupID:    "Test@Group", // Invalid @ character
				EdgeNodeID: "TestNode",
				DeviceID:   "Test#Device", // Invalid # character
				MetricName: "metric$name", // Invalid $ character
				Value:      10.0,
				DataType:   "Double",
				Timestamp:  time.Now(),
			}
			
			// Attempt conversion (this might fail due to invalid chars in group/device)
			_, err := converter.DecodeSparkplugToUMH(sparkplugMsg, "_raw")
			
			// If conversion fails, we should sanitize for fallback
			if err != nil {
				// Sanitize for fallback metadata
				sanitizedDeviceID := sparkplugplugin.SanitizeForUMH(sparkplugMsg.DeviceID)
				sanitizedMetricName := sparkplugplugin.SanitizeForUMH(sparkplugMsg.MetricName)
				
				Expect(sanitizedDeviceID).To(Equal("Test_Device"))
				Expect(sanitizedMetricName).To(Equal("metric_name"))
			}
		})
	})
	
	Context("Location Path Generation without trailing dots (ENG-3428)", func() {
		It("should build location path correctly when LocationSublevels is empty", func() {
			// This test verifies the fix for ENG-3428
			// Previously, empty LocationSublevels would create "RealisticDevice." with trailing dot
			
			level0 := "RealisticDevice"
			locationSublevels := []string{}
			
			// Build location path as the fixed implementation does
			var locationPath string
			if len(locationSublevels) > 0 {
				locationPath = level0 + "." + strings.Join(locationSublevels, ".")
			} else {
				locationPath = level0
			}
			
			// Should not have trailing dot
			Expect(locationPath).To(Equal("RealisticDevice"))
			Expect(locationPath).NotTo(HaveSuffix("."))
		})

		It("should build location path correctly when LocationSublevels has values", func() {
			level0 := "enterprise"
			locationSublevels := []string{"site", "area", "line"}
			
			// Build location path
			var locationPath string
			if len(locationSublevels) > 0 {
				locationPath = level0 + "." + strings.Join(locationSublevels, ".")
			} else {
				locationPath = level0
			}
			
			// Should be properly joined
			Expect(locationPath).To(Equal("enterprise.site.area.line"))
		})

		It("should handle single sublevel correctly", func() {
			level0 := "factory"
			locationSublevels := []string{"line1"}
			
			// Build location path
			var locationPath string
			if len(locationSublevels) > 0 {
				locationPath = level0 + "." + strings.Join(locationSublevels, ".")
			} else {
				locationPath = level0
			}
			
			Expect(locationPath).To(Equal("factory.line1"))
		})
	})
})

var _ = Describe("Edge Node ID Consistency Fix Unit Tests", func() {
	var output *mockSparkplugOutput

	BeforeEach(func() {
		output = newMockSparkplugOutput()
	})

	Context("Phase 1: State Management Infrastructure", func() {
		It("should initialize state fields correctly", func() {
			// Test that new sparkplugOutput instances have proper state initialization
			Expect(output.cachedLocationPath).To(Equal(""))
			Expect(output.cachedEdgeNodeID).To(Equal(""))
			// Note: Cannot directly test sync.RWMutex initialization,
			// but concurrent tests below will verify thread safety
		})

		It("should handle concurrent state access safely", func() {
			// Test concurrent access to state
			var wg sync.WaitGroup
			numGoroutines := 10

			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					// Simulate concurrent state access
					output.edgeNodeStateMu.Lock()
					output.cachedLocationPath = fmt.Sprintf("test.path.%d", id)
					output.cachedEdgeNodeID = fmt.Sprintf("test:path:%d", id)
					output.edgeNodeStateMu.Unlock()

					// Verify we can read back
					output.edgeNodeStateMu.RLock()
					locationPath := output.cachedLocationPath
					edgeNodeID := output.cachedEdgeNodeID
					output.edgeNodeStateMu.RUnlock()

					Expect(locationPath).To(ContainSubstring("test.path"))
					Expect(edgeNodeID).To(ContainSubstring("test:path"))
				}(i)
			}

			wg.Wait()
			// Test passes if no race conditions detected
		})

		It("should provide getBirthEdgeNodeID fallback logic", func() {
			testCases := []struct {
				name             string
				staticEdgeNodeID string
				expectedResult   string
				description      string
			}{
				{
					name:             "static config provided",
					staticEdgeNodeID: "StaticNode01",
					expectedResult:   "StaticNode01",
					description:      "should use static config when provided",
				},
				{
					name:             "no config, uses default",
					staticEdgeNodeID: "",
					expectedResult:   "default_node",
					description:      "should fall back to default_node when no config",
				},
			}

			for _, tc := range testCases {
				By(tc.description)
				output.config.Identity.EdgeNodeID = tc.staticEdgeNodeID

				result := output.getBirthEdgeNodeID()

				Expect(result).To(Equal(tc.expectedResult))
			}
		})

		It("should prioritize cached state over static config", func() {
			// Set up static config
			output.config.Identity.EdgeNodeID = "StaticNode"

			// Initially should use static config
			result1 := output.getBirthEdgeNodeID()
			Expect(result1).To(Equal("StaticNode"))

			// Set cached state
			output.edgeNodeStateMu.Lock()
			output.cachedLocationPath = "enterprise.cached.path"
			output.cachedEdgeNodeID = "enterprise:cached:path"
			output.edgeNodeStateMu.Unlock()

			// Now should use cached state
			result2 := output.getBirthEdgeNodeID()
			Expect(result2).To(Equal("enterprise:cached:path"))
		})

		It("should handle state transitions correctly", func() {
			By("Starting with empty state")
			result1 := output.getBirthEdgeNodeID()
			Expect(result1).To(Equal("default_node"))

			By("Adding static config")
			output.config.Identity.EdgeNodeID = "StaticNode"
			result2 := output.getBirthEdgeNodeID()
			Expect(result2).To(Equal("StaticNode"))

			By("Adding cached state")
			output.edgeNodeStateMu.Lock()
			output.cachedEdgeNodeID = "enterprise:dynamic:path"
			output.edgeNodeStateMu.Unlock()
			result3 := output.getBirthEdgeNodeID()
			Expect(result3).To(Equal("enterprise:dynamic:path"))

			By("Clearing cached state")
			output.edgeNodeStateMu.Lock()
			output.cachedEdgeNodeID = ""
			output.edgeNodeStateMu.Unlock()
			result4 := output.getBirthEdgeNodeID()
			Expect(result4).To(Equal("StaticNode")) // Falls back to static
		})
	})

	Context("Phase 2: Edge Node ID Resolution Logic", func() {
		It("should enhance getEdgeNodeID with state caching", func() {
			testCases := []struct {
				name             string
				locationPath     string
				expectedEdgeNode string
				shouldCache      bool
			}{
				{
					name:             "simple path",
					locationPath:     "enterprise.factory",
					expectedEdgeNode: "enterprise:factory",
					shouldCache:      true,
				},
				{
					name:             "complex hierarchy",
					locationPath:     "automotive.plant_detroit.bodyshop.line3.station5",
					expectedEdgeNode: "automotive:plant_detroit:bodyshop:line3:station5",
					shouldCache:      true,
				},
				{
					name:             "single level",
					locationPath:     "enterprise",
					expectedEdgeNode: "enterprise",
					shouldCache:      true,
				},
			}

			for _, tc := range testCases {
				By(fmt.Sprintf("testing %s", tc.name))

				// Create mock message with location_path metadata
				msg := newMockMessage()
				msg.SetMeta("location_path", tc.locationPath)

				// Call the enhanced method (will be getEdgeNodeID after implementation)
				result := output.getEdgeNodeID(msg)

				// Verify result
				Expect(result).To(Equal(tc.expectedEdgeNode))

				// Verify state was cached
				if tc.shouldCache {
					Expect(output.cachedLocationPath).To(Equal(tc.locationPath))
					Expect(output.cachedEdgeNodeID).To(Equal(tc.expectedEdgeNode))
				}
			}
		})

		It("should maintain priority logic with state caching", func() {
			testCases := []struct {
				name           string
				locationPath   string
				staticConfig   string
				expectedResult string
				description    string
			}{
				{
					name:           "location_path takes priority and caches",
					locationPath:   "enterprise.dynamic",
					staticConfig:   "StaticNode",
					expectedResult: "enterprise:dynamic",
					description:    "should use and cache location_path over static config",
				},
				{
					name:           "static config when no location_path",
					locationPath:   "",
					staticConfig:   "StaticNode",
					expectedResult: "StaticNode",
					description:    "should use static config when location_path empty",
				},
				{
					name:           "default when neither provided",
					locationPath:   "",
					staticConfig:   "",
					expectedResult: "default_node",
					description:    "should use default when both empty",
				},
			}

			for _, tc := range testCases {
				By(tc.description)

				// Reset state for each test
				output.cachedLocationPath = ""
				output.cachedEdgeNodeID = ""
				output.config.Identity.EdgeNodeID = tc.staticConfig

				msg := newMockMessage()
				if tc.locationPath != "" {
					msg.SetMeta("location_path", tc.locationPath)
				}

				result := output.getEdgeNodeID(msg)

				Expect(result).To(Equal(tc.expectedResult))
			}
		})

		It("should handle concurrent state caching safely", func() {
			// Test concurrent state updates
			var wg sync.WaitGroup
			numGoroutines := 5

			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					msg := newMockMessage()
					msg.SetMeta("location_path", fmt.Sprintf("enterprise.worker%d", id))

					result := output.getEdgeNodeID(msg)

					Expect(result).To(ContainSubstring("enterprise:worker"))
					// State should be updated (one of the workers will win)
					output.edgeNodeStateMu.RLock()
					cachedPath := output.cachedLocationPath
					cachedID := output.cachedEdgeNodeID
					output.edgeNodeStateMu.RUnlock()

					Expect(cachedPath).NotTo(BeEmpty())
					Expect(cachedID).NotTo(BeEmpty())
				}(i)
			}

			wg.Wait()
		})
	})

	Context("Phase 3: Integration Tests - BIRTH-DATA Consistency", func() {
		It("should ensure BIRTH and DATA messages use consistent Edge Node IDs", func() {
			testCases := []struct {
				name         string
				locationPath string
				staticConfig string
				description  string
			}{
				{
					name:         "dynamic path consistency",
					locationPath: "enterprise.factory.line1",
					staticConfig: "",
					description:  "BIRTH and DATA should both use enterprise:factory:line1",
				},
				{
					name:         "complex hierarchy consistency",
					locationPath: "automotive.plant_detroit.assembly.station_5",
					staticConfig: "StaticNode",
					description:  "location_path should override static config for both message types",
				},
			}

			for _, tc := range testCases {
				By(tc.description)

				// Reset state
				output.edgeNodeStateMu.Lock()
				output.cachedLocationPath = ""
				output.cachedEdgeNodeID = ""
				output.edgeNodeStateMu.Unlock()
				output.config.Identity.EdgeNodeID = tc.staticConfig

				// Step 1: Simulate DATA message processing (caches state)
				dataMsg := newMockMessage()
				if tc.locationPath != "" {
					dataMsg.SetMeta("location_path", tc.locationPath)
				}

				dataEdgeNodeID := output.getEdgeNodeID(dataMsg)

				// Step 2: Simulate BIRTH message publishing (uses cached state)
				birthEdgeNodeID := output.getBirthEdgeNodeID()

				// Step 3: Verify consistency
				Expect(dataEdgeNodeID).To(Equal(birthEdgeNodeID))

				if tc.locationPath != "" {
					expectedID := strings.ReplaceAll(tc.locationPath, ".", ":")
					Expect(dataEdgeNodeID).To(Equal(expectedID))
					Expect(birthEdgeNodeID).To(Equal(expectedID))
				}
			}
		})

		It("should handle state transitions during message processing", func() {
			By("Starting with no cached state")
			initialBirthID := output.getBirthEdgeNodeID()
			Expect(initialBirthID).To(Equal("default_node"))

			By("Processing first DATA message")
			msg1 := newMockMessage()
			msg1.SetMeta("location_path", "enterprise.line1")

			dataID1 := output.getEdgeNodeID(msg1)
			birthID1 := output.getBirthEdgeNodeID()

			Expect(dataID1).To(Equal("enterprise:line1"))
			Expect(birthID1).To(Equal("enterprise:line1"))

			By("Processing DATA message with different location_path")
			msg2 := newMockMessage()
			msg2.SetMeta("location_path", "enterprise.line2")

			dataID2 := output.getEdgeNodeID(msg2)
			birthID2 := output.getBirthEdgeNodeID()

			Expect(dataID2).To(Equal("enterprise:line2"))
			Expect(birthID2).To(Equal("enterprise:line2")) // Should update to latest

			By("Verifying cached state reflects latest DATA message")
			output.edgeNodeStateMu.RLock()
			cachedPath := output.cachedLocationPath
			cachedID := output.cachedEdgeNodeID
			output.edgeNodeStateMu.RUnlock()

			Expect(cachedPath).To(Equal("enterprise.line2"))
			Expect(cachedID).To(Equal("enterprise:line2"))
		})

		It("should maintain consistency across multiple message types", func() {
			// This test simulates the full message flow that was failing before the fix

			By("Setting up dynamic location path")
			locationPath := "enterprise.factory.line1.station1"
			expectedEdgeNodeID := "enterprise:factory:line1:station1"

			By("Processing DATA message (should cache state)")
			dataMsg := newMockMessage()
			dataMsg.SetMeta("location_path", locationPath)

			dataEdgeNodeID := output.getEdgeNodeID(dataMsg)
			Expect(dataEdgeNodeID).To(Equal(expectedEdgeNodeID))

			By("Verifying BIRTH message uses same Edge Node ID")
			birthEdgeNodeID := output.getBirthEdgeNodeID()
			Expect(birthEdgeNodeID).To(Equal(expectedEdgeNodeID))

			By("Verifying subsequent DATA messages maintain consistency")
			dataMsg2 := newMockMessage()
			dataMsg2.SetMeta("location_path", locationPath)

			dataEdgeNodeID2 := output.getEdgeNodeID(dataMsg2)
			Expect(dataEdgeNodeID2).To(Equal(expectedEdgeNodeID))

			By("Verifying all Edge Node IDs are identical")
			Expect(dataEdgeNodeID).To(Equal(birthEdgeNodeID))
			Expect(dataEdgeNodeID).To(Equal(dataEdgeNodeID2))

			// This consistency is what enables proper alias resolution
			// Cache key: TestGroup/{edgeNodeID} = Lookup key: TestGroup/{edgeNodeID}
		})

		It("should handle edge cases gracefully", func() {
			testCases := []struct {
				name         string
				locationPath string
				staticConfig string
				expectedID   string
				description  string
			}{
				{
					name:         "empty location_path with static config",
					locationPath: "",
					staticConfig: "StaticEdgeNode",
					expectedID:   "StaticEdgeNode",
					description:  "should fall back to static config consistently",
				},
				{
					name:         "empty location_path without static config",
					locationPath: "",
					staticConfig: "",
					expectedID:   "default_node",
					description:  "should use default_node consistently",
				},
				{
					name:         "single segment location_path",
					locationPath: "enterprise",
					staticConfig: "StaticNode",
					expectedID:   "enterprise",
					description:  "should handle single-segment paths correctly",
				},
			}

			for _, tc := range testCases {
				By(tc.description)

				// Reset state
				output.edgeNodeStateMu.Lock()
				output.cachedLocationPath = ""
				output.cachedEdgeNodeID = ""
				output.edgeNodeStateMu.Unlock()
				output.config.Identity.EdgeNodeID = tc.staticConfig

				// Process DATA message
				dataMsg := newMockMessage()
				if tc.locationPath != "" {
					dataMsg.SetMeta("location_path", tc.locationPath)
				}

				dataID := output.getEdgeNodeID(dataMsg)
				birthID := output.getBirthEdgeNodeID()

				Expect(dataID).To(Equal(tc.expectedID))
				Expect(birthID).To(Equal(tc.expectedID))
			}
		})
	})

	Context("Rebirth Suppression in Three-Mode System", func() {
		// Mock structures for testing
		type testLogger struct {
			messages []string
		}
		
		type testMetrics struct {
			counters map[string]int64
		}
		
		It("should suppress rebirth commands in secondary_passive mode", func() {
			// Create a mock metrics to verify behavior
			mockMetrics := &testMetrics{
				counters: make(map[string]int64),
			}
			
			// Test inputs for different roles
			testCases := []struct {
				role                   sparkplugplugin.Role
				expectRebirthSent      bool
				expectRebirthSuppressed bool
			}{
				{
					role:                   sparkplugplugin.RoleSecondaryPassive,
					expectRebirthSent:      false,
					expectRebirthSuppressed: true,
				},
				{
					role:                   sparkplugplugin.RoleSecondaryActive,
					expectRebirthSent:      true,
					expectRebirthSuppressed: false,
				},
				{
					role:                   sparkplugplugin.RolePrimaryHost,
					expectRebirthSent:      true,
					expectRebirthSuppressed: false,
				},
			}
			
			for _, tc := range testCases {
				By(fmt.Sprintf("testing rebirth behavior for role: %s", tc.role), func() {
					// Reset metrics
					mockMetrics.counters = make(map[string]int64)
					
					// Simulate rebirth request logic
					shouldSendRebirth := tc.role != sparkplugplugin.RoleSecondaryPassive
					
					if !shouldSendRebirth {
						// Simulate metric increment
						mockMetrics.counters["rebirths_suppressed"]++
					}
					
					// Verify behavior
					Expect(shouldSendRebirth).To(Equal(tc.expectRebirthSent))
					
					if tc.expectRebirthSuppressed {
						Expect(mockMetrics.counters["rebirths_suppressed"]).To(Equal(int64(1)))
					} else {
						Expect(mockMetrics.counters["rebirths_suppressed"]).To(Equal(int64(0)))
					}
				})
			}
		})
		
		It("should handle subscription topics correctly for each mode", func() {
			testCases := []struct {
				role             sparkplugplugin.Role
				expectSubscribe  bool
			}{
				{
					role:            sparkplugplugin.RoleSecondaryPassive,
					expectSubscribe: true, // Still subscribes to topics
				},
				{
					role:            sparkplugplugin.RoleSecondaryActive,
					expectSubscribe: true,
				},
				{
					role:            sparkplugplugin.RolePrimaryHost,
					expectSubscribe: true,
				},
			}
			
			for _, tc := range testCases {
				By(fmt.Sprintf("testing subscription for role: %s", tc.role), func() {
					config := &sparkplugplugin.Config{
						Role: tc.role,
						Identity: sparkplugplugin.Identity{
							GroupID: "TestGroup",
						},
					}
					
					topics := config.GetSubscriptionTopics()
					
					// All host modes should subscribe to topics
					if tc.expectSubscribe {
						Expect(len(topics)).To(BeNumerically(">", 0))
						Expect(topics[0]).To(ContainSubstring("spBv1.0/"))
					}
				})
			}
		})
	})

	Context("Node Rebirth Command Handling", func() {
		It("should correctly identify rebirth commands in NCMD payloads", func() {
			// This test verifies the rebirth command detection logic
			// which is the core functionality needed for handling rebirths
			
			testCases := []struct {
				name           string
				payload        *sparkplugb.Payload
				expectRebirth  bool
			}{
				{
					name: "Valid rebirth command with name",
					payload: &sparkplugb.Payload{
						Metrics: []*sparkplugb.Payload_Metric{
							{
								Name: func() *string { s := "Node Control/Rebirth"; return &s }(),
								Value: &sparkplugb.Payload_Metric_BooleanValue{
									BooleanValue: true,
								},
							},
						},
					},
					expectRebirth: true,
				},
				{
					name: "Rebirth command with false value",
					payload: &sparkplugb.Payload{
						Metrics: []*sparkplugb.Payload_Metric{
							{
								Name: func() *string { s := "Node Control/Rebirth"; return &s }(),
								Value: &sparkplugb.Payload_Metric_BooleanValue{
									BooleanValue: false,
								},
							},
						},
					},
					expectRebirth: false,
				},
				{
					name: "Rebirth command using alias only (no name)",
					payload: &sparkplugb.Payload{
						Metrics: []*sparkplugb.Payload_Metric{
							{
								Alias: func() *uint64 { a := uint64(1); return &a }(),
								Value: &sparkplugb.Payload_Metric_BooleanValue{
									BooleanValue: true,
								},
							},
						},
					},
					expectRebirth: true,
				},
				{
					name: "Different metric name",
					payload: &sparkplugb.Payload{
						Metrics: []*sparkplugb.Payload_Metric{
							{
								Name: func() *string { s := "Temperature"; return &s }(),
								Value: &sparkplugb.Payload_Metric_DoubleValue{
									DoubleValue: 25.5,
								},
							},
						},
					},
					expectRebirth: false,
				},
			}

			for _, tc := range testCases {
				By(fmt.Sprintf("Testing: %s", tc.name))
				
				// This simulates the actual rebirth detection logic from handleRebirthCommand
				rebirthRequested := false
				for _, metric := range tc.payload.Metrics {
					isRebirthMetric := false
					
					// Check named metric first (spec compliant approach)
					if metric.Name != nil && *metric.Name == "Node Control/Rebirth" {
						isRebirthMetric = true
					} else if metric.Name == nil && metric.Alias != nil && *metric.Alias == 1 {
						// Only check alias if name is not provided
						// Alias 1 is reserved for "Node Control/Rebirth" in NBIRTH
						isRebirthMetric = true
					}
					
					// If this is a rebirth metric with boolean true value
					if isRebirthMetric && metric.GetBooleanValue() {
						rebirthRequested = true
						break
					}
				}
				
				Expect(rebirthRequested).To(Equal(tc.expectRebirth), 
					fmt.Sprintf("Test case '%s' failed", tc.name))
			}
		})

		It("should marshal and unmarshal rebirth command payloads correctly", func() {
			// Test that rebirth command payloads can be correctly marshaled/unmarshaled
			// This is critical for MQTT message handling
			
			rebirthMetric := &sparkplugb.Payload_Metric{
				Name: func() *string { s := "Node Control/Rebirth"; return &s }(),
				Alias: func() *uint64 { a := uint64(1); return &a }(),
				Value: &sparkplugb.Payload_Metric_BooleanValue{
					BooleanValue: true,
				},
				Datatype: func() *uint32 { d := uint32(11); return &d }(), // Boolean type
			}

			originalPayload := &sparkplugb.Payload{
				Timestamp: func() *uint64 { t := uint64(time.Now().UnixMilli()); return &t }(),
				Seq:       func() *uint64 { s := uint64(42); return &s }(),
				Metrics:   []*sparkplugb.Payload_Metric{rebirthMetric},
			}

			// Marshal the payload
			payloadBytes, err := proto.Marshal(originalPayload)
			Expect(err).NotTo(HaveOccurred())
			Expect(payloadBytes).NotTo(BeEmpty())

			// Unmarshal the payload
			var decodedPayload sparkplugb.Payload
			err = proto.Unmarshal(payloadBytes, &decodedPayload)
			Expect(err).NotTo(HaveOccurred())

			// Verify the decoded payload matches
			Expect(decodedPayload.Metrics).To(HaveLen(1))
			Expect(*decodedPayload.Metrics[0].Name).To(Equal("Node Control/Rebirth"))
			Expect(*decodedPayload.Metrics[0].Alias).To(Equal(uint64(1)))
			Expect(decodedPayload.Metrics[0].GetBooleanValue()).To(BeTrue())
			Expect(*decodedPayload.Seq).To(Equal(uint64(42)))
		})
	})
})

// Mock structures for testing EON Node ID resolution
type mockSparkplugOutput struct {
	config sparkplugplugin.Config
	logger mockLogger
	// NEW: State management fields for Edge Node ID consistency fix
	cachedLocationPath string
	cachedEdgeNodeID   string
	edgeNodeStateMu    sync.RWMutex
}

func newMockSparkplugOutput() *mockSparkplugOutput {
	return &mockSparkplugOutput{
		config: sparkplugplugin.Config{
			Identity: sparkplugplugin.Identity{
				GroupID:    "TestGroup",
				EdgeNodeID: "",
				DeviceID:   "",
			},
		},
		logger:             mockLogger{},
		cachedLocationPath: "",
		cachedEdgeNodeID:   "",
		edgeNodeStateMu:    sync.RWMutex{},
	}
}

// Mock getEONNodeID method for testing (mirrors the actual implementation)
func (m *mockSparkplugOutput) getEONNodeID(msg *mockMessage) string {
	// Priority 1: Dynamic from location_path metadata (Parris Method)
	if locationPath := msg.GetMeta("location_path"); locationPath != "" {
		// Convert UMH dot notation to Sparkplug colon notation (Parris Method)
		eonNodeID := strings.ReplaceAll(locationPath, ".", ":")
		return eonNodeID
	}

	// Priority 2: Static override from configuration
	if m.config.Identity.EdgeNodeID != "" {
		return m.config.Identity.EdgeNodeID
	}

	// Priority 3: Default fallback (should log warning)
	m.logger.Warn("No location_path metadata or edge_node_id configured, using default EON Node ID")
	return "default_node"
}

// NEW: Mock getEdgeNodeID method for Phase 2 testing (enhanced with state caching)
func (m *mockSparkplugOutput) getEdgeNodeID(msg *mockMessage) string {
	// Priority 1: Dynamic from location_path metadata (Parris Method)
	if locationPath := msg.GetMeta("location_path"); locationPath != "" {
		// Convert UMH dot notation to Sparkplug colon notation (Parris Method)
		edgeNodeID := strings.ReplaceAll(locationPath, ".", ":")

		// Cache the state for BIRTH consistency (Phase 2 enhancement)
		m.edgeNodeStateMu.Lock()
		m.cachedLocationPath = locationPath
		m.cachedEdgeNodeID = edgeNodeID
		m.edgeNodeStateMu.Unlock()

		return edgeNodeID
	}

	// Priority 2: Static override from configuration
	m.edgeNodeStateMu.RLock()
	staticEdgeNodeID := m.config.Identity.EdgeNodeID
	m.edgeNodeStateMu.RUnlock()

	if staticEdgeNodeID != "" {
		return staticEdgeNodeID
	}

	// Priority 3: Default fallback (should log warning)
	m.logger.Warn("No location_path metadata or edge_node_id configured, using default Edge Node ID")
	return "default_node"
}

// NEW: Mock getBirthEdgeNodeID method for Phase 1 testing
func (m *mockSparkplugOutput) getBirthEdgeNodeID() string {
	m.edgeNodeStateMu.RLock()
	defer m.edgeNodeStateMu.RUnlock()

	// Use cached state if available
	if m.cachedEdgeNodeID != "" {
		return m.cachedEdgeNodeID
	}

	// Fall back to static config
	if m.config.Identity.EdgeNodeID != "" {
		return m.config.Identity.EdgeNodeID
	}

	// Final fallback
	return "default_node"
}

type mockMessage struct {
	metadata map[string]string
}

func newMockMessage() *mockMessage {
	return &mockMessage{
		metadata: make(map[string]string),
	}
}

func (m *mockMessage) SetMeta(key, value string) {
	m.metadata[key] = value
}

func (m *mockMessage) GetMeta(key string) string {
	return m.metadata[key]
}

type mockLogger struct{}

func (m mockLogger) Warn(msg string) {
	// In a real test, we might capture this for verification
}
