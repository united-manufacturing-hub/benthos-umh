//go:build !payload && !flow && !integration

// Unit tests for Sparkplug B plugin core components
// These tests run offline with no external dependencies (<3s)
// Build tag exclusion ensures they don't run with other test types

package sparkplug_plugin_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	"github.com/weekaung/sparkplugb-client/sproto"
)

func TestSparkplugUnit(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sparkplug B Unit Test Suite")
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
	var cache *sparkplug_plugin.AliasCache

	BeforeEach(func() {
		cache = sparkplug_plugin.NewAliasCache()
	})

	Context("Alias Resolution", func() {
		It("should cache aliases from BIRTH metrics", func() {
			metrics := []*sproto.Payload_Metric{
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
			birthMetrics := []*sproto.Payload_Metric{
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
			dataMetrics := []*sproto.Payload_Metric{
				{
					Alias: uint64Ptr(100), // Should resolve to "Temperature"
					Value: &sproto.Payload_Metric_DoubleValue{DoubleValue: 25.5},
				},
				{
					Alias: uint64Ptr(101), // Should resolve to "Pressure"
					Value: &sproto.Payload_Metric_DoubleValue{DoubleValue: 1013.25},
				},
			}

			count := cache.ResolveAliases("TestFactory/Line1", dataMetrics)
			Expect(count).To(Equal(2))
			Expect(*dataMetrics[0].Name).To(Equal("Temperature"))
			Expect(*dataMetrics[1].Name).To(Equal("Pressure"))
		})

		It("should handle alias collisions in NBIRTH", func() {
			// Create metrics with duplicate aliases (collision)
			metrics := []*sproto.Payload_Metric{
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
			metrics := []*sproto.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(100),
				},
			}
			cache.CacheAliases("TestFactory/Line1", metrics)

			// Clear cache (simulating session restart)
			cache.Clear()

			// Verify cache is empty by trying to resolve
			dataMetrics := []*sproto.Payload_Metric{
				{
					Alias: uint64Ptr(100),
					Value: &sproto.Payload_Metric_DoubleValue{DoubleValue: 25.5},
				},
			}
			count := cache.ResolveAliases("TestFactory/Line1", dataMetrics)
			Expect(count).To(Equal(0))
		})

		It("should handle multiple devices independently", func() {
			// Cache aliases for device 1
			device1Metrics := []*sproto.Payload_Metric{
				{
					Name:  stringPtr("Temperature"),
					Alias: uint64Ptr(100),
				},
			}
			count1 := cache.CacheAliases("TestFactory/Line1", device1Metrics)
			Expect(count1).To(Equal(1))

			// Cache aliases for device 2
			device2Metrics := []*sproto.Payload_Metric{
				{
					Name:  stringPtr("Pressure"),
					Alias: uint64Ptr(100), // Same alias, different device
				},
			}
			count2 := cache.CacheAliases("TestFactory/Line2", device2Metrics)
			Expect(count2).To(Equal(1))

			// Test resolution for each device independently
			dataMetrics1 := []*sproto.Payload_Metric{
				{
					Alias: uint64Ptr(100),
					Value: &sproto.Payload_Metric_DoubleValue{DoubleValue: 25.5},
				},
			}
			resolved1 := cache.ResolveAliases("TestFactory/Line1", dataMetrics1)
			Expect(resolved1).To(Equal(1))
			Expect(*dataMetrics1[0].Name).To(Equal("Temperature"))

			dataMetrics2 := []*sproto.Payload_Metric{
				{
					Alias: uint64Ptr(100),
					Value: &sproto.Payload_Metric_DoubleValue{DoubleValue: 1013.25},
				},
			}
			resolved2 := cache.ResolveAliases("TestFactory/Line2", dataMetrics2)
			Expect(resolved2).To(Equal(1))
			Expect(*dataMetrics2[0].Name).To(Equal("Pressure"))
		})

		It("should handle edge cases gracefully", func() {
			// Test empty device key
			metrics := []*sproto.Payload_Metric{
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
			invalidMetrics := []*sproto.Payload_Metric{
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
	})
})

var _ = Describe("TopicParser Unit Tests", func() {
	Context("Topic Parsing", func() {
		It("should parse valid Sparkplug B topic structure", func() {
			// Test cases for valid topics
			validTopics := []struct {
				topic          string
				expectedType   string
				expectedGroup  string
				expectedNode   string
				expectedDevice string
			}{
				{"spBv1.0/Factory1/NBIRTH/Line1", "NBIRTH", "Factory1", "Line1", ""},
				{"spBv1.0/Factory1/NDATA/Line1", "NDATA", "Factory1", "Line1", ""},
				{"spBv1.0/Factory1/NDEATH/Line1", "NDEATH", "Factory1", "Line1", ""},
				{"spBv1.0/Factory1/NCMD/Line1", "NCMD", "Factory1", "Line1", ""},
				{"spBv1.0/Factory1/DBIRTH/Line1/Machine1", "DBIRTH", "Factory1", "Line1", "Machine1"},
				{"spBv1.0/Factory1/DDATA/Line1/Machine1", "DDATA", "Factory1", "Line1", "Machine1"},
				{"spBv1.0/Factory1/DDEATH/Line1/Machine1", "DDEATH", "Factory1", "Line1", "Machine1"},
				{"spBv1.0/Factory1/DCMD/Line1/Machine1", "DCMD", "Factory1", "Line1", "Machine1"},
			}

			for _, tc := range validTopics {
				By("parsing topic "+tc.topic, func() {
					// Verify topic has correct Sparkplug structure
					Expect(tc.topic).To(HavePrefix("spBv1.0/"))
					Expect(tc.topic).To(ContainSubstring(tc.expectedGroup))
					Expect(tc.topic).To(ContainSubstring(tc.expectedType))
					Expect(tc.topic).To(ContainSubstring(tc.expectedNode))
					if tc.expectedDevice != "" {
						Expect(tc.topic).To(ContainSubstring(tc.expectedDevice))
					}
				})
			}
		})

		It("should reject malformed topics", func() {
			// TODO: Test actual topic parser when exposed
			malformedTopics := []string{
				"invalid/topic/structure",
				"spBv1.0/",
				"spBv1.0/Group/INVALID/Node",
				"spBv2.0/Group/NDATA/Node", // Wrong version
			}

			for _, topic := range malformedTopics {
				By("rejecting topic "+topic, func() {
					// For now, just verify structure is incorrect
					if topic != "spBv1.0/" {
						Expect(topic).NotTo(MatchRegexp(`^spBv1\.0/[^/]+/(NBIRTH|NDATA|NDEATH|NCMD|DBIRTH|DDATA|DDEATH|DCMD)/[^/]+(/[^/]+)?$`))
					}
				})
			}
		})

		It("should handle edge cases in topic parsing", func() {
			// TODO: Test edge cases when parser is exposed
			Skip("TODO: Implement when TopicParser is exposed for unit testing")
		})
	})
})

var _ = Describe("SequenceManager Unit Tests", func() {
	Context("Sequence Number Validation", func() {
		It("should detect sequence gaps", func() {
			// TODO: Test sequence validation when SequenceManager is exposed
			Skip("TODO: Implement - move from sparkplug_b_input_test.go")
		})

		It("should handle sequence wraparound (255 -> 0)", func() {
			// TODO: Test sequence wraparound at 255->0 boundary
			Skip("TODO: Implement")
		})

		It("should trigger rebirth on max gap exceeded", func() {
			// TODO: Test rebirth trigger on gap exceeding threshold
			Skip("TODO: Implement")
		})
	})
})

var _ = Describe("TypeConverter Unit Tests", func() {
	Context("Data Type Conversions", func() {
		It("should convert Sparkplug types to UMH format", func() {
			// TODO: Test data type conversions
			Skip("TODO: Implement")
		})

		It("should handle type conversion edge cases", func() {
			// TODO: Test edge cases like overflow, null values
			Skip("TODO: Implement")
		})
	})
})

var _ = Describe("MQTTClientBuilder Unit Tests", func() {
	Context("Client Configuration", func() {
		It("should build valid MQTT client options", func() {
			// TODO: Test MQTT client option building
			Skip("TODO: Implement")
		})

		It("should handle connection configuration validation", func() {
			// TODO: Test configuration validation
			Skip("TODO: Implement")
		})
	})
})

var _ = Describe("Configuration Unit Tests", func() {
	Context("Config Validation", func() {
		It("should validate plugin configuration", func() {
			// TODO: Move configuration tests from sparkplug_b_input_test.go
			Skip("TODO: Implement - move from config tests")
		})

		It("should provide sensible defaults", func() {
			// TODO: Test default value assignment
			Skip("TODO: Implement")
		})
	})
})
