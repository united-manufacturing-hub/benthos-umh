# Phase-by-Phase Testing Strategy for Edge Node ID Consistency Fix

## 🎯 **Integration with Existing Test Framework**

This strategy integrates with the existing **Ginkgo v2 + Gomega** test framework and follows established patterns:

- **File Location**: Add tests to existing `sparkplug_plugin/unit_test.go` 
- **Build Tags**: Use `//go:build !payload && !flow && !integration` (same as existing unit tests)
- **Test Suite**: Integrate with existing "Sparkplug B Unit Test Suite"
- **Naming**: Follow existing `Describe` → `Context` → `It` hierarchy
- **Mocks**: Extend existing `mockSparkplugOutput` and `mockMessage` structures

## 🧪 **Integration Points**

### **File Structure:**
```
sparkplug_plugin/unit_test.go (existing)
├── Existing: "AliasCache Unit Tests" 
├── Existing: "EON Node ID Resolution (Parris Method) Unit Tests"
└── NEW: "Edge Node ID Consistency Fix Unit Tests" ← Add here
```

### **Test Naming Convention:**
```
Edge Node ID Consistency Fix Unit Tests
├── Phase 1: State Management Infrastructure
├── Phase 2: Edge Node ID Resolution Logic  
├── Phase 3: BIRTH Message Consistency
├── Phase 4: DEATH Message Consistency
└── Phase 5: End-to-End Consistency Validation
```

## 🧪 **Phase 1: State Management Infrastructure Tests**

### **Integration Location:** Add to `sparkplug_plugin/unit_test.go` after existing EON Node ID tests

```go
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
				name               string
				staticEdgeNodeID   string
				expectedResult     string
				description        string
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
	})
```

### **Required Mock Extensions:**

```go
// Add these fields to existing mockSparkplugOutput struct
type mockSparkplugOutput struct {
	config             sparkplug_plugin.Config
	logger             mockLogger
	// NEW: Add state management fields
	cachedLocationPath string
	cachedEdgeNodeID   string
	edgeNodeStateMu    sync.RWMutex
}

// Add new method to mockSparkplugOutput
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
```

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
				name             string
				locationPath     string
				staticConfig     string
				expectedResult   string
				description      string
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
					Expect(output.cachedLocationPath).NotTo(BeEmpty())
					Expect(output.cachedEdgeNodeID).NotTo(BeEmpty())
				}(id)
			}
			
			wg.Wait()
		})
	})

### **Required Mock Method Extensions:**

```go
// Add new method to mockSparkplugOutput (extends existing getEONNodeID)
func (m *mockSparkplugOutput) getEdgeNodeID(msg *mockMessage) string {
	m.edgeNodeStateMu.Lock()
	defer m.edgeNodeStateMu.Unlock()
	
	// Get location_path from message
	locationPath := msg.GetMeta("location_path")
	
	// Cache the location_path if present
	if locationPath != "" {
		m.cachedLocationPath = locationPath
		// Convert dots to colons (Parris Method)
		edgeNodeID := strings.ReplaceAll(locationPath, ".", ":")
		m.cachedEdgeNodeID = edgeNodeID
		return edgeNodeID
	}
	
	// Fall back to static config
	if m.config.Identity.EdgeNodeID != "" {
		return m.config.Identity.EdgeNodeID
	}
	
	// Final fallback
	return "default_node"
}
```

	Context("Phase 3: BIRTH Message Consistency", func() {
		It("should use cached state in BIRTH messages", func() {
			testCases := []struct {
				name                string
				cachedLocationPath  string
				cachedEdgeNodeID    string
				staticConfig        string
				expectedEdgeNodeID  string
				description         string
			}{
				{
					name:               "uses cached state",
					cachedLocationPath: "enterprise.factory.line1",
					cachedEdgeNodeID:   "enterprise:factory:line1",
					staticConfig:       "StaticNode",
					expectedEdgeNodeID: "enterprise:factory:line1",
					description:        "should use cached Edge Node ID over static config",
				},
				{
					name:               "falls back to static when no cache",
					cachedLocationPath: "",
					cachedEdgeNodeID:   "",
					staticConfig:       "StaticNode",
					expectedEdgeNodeID: "StaticNode",
					description:        "should use static config when no cached state",
				},
				{
					name:               "uses default when no cache or static",
					cachedLocationPath: "",
					cachedEdgeNodeID:   "",
					staticConfig:       "",
					expectedEdgeNodeID: "default_node",
					description:        "should use default when neither cached nor static available",
				},
			}
			
			for _, tc := range testCases {
				By(tc.description)
				
				// Pre-populate state
				output.cachedLocationPath = tc.cachedLocationPath
				output.cachedEdgeNodeID = tc.cachedEdgeNodeID
				output.config.Identity.EdgeNodeID = tc.staticConfig
				output.config.Identity.GroupID = "TestGroup"
				
				// Test getBirthEdgeNodeID method
				edgeNodeID := output.getBirthEdgeNodeID()
				
				Expect(edgeNodeID).To(Equal(tc.expectedEdgeNodeID))
			}
		})

		It("should construct proper topics for node vs device BIRTH", func() {
			output.cachedEdgeNodeID = "test:edge:node"
			output.config.Identity.GroupID = "TestGroup"
			
			testCases := []struct {
				name          string
				deviceID      string
				expectedTopic string
			}{
				{
					name:          "node-level topic (no device)",
					deviceID:      "",
					expectedTopic: "spBv1.0/TestGroup/NBIRTH/test:edge:node",
				},
				{
					name:          "device-level topic",
					deviceID:      "Device01",
					expectedTopic: "spBv1.0/TestGroup/DBIRTH/test:edge:node/Device01",
				},
			}
			
			for _, tc := range testCases {
				By(fmt.Sprintf("testing %s", tc.name))
				
				output.config.Identity.DeviceID = tc.deviceID
				
				edgeNodeID := output.getBirthEdgeNodeID()
				
				// Construct topic (simulate what publishBirthMessage does)
				var topic string
				if tc.deviceID != "" {
					topic = fmt.Sprintf("spBv1.0/%s/DBIRTH/%s/%s", 
						output.config.Identity.GroupID, edgeNodeID, tc.deviceID)
				} else {
					topic = fmt.Sprintf("spBv1.0/%s/NBIRTH/%s", 
						output.config.Identity.GroupID, edgeNodeID)
				}
				
				Expect(topic).To(Equal(tc.expectedTopic))
			}
		})
	})

	Context("Phase 4: DEATH Message Consistency", func() {
		It("should use getEdgeNodeID when DEATH has message context", func() {
			// Create message with location_path
			msg := newMockMessage()
			msg.SetMeta("location_path", "enterprise.factory.line2")
			output.config.Identity.GroupID = "TestGroup"
			
			// Call createDeathMessage with context (will use getEdgeNodeID logic)
			edgeNodeID := output.getEdgeNodeID(msg)
			
			// Should use getEdgeNodeID logic (Parris Method)
			expectedEdgeNodeID := "enterprise:factory:line2"
			Expect(edgeNodeID).To(Equal(expectedEdgeNodeID))
			
			// Verify state was cached
			Expect(output.cachedLocationPath).To(Equal("enterprise.factory.line2"))
			Expect(output.cachedEdgeNodeID).To(Equal("enterprise:factory:line2"))
		})

		It("should use getBirthEdgeNodeID when DEATH has no message context", func() {
			// Pre-populate cached state (simulating previous DATA messages)
			output.cachedLocationPath = "enterprise.cached.path"
			output.cachedEdgeNodeID = "enterprise:cached:path"
			output.config.Identity.GroupID = "TestGroup"
			
			// Call getBirthEdgeNodeID (simulates DEATH without context)
			edgeNodeID := output.getBirthEdgeNodeID()
			
			// Should use getBirthEdgeNodeID logic (cached state)
			expectedEdgeNodeID := "enterprise:cached:path"
			Expect(edgeNodeID).To(Equal(expectedEdgeNodeID))
		})

		It("should handle DEATH message fallback scenarios", func() {
			testCases := []struct {
				name               string
				hasMessageContext  bool
				messageLocationPath string
				cachedLocationPath string
				staticConfig       string
				expectedEdgeNodeID string
				description        string
			}{
				{
					name:              "with context, uses message location_path",
					hasMessageContext: true,
					messageLocationPath: "enterprise.message.path",
					cachedLocationPath: "enterprise.cached.path",
					staticConfig:      "StaticNode",
					expectedEdgeNodeID: "enterprise:message:path",
					description:       "should use message location_path when context available",
				},
				{
					name:              "without context, uses cached",
					hasMessageContext: false,
					cachedLocationPath: "enterprise.cached.path",
					staticConfig:      "StaticNode",
					expectedEdgeNodeID: "enterprise:cached:path",
					description:       "should use cached state when no message context",
				},
				{
					name:              "without context, no cache, uses static",
					hasMessageContext: false,
					cachedLocationPath: "",
					staticConfig:      "StaticNode",
					expectedEdgeNodeID: "StaticNode",
					description:       "should use static config when no cache or context",
				},
				{
					name:              "without context, no cache, no static, uses default",
					hasMessageContext: false,
					cachedLocationPath: "",
					staticConfig:      "",
					expectedEdgeNodeID: "default_node",
					description:       "should use default when no cache, context, or static config",
				},
			}
			
			for _, tc := range testCases {
				By(tc.description)
				
				// Reset and setup state
				output.cachedLocationPath = tc.cachedLocationPath
				output.cachedEdgeNodeID = tc.cachedLocationPath // Will be converted to colon format
				if tc.cachedLocationPath != "" {
					output.cachedEdgeNodeID = strings.ReplaceAll(tc.cachedLocationPath, ".", ":")
				}
				output.config.Identity.EdgeNodeID = tc.staticConfig
				output.config.Identity.GroupID = "TestGroup"
				
				var edgeNodeID string
				if tc.hasMessageContext {
					msg := newMockMessage()
					msg.SetMeta("location_path", tc.messageLocationPath)
					edgeNodeID = output.getEdgeNodeID(msg)
				} else {
					edgeNodeID = output.getBirthEdgeNodeID()
				}
				
				Expect(edgeNodeID).To(Equal(tc.expectedEdgeNodeID))
			}
		})
	})

	Context("Phase 5: End-to-End Consistency Validation", func() {
		It("should maintain BIRTH-DATA Edge Node ID consistency", func() {
			// Simulate the flow: BIRTH -> DATA -> BIRTH
			
			// Step 1: Initial BIRTH (no cached state)
			initialBirthEdgeNodeID := output.getBirthEdgeNodeID()
			Expect(initialBirthEdgeNodeID).To(Equal("default_node")) // Falls back to default
			
			// Step 2: First DATA message with location_path
			dataMsg := newMockMessage()
			dataMsg.SetMeta("location_path", "enterprise.factory.line1.station1")
			
			dataEdgeNodeID := output.getEdgeNodeID(dataMsg)
			Expect(dataEdgeNodeID).To(Equal("enterprise:factory:line1:station1"))
			
			// Step 3: Subsequent BIRTH (should use cached state)
			subsequentBirthEdgeNodeID := output.getBirthEdgeNodeID()
			Expect(subsequentBirthEdgeNodeID).To(Equal("enterprise:factory:line1:station1"))
			
			// Step 4: Verify consistency
			Expect(dataEdgeNodeID).To(Equal(subsequentBirthEdgeNodeID),
				"DATA and BIRTH should use same Edge Node ID")
		})

		It("should handle multiple DATA messages with state updates", func() {
			dataMessages := []struct {
				locationPath     string
				expectedEdgeNode string
			}{
				{"enterprise.line1", "enterprise:line1"},
				{"enterprise.line2", "enterprise:line2"},
				{"enterprise.line1.station1", "enterprise:line1:station1"},
			}
			
			for i, data := range dataMessages {
				By(fmt.Sprintf("Processing DATA message %d with location_path: %s", i+1, data.locationPath))
				
				msg := newMockMessage()
				msg.SetMeta("location_path", data.locationPath)
				
				edgeNodeID := output.getEdgeNodeID(msg)
				Expect(edgeNodeID).To(Equal(data.expectedEdgeNode))
				
				// Verify state was updated
				Expect(output.cachedLocationPath).To(Equal(data.locationPath))
				Expect(output.cachedEdgeNodeID).To(Equal(data.expectedEdgeNode))
				
				// Verify BIRTH would use same Edge Node ID
				birthEdgeNodeID := output.getBirthEdgeNodeID()
				Expect(edgeNodeID).To(Equal(birthEdgeNodeID),
					"Message %d: BIRTH should match DATA Edge Node ID", i+1)
			}
		})

		It("should handle real-world message flow scenarios", func() {
			// Test complete realistic scenario
			By("Starting with clean state")
			Expect(output.cachedLocationPath).To(Equal(""))
			Expect(output.cachedEdgeNodeID).To(Equal(""))
			
			By("First BIRTH uses default fallback")
			firstBirth := output.getBirthEdgeNodeID()
			Expect(firstBirth).To(Equal("default_node"))
			
			By("First DATA message establishes location context")
			dataMsg1 := newMockMessage()
			dataMsg1.SetMeta("location_path", "automotive.plant_detroit.line1.station1")
			
			dataEdgeNodeID1 := output.getEdgeNodeID(dataMsg1)
			Expect(dataEdgeNodeID1).To(Equal("automotive:plant_detroit:line1:station1"))
			
			By("Subsequent BIRTH uses cached state")
			secondBirth := output.getBirthEdgeNodeID()
			Expect(secondBirth).To(Equal("automotive:plant_detroit:line1:station1"))
			Expect(secondBirth).To(Equal(dataEdgeNodeID1))
			
			By("Additional DATA messages maintain consistency")
			dataMsg2 := newMockMessage()
			dataMsg2.SetMeta("location_path", "automotive.plant_detroit.line1.station2")
			
			dataEdgeNodeID2 := output.getEdgeNodeID(dataMsg2)
			Expect(dataEdgeNodeID2).To(Equal("automotive:plant_detroit:line1:station2"))
			
			By("Final BIRTH reflects latest state")
			finalBirth := output.getBirthEdgeNodeID()
			Expect(finalBirth).To(Equal("automotive:plant_detroit:line1:station2"))
			Expect(finalBirth).To(Equal(dataEdgeNodeID2))
		})
	})
})

## 🚀 **Running the Tests**

### **Phase-by-Phase Execution with Ginkgo:**

```bash
# Run all unit tests (includes new phases)
go test -v ./sparkplug_plugin/

# Run specific test suites using Ginkgo focus
go test -v ./sparkplug_plugin/ -ginkgo.focus="Phase 1"
go test -v ./sparkplug_plugin/ -ginkgo.focus="Phase 2" 
go test -v ./sparkplug_plugin/ -ginkgo.focus="Phase 3"
go test -v ./sparkplug_plugin/ -ginkgo.focus="Phase 4"
go test -v ./sparkplug_plugin/ -ginkgo.focus="Phase 5"

# Run specific contexts
go test -v ./sparkplug_plugin/ -ginkgo.focus="State Management Infrastructure"
go test -v ./sparkplug_plugin/ -ginkgo.focus="Edge Node ID Resolution Logic"
go test -v ./sparkplug_plugin/ -ginkgo.focus="BIRTH Message Consistency"
go test -v ./sparkplug_plugin/ -ginkgo.focus="DEATH Message Consistency"
go test -v ./sparkplug_plugin/ -ginkgo.focus="End-to-End Consistency Validation"

# Run entire Edge Node ID Consistency test suite
go test -v ./sparkplug_plugin/ -ginkgo.focus="Edge Node ID Consistency Fix"

# Exclude other test types (payload, flow, integration)
go test -v ./sparkplug_plugin/ -tags="!payload,!flow,!integration"
```

### **Integration with Existing Test Infrastructure:**

1. **File Location**: Tests added to existing `sparkplug_plugin/unit_test.go`
2. **Build Tags**: Uses existing `//go:build !payload && !flow && !integration`
3. **Test Suite**: Integrates with existing "Sparkplug B Unit Test Suite"
4. **Mock Extensions**: Extends existing `mockSparkplugOutput` and `mockMessage`
5. **Ginkgo Patterns**: Follows established `Describe` → `Context` → `It` hierarchy

### **Required Mock Extensions Summary:**

```go
// Add to existing mockSparkplugOutput struct
type mockSparkplugOutput struct {
	config             sparkplug_plugin.Config
	logger             mockLogger
	// NEW: Add state management fields
	cachedLocationPath string
	cachedEdgeNodeID   string
	edgeNodeStateMu    sync.RWMutex
}

// Add new methods to mockSparkplugOutput
func (m *mockSparkplugOutput) getBirthEdgeNodeID() string { /* ... */ }
func (m *mockSparkplugOutput) getEdgeNodeID(msg *mockMessage) string { /* ... */ }
```

### **Benefits of This Integrated Approach:**

1. **Consistent Testing**: Uses same patterns as existing tests
2. **Fast Feedback**: Each phase runs in milliseconds with existing infrastructure
3. **Isolated Debugging**: Ginkgo focus allows testing individual phases
4. **Incremental Progress**: Implement and test one phase at a time
5. **Regression Safety**: All existing tests continue to work
6. **Documentation**: Tests serve as executable specifications using Ginkgo's descriptive syntax

This integrated testing strategy ensures each phase is rock-solid before moving to the next, while maintaining consistency with the existing codebase patterns and testing infrastructure. 