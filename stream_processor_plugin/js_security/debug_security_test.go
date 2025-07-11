package js_security

import (
	"fmt"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/js_engine"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/pools"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestSecurityBlockerDebug(t *testing.T) {
	// Create test resources
	resources := service.MockResources()

	// Create pools and JS engine
	pools := pools.NewObjectPools([]string{"press"}, resources.Logger())
	jsEngine := js_engine.NewJSEngine(resources.Logger(), []string{"press"}, pools)
	defer func() {
		err := jsEngine.Close()
		if err != nil {
			t.Errorf("Failed to close JS engine: %v", err)
		}
	}()

	dangerousOperations := []string{
		"eval('1+1')",
		"Function('return 1+1')()",
		"require('fs')",
		"console.log('test')",
		"typeof eval",
		"typeof Function",
		"typeof require",
		"typeof console",
	}

	fmt.Println("\n=== Security Blocker Debug Test ===")
	for _, expr := range dangerousOperations {
		fmt.Printf("\nTesting: %s\n", expr)
		result := jsEngine.EvaluateStatic(expr)
		fmt.Printf("  Success: %t\n", result.Success)
		fmt.Printf("  Value: %v (type: %T)\n", result.Value, result.Value)
		fmt.Printf("  Error: %s\n", result.Error)
	}

	// Test calling the security blockers directly
	fmt.Println("\n=== Direct Security Blocker Calls ===")
	directCalls := []string{
		"eval()",
		"require()",
		"Function()",
		"console()",
	}

	for _, expr := range directCalls {
		fmt.Printf("\nTesting direct call: %s\n", expr)
		result := jsEngine.EvaluateStatic(expr)
		fmt.Printf("  Success: %t\n", result.Success)
		fmt.Printf("  Value: %v (type: %T)\n", result.Value, result.Value)
		fmt.Printf("  Error: %s\n", result.Error)
	}
}
