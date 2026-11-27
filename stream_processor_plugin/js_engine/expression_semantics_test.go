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

package js_engine_test

import (
	"math"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/config"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/js_engine"
)

// TestMappingExpressionSemanticsUnchanged pins that mapping expressions
// evaluate with exactly the semantics the shipped engine had: raw (unwrapped),
// non-strict, with the program's completion value as the result.
//
// Why this matters: every expression shape below is ACCEPTED by the shipped
// config analyzer (asserted per row), so each is a config a customer can
// already be running. A wrap like (function(){'use strict'; ... })() — the one
// pkg/js_runtime.Compile applies — changes all of them: strict mode rejects
// the octal literal at compile time (which makes PrecompileExpressions fail
// and the pipeline not start) and the undeclared assignment at run time, the
// wrap discards the completion value (a trailing semicolon becomes a nil
// result), and it reparses {a: 1} as an object literal instead of a labeled
// block. That is why the engine keeps the raw, non-strict compile it always
// had, like tag_processor's if conditions. The next person to touch the wrap
// should see this test go red.
func TestMappingExpressionSemanticsUnchanged(t *testing.T) {
	rows := []struct {
		name string
		expr string
		// vars != nil evaluates through EvaluateDynamic with these variables
		vars map[string]interface{}
		want float64
	}{
		{"plain expression", "42", nil, 42},
		{"trailing semicolon", "press * 2;", map[string]interface{}{"press": 25.5}, 51},
		{"octal literal", "010", nil, 8},
		{"undeclared assignment", "tmp = 1", nil, 1},
		{"object literal is a labeled block", "{a: 1}", nil, 1},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			// The shipped config analyzer must accept this shape: if it does,
			// the expression is a config customers can already be running.
			detector := js_engine.NewStaticDetector(map[string]string{"press": "topic.aawd._raw.press"})
			if _, _, err := detector.IsStatic(row.expr); err != nil {
				t.Fatalf("config analyzer rejects %q, so the row is not a shippable config: %v", row.expr, err)
			}

			// Fresh engine per row: rows that create globals (tmp = 1) must
			// not see each other's state.
			resources := service.MockResources()
			engine := js_engine.NewJSEngine(resources.Logger(), []string{"press"})

			var result js_engine.JSExecutionResult
			if row.vars == nil {
				result = engine.EvaluateStatic(row.expr)
			} else {
				result = engine.EvaluateDynamic(row.expr, row.vars)
			}

			if !result.Success {
				t.Fatalf("%q: evaluation failed: %s", row.expr, result.Error)
			}
			if result.Error != "" {
				t.Fatalf("%q: unexpected error on success: %s", row.expr, result.Error)
			}
			got, ok := asFloat(result.Value)
			if !ok {
				t.Fatalf("%q: result is not numeric: %#v", row.expr, result.Value)
			}
			if math.Abs(got-row.want) > 1e-9 {
				t.Fatalf("%q: got %v, want %v", row.expr, got, row.want)
			}
		})
	}

	// Severity check: an octal mapping must not fail PRECOMPILATION, because
	// PrecompileExpressions errors abort NewStreamProcessor and the pipeline
	// does not start.
	t.Run("octal literal must precompile so the pipeline starts", func(t *testing.T) {
		resources := service.MockResources()
		engine := js_engine.NewJSEngine(resources.Logger(), []string{"press"})

		static := map[string]config.MappingInfo{"octal": {Expression: "010"}}
		if err := engine.PrecompileExpressions(static, map[string]config.MappingInfo{}); err != nil {
			t.Fatalf("PrecompileExpressions rejected a shipped-config expression: %v", err)
		}

		result := engine.EvaluateStaticPrecompiled("010")
		if !result.Success {
			t.Fatalf("precompiled 010 failed: %s", result.Error)
		}
		got, ok := asFloat(result.Value)
		if !ok || math.Abs(got-8) > 1e-9 {
			t.Fatalf("precompiled 010: got %#v, want 8", result.Value)
		}
	})
}

func asFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}
