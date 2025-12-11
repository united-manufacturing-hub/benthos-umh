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

package js_engine

import (
	"fmt"

	"github.com/dop251/goja/ast"
	"github.com/dop251/goja/parser"

	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/config"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/mapping"
)

// StaticDetector analyzes JavaScript expressions to identify static vs dynamic mappings
type StaticDetector struct {
	sourceVariables map[string]bool // Set of configured source variables
}

// NewStaticDetector creates a new static detector with the given source variables
func NewStaticDetector(sources map[string]string) *StaticDetector {
	variables := make(map[string]bool)
	for alias := range sources {
		variables[alias] = true
	}
	return &StaticDetector{sourceVariables: variables}
}

// MappingAnalysis contains the result of analyzing a mapping expression
type MappingAnalysis struct {
	Expression   string
	Type         config.MappingType
	Dependencies []string
}

// AnalyzeMapping analyzes a JavaScript expression to determine if it's static or dynamic
func (sd *StaticDetector) AnalyzeMapping(expression string) (MappingAnalysis, error) {
	isStatic, deps, err := sd.IsStatic(expression)
	if err != nil {
		return MappingAnalysis{}, fmt.Errorf("invalid JavaScript expression '%s': %w", expression, err)
	}

	mappingType := config.DynamicMapping
	if isStatic {
		mappingType = config.StaticMapping
	}

	return MappingAnalysis{
		Expression:   expression,
		Type:         mappingType,
		Dependencies: deps,
	}, nil
}

// IsStatic determines if a JavaScript expression is static (no source variable dependencies)
func (sd *StaticDetector) IsStatic(expression string) (bool, []string, error) {
	// Check for empty expressions
	if expression == "" {
		return false, nil, fmt.Errorf("empty expression")
	}

	// Try parsing as expression first (for object literals)
	program, err := parser.ParseFile(nil, "", "("+expression+")", 0)
	if err != nil {
		// If wrapping in parentheses fails, try parsing as-is
		program, err = parser.ParseFile(nil, "", expression, 0)
		if err != nil {
			return false, nil, err
		}
	}

	// Validate that this is an expression, not a statement
	if len(program.Body) != 1 {
		return false, nil, fmt.Errorf("multiple statements not allowed")
	}

	// Check if the single statement is an expression statement
	stmt, ok := program.Body[0].(*ast.ExpressionStatement)
	if !ok {
		return false, nil, fmt.Errorf("only expressions are allowed, not statements")
	}

	// Reject function expressions - mappings should be value expressions only
	if _, isFunctionExpr := stmt.Expression.(*ast.FunctionLiteral); isFunctionExpr {
		return false, nil, fmt.Errorf("function expressions are not allowed in mappings")
	}

	// Extract variable references
	dependencies := sd.extractVariables(stmt.Expression)

	// Check if any dependencies match source variables
	var sourceDeps []string
	for _, dep := range dependencies {
		if sd.sourceVariables[dep] {
			sourceDeps = append(sourceDeps, dep)
		}
	}

	return len(sourceDeps) == 0, sourceDeps, nil
}

// extractVariables walks the AST and extracts all variable references
func (sd *StaticDetector) extractVariables(node ast.Node) []string {
	var variables []string
	sd.walkAST(node, &variables)
	return sd.deduplicateStrings(variables)
}

// walkAST recursively walks the AST and collects variable identifiers
func (sd *StaticDetector) walkAST(node ast.Node, variables *[]string) {
	if node == nil {
		return
	}

	switch n := node.(type) {
	case *ast.Identifier:
		// Convert unistring.String to string and skip built-in JavaScript objects/functions
		name := n.Name.String()
		if !sd.isBuiltIn(name) {
			*variables = append(*variables, name)
		}
	case *ast.Program:
		for _, stmt := range n.Body {
			sd.walkAST(stmt, variables)
		}
	case *ast.ExpressionStatement:
		sd.walkAST(n.Expression, variables)
	case *ast.BinaryExpression:
		sd.walkAST(n.Left, variables)
		sd.walkAST(n.Right, variables)
	case *ast.UnaryExpression:
		sd.walkAST(n.Operand, variables)
	case *ast.CallExpression:
		sd.walkAST(n.Callee, variables)
		for _, arg := range n.ArgumentList {
			sd.walkAST(arg, variables)
		}
	case *ast.DotExpression:
		sd.walkAST(n.Left, variables)
		// Don't walk the right side as it's a property name, not a variable
	case *ast.BracketExpression:
		sd.walkAST(n.Left, variables)
		sd.walkAST(n.Member, variables)
	case *ast.ConditionalExpression:
		sd.walkAST(n.Test, variables)
		sd.walkAST(n.Consequent, variables)
		sd.walkAST(n.Alternate, variables)
	case *ast.ArrayLiteral:
		for _, element := range n.Value {
			sd.walkAST(element, variables)
		}
	// Literals don't contain variable references
	case *ast.StringLiteral, *ast.NumberLiteral, *ast.BooleanLiteral, *ast.NullLiteral:
		// No variables to extract
	default:
		// For any other node types, we'll be conservative and not extract variables
		// This avoids false positives while still catching the common cases above
	}
}

// isBuiltIn checks if a name is a built-in JavaScript object or function
// This list is specifically tailored for Goja which implements ES5.1 + select ES6 features
func (sd *StaticDetector) isBuiltIn(name string) bool {
	builtIns := map[string]bool{
		// ===== ES5.1 GLOBAL VALUES =====
		"undefined": true,
		"null":      true,
		"true":      true,
		"false":     true,
		"Infinity":  true,
		"NaN":       true,

		// ===== ES5.1 GLOBAL FUNCTIONS =====
		"eval":               true,
		"parseInt":           true,
		"parseFloat":         true,
		"isNaN":              true,
		"isFinite":           true,
		"decodeURI":          true,
		"decodeURIComponent": true,
		"encodeURI":          true,
		"encodeURIComponent": true,
		"escape":             true, // Deprecated but still exists
		"unescape":           true, // Deprecated but still exists

		// ===== ES5.1 CONSTRUCTOR FUNCTIONS =====
		"Object":   true,
		"Function": true,
		"Array":    true,
		"String":   true,
		"Boolean":  true,
		"Number":   true,
		"Date":     true,
		"RegExp":   true,

		// ===== ES5.1 ERROR CONSTRUCTORS =====
		"Error":          true,
		"EvalError":      true,
		"RangeError":     true,
		"ReferenceError": true,
		"SyntaxError":    true,
		"TypeError":      true,
		"URIError":       true,

		// ===== ES5.1 NON-CONSTRUCTOR GLOBAL OBJECTS =====
		"Math": true,
		"JSON": true,

		// ===== ES6 FEATURES SUPPORTED BY GOJA =====
		"Map":     true,
		"Set":     true,
		"WeakMap": true,
		"WeakSet": true,
		"Promise": true,
		"Symbol":  true,
		"Proxy":   true,
		"Reflect": true,

		// ===== COMMONLY AVAILABLE (NOT ECMASCRIPT STANDARD) =====
		"console": true, // Widely available, often considered built-in

		// ===== ES6+ FEATURES NOT SUPPORTED BY GOJA =====
		// NOTE: These are commented out because Goja doesn't implement them
		// If you're using a different JS engine, you might want to include these:
		//
		// "BigInt":               true, // Not implemented in Goja
		// "WeakRef":              true, // Not possible due to Go GC
		// "FinalizationRegistry": true, // Not possible due to Go GC
		// "ArrayBuffer":          true, // Limited/no support
		// "DataView":             true, // Limited/no support
		// "Int8Array":            true, // Limited/no support
		// "Uint8Array":           true, // Limited/no support
		// "Float32Array":         true, // Limited/no support
		// etc. (other TypedArrays)
		//
		// "Intl":                 true, // Not implemented
		// "Atomics":              true, // Not implemented
		// "SharedArrayBuffer":    true, // Not implemented
		// "AggregateError":       true, // ES2021, not supported
	}
	return builtIns[name]
}

// deduplicateStrings removes duplicate strings from a slice
func (sd *StaticDetector) deduplicateStrings(slice []string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, item := range slice {
		if !seen[item] {
			seen[item] = true
			result = append(result, item)
		}
	}
	return result
}

// AnalyzeMappingsWithDetection performs the actual mapping analysis using AST parsing
func AnalyzeMappingsWithDetection(cfg *config.StreamProcessorConfig) error {
	cfg.StaticMappings = make(map[string]config.MappingInfo)
	cfg.DynamicMappings = make(map[string]config.MappingInfo)

	if cfg.Mapping == nil {
		return nil
	}

	detector := NewStaticDetector(cfg.Sources)

	// Flatten nested mappings and analyze each
	flattened := mapping.FlattenMappings(cfg.Mapping)
	for virtualPath, expression := range flattened {
		analysis, err := detector.AnalyzeMapping(expression)
		if err != nil {
			return fmt.Errorf("failed to analyze mapping '%s': %w", virtualPath, err)
		}

		mappingInfo := config.MappingInfo{
			VirtualPath:  virtualPath,
			Expression:   expression,
			Type:         analysis.Type,
			Dependencies: analysis.Dependencies,
		}

		if analysis.Type == config.StaticMapping {
			cfg.StaticMappings[virtualPath] = mappingInfo
		} else {
			cfg.DynamicMappings[virtualPath] = mappingInfo
		}
	}

	return nil
}
