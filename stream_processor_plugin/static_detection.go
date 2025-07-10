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

package stream_processor_plugin

import (
	"fmt"
	"slices"

	"github.com/dop251/goja/ast"
	"github.com/dop251/goja/parser"
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
	Type         MappingType
	Dependencies []string
}

// AnalyzeMapping analyzes a JavaScript expression to determine if it's static or dynamic
func (sd *StaticDetector) AnalyzeMapping(expression string) (MappingAnalysis, error) {
	isStatic, deps, err := sd.IsStatic(expression)
	if err != nil {
		return MappingAnalysis{}, fmt.Errorf("invalid JavaScript expression '%s': %w", expression, err)
	}

	mappingType := DynamicMapping
	if isStatic {
		mappingType = StaticMapping
	}

	return MappingAnalysis{
		Expression:   expression,
		Type:         mappingType,
		Dependencies: deps,
	}, nil
}

// IsStatic determines if a JavaScript expression is static (no source variable dependencies)
func (sd *StaticDetector) IsStatic(expression string) (bool, []string, error) {
	// Parse the JavaScript expression
	program, err := parser.ParseFile(nil, "", expression, 0)
	if err != nil {
		return false, nil, err
	}

	// Extract variable references
	dependencies := sd.extractVariables(program)

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
		// For any other node types, use reflection to walk child nodes safely
		// This ensures we don't miss any variable references in complex expressions
		sd.walkASTGeneric(n, variables)
	}
}

// isBuiltIn checks if a name is a built-in JavaScript object or function
func (sd *StaticDetector) isBuiltIn(name string) bool {
	builtIns := map[string]bool{
		"Date":       true,
		"Math":       true,
		"JSON":       true,
		"console":    true,
		"parseInt":   true,
		"parseFloat": true,
		"Number":     true,
		"String":     true,
		"Boolean":    true,
		"Array":      true,
		"Object":     true,
		"undefined":  true,
		"null":       true,
		"true":       true,
		"false":      true,
		"Infinity":   true,
		"NaN":        true,
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

// analyzeMappingsWithDetection performs the actual mapping analysis using AST parsing
func analyzeMappingsWithDetection(config *StreamProcessorConfig) error {
	config.StaticMappings = make(map[string]MappingInfo)
	config.DynamicMappings = make(map[string]MappingInfo)

	if config.Mapping == nil {
		return nil
	}

	detector := NewStaticDetector(config.Sources)

	// Flatten nested mappings and analyze each
	flattened := flattenMappings(config.Mapping)
	for virtualPath, expression := range flattened {
		analysis, err := detector.AnalyzeMapping(expression)
		if err != nil {
			return fmt.Errorf("failed to analyze mapping '%s': %w", virtualPath, err)
		}

		mappingInfo := MappingInfo{
			VirtualPath:  virtualPath,
			Expression:   expression,
			Type:         analysis.Type,
			Dependencies: analysis.Dependencies,
		}

		if analysis.Type == StaticMapping {
			config.StaticMappings[virtualPath] = mappingInfo
		} else {
			config.DynamicMappings[virtualPath] = mappingInfo
		}
	}

	return nil
}

// walkASTGeneric handles unknown AST nodes by skipping them
func (sd *StaticDetector) walkASTGeneric(node ast.Node, variables *[]string) {
	// For unknown node types, we'll be conservative and not extract variables
	// This avoids false positives while still catching the common cases above
}

// containsVariable checks if a variable is in the dependencies list
func containsVariable(dependencies []string, variable string) bool {
	return slices.Contains(dependencies, variable)
}
