# Static Field Detection Implementation

## Overview

To identify static mappings (expressions that don't reference any source variables), we analyze JavaScript expressions using AST parsing at configuration load time. This creates two separate maps for efficient runtime processing.

## AST Parsing Approach

Use `goja`'s built-in parser to create an Abstract Syntax Tree and extract variable references.

```go
package main

import (
    "github.com/dop251/goja"
    "github.com/dop251/goja/ast"
    "github.com/dop251/goja/parser"
)

type StaticDetector struct {
    sourceVariables map[string]bool // Set of configured source variables
}

func NewStaticDetector(sources map[string]string) *StaticDetector {
    variables := make(map[string]bool)
    for alias := range sources {
        variables[alias] = true
    }
    return &StaticDetector{sourceVariables: variables}
}

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

func (sd *StaticDetector) extractVariables(node ast.Node) []string {
    var variables []string
    
    ast.Walk(node, func(n ast.Node) bool {
        switch node := n.(type) {
        case *ast.Identifier:
            // Skip built-in JavaScript objects/functions
            if !sd.isBuiltIn(node.Name) {
                variables = append(variables, node.Name)
            }
        }
        return true
    })
    
    return sd.deduplicateStrings(variables)
}

func (sd *StaticDetector) isBuiltIn(name string) bool {
    builtIns := map[string]bool{
        "Date": true, "Math": true, "JSON": true, "console": true,
        "parseInt": true, "parseFloat": true, "Number": true,
        "String": true, "Boolean": true, "Array": true, "Object": true,
    }
    return builtIns[name]
}

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
```

## Implementation

```go
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

type MappingAnalysis struct {
    Expression   string
    Type         MappingType
    Dependencies []string
}
```

## Usage Example

```go
// Configuration
sources := map[string]string{
    "press": "umh.v1.corpA.plant-A.aawd._raw.press",
    "tF":    "umh.v1.corpA.plant-A.aawd._raw.tempF",
    "r":     "umh.v1.corpA.plant-A.aawd._raw.run",
}

detector := NewStaticDetector(sources)

// Test expressions
expressions := map[string]string{
    "pressure":     "press+4.00001",        // Dynamic: depends on 'press'
    "temperature":  "tF*69/31",             // Dynamic: depends on 'tF'
    "serialNumber": `"SN-P42-008"`,         // Static: string constant
    "deviceType":   `"pump"`,               // Static: string constant
    "timestamp":    "Date.now()",           // Static: no source variables
    "version":      "42",                   // Static: numeric constant
    "combined":     "press + tF",           // Dynamic: depends on 'press', 'tF'
}

for name, expr := range expressions {
    analysis, err := detector.AnalyzeMapping(expr)
    if err != nil {
        fmt.Printf("%s: ERROR - %v\n", name, err)
        continue
    }
    fmt.Printf("%s: %s - deps: %v\n", 
        name, analysis.Type, analysis.Dependencies)
}
```

## Expected Output

```
pressure: DynamicMapping - deps: [press]
temperature: DynamicMapping - deps: [tF]
serialNumber: StaticMapping - deps: []
deviceType: StaticMapping - deps: []
timestamp: StaticMapping - deps: []
version: StaticMapping - deps: []
combined: DynamicMapping - deps: [press tF]
```

## Integration with Config Parser

```go
func (c *Config) analyzeMappings() error {
    detector := NewStaticDetector(c.Sources)
    
    c.StaticMappings = make(map[string]MappingInfo)
    c.DynamicMappings = make(map[string]MappingInfo)
    
    // Flatten nested mappings and analyze each
    for virtualPath, expression := range c.flattenMappings() {
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
            c.StaticMappings[virtualPath] = mappingInfo
        } else {
            c.DynamicMappings[virtualPath] = mappingInfo
        }
    }
    
    return nil
}
```

## Advantages of Each Approach

1. **AST Parsing**: Most accurate, handles complex expressions, understands JavaScript semantics
2. **Runtime Evaluation**: Simple, catches edge cases, validates expression syntax
3. **String Analysis**: Fast, simple fallback, no external dependencies

## Recommendation

Use the **hybrid approach** starting with AST parsing for accuracy, falling back to runtime evaluation, and finally string analysis if needed. This provides the best balance of accuracy and robustness. 