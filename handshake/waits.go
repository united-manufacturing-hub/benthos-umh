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

package handshake

import (
	"math"
	"reflect"
	"time"

	"github.com/dop251/goja/ast"
	"github.com/dop251/goja/file"
	"github.com/dop251/goja/token"
)

const (
	// maxLiteralWait is what literalWaitTotal reports for a literal, or a
	// sum, that a Duration cannot hold. It is larger than any budget, so it
	// always loses the comparison it exists for.
	maxLiteralWait = time.Duration(math.MaxInt64)
	// maxLiteralMillis is the largest millisecond literal that converts to a
	// Duration without overflowing.
	maxLiteralMillis = float64(maxLiteralWait / time.Millisecond)
)

// literalWaitTotal sums the literal timeouts of a block's wait_until calls,
// read from the parsed block. A call counts when its callee is named
// wait_until, as opcua.wait_until or bare; Validate does not know the
// binding's namespace. A timeout counts when it is a number literal, with or
// without a unary plus. Anything else is computed, hence a person's to check
// (SCR-4), and counts as nothing rather than refusing what it cannot read.
//
// The sum saturates at maxLiteralWait rather than wrapping: a literal too
// big for a Duration is over every budget, which is the answer the check
// wants.
func literalWaitTotal(tree *ast.Program) time.Duration {
	total := time.Duration(0)
	for _, call := range waitCalls(tree) {
		if len(call.ArgumentList) < 3 {
			continue
		}
		ms, ok := literalMillis(call.ArgumentList[2])
		if !ok {
			continue
		}
		if ms >= maxLiteralMillis {
			return maxLiteralWait
		}
		d := time.Duration(ms) * time.Millisecond
		if total > maxLiteralWait-d {
			return maxLiteralWait
		}
		total += d
	}
	return total
}

// waitCalls returns every call to wait_until in the tree, each once.
func waitCalls(tree *ast.Program) []*ast.CallExpression {
	var calls []*ast.CallExpression
	seen := map[*ast.CallExpression]bool{}
	walk(reflect.ValueOf(tree), func(call *ast.CallExpression) {
		// A function's DeclarationList holds its var declarations a second
		// time, beside its body, so a call can be reached twice.
		if !seen[call] && calleeName(call.Callee) == "wait_until" {
			seen[call] = true
			calls = append(calls, call)
		}
	})
	return calls
}

// walk visits every call expression under v. goja's ast package has no
// walker, and a hand-written one would have to list every node type; the
// nodes are plain structs, slices and interfaces, so reflection reaches all
// of them. The source file is skipped: it holds text, not nodes.
func walk(v reflect.Value, visit func(*ast.CallExpression)) {
	switch v.Kind() {
	case reflect.Interface:
		if !v.IsNil() {
			walk(v.Elem(), visit)
		}
	case reflect.Pointer:
		if v.IsNil() || v.Type() == reflect.TypeFor[*file.File]() {
			return
		}
		if call, ok := v.Interface().(*ast.CallExpression); ok {
			visit(call)
		}
		walk(v.Elem(), visit)
	case reflect.Struct:
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				walk(v.Field(i), visit)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := range v.Len() {
			walk(v.Index(i), visit)
		}
	default:
	}
}

// calleeName is the name a call is made by: the property of a dot
// expression, or a bare identifier. Anything else has no name to match.
func calleeName(callee ast.Expression) string {
	switch c := callee.(type) {
	case *ast.DotExpression:
		return string(c.Identifier.Name)
	case *ast.Identifier:
		return string(c.Name)
	default:
		return ""
	}
}

// literalMillis reads a number literal, or one with a unary plus, which is
// applied to a literal rather than computed from one. Every spelling of a
// plain number (5000, 5e3, .5e4, 0x1388, 5_000) reaches the parser as the
// same value, so none of them slips past as computed.
func literalMillis(arg ast.Expression) (float64, bool) {
	if u, ok := arg.(*ast.UnaryExpression); ok && u.Operator == token.PLUS && !u.Postfix {
		arg = u.Operand
	}
	n, ok := arg.(*ast.NumberLiteral)
	if !ok {
		return 0, false
	}
	switch v := n.Value.(type) {
	case int64:
		return float64(v), v >= 0
	case float64:
		return v, v >= 0 && !math.IsNaN(v)
	default:
		return 0, false
	}
}
