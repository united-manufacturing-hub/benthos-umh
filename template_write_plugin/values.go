// Copyright 2026 UMH Systems GmbH
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

package template_write_plugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

// Helpers every codec needs to turn a decoded JSON value into a typed one.
// They are strict on purpose: a value that does not fit is an error, never a
// silent wrap or cut, because a wrapped setpoint is worse than no write.

// ErrMissingField means the message has no value at the mapped path.
var ErrMissingField = errors.New("message has no such field")

// Field returns the value at a dot-separated path, such as "recipe.speed".
func Field(structured any, path string) (any, error) {
	current := structured
	for _, key := range strings.Split(path, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: %q (%q is not an object)", ErrMissingField, path, key)
		}
		current, ok = object[key]
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrMissingField, path)
		}
	}
	return current, nil
}

// Bool accepts only JSON booleans. 0 and 1 are refused, so a number meant for
// another address cannot flip a bit by accident.
func Bool(value any) (bool, error) {
	typed, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("%w: need a bool, got %T", ErrInvalidValue, value)
	}
	return typed, nil
}

// String accepts only JSON strings.
func String(value any) (string, error) {
	typed, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%w: need a string, got %T", ErrInvalidValue, value)
	}
	return typed, nil
}

// IntegerInRange accepts JSON numbers and Go integers, rejects fractions, and
// checks the result against [minimum, maximum].
func IntegerInRange(value any, minimum int64, maximum int64) (int64, error) {
	var number int64
	switch typed := value.(type) {
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, fmt.Errorf("%w: %q is not an integer", ErrInvalidValue, typed)
		}
		number = parsed
	case float64:
		if typed != math.Trunc(typed) || typed < math.MinInt64 || typed > math.MaxInt64 {
			return 0, fmt.Errorf("%w: %v is not an integer", ErrInvalidValue, typed)
		}
		number = int64(typed)
	case int:
		number = int64(typed)
	case int64:
		number = typed
	default:
		return 0, fmt.Errorf("%w: need an integer, got %T", ErrInvalidValue, value)
	}

	if number < minimum || number > maximum {
		return 0, fmt.Errorf("%w: %d is outside %d..%d", ErrInvalidValue, number, minimum, maximum)
	}
	return number, nil
}

// Float32 accepts any finite number that fits a 32-bit float.
func Float32(value any) (float32, error) {
	number, err := Float64(value)
	if err != nil {
		return 0, err
	}
	if math.Abs(number) > math.MaxFloat32 {
		return 0, fmt.Errorf("%w: %v does not fit a 32-bit float", ErrInvalidValue, number)
	}
	return float32(number), nil
}

// Float64 accepts any finite number.
func Float64(value any) (float64, error) {
	var number float64
	switch typed := value.(type) {
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return 0, fmt.Errorf("%w: %q is not a number", ErrInvalidValue, typed)
		}
		number = parsed
	case float64:
		number = typed
	case int:
		number = float64(typed)
	case int64:
		number = float64(typed)
	default:
		return 0, fmt.Errorf("%w: need a number, got %T", ErrInvalidValue, value)
	}
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return 0, fmt.Errorf("%w: %v is not finite", ErrInvalidValue, number)
	}
	return number, nil
}
