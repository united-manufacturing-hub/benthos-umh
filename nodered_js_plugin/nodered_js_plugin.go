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

package nodered_js_plugin

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/dop251/goja"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// NodeREDJSProcessor defines the processor that wraps the JavaScript processor.
type NodeREDJSProcessor struct {
	program           *goja.Program
	originalCode      string
	vmpool            sync.Pool
	logger            *service.Logger
	messagesProcessed *service.MetricCounter
	messagesErrored   *service.MetricCounter
	messagesDropped   *service.MetricCounter
	vmPoolHits        *service.MetricCounter
	vmPoolMisses      *service.MetricCounter
}

// NewNodeREDJSProcessor creates a new NodeREDJSProcessor instance.
func NewNodeREDJSProcessor(code string, logger *service.Logger, metrics *service.Metrics) (*NodeREDJSProcessor, error) {
	// Compile the JavaScript code once
	program, err := goja.Compile("nodered-fn.js", code, false)
	if err != nil {
		return nil, fmt.Errorf("failed to compile JavaScript code: %w", err)
	}

	processor := &NodeREDJSProcessor{
		program:           program,
		originalCode:      code,
		vmpool:            sync.Pool{}, // No New function - Get() will return nil when pool is empty
		logger:            logger,
		messagesProcessed: metrics.NewCounter("messages_processed"),
		messagesErrored:   metrics.NewCounter("messages_errored"),
		messagesDropped:   metrics.NewCounter("messages_dropped"),
		vmPoolHits:        metrics.NewCounter("vm_pool_hits"),
		vmPoolMisses:      metrics.NewCounter("vm_pool_misses"),
	}

	return processor, nil
}

// GetVM acquires a VM from the pool and tracks metrics
func (u *NodeREDJSProcessor) GetVM() *goja.Runtime {
	poolResult := u.vmpool.Get()
	if poolResult == nil {
		u.vmPoolMisses.Incr(1)
		return goja.New()
	}
	u.vmPoolHits.Incr(1)
	return poolResult.(*goja.Runtime)
}

// PutVM returns a VM to the pool after comprehensive cleanup
func (u *NodeREDJSProcessor) PutVM(vm *goja.Runtime) {
	// Comprehensive VM cleanup to prevent state leakage
	if err := u.clearVMState(vm); err != nil {
		u.logger.Errorf("Failed to clear VM state: %v", err)
		// In case of an error, we do not return the VM to the pool
		// because it might be in an invalid state
		return
	}
	// Return cleaned VM to pool for reuse
	u.vmpool.Put(vm)
}

// clearVMState performs comprehensive cleanup of VM state
func (u *NodeREDJSProcessor) clearVMState(vm *goja.Runtime) error {
	// Clear any interrupt flag that might be set
	vm.ClearInterrupt()

	return vm.GlobalObject().Set("msg", nil)
}

// getVM acquires a VM from the pool and tracks metrics (internal method)
func (u *NodeREDJSProcessor) getVM() *goja.Runtime {
	return u.GetVM()
}

// putVM returns a VM to the pool after proper cleanup (internal method)
func (u *NodeREDJSProcessor) putVM(vm *goja.Runtime) {
	u.PutVM(vm)
}

// ConvertMessageToJSObject converts a Benthos message to a JavaScript-compatible object with the payload being in the payload field.
func ConvertMessageToJSObject(msg *service.Message) (map[string]interface{}, error) {
	// Try to get structured data first
	structured, err := msg.AsStructured()
	if err != nil {
		// If message can't be converted to structured format, wrap raw bytes in a payload field
		bytes, err := msg.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("failed to convert message to bytes: %w", err)
		}
		return map[string]interface{}{
			"payload": string(bytes),
		}, nil
	}

	// Always wrap the structured data in a payload field
	return map[string]interface{}{
		"payload": structured,
	}, nil
}

func isEscapedString(data string) bool {
	return !strings.ContainsAny(data, " '\"`\\\n\r\t\b\f")
}

// Return either an escaped version of k if it contains
// any special character or just return plain k
func escapeKey(k string) string {
	if !isEscapedString(k) {
		return escapeString(k)
	}
	return k
}

// Escape a given string for printing within logs and optimized
// for being embedded in JSON by using single quotes rather than
// double quotes.
func escapeString(data string) string {
	var builder strings.Builder
	builder.Grow(len(data) + 2 + len(data)/5) // string length + 2 slots for quotes + 20% headroom for escaped characters to avoid additional allocation
	builder.WriteByte('\'')
	for _, rune := range data {
		switch rune {
		case '\'':
			builder.WriteString(`\'`)
		case '\\':
			builder.WriteString(`\\`)
		case '\n':
			builder.WriteString(`\n`)
		case '\r':
			builder.WriteString(`\r`)
		case '\t':
			builder.WriteString(`\t`)
		case '\b':
			builder.WriteString(`\b`)
		case '\f':
			builder.WriteString(`\f`)
		default:
			builder.WriteRune(rune)
		}
	}
	builder.WriteByte('\'')
	return builder.String()
}

// Prints any object in a string format that is close to how the NodeJS console.log
// implementation formats objects. This format is optimized to have as few escaped
// characters as possible when it is embedded within a JSON payload.
func stringify(data any, depth uint8) (string, error) {
	depth += 1
	if depth == math.MaxUint8 {
		return "", fmt.Errorf("maximum depth reached")
	}
	if data == nil {
		return "null", nil
	}
	if d, ok := data.(string); ok {
		return escapeString(d), nil
	}
	if d, ok := data.(bool); ok {
		return strconv.FormatBool(d), nil
	}
	if d, ok := data.(int); ok {
		return strconv.FormatInt(int64(d), 10), nil
	}
	if d, ok := data.(int8); ok {
		return strconv.FormatInt(int64(d), 10), nil
	}
	if d, ok := data.(int16); ok {
		return strconv.FormatInt(int64(d), 10), nil
	}
	if d, ok := data.(int32); ok {
		return strconv.FormatInt(int64(d), 10), nil
	}
	if d, ok := data.(int64); ok {
		return strconv.FormatInt(d, 10), nil
	}
	if d, ok := data.(uint); ok {
		return strconv.FormatUint(uint64(d), 10), nil
	}
	if d, ok := data.(uint8); ok {
		return strconv.FormatUint(uint64(d), 10), nil
	}
	if d, ok := data.(uint16); ok {
		return strconv.FormatUint(uint64(d), 10), nil
	}
	if d, ok := data.(uint32); ok {
		return strconv.FormatUint(uint64(d), 10), nil
	}
	if d, ok := data.(uint64); ok {
		return strconv.FormatUint(d, 10), nil
	}
	if d, ok := data.(float64); ok {
		if math.IsInf(d, 1) {
			return "Infinity", nil
		}
		if math.IsInf(d, -1) {
			return "-Infinity", nil
		}
		if math.IsNaN(d) {
			return "NaN", nil
		}

		return strconv.FormatFloat(d, 'g', -1, 64), nil
	}
	if d, ok := data.(*big.Int); ok {
		return d.String(), nil // directly return the .String() output without quotes as this is a number
	}
	if d, ok := data.(*big.Float); ok {
		return d.String(), nil // directly return the .String() output without quotes as this is a number
	}
	if err, ok := data.(error); ok {
		return escapeString(err.Error()), nil // go through stringify to properly escape and display the now converted string
	}
	if d, ok := data.(fmt.Stringer); ok {
		return escapeString(d.String()), nil // go through stringify to properly escape and display the now converted string
	}
	if d, ok := data.([]any); ok {
		if len(d) == 0 {
			return "[]", nil
		}
		var buff strings.Builder
		buff.WriteString("[ ")
		for idx, value := range d {
			valueString, err := stringify(value, depth)
			if err != nil {
				return "", err
			}
			if idx > 0 {
				buff.WriteString(", ")
			}
			buff.WriteString(valueString)
		}
		buff.WriteString(" ]")
		return buff.String(), nil
	}
	if d, ok := data.(map[string]any); ok {
		keys := slices.Collect(maps.Keys(d))
		if len(keys) == 0 {
			return "{}", nil
		}
		slices.Sort(keys)
		var buff strings.Builder
		buff.WriteString("{ ")
		for idx, key := range keys {
			value := d[key]
			valueString, err := stringify(value, depth)
			if err != nil {
				return "", err
			}
			if idx > 0 {
				buff.WriteString(", ")
			}
			escapedKey := escapeKey(key)
			keyValueSeparator := ": "
			buff.Grow(len(escapedKey) + len(keyValueSeparator) + len(valueString))
			buff.WriteString(escapedKey)
			buff.WriteString(keyValueSeparator)
			buff.WriteString(valueString)
		}
		buff.WriteString(" }")
		return buff.String(), nil
	}

	// fallback to encode unknown values
	return fmt.Sprintf("%#v", data), nil
}

// SetupJSEnvironment sets up the JavaScript VM environment.
func (u *NodeREDJSProcessor) SetupJSEnvironment(vm *goja.Runtime, jsMsg map[string]interface{}) error {
	// Set up the msg variable in the JS environment
	if err := vm.Set("msg", jsMsg); err != nil {
		return fmt.Errorf("failed to set message in JS environment: %w", err)
	}

	// Set up console for logging that uses Benthos logger
	console := map[string]any{
		"debug": func(data ...any) { u.logger.Debug(FormatConsoleLogMsg(data)) },
		"log":   func(data ...any) { u.logger.Info(FormatConsoleLogMsg(data)) },
		"info":  func(data ...any) { u.logger.Info(FormatConsoleLogMsg(data)) },
		"warn":  func(data ...any) { u.logger.Warn(FormatConsoleLogMsg(data)) },
		"error": func(data ...any) { u.logger.Error(FormatConsoleLogMsg(data)) },
	}

	if err := vm.Set("console", console); err != nil {
		return fmt.Errorf("failed to set console in JS environment: %w", err)
	}

	return nil
}

// HandleExecutionResult handles JavaScript execution results.
func (u *NodeREDJSProcessor) HandleExecutionResult(result goja.Value) (*service.Message, bool, error) {
	// Handle null/undefined returns
	if result.Equals(goja.Undefined()) || result.Equals(goja.Null()) {
		u.messagesDropped.Incr(1)
		u.logger.Debug("Message dropped due to null/undefined return")
		return service.NewMessage(nil), false, nil
	}

	// Convert return value to message object
	returnedMsg, ok := result.Export().(map[string]interface{})
	if !ok {
		return service.NewMessage(nil), false, fmt.Errorf("function must return a message object or null")
	}

	// Create new message with returned content
	newMsg := service.NewMessage(nil)
	if payload, exists := returnedMsg["payload"]; exists {
		newMsg.SetStructured(payload)
	}
	if meta, exists := returnedMsg["meta"].(map[string]interface{}); exists {
		for k, v := range meta {
			if str, ok := v.(string); ok {
				newMsg.MetaSet(k, str)
			}
		}
	}

	return newMsg, true, nil
}

func FormatConsoleLogMsg(data []any) string {
	buf := make([]string, 0, len(data))
	for _, d := range data {
		serialized, err := stringify(d, 0)
		if err != nil {
			// if conversion for the whole object fails fall back to golangs debug print
			// this may happen if the recursion depth limit is reached
			serialized = fmt.Sprintf("%#v", d)
		}
		buf = append(buf, serialized)
	}
	return strings.Join(buf, " ")
}

// ProcessBatch applies the JavaScript code to each message in the batch.
func (u *NodeREDJSProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var resultBatch service.MessageBatch

	for _, msg := range batch {
		u.messagesProcessed.Incr(1)

		// Process single message and return VM to pool immediately
		processedMsg, shouldKeep, err := u.processSingleMessage(msg)
		if err != nil {
			return nil, err
		}
		if shouldKeep {
			resultBatch = append(resultBatch, processedMsg)
		}
	}

	if len(resultBatch) == 0 {
		return []service.MessageBatch{}, nil
	}

	return []service.MessageBatch{resultBatch}, nil
}

// processSingleMessage processes a single message using a VM from the pool
func (u *NodeREDJSProcessor) processSingleMessage(msg *service.Message) (*service.Message, bool, error) {
	vm := u.getVM()
	defer u.putVM(vm)

	// Convert message to JS object
	jsMsg, err := ConvertMessageToJSObject(msg)
	if err != nil {
		u.messagesErrored.Incr(1)
		u.logger.Errorf("%v\nOriginal message: %v", err, msg)
		return nil, false, nil
	}

	// Add metadata to the message wrapper
	meta := make(map[string]interface{})
	if err = msg.MetaWalkMut(func(key string, value any) error {
		meta[key] = value
		return nil
	}); err != nil {
		u.messagesErrored.Incr(1)
		u.logger.Errorf("Failed to walk message metadata: %v\nOriginal message: %v", err, msg)
		return nil, false, nil
	}
	jsMsg["meta"] = meta

	// Setup JS environment
	if err = u.SetupJSEnvironment(vm, jsMsg); err != nil {
		u.messagesErrored.Incr(1)
		u.logger.Errorf("%v\nMessage content: %v", err, jsMsg)
		return nil, false, nil
	}

	// Execute the compiled JavaScript program
	result, err := vm.RunProgram(u.program)
	if err != nil {
		u.messagesErrored.Incr(1)
		u.logJSError(err, jsMsg)
		return nil, false, nil
	}

	// Handle the execution result
	newMsg, shouldKeep, err := u.HandleExecutionResult(result)
	if err != nil {
		u.messagesErrored.Incr(1)
		u.logger.Errorf("%v\nMessage content: %v\nReturned value: %v", err, jsMsg, result.Export())
		return nil, false, err
	}

	return newMsg, shouldKeep, nil
}

// Helper function to log JavaScript errors
func (u *NodeREDJSProcessor) logJSError(err error, jsMsg interface{}) {
	jsErr := &goja.Exception{}
	if errors.As(err, &jsErr) {
		stack := jsErr.String()
		u.logger.Errorf(`JavaScript execution failed:
Error: %v
Stack: %v
Code:
%v
Message content: %v`,
			jsErr.Error(),
			stack,
			u.originalCode,
			jsMsg)
	} else {
		u.logger.Errorf(`JavaScript execution failed:
Error: %v
Code:
%v
Message content: %v`,
			err,
			u.originalCode,
			jsMsg)
	}
}

// Close gracefully shuts down the processor.
func (u *NodeREDJSProcessor) Close(ctx context.Context) error {
	return nil
}

func init() {
	spec := service.NewConfigSpec().
		Version("1.0.0").
		Summary("A Node-RED style JavaScript processor.").
		Description("Executes user-defined JavaScript code to process messages in a format similar to Node-RED functions.").
		Field(service.NewStringField("code").
			Description("The JavaScript code to execute. The code should be a function that processes the message.").
			Example(`// Node-RED style function that returns the modified message
// Example 1: Return message as-is
return msg;

// Example 2: Modify message payload
msg.payload = msg.payload.toString().length;
return msg;

// Example 3: Create new message
var newMsg = { payload: msg.payload.length };
return newMsg;

// Example 4: Drop/stop processing this message
console.log("Dropping message");
return null;

// Example 5: Log message content
console.log("Processing message with payload:", msg.payload);
console.log("Message metadata:", msg.meta);

// Example 6: Modify metadata
msg.meta.processed = true;
msg.meta.count = (msg.meta.count || 0) + 1;
return msg;`))

	err := service.RegisterBatchProcessor(
		"nodered_js",
		spec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			code, err := conf.FieldString("code")
			if err != nil {
				return nil, err
			}
			// Wrap the user's code in a function that handles the return value
			wrappedCode := fmt.Sprintf(`
				(function(){
					'use strict';
					%s
				})()
			`, code)
			processor, err := NewNodeREDJSProcessor(wrappedCode, mgr.Logger(), mgr.Metrics())
			if err != nil {
				return nil, err
			}
			return processor, nil
		})
	if err != nil {
		panic(err)
	}
}
