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
	"encoding/json"
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

	"github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin/cache"
)

// NodeREDJSProcessor defines the processor that wraps the JavaScript processor.
type NodeREDJSProcessor struct {
	program           *goja.Program
	originalCode      string
	vmpool            sync.Pool
	logger            *service.Logger
	cache             cache.Cache
	messagesProcessed *service.MetricCounter
	messagesErrored   *service.MetricCounter
	messagesDropped   *service.MetricCounter
	vmPoolHits        *service.MetricCounter
	vmPoolMisses      *service.MetricCounter
}

// NewNodeREDJSProcessor creates a new NodeREDJSProcessor instance.
func NewNodeREDJSProcessor(code string, logger *service.Logger, metrics *service.Metrics, c cache.Cache) (*NodeREDJSProcessor, error) {
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
		cache:             c,
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
func ConvertMessageToJSObject(msg *service.Message) (map[string]any, error) {
	msgBytes, err := msg.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to convert message to bytes: %w", err)
	}

	var jsondata any
	err = json.Unmarshal(msgBytes, &jsondata)
	if err == nil {
		return map[string]any{
			"payload": jsondata,
		}, nil
	}

	return map[string]any{
		"payload": string(msgBytes),
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
	depth++
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
func (u *NodeREDJSProcessor) SetupJSEnvironment(ctx context.Context, vm *goja.Runtime, jsMsg map[string]interface{}) error {
	err := vm.Set("msg", jsMsg)
	if err != nil {
		return fmt.Errorf("failed to set message in JS environment: %w", err)
	}

	err = u.setupConsole(vm)
	if err != nil {
		return fmt.Errorf("failed to set console in JS environment: %w", err)
	}

	err = u.setupCache(ctx, vm)
	if err != nil {
		return fmt.Errorf("failed to set cache in JS environment: %w", err)
	}

	return nil
}

func (u *NodeREDJSProcessor) setupConsole(vm *goja.Runtime) error {
	console := map[string]any{
		"debug": func(data ...any) { u.logger.Debug(FormatConsoleLogMsg(data)) },
		"log":   func(data ...any) { u.logger.Info(FormatConsoleLogMsg(data)) },
		"info":  func(data ...any) { u.logger.Info(FormatConsoleLogMsg(data)) },
		"warn":  func(data ...any) { u.logger.Warn(FormatConsoleLogMsg(data)) },
		"error": func(data ...any) { u.logger.Error(FormatConsoleLogMsg(data)) },
	}
	return vm.Set("console", console)
}

func (u *NodeREDJSProcessor) setupCache(ctx context.Context, vm *goja.Runtime) error {
	cacheObj := map[string]any{
		"set": func(key string, value any) {
			err := u.cache.Set(ctx, key, value)
			if err != nil {
				u.logger.Errorf("cache.set failed: %v", err)
			}
		},
		"get": func(key string) any {
			v, ok := u.cache.Get(ctx, key)
			if !ok {
				u.logger.Errorf("cache.get: key %q not found. Use cache.exists(key) to check before reading.", key)
				return goja.Undefined()
			}
			return v
		},
		"exists": func(key string) bool {
			_, exists := u.cache.Get(ctx, key)
			return exists
		},
		"delete": func(key string) {
			err := u.cache.Delete(ctx, key)
			if err != nil {
				u.logger.Errorf("cache.delete failed: %v", err)
			}
		},
	}
	return vm.Set("cache", cacheObj)
}

// HandleExecutionResult handles JavaScript execution results.
//
// A function may return:
//   - null / undefined: the message is dropped,
//   - a single message object: one output,
//   - an array of message objects: one output per element (fan-out).
//
// null/undefined elements within a returned array are skipped during
// fan-out. If every element is nil/undefined, or the array is empty,
// the whole input is treated as a single drop: messagesDropped is
// incremented once and no outputs are produced. Otherwise (>=1
// surviving output) nil/undefined elements are skipped with no drop
// bump. Non-object elements still error the batch.
func (u *NodeREDJSProcessor) HandleExecutionResult(result goja.Value) ([]*service.Message, error) {
	// Handle null/undefined returns: drop (caller bumps messagesDropped).
	if result.Equals(goja.Undefined()) || result.Equals(goja.Null()) {
		return nil, nil
	}

	exported := result.Export()

	if arr, ok := exported.([]interface{}); ok {
		out := make([]*service.Message, 0, len(arr))
		for i, el := range arr {
			if el == nil {
				continue
			}
			msg, err := messageFromReturnValue(el)
			if err != nil {
				return nil, fmt.Errorf("array element %d must be a message object, got %T", i, el)
			}
			out = append(out, msg)
		}
		return out, nil
	}

	msg, err := messageFromReturnValue(exported)
	if err != nil {
		return nil, err
	}
	return []*service.Message{msg}, nil
}

// messageFromReturnValue builds a service.Message from a single JS return
// value, which must be a message object (a map with payload/meta).
// NewMessage(nil) is safe: the engine's v2BatchedToV1Processor wrapper
// restores the input context onto outputs in production, so
// Copy()/WithContext() is a no-op.
func messageFromReturnValue(v interface{}) (*service.Message, error) {
	returnedMsg, ok := v.(map[string]interface{})
	if !ok {
		return service.NewMessage(nil), fmt.Errorf("function must return a message object or null")
	}

	newMsg := service.NewMessage(nil)
	if payload, exists := returnedMsg["payload"]; exists {
		newMsg.SetStructured(payload)
	}
	if meta, exists := returnedMsg["meta"].(map[string]interface{}); exists {
		SetMetaFromJS(newMsg, meta)
	}
	return newMsg, nil
}

// SetMetaFromJS transfers JavaScript-origin meta values onto a
// service.Message, skipping nil top-level values. Map and slice values are
// JSON-marshalled so nested nil leaves become valid JSON null instead of
// Go-syntax "<nil>". All other values are stringified with fmt %v. It is
// exported so tag_processor can reuse this logic.
func SetMetaFromJS(newMsg *service.Message, meta map[string]interface{}) {
	for k, val := range meta {
		if val == nil {
			continue
		}
		switch val.(type) {
		case map[string]interface{}, []interface{}:
			b, err := json.Marshal(val)
			if err != nil {
				// json.Marshal errors on NaN/+Inf nested in maps or slices;
				// fall back to a non-empty value rather than writing an empty
				// Kafka header (matches tag_processor's autoConvertValue).
				newMsg.MetaSet(k, fmt.Sprintf("%v", val))
				continue
			}
			newMsg.MetaSet(k, string(b))
		default:
			newMsg.MetaSet(k, fmt.Sprintf("%v", val))
		}
	}
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
	batchSize := len(batch)
	droppedCount := 0

	for _, msg := range batch {
		processedMsgs, dropped, err := u.processSingleMessage(ctx, msg)
		if err != nil {
			// Batch-fatal: bump messages_errored for the whole aborted batch
			// (all batchSize messages, including any already iterated this
			// attempt, which the engine re-marks as errored). messagesDropped
			// and messagesProcessed are NOT bumped here (deferred below) so a
			// mid-loop fatal does not double-count the same messages as both
			// processed/dropped and errored.
			u.messagesErrored.Incr(int64(batchSize))
			return nil, err
		}
		if dropped {
			droppedCount++
		}
		resultBatch = append(resultBatch, processedMsgs...)
	}

	// Bump messagesDropped once for all genuine drops in this batch attempt.
	// Deferred from the loop so a mid-loop batch-fatal error skips the bump
	// (those messages are counted as errored by the batch-fatal path above,
	// not as dropped, and the batch is forwarded with errors marked for
	// downstream error handling).
	if droppedCount > 0 {
		u.messagesDropped.Incr(int64(droppedCount))
	}

	// messages_processed counts successfully produced outputs, bumped once
	// after the loop so a mid-loop batch-fatal leaves it at 0 (the whole
	// batch is errored, not processed; outputs from messages before the
	// throw are discarded on abort). Matches tag_processor, whose processed
	// bump sits in the construction stage that never runs when the program
	// stage aborts the batch. Counts outputs (1 input -> N outputs = N) to
	// match tag_processor's per-finalMsg bump.
	u.messagesProcessed.Incr(int64(len(resultBatch)))

	if len(resultBatch) == 0 {
		return []service.MessageBatch{}, nil
	}

	return []service.MessageBatch{resultBatch}, nil
}

// processSingleMessage processes a single message using a VM from the pool.
// Returns (messages, wasDropped, err): wasDropped is true only for a genuine
// drop (null/undefined/empty/all-nil array return from HandleExecutionResult).
// err is non-nil for any batch-fatal error (JS execution failure, conversion
// failure, or HandleExecutionResult validation error). The caller bumps
// messagesErrored by batchSize on any err. This matches tag_processor's
// JS-execution stage (processMessageBatchWithProgram), where stage errors
// are batch-fatal. tag_processor additionally swallows
// condition/validation/construction errors per-message, which nodered_js
// has no equivalent of.
func (u *NodeREDJSProcessor) processSingleMessage(ctx context.Context, msg *service.Message) ([]*service.Message, bool, error) {
	vm := u.getVM()
	defer u.putVM(vm)

	// Convert message to JS object
	jsMsg, err := ConvertMessageToJSObject(msg)
	if err != nil {
		u.logger.Errorf("%v\nOriginal message: %v", err, msg)
		return nil, false, err
	}

	// Add metadata to the message wrapper
	meta := make(map[string]interface{})
	if err = msg.MetaWalkMut(func(key string, value any) error {
		meta[key] = value
		return nil
	}); err != nil {
		u.logger.Errorf("Failed to walk message metadata: %v\nOriginal message: %v", err, msg)
		return nil, false, err
	}
	jsMsg["meta"] = meta

	// Setup JS environment
	if err = u.SetupJSEnvironment(ctx, vm, jsMsg); err != nil {
		u.logger.Errorf("%v\nMessage content: %v", err, jsMsg)
		return nil, false, err
	}

	// Execute the compiled JavaScript program
	result, err := vm.RunProgram(u.program)
	if err != nil {
		u.logJSError(err, jsMsg)
		return nil, false, err
	}

	// Handle the execution result
	newMsgs, err := u.HandleExecutionResult(result)
	if err != nil {
		u.logger.Errorf("%v\nMessage content: %v\nReturned value: %v", err, jsMsg, result.Export())
		return nil, false, err
	}

	if len(newMsgs) == 0 {
		return nil, true, nil
	}

	return newMsgs, false, nil
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
func (u *NodeREDJSProcessor) Close(_ context.Context) error {
	return u.cache.Close()
}

// NodeREDJSConfigSpec defines the configuration options for the nodered_js processor.
var NodeREDJSConfigSpec = service.NewConfigSpec().
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
return msg;

// Example 7: Persistent counter across messages using cache
var count = 0;
if (cache.exists("count")) { count = cache.get("count"); }
count++;
cache.set("count", count);
msg.payload = count;
return msg;

// Example 8: Alarm state that only fires once per active condition
var alarmed = cache.exists("alarm_active") ? cache.get("alarm_active") : false;
if (msg.payload.value > 100 && !alarmed) {
  cache.set("alarm_active", true);
  msg.meta.alarm = "triggered";
  return msg;
}
if (msg.payload.value <= 100 && alarmed) {
  cache.set("alarm_active", false);
  msg.meta.alarm = "cleared";
  return msg;
}
return msg;`))

func newNodeREDJSProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	code, err := conf.FieldString("code")
	if err != nil {
		return nil, err
	}

	wrappedCode := fmt.Sprintf(`
		(function(){
			'use strict';
			%s
		})()
	`, code)

	return NewNodeREDJSProcessor(wrappedCode, mgr.Logger(), mgr.Metrics(), cache.NewMemoryStore(0))
}

func init() {
	err := service.RegisterBatchProcessor(
		"nodered_js",
		NodeREDJSConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newNodeREDJSProcessor(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}
