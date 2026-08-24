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
	"time"

	"github.com/dop251/goja"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin/cache"
	"github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin/protobuf"
)

// cacheStatsInterval controls how often the cache metrics are sampled.
const cacheStatsInterval = 30 * time.Second

// NodeREDJSProcessor defines the processor that wraps the JavaScript processor.
type NodeREDJSProcessor struct {
	program           *goja.Program
	originalCode      string
	vmpool            sync.Pool
	logger            *service.Logger
	cache             cache.Cache
	messagesProcessed *service.MetricCounter
	messagesDropped   *service.MetricCounter
	vmPoolHits        *service.MetricCounter
	vmPoolMisses      *service.MetricCounter
	cacheKeys         *service.MetricGauge
	cacheDiskBytes    *service.MetricGauge
	metricsCancel     context.CancelFunc
	metricsWG         sync.WaitGroup
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
		messagesDropped:   metrics.NewCounter("messages_dropped", "reason"),
		vmPoolHits:        metrics.NewCounter("vm_pool_hits"),
		vmPoolMisses:      metrics.NewCounter("vm_pool_misses"),
		cacheKeys:         metrics.NewGauge("cache_keys"),
		cacheDiskBytes:    metrics.NewGauge("cache_disk_bytes"),
	}

	metricsCtx, cancel := context.WithCancel(context.Background())
	processor.metricsCancel = cancel
	processor.metricsWG.Add(1)
	go processor.getCacheMetrics(metricsCtx)

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

// escapeString escapes a string for log output, using single quotes for JSON embeddability.
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

// stringify formats objects like NodeJS console.log, optimized for JSON embedding.
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
func (u *NodeREDJSProcessor) SetupJSEnvironment(ctx context.Context, vm *goja.Runtime, jsMsg map[string]any) error {
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

	err = u.setupProtobuf(vm)
	if err != nil {
		return fmt.Errorf("failed to set protobuf in JS environment: %w", err)
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

// setupCache binds the store to the JS runtime; validation + timestamp gating live in the cache pkg.
func (u *NodeREDJSProcessor) setupCache(ctx context.Context, vm *goja.Runtime) error {
	cacheObj := map[string]any{
		"set": func(key string, msg map[string]any) {
			payload, err := cache.ParsePayload(msg)
			if err != nil {
				u.logger.Errorf("cache.set: %v (got %v)", err, msg)
				return
			}
			err = u.cache.Set(ctx, key, payload)
			if errors.Is(err, cache.ErrOldTimestamp) {
				u.logger.Warnf("cache.set: dropped stale write for key %q (timestamp_ms=%d not newer than stored)", key, payload.TimestampMs)
				return
			}
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

// setupProtobuf exposes protobuf.decode/encode to the JS runtime. The functions
// decode/encode against an inline base64 FileDescriptorSet and throw on error
// (goja converts the Go (T, error) return into a throwing JS function). The
// tag_processor shares this environment, so it gets `protobuf` too (ENG-5243).
func (u *NodeREDJSProcessor) setupProtobuf(vm *goja.Runtime) error {
	// Recover any panic into an error: the descriptor set and data are untrusted, and
	// goja does not recover Go panics from native functions, so a panic here would crash
	// the whole process rather than throw a catchable JS error.
	protobufObj := map[string]any{
		"decode": func(dataB64 string, descriptorSetB64 string, msgName string) (_ map[string]any, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("protobuf.decode panicked: %v", r)
				}
			}()
			return protobuf.Decode(dataB64, descriptorSetB64, msgName)
		},
		"encode": func(obj any, descriptorSetB64 string, msgName string) (_ string, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("protobuf.encode panicked: %v", r)
				}
			}()
			return protobuf.Encode(obj, descriptorSetB64, msgName)
		},
	}
	return vm.Set("protobuf", protobufObj)
}

// HandleExecutionResult converts a JS return value into output messages.
// null/undefined drops; an array fans out (nil elements skipped, all-nil = drop).
func (u *NodeREDJSProcessor) HandleExecutionResult(result goja.Value) ([]*service.Message, string, error) {
	// Handle null/undefined returns: drop (caller bumps messagesDropped).
	if result.Equals(goja.Undefined()) || result.Equals(goja.Null()) {
		return nil, "", nil
	}

	exported := result.Export()

	if arr, ok := exported.([]any); ok {
		out := make([]*service.Message, 0, len(arr))
		for i, el := range arr {
			if el == nil {
				continue
			}
			msg, err := messageFromReturnValue(el)
			if err != nil {
				return nil, "bad_array_element", fmt.Errorf("array element %d: %w", i, err)
			}
			out = append(out, msg)
		}
		return out, "", nil
	}

	msg, err := messageFromReturnValue(exported)
	if err != nil {
		return nil, "bad_return", err
	}
	return []*service.Message{msg}, "", nil
}

// messageFromReturnValue builds a service.Message from a JS return value (map with payload/meta).
// NewMessage(nil) is safe: the engine wrapper (v2BatchedToV1Processor) restores input context onto outputs.
func messageFromReturnValue(v any) (*service.Message, error) {
	returnedMsg, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("function must return a message object or null")
	}

	newMsg := service.NewMessage(nil)
	if payload, exists := returnedMsg["payload"]; exists {
		newMsg.SetStructured(payload)
	}
	if meta, exists := returnedMsg["meta"]; exists && meta != nil {
		metaMap, ok := meta.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("message meta must be an object, got %T", meta)
		}
		SetMetaFromJS(newMsg, metaMap)
	}
	return newMsg, nil
}

// SetMetaFromJS copies JS meta values onto a service.Message, skipping nil top-level values.
// Maps/slices are JSON-marshaled; other values use fmt %v. Exported for tag_processor reuse.
func SetMetaFromJS(newMsg *service.Message, meta map[string]any) {
	for k, val := range meta {
		if val == nil {
			continue
		}
		switch val.(type) {
		case map[string]any, []any:
			b, err := json.Marshal(val)
			if err != nil {
				// json.Marshal errors on NaN/+Inf; fall back to %v to avoid an empty Kafka header.
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

// RecordDrop drops a poisoned message loudly (bumps messages_dropped + Warn log); never errors or panics.
// Dropping (not forwarding) is intentional: errored messages lack umh_topic, so forwarding would nack the batch.
func RecordDrop(counter *service.MetricCounter, logger *service.Logger, reason string, plugin string, stage string, msg *service.Message, err error) {
	counter.Incr(1, reason)

	topic, exists := msg.MetaGet("umh_topic")
	if !exists || topic == "" {
		topic = "<none>"
	}

	logger.Warnf("%s: dropped message (reason=%s, umh_topic=%s, stage=%s) %v", plugin, reason, topic, stage, err)
}

// cacheBegin acquires the cache mutex and opens the batch tx; pair with defer cacheCommit.
func (u *NodeREDJSProcessor) cacheBegin(ctx context.Context) error {
	u.cache.Lock()
	err := u.cache.Begin(ctx)
	if err != nil {
		u.cache.Unlock()
		return err
	}
	return nil
}

// cacheCommit commits the batch tx and releases the mutex.
func (u *NodeREDJSProcessor) cacheCommit(ctx context.Context) {
	err := u.cache.Commit(ctx)
	if err != nil {
		u.logger.Errorf("cache commit failed: %v", err)
	}
	u.cache.Unlock()
}

func (u *NodeREDJSProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	err := u.cacheBegin(ctx)
	if err != nil {
		return nil, err
	}
	defer u.cacheCommit(ctx)

	var resultBatch service.MessageBatch
	processedCount := 0

	for _, msg := range batch {
		if msg == nil {
			continue
		}

		processedMsgs, dropped, reason, err := u.processSingleMessage(ctx, msg)
		if err != nil {
			// Drop-loudly: the poisoned message is absent from the output
			// batch. The good messages flow.
			RecordDrop(u.messagesDropped, u.logger, reason, "nodered_js", "processSingleMessage", msg, err)
			continue
		}
		if dropped {
			u.messagesDropped.Incr(1, "deliberate")
			continue
		}
		resultBatch = append(resultBatch, processedMsgs...)
		processedCount += len(processedMsgs)
	}

	u.messagesProcessed.Incr(int64(processedCount))

	if len(resultBatch) == 0 {
		return []service.MessageBatch{}, nil
	}

	return []service.MessageBatch{resultBatch}, nil
}

// processSingleMessage runs JS on one message. Returns (messages, wasDropped, err, reason).
// wasDropped=true for null/undefined/empty returns; err non-nil for JS throw/infra/bad-return.
func (u *NodeREDJSProcessor) processSingleMessage(ctx context.Context, msg *service.Message) ([]*service.Message, bool, string, error) {
	vm := u.getVM()
	defer u.putVM(vm)

	// Convert message to JS object
	// defensive: AsBytes never errors as of benthos v4.74.0 (TODO upstream); kept for future-proofing
	jsMsg, err := ConvertMessageToJSObject(msg)
	if err != nil {
		u.logger.Warnf("%v\nOriginal message: %v", err, msg)
		return nil, false, "infra_failed", err
	}

	// Add metadata to the message wrapper
	// defensive: MetaWalkMut callback never errors; kept for future-proofing
	meta := make(map[string]any)
	if err = msg.MetaWalkMut(func(key string, value any) error {
		meta[key] = value
		return nil
	}); err != nil {
		u.logger.Warnf("Failed to walk message metadata: %v\nOriginal message: %v", err, msg)
		return nil, false, "infra_failed", err
	}
	jsMsg["meta"] = meta

	// Setup JS environment
	// defensive: vm.Set unreachable from normal messages; kept for future-proofing
	if err = u.SetupJSEnvironment(ctx, vm, jsMsg); err != nil {
		u.logger.Warnf("%v\nMessage content: %v", err, jsMsg)
		return nil, false, "infra_failed", err
	}

	// Execute the compiled JavaScript program
	result, err := vm.RunProgram(u.program)
	if err != nil {
		u.logJSError(err, jsMsg)
		return nil, false, "js_throw", err
	}

	// Handle the execution result
	newMsgs, reason, err := u.HandleExecutionResult(result)
	if err != nil {
		u.logger.Warnf("%v\nMessage content: %v\nReturned value: %v", err, jsMsg, result.Export())
		return nil, false, reason, err
	}

	if len(newMsgs) == 0 {
		return nil, true, "", nil
	}

	return newMsgs, false, "", nil
}

// logJSError logs JavaScript execution errors with code context at Warn level.
// Warn (not Error) because umh-core's benthos FSM treats Error-level logs as deploy-blocking;
// a routine JS throw drops the message (RecordDrop Warns) but must not block the deploy.
func (u *NodeREDJSProcessor) logJSError(err error, jsMsg any) {
	jsErr := &goja.Exception{}
	if errors.As(err, &jsErr) {
		stack := jsErr.String()
		u.logger.Warnf(`JavaScript execution failed:
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
		u.logger.Warnf(`JavaScript execution failed:
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
	if u.metricsCancel != nil {
		u.metricsCancel()
	}
	u.metricsWG.Wait()
	return u.cache.Close()
}

// getCacheMetrics periodically samples the cache and updates the gauges. It
// exits when ctx is canceled by Close.
func (u *NodeREDJSProcessor) getCacheMetrics(ctx context.Context) {
	defer u.metricsWG.Done()

	ticker := time.NewTicker(cacheStatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats, err := u.cache.Stats(ctx)
			if err != nil {
				if ctx.Err() == nil {
					u.logger.Warnf("cache.stats failed: %v", err)
				}
				continue
			}
			u.cacheKeys.Set(stats.Keys)
			u.cacheDiskBytes.Set(stats.DiskBytes)
		}
	}
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
return msg;`)).
	Field(service.NewObjectField(
		"cache",
		service.NewStringField("backend").
			Description("Cache backend. 'memory' is in-process and lost on restart. 'persistent' writes to a file on disk and survives restarts.").
			Default("memory").
			Examples("memory", "persistent"),
		service.NewStringField("name").
			Description("Sharing identifier. Processors with the same backend and name share one cache instance in this benthos process — keys written by one are visible to the others. The default 'shared' means two nodered_js processors with no explicit cache config will already share state; set a different value to isolate groups, or use unique names if you want per-processor caches.").
			Default("shared"),
		service.NewStringField("path").
			Description("File path for the 'persistent' backend. Used by the first processor that opens the cache under a given name; later processors attaching by name may omit it. Relative paths resolve against the directory where the benthos process was started (under UMH Core: the S6 service directory). Use an absolute path to avoid ambiguity. Leading '~' expands to the home directory.").
			Default("./cache.db"),
		service.NewDurationField("ttl").
			Description("Time-to-live for cached entries. 0 (default) keeps entries until explicit delete or restart. Set a positive duration (e.g. '1h') to auto-expire entries N after the last write.").
			Default("0s"),
	).
		Description("Cache configuration for state across messages.").
		Default(map[string]any{}).
		Advanced())

func newNodeREDJSProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	code, err := conf.FieldString("code")
	if err != nil {
		return nil, err
	}

	backend, err := conf.FieldString("cache", "backend")
	if err != nil {
		return nil, fmt.Errorf("parse cache.backend: %w", err)
	}

	cacheName, err := conf.FieldString("cache", "name")
	if err != nil {
		return nil, fmt.Errorf("parse cache.name: %w", err)
	}

	path, err := conf.FieldString("cache", "path")
	if err != nil {
		return nil, fmt.Errorf("parse cache.path: %w", err)
	}

	ttl, err := conf.FieldDuration("cache", "ttl")
	if err != nil {
		return nil, fmt.Errorf("parse cache.ttl: %w", err)
	}

	store, err := cache.New(backend, cacheName, path, ttl)
	if err != nil {
		return nil, err
	}

	wrappedCode := fmt.Sprintf(`
		(function(){
			'use strict';
			%s
		})()
	`, code)

	processor, err := NewNodeREDJSProcessor(wrappedCode, mgr.Logger(), mgr.Metrics(), store)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return processor, nil
}

func init() {
	err := service.RegisterBatchProcessor(
		"nodered_js",
		NodeREDJSConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newNodeREDJSProcessor(conf, mgr)
		},
	)
	if err != nil {
		panic(err)
	}
}
