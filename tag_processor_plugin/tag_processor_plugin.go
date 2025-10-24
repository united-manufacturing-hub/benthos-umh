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

package tag_processor_plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
)

// sentinelNoTimestamp is used to indicate that no valid timestamp was found
// We use math.MinInt64 instead of 0 or -1 because:
// - 0 is valid (Unix epoch: 1970-01-01T00:00:00Z)
// - -1 is technically valid (1969-12-31T23:59:59.999Z)
// - math.MinInt64 represents a date ~292 billion years in the past (impossible)
const sentinelNoTimestamp = math.MinInt64

type TagProcessorConfig struct {
	Defaults           string            `json:"defaults" yaml:"defaults"`
	Conditions         []ConditionConfig `json:"conditions" yaml:"conditions"`
	AdvancedProcessing string            `json:"advancedProcessing" yaml:"advancedProcessing"`
}

type ConditionConfig struct {
	If   string `json:"if" yaml:"if"`
	Then string `json:"then" yaml:"then"`
}

func init() {
	spec := service.NewConfigSpec().
		Version("1.0.0").
		Summary("A processor for standardizing tag metadata and topics").
		Description(`The tagProcessor sets up a canonical metadata structure for constructing standardized topic and payload schemas.
It applies defaults, conditional transformations, and optional advanced processing using a Node-RED style JavaScript environment.

Required metadata fields:
- location_path: Hierarchical location path in dot notation (e.g., "enterprise.site.area.line.workcell.plc123")
- data_contract: Data schema identifier (e.g., "_historian", "_analytics")
- tag_name: Name of the tag/variable (e.g., "temperature")

Optional metadata fields:
- virtual_path: Logical, non-physical grouping path in dot notation (e.g., "axis.x.position")

The final topic will be constructed as:
umh.v1.<location_path>.<data_contract>.<virtual_path>.<tag_name>

Empty or undefined fields will be omitted from the topic.`).
		Field(service.NewStringField("defaults").
			Description("JavaScript code to set initial metadata values").
			Default("")).
		Field(service.NewObjectListField("conditions",
			service.NewStringField("if").Description("JavaScript condition expression"),
			service.NewStringField("then").Description("JavaScript code to execute if condition is true"),
		).Description("List of conditions to evaluate").
			Optional()).
		Field(service.NewStringField("advancedProcessing").
			Description("Optional JavaScript code for advanced message processing").
			Default("").
			Optional())

	err := service.RegisterBatchProcessor(
		"tag_processor",
		spec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			defaults, err := conf.FieldString("defaults")
			if err != nil {
				return nil, err
			}

			var conditions []ConditionConfig
			if conf.Contains("conditions") {
				conditionsArray, err := conf.FieldObjectList("conditions")
				if err != nil {
					return nil, err
				}

				for _, condObj := range conditionsArray {
					ifExpr, err := condObj.FieldString("if")
					if err != nil {
						return nil, err
					}

					thenCode, err := condObj.FieldString("then")
					if err != nil {
						return nil, err
					}

					conditions = append(conditions, ConditionConfig{
						If:   ifExpr,
						Then: thenCode,
					})
				}
			}

			advancedProcessing, _ := conf.FieldString("advancedProcessing")

			config := TagProcessorConfig{
				Defaults:           defaults,
				Conditions:         conditions,
				AdvancedProcessing: advancedProcessing,
			}

			return newTagProcessor(config, mgr.Logger(), mgr.Metrics())
		})
	if err != nil {
		panic(err)
	}
}

type TagProcessor struct {
	config TagProcessorConfig
	logger *service.Logger

	// VM pooling infrastructure - Phase 1 optimization
	vmpool       sync.Pool
	vmPoolHits   *service.MetricCounter
	vmPoolMisses *service.MetricCounter

	// Compiled programs - Phase 2 optimization
	defaultsProgram       *goja.Program   // Pre-compiled defaults code
	conditionPrograms     []*goja.Program // Pre-compiled condition evaluations
	conditionThenPrograms []*goja.Program // Pre-compiled condition actions
	advancedProgram       *goja.Program   // Pre-compiled advanced processing

	// Original code for error logging - Phase 2 enhancement
	originalDefaults   string
	originalConditions []ConditionConfig
	originalAdvanced   string

	// Existing metrics
	messagesProcessed *service.MetricCounter
	messagesErrored   *service.MetricCounter
	messagesDropped   *service.MetricCounter

	// Keep for JS environment setup helper methods
	jsProcessor *nodered_js_plugin.NodeREDJSProcessor
}

func newTagProcessor(config TagProcessorConfig, logger *service.Logger, metrics *service.Metrics) (*TagProcessor, error) {
	// Create a NodeREDJSProcessor for SetupJSEnvironment helper
	jsProcessor, err := nodered_js_plugin.NewNodeREDJSProcessor("", logger, metrics)
	if err != nil {
		return nil, fmt.Errorf("failed to create JS processor: %v", err)
	}

	processor := &TagProcessor{
		config: config,
		logger: logger,

		// Initialize VM pool without New function - Get() will return nil when pool is empty
		vmpool: sync.Pool{},

		// VM pool metrics
		vmPoolHits:   metrics.NewCounter("vm_pool_hits"),
		vmPoolMisses: metrics.NewCounter("vm_pool_misses"),

		// Store original code for error logging - Phase 2 enhancement
		originalDefaults:   config.Defaults,
		originalConditions: config.Conditions,
		originalAdvanced:   config.AdvancedProcessing,

		// Existing metrics
		messagesProcessed: metrics.NewCounter("messages_processed"),
		messagesErrored:   metrics.NewCounter("messages_errored"),
		messagesDropped:   metrics.NewCounter("messages_dropped"),

		jsProcessor: jsProcessor,
	}

	// Phase 2: Compile all JavaScript once at startup to catch errors early and optimize runtime
	if err := processor.compilePrograms(); err != nil {
		return nil, fmt.Errorf("failed to compile JavaScript programs: %v", err)
	}

	return processor, nil
}

// VM Pool Management Methods - Phase 1 Optimization

// getVM acquires a VM from the pool and tracks metrics
func (p *TagProcessor) getVM() *goja.Runtime {
	poolResult := p.vmpool.Get()
	if poolResult == nil {
		// Pool is empty, create new VM and track as miss
		p.vmPoolMisses.Incr(1)
		return goja.New()
	}
	// Successfully reused VM from pool, track as hit
	p.vmPoolHits.Incr(1)
	return poolResult.(*goja.Runtime)
}

// putVM returns a VM to the pool after proper cleanup
func (p *TagProcessor) putVM(vm *goja.Runtime) {
	// Comprehensive VM cleanup to prevent state leakage
	if err := p.clearVMState(vm); err != nil {
		p.logger.Errorf("Failed to clear VM state: %v", err)
		// In case of an error, we do not return the VM to the pool
		// because it might be in an invalid state
		return
	}
	// Return cleaned VM to pool for reuse
	p.vmpool.Put(vm)
}

// clearVMState performs comprehensive cleanup of VM state
func (p *TagProcessor) clearVMState(vm *goja.Runtime) error {
	// Clear any interrupt flag that might be set to prevent state pollution
	vm.ClearInterrupt()
	return vm.GlobalObject().Set("msg", nil)
}

// setupMessageForVM prepares a VM with message data for execution
func (p *TagProcessor) setupMessageForVM(vm *goja.Runtime, msg *service.Message, jsMsg map[string]interface{}) error {
	// Initialize meta if it doesn't exist
	if _, exists := jsMsg["meta"]; !exists {
		jsMsg["meta"] = make(map[string]interface{})
	}

	// Add existing metadata to jsMsg.meta
	meta := jsMsg["meta"].(map[string]interface{})
	if err := msg.MetaWalkMut(func(key string, value any) error {
		meta[key] = value
		return nil
	}); err != nil {
		return fmt.Errorf("metadata extraction failed: %v", err)
	}

	// Setup JS environment using helper from NodeREDJSProcessor
	if err := p.jsProcessor.SetupJSEnvironment(vm, jsMsg); err != nil {
		return fmt.Errorf("JS environment setup failed: %v", err)
	}

	// Set the msg variable in the JavaScript VM
	if err := vm.Set("msg", jsMsg); err != nil {
		return fmt.Errorf("JS message setup failed: %v", err)
	}

	return nil
}

// TODO: Each time there is any execution error, output the code where the error happened as well as the message that caused it (see nodered_js_plugin). Double-check that it is not being outputted twice.
func (p *TagProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	// ───────────────── Store incoming metadata ────────────────────────────────
	// For each message, capture its current meta fields and store them as JSON
	// in msg.meta._initialMetadata. Also, record the original keys in _incomingKeys.
	for _, msg := range batch {
		// Only store if not already set (for example, in case of multiple processing steps).
		if _, exists := msg.MetaGet("_initialMetadata"); !exists {
			originalMeta := make(map[string]string)
			_ = msg.MetaWalkMut(func(key string, value any) error {
				if str, ok := value.(string); ok {
					originalMeta[key] = str
				}
				return nil
			})
			if encoded, err := json.Marshal(originalMeta); err != nil {
				p.logger.Errorf("failed to marshal original metadata: %v", err)
			} else {
				msg.MetaSet("_initialMetadata", string(encoded))
			}
			// Record the list of original keys as a comma-separated string.
			var keys []string
			for k := range originalMeta {
				keys = append(keys, k)
			}
			msg.MetaSet("_incomingKeys", strings.Join(keys, ","))
		}
	}
	// ─────────────────────────────────────────────────────────────────────────────

	// Process defaults with compiled program (Phase 2 optimization)
	if p.defaultsProgram != nil {
		var err error
		batch, err = p.processMessageBatchWithProgram(batch, p.defaultsProgram, "defaults")
		if err != nil {
			return nil, fmt.Errorf("error in defaults processing: %v", err)
		}
	}

	// Process conditions with compiled programs (Phase 2 optimization)
	for i := range p.config.Conditions {
		var newBatch service.MessageBatch

		for _, msg := range batch {
			processedMsgs, err := p.processConditionForMessageWithProgram(i, msg)
			if err != nil {
				p.logError(err, "condition evaluation", msg)
				continue
			}
			// Append all returned messages (could be 0, 1, or multiple)
			newBatch = append(newBatch, processedMsgs...)
		}

		batch = newBatch
	}

	// Process advanced processing with compiled program (Phase 2 optimization)
	if p.advancedProgram != nil {
		var err error
		batch, err = p.processMessageBatchWithProgram(batch, p.advancedProgram, "advanced")
		if err != nil {
			return nil, fmt.Errorf("error in advanced processing: %v", err)
		}
	}

	// Validate and construct final messages
	var resultBatch service.MessageBatch
	for _, msg := range batch {
		if msg == nil {
			continue
		}

		if err := p.validateMessage(msg); err != nil {
			p.messagesErrored.Incr(1)
			p.logger.Errorf("Message validation failed: %v", err)
			continue
		}

		finalMsg, err := p.constructFinalMessage(msg)
		if err != nil {
			p.messagesErrored.Incr(1)
			p.logger.Errorf("Failed to construct final message: %v", err)
			continue
		}

		resultBatch = append(resultBatch, finalMsg)
		p.messagesProcessed.Incr(1)
	}

	if len(resultBatch) == 0 {
		// Return empty slice instead of nil to indicate successful filtering
		// See: github.com/redpanda-data/benthos/v4/internal/component/processor/auto_observed.go:263-264
		return []service.MessageBatch{}, nil
	}

	return []service.MessageBatch{resultBatch}, nil
}

// logJSError logs JavaScript execution errors with code context
func (p *TagProcessor) logJSError(err error, code string, jsMsg map[string]interface{}) {
	if jsErr, ok := err.(*goja.Exception); ok {
		stack := jsErr.String()
		p.logger.Errorf(`JavaScript execution failed:
Error: %v
Stack: %v
Code:
%v
Message content: %v`,
			jsErr.Error(),
			stack,
			code,
			jsMsg)
	} else {
		p.logger.Errorf(`JavaScript execution failed:
Error: %v
Code:
%v
Message content: %v`,
			err,
			code,
			jsMsg)
	}
}

// logError logs general processing errors with message context
func (p *TagProcessor) logError(err error, stage string, msg interface{}) {
	p.logger.Errorf(`Processing failed at stage '%s':
Error: %v
Message content: %v`,
		stage,
		err,
		msg)
}

func (p *TagProcessor) executeJSCode(vm *goja.Runtime, code string, jsMsg map[string]interface{}) ([]map[string]interface{}, error) {
	wrappedCode := fmt.Sprintf(`(function(){'use strict';%s})()`, code)
	result, err := vm.RunString(wrappedCode)
	if err != nil {
		p.logJSError(err, code, jsMsg)
		return nil, fmt.Errorf("JavaScript error in code: %v", err)
	}

	if result == nil || goja.IsNull(result) || goja.IsUndefined(result) {
		return nil, nil
	}

	switch v := result.Export().(type) {
	case []interface{}:
		messages := make([]map[string]interface{}, 0, len(v))
		for _, msg := range v {
			if msg == nil {
				continue
			}
			if msgMap, ok := msg.(map[string]interface{}); ok {
				messages = append(messages, msgMap)
			} else {
				return nil, fmt.Errorf("array elements must be message objects")
			}
		}
		return messages, nil
	case map[string]interface{}:
		return []map[string]interface{}{v}, nil
	default:
		return nil, fmt.Errorf("code must return a message object or array of message objects")
	}
}

// validateMessage checks if a message has all required metadata fields
func (p *TagProcessor) validateMessage(msg *service.Message) error {
	requiredFields := []string{"location_path", "data_contract", "tag_name"}
	missingFields := []string{}

	// Get current metadata for context
	metadata := map[string]string{}
	err := msg.MetaWalkMut(func(key string, value any) error {
		if str, ok := value.(string); ok {
			metadata[key] = str
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to walk message metadata: %v", err)
	}

	// Check for missing fields
	for _, field := range requiredFields {
		if _, exists := msg.MetaGet(field); !exists {
			missingFields = append(missingFields, field)
		}
	}

	if len(missingFields) > 0 {
		var payloadStr string
		payload, err := msg.AsStructuredMut()
		if err != nil {
			payloadStr = "unable to parse message payload"
		} else {
			// Try to format as JSON first
			payloadJSON, err := json.MarshalIndent(payload, "", "  ")
			if err != nil {
				// If JSON formatting fails, use string representation
				payloadStr = fmt.Sprintf("%v", payload)
			} else {
				payloadStr = string(payloadJSON)
			}
		}

		// Convert metadata to pretty JSON
		metadataJSON, err := json.MarshalIndent(metadata, "", "  ")
		if err != nil {
			metadataJSON = []byte("unable to format metadata as JSON")
		}

		// Create detailed error message with better formatting
		errorMsg := fmt.Sprintf(`Message validation failed
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Missing fields: %s

Current message:
┌ Payload:
%s

└ Metadata:
%s

To fix: Set required fields (msg.meta.location_path, msg.meta.data_contract, msg.meta.tag_name) in defaults, conditions, or advancedProcessing.`,
			strings.Join(missingFields, ", "),
			payloadStr,
			string(metadataJSON))

		err = errors.New(errorMsg)
		p.logError(err, "metadata validation", msg)
		return err
	}
	return nil
}

// constructFinalMessage creates the final message with proper topic and payload structure
// and filters out internal metadata.
func (p *TagProcessor) constructFinalMessage(msg *service.Message) (*service.Message, error) {
	newMsg := service.NewMessage(nil)

	// Retrieve the original incoming metadata stored in _initialMetadata.
	originalMetaRaw, _ := msg.MetaGet("_initialMetadata")
	originalMeta := map[string]string{}
	if originalMetaRaw != "" {
		if err := json.Unmarshal([]byte(originalMetaRaw), &originalMeta); err != nil {
			p.logger.Warnf("failed to unmarshal _initialMetadata: %v", err)
		}
	}

	// Copy all metadata to the new message
	err := msg.MetaWalkMut(func(key string, value any) error {
		if str, ok := value.(string); ok {
			newMsg.MetaSet(key, str)
		} else if stringer, ok := value.(fmt.Stringer); ok {
			newMsg.MetaSet(key, stringer.String())
		} else {
			newMsg.MetaSet(key, fmt.Sprintf("%v", value))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to copy metadata: %v", err)
	}

	// Get the structured payload from the original message
	structured, err := msg.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("failed to get structured payload: %v", err)
	}
	value := p.convertValue(structured)

	// Determine timestamp - use metadata timestamp_ms if available, otherwise current time
	timestamp := time.Now().UnixMilli()

	parsed := p.parseTimestamp(msg)
	if parsed != sentinelNoTimestamp {
		timestamp = parsed
	}

	// Build the final payload object
	finalPayload := map[string]interface{}{
		"value":        value,
		"timestamp_ms": timestamp,
	}

	newMsg.SetStructured(finalPayload)

	// Set the topic in the new message metadata
	topic, err := p.constructUMHTopic(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to construct UMH topic: %v", err)
	}
	newMsg.MetaSet("topic", topic) // topic is deprecated, use umh_topic instead for easy to understand difference between MQTT Topic, Kafka Topic and UMH Topic
	newMsg.MetaSet("umh_topic", topic)

	return newMsg, nil
}

// parseTimestamp attempts to parse timestamp from metadata in specific order.
// Returns: parsed timestamp in Unix milliseconds, or sentinelNoTimestamp if parsing failed.
// Fallback order: timestamp_ms → timestamp → sentinelNoTimestamp
func (p *TagProcessor) parseTimestamp(msg *service.Message) int64 {
	timestampMsStr, exists := msg.MetaGet("timestamp_ms")
	if exists && timestampMsStr != "" {
		parsed := p.parseTimestampToRFC3339Nano(timestampMsStr)
		if parsed != sentinelNoTimestamp {
			return parsed
		}
		p.logger.Warnf("Failed to parse timestamp_ms metadata '%s', trying timestamp field", timestampMsStr)
	}

	timestampStr, exists := msg.MetaGet("timestamp")
	if exists && timestampStr != "" {
		parsed := p.parseTimestampToRFC3339Nano(timestampStr)
		if parsed != sentinelNoTimestamp {
			return parsed
		}
		p.logger.Warnf("Failed to parse timestamp metadata '%s', using current time", timestampStr)
	}

	return sentinelNoTimestamp
}

// parseTimestampToRFC3339Nano parses a timestamp value to Unix milliseconds.
// Supports two formats:
// 1. Unix milliseconds as string (e.g., "1640995200000")
// 2. RFC3339Nano format (e.g., "2022-01-01T00:00:00.000Z")
// Returns: parsed timestamp in milliseconds, or sentinelNoTimestamp on failure.
func (p *TagProcessor) parseTimestampToRFC3339Nano(value string) int64 {
	if parsedMs, err := strconv.ParseInt(value, 10, 64); err == nil {
		return parsedMs
	}

	if parsedTime, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsedTime.UnixMilli()
	}

	return sentinelNoTimestamp
}

// convertValue recursively converts values to their appropriate types
func (p *TagProcessor) convertValue(v interface{}) interface{} {
	switch val := v.(type) {
	case bool:
		return val
	case string:
		if num, err := strconv.ParseFloat(val, 64); err == nil {
			return json.Number(fmt.Sprintf("%v", num))
		}
		return val
	case float64, float32, int, int32, int64, uint, uint32, uint64:
		return json.Number(fmt.Sprintf("%v", val))
	case []interface{}:
		return fmt.Sprintf("%v", val)
	case map[string]interface{}:
		jsonBytes, err := json.Marshal(val)
		if err != nil {
			return fmt.Sprintf("%v", val)
		}
		return string(jsonBytes)
	default:
		str := fmt.Sprintf("%v", val)
		if num, err := strconv.ParseFloat(str, 64); err == nil {
			return json.Number(fmt.Sprintf("%v", num))
		}
		return str
	}
}

// constructUMHTopic creates the topic string from message metadata using the Builder pattern
func (p *TagProcessor) constructUMHTopic(msg *service.Message) (string, error) {
	builder := topic.NewBuilder()

	// Set location path
	var locationPath string
	if value, exists := msg.MetaGet("location_path"); exists && value != "" {
		locationPath = value
		builder.SetLocationPath(locationPath)
	}

	// Set data contract
	var dataContract string
	if value, exists := msg.MetaGet("data_contract"); exists && value != "" {
		dataContract = value
		builder.SetDataContract(dataContract)
	}

	// Set virtual path
	var virtualPath string
	if value, exists := msg.MetaGet("virtual_path"); exists && value != "" {
		virtualPath = value
		builder.SetVirtualPath(virtualPath)
	}

	// Set tag name
	var tagName string
	if value, exists := msg.MetaGet("tag_name"); exists && value != "" {
		tagName = value
		builder.SetName(tagName)
	}

	// Build the topic string
	topicStr, err := builder.BuildString()
	if err != nil {
		p.logger.Errorf("Failed to build UMH topic: %v (locationPath: %s, dataContract: %s, virtualPath: %s, tagName: %s)", err, locationPath, dataContract, virtualPath, tagName)
		return "", fmt.Errorf("failed to build UMH topic: %v", err)
	}

	return topicStr, nil
}

func (p *TagProcessor) Close(ctx context.Context) error {
	return nil
}

// compilePrograms compiles all JavaScript code once at startup for optimal runtime performance
func (p *TagProcessor) compilePrograms() error {
	var err error

	// Compile defaults code
	// WHY: Defaults processing happens for every message batch, so optimization is critical
	if p.originalDefaults != "" {
		wrappedCode := fmt.Sprintf(`(function(){'use strict';%s})()`, p.originalDefaults)
		p.defaultsProgram, err = goja.Compile("defaults.js", wrappedCode, false)
		if err != nil {
			return fmt.Errorf("failed to compile defaults code: %v", err)
		}
	}

	// Compile condition programs
	// WHY: Conditions are evaluated for every message, making this the highest impact optimization
	p.conditionPrograms = make([]*goja.Program, len(p.originalConditions))
	p.conditionThenPrograms = make([]*goja.Program, len(p.originalConditions))

	for i, condition := range p.originalConditions {
		// Compile condition evaluation code
		// WHY: Boolean condition evaluation happens most frequently
		p.conditionPrograms[i], err = goja.Compile(
			fmt.Sprintf("condition-%d-if.js", i),
			condition.If,
			false)
		if err != nil {
			return fmt.Errorf("failed to compile condition %d 'if' code: %v", i, err)
		}

		// Compile condition action code
		// WHY: Action code executes when conditions are true, also frequent
		wrappedThenCode := fmt.Sprintf(`(function(){'use strict';%s})()`, condition.Then)
		p.conditionThenPrograms[i], err = goja.Compile(
			fmt.Sprintf("condition-%d-then.js", i),
			wrappedThenCode,
			false)
		if err != nil {
			return fmt.Errorf("failed to compile condition %d 'then' code: %v", i, err)
		}
	}

	// Compile advanced processing code
	// WHY: Advanced processing is less frequent but still benefits from compilation
	if p.originalAdvanced != "" {
		wrappedCode := fmt.Sprintf(`(function(){'use strict';%s})()`, p.originalAdvanced)
		p.advancedProgram, err = goja.Compile("advanced.js", wrappedCode, false)
		if err != nil {
			return fmt.Errorf("failed to compile advanced processing code: %v", err)
		}
	}

	return nil
}

// executeCompiledProgram executes a pre-compiled JavaScript program for optimal performance
func (p *TagProcessor) executeCompiledProgram(vm *goja.Runtime, program *goja.Program, jsMsg map[string]interface{}, stageName string) ([]map[string]interface{}, error) {
	if program == nil {
		return nil, nil
	}

	result, err := vm.RunProgram(program)
	if err != nil {
		// Use original code for error logging context
		var originalCode string
		switch stageName {
		case "defaults":
			originalCode = p.originalDefaults
		case "advanced":
			originalCode = p.originalAdvanced
		default:
			originalCode = fmt.Sprintf("compiled program: %s", stageName)
		}
		p.logJSError(err, originalCode, jsMsg)
		return nil, fmt.Errorf("JavaScript error in %s: %v", stageName, err)
	}

	if result == nil || goja.IsNull(result) || goja.IsUndefined(result) {
		return nil, nil
	}

	switch v := result.Export().(type) {
	case []interface{}:
		messages := make([]map[string]interface{}, 0, len(v))
		for _, msg := range v {
			if msg == nil {
				continue
			}
			if msgMap, ok := msg.(map[string]interface{}); ok {
				messages = append(messages, msgMap)
			} else {
				return nil, fmt.Errorf("array elements must be message objects")
			}
		}
		return messages, nil
	case map[string]interface{}:
		return []map[string]interface{}{v}, nil
	default:
		return nil, fmt.Errorf("code must return a message object or array of message objects")
	}
}

// processMessageBatchWithProgram processes a batch using a compiled program for Phase 2 optimization
func (p *TagProcessor) processMessageBatchWithProgram(batch service.MessageBatch, program *goja.Program, stageName string) (service.MessageBatch, error) {
	if program == nil {
		return batch, nil
	}

	var resultBatch service.MessageBatch

	for _, msg := range batch {
		// Use VM pool for consistent performance
		vm := p.getVM()

		// Convert message to JS object
		jsMsg, err := nodered_js_plugin.ConvertMessageToJSObject(msg)
		if err != nil {
			p.logError(err, "message conversion", msg)
			p.putVM(vm)
			return nil, fmt.Errorf("failed to convert message to JavaScript object: %v", err)
		}

		// Setup VM environment
		if err := p.setupMessageForVM(vm, msg, jsMsg); err != nil {
			p.logError(err, "JS environment setup", jsMsg)
			p.putVM(vm)
			return nil, fmt.Errorf("failed to setup JavaScript environment: %v", err)
		}

		// Execute compiled program for maximum performance
		messages, err := p.executeCompiledProgram(vm, program, jsMsg, stageName)
		if err != nil {
			p.putVM(vm)
			return nil, err
		}

		// Handle message dropping
		if messages == nil {
			p.putVM(vm)
			continue
		}

		// Convert results back to Benthos messages
		for _, resultMsg := range messages {
			newMsg := service.NewMessage(nil)
			// Set metadata from the JS message
			if meta, ok := resultMsg["meta"].(map[string]interface{}); ok {
				for k, v := range meta {
					newMsg.MetaSet(k, fmt.Sprintf("%v", v))
				}
			}
			// Set payload
			if payload, exists := resultMsg["payload"]; exists {
				newMsg.SetStructured(payload)
			} else {
				newMsg.SetStructured(jsMsg["payload"])
			}

			resultBatch = append(resultBatch, newMsg)
		}

		// Return VM to pool after processing this message
		p.putVM(vm)
	}

	return resultBatch, nil
}

// processConditionForMessageWithProgram evaluates a condition using compiled programs (Phase 2 optimization)
func (p *TagProcessor) processConditionForMessageWithProgram(conditionIndex int, msg *service.Message) (service.MessageBatch, error) {
	// Get VM from pool and ensure it's returned
	vm := p.getVM()
	defer p.putVM(vm)

	// Convert message to JS object for condition check
	jsMsg, err := nodered_js_plugin.ConvertMessageToJSObject(msg)
	if err != nil {
		return nil, fmt.Errorf("message conversion failed: %v", err)
	}

	// Setup VM environment using optimized helper method
	if err := p.setupMessageForVM(vm, msg, jsMsg); err != nil {
		return nil, fmt.Errorf("JS environment setup failed: %v", err)
	}

	// Evaluate condition using compiled program
	ifResult, err := vm.RunProgram(p.conditionPrograms[conditionIndex])
	if err != nil {
		p.logJSError(err, p.originalConditions[conditionIndex].If, jsMsg)
		return nil, fmt.Errorf("condition evaluation failed: %v", err)
	}

	// If condition is true, process the message with the compiled condition action
	if ifResult.ToBoolean() {
		conditionBatch, err := p.processMessageBatchWithProgram(
			service.MessageBatch{msg},
			p.conditionThenPrograms[conditionIndex],
			fmt.Sprintf("condition-%d-then", conditionIndex))
		if err != nil {
			return nil, fmt.Errorf("condition processing failed: %v", err)
		}
		// Return all messages produced by the condition action (could be 0, 1, or multiple)
		return conditionBatch, nil
	}

	// Condition was false, return original message
	return service.MessageBatch{msg}, nil
}
