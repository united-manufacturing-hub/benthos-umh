package tag_processor_plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dop251/goja"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin"
)

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
	config            TagProcessorConfig
	logger            *service.Logger
	messagesProcessed *service.MetricCounter
	messagesErrored   *service.MetricCounter
	messagesDropped   *service.MetricCounter
	jsProcessor       *nodered_js_plugin.NodeREDJSProcessor
}

func newTagProcessor(config TagProcessorConfig, logger *service.Logger, metrics *service.Metrics) (*TagProcessor, error) {
	return &TagProcessor{
		config:            config,
		logger:            logger,
		messagesProcessed: metrics.NewCounter("messages_processed"),
		messagesErrored:   metrics.NewCounter("messages_errored"),
		messagesDropped:   metrics.NewCounter("messages_dropped"),
		jsProcessor:       nodered_js_plugin.NewNodeREDJSProcessor("", logger, metrics),
	}, nil
}

// TODO that everytime htere is any execution error, the code where the error happened is outputted as well as the message that caused it (see nodered_js_plugin). Need to double check that it is not double outputted with the ndoered_js_plugin
func (p *TagProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var resultBatch service.MessageBatch

	// Process defaults
	if p.config.Defaults != "" {
		var err error
		batch, err = p.processMessageBatch(batch, p.config.Defaults)
		if err != nil {
			return nil, fmt.Errorf("error in defaults processing: %v", err)
		}
	}

	// Process conditions
	for _, condition := range p.config.Conditions {
		var newBatch service.MessageBatch

		for _, msg := range batch {
			// Create JavaScript runtime for condition evaluation
			vm := goja.New()

			// Convert message to JS object for condition check
			jsMsg, err := nodered_js_plugin.ConvertMessageToJSObject(msg)
			if err != nil {
				p.logError(err, "message conversion", msg)
				continue
			}

			// Initialize meta if it doesn't exist
			if _, exists := jsMsg["meta"]; !exists {
				jsMsg["meta"] = make(map[string]interface{})
			}

			// Add existing metadata
			meta := jsMsg["meta"].(map[string]interface{})
			if err := msg.MetaWalkMut(func(key string, value any) error {
				meta[key] = value
				return nil
			}); err != nil {
				p.logError(err, "metadata extraction", msg)
				continue
			}

			// Setup JS environment
			if err := p.jsProcessor.SetupJSEnvironment(vm, jsMsg); err != nil {
				p.logError(err, "JS environment setup", jsMsg)
				continue
			}

			// Set msg variable in VM
			if err := vm.Set("msg", jsMsg); err != nil {
				p.logError(err, "JS message setup", jsMsg)
				continue
			}

			// Evaluate condition
			ifResult, err := vm.RunString(condition.If)
			if err != nil {
				p.logJSError(err, condition.If, jsMsg)
				continue
			}

			// If condition is true, process the message
			if ifResult.ToBoolean() {
				conditionBatch, err := p.processMessageBatch(service.MessageBatch{msg}, condition.Then)
				if err != nil {
					p.logError(err, "condition processing", msg)
					continue
				}
				if len(conditionBatch) > 0 {
					newBatch = append(newBatch, conditionBatch...)
				}
			} else {
				newBatch = append(newBatch, msg)
			}
		}

		batch = newBatch
	}

	// Process advanced processing
	if p.config.AdvancedProcessing != "" {
		var err error
		batch, err = p.processMessageBatch(batch, p.config.AdvancedProcessing)
		if err != nil {
			return nil, fmt.Errorf("error in advanced processing: %v", err)
		}
	}

	// Validate and construct final messages
	for _, msg := range batch {
		// Skip nil messages
		if msg == nil {
			continue
		}

		// Validate required fields
		if err := p.validateMessage(msg); err != nil {
			p.messagesErrored.Incr(1)
			p.logger.Errorf("Message validation failed: %v", err)
			continue
		}

		// Construct final message
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
		return nil, nil
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

// executeJSCode executes JavaScript code and returns an array of message objects
func (p *TagProcessor) executeJSCode(vm *goja.Runtime, code string, jsMsg map[string]interface{}) ([]map[string]interface{}, error) {
	wrappedCode := fmt.Sprintf(`(function(){%s})()`, code)
	result, err := vm.RunString(wrappedCode)
	if err != nil {
		p.logJSError(err, code, jsMsg)
		return nil, fmt.Errorf("JavaScript error in code: %v", err)
	}

	// Handle null/undefined returns (message dropped)
	if result == nil || goja.IsNull(result) || goja.IsUndefined(result) {
		return nil, nil
	}

	// Handle both single message and array returns
	switch v := result.Export().(type) {
	case []interface{}:
		// Handle array of messages
		messages := make([]map[string]interface{}, 0, len(v))
		for _, msg := range v {
			if msg == nil {
				continue // Skip nil messages in array
			}
			if msgMap, ok := msg.(map[string]interface{}); ok {
				messages = append(messages, msgMap)
			} else {
				return nil, fmt.Errorf("array elements must be message objects")
			}
		}
		return messages, nil
	case map[string]interface{}:
		// Handle single message
		return []map[string]interface{}{v}, nil
	default:
		return nil, fmt.Errorf("code must return a message object or array of message objects")
	}
}

// processMessageBatch processes a batch of messages with the given JavaScript code
func (p *TagProcessor) processMessageBatch(batch service.MessageBatch, code string) (service.MessageBatch, error) {
	if code == "" {
		return batch, nil
	}

	var resultBatch service.MessageBatch

	for _, msg := range batch {
		// Create JavaScript runtime for this message
		vm := goja.New()

		// Convert message to JS object with the payload being in the payload field
		jsMsg, err := nodered_js_plugin.ConvertMessageToJSObject(msg)
		if err != nil {
			p.logError(err, "message conversion", msg)
			return nil, fmt.Errorf("failed to convert message to JavaScript object: %v", err)
		}

		// Initialize meta if it doesn't exist
		if _, exists := jsMsg["meta"]; !exists {
			jsMsg["meta"] = make(map[string]interface{})
		}

		// Add existing metadata
		meta := jsMsg["meta"].(map[string]interface{})
		if err := msg.MetaWalkMut(func(key string, value any) error {
			meta[key] = value
			return nil
		}); err != nil {
			p.logError(err, "metadata extraction", msg)
			return nil, fmt.Errorf("failed to extract metadata: %v", err)
		}

		// Setup JS environment
		if err := p.jsProcessor.SetupJSEnvironment(vm, jsMsg); err != nil {
			p.logError(err, "JS environment setup", jsMsg)
			return nil, fmt.Errorf("failed to setup JavaScript environment: %v", err)
		}

		// Set msg variable in VM
		if err := vm.Set("msg", jsMsg); err != nil {
			p.logError(err, "JS message setup", jsMsg)
			return nil, fmt.Errorf("failed to set message in JavaScript environment: %v", err)
		}

		// Execute the code
		messages, err := p.executeJSCode(vm, code, jsMsg)
		if err != nil {
			return nil, err
		}

		// Skip if message was dropped (null return)
		if messages == nil {
			continue
		}

		// Convert resulting messages back to Benthos messages
		for _, resultMsg := range messages {
			newMsg := service.NewMessage(nil)

			// Set metadata
			if meta, ok := resultMsg["meta"].(map[string]interface{}); ok {
				for k, v := range meta {
					if str, ok := v.(string); ok {
						newMsg.MetaSet(k, str)
					}
				}
			}

			// Set payload
			if payload, exists := resultMsg["payload"]; exists {
				newMsg.SetStructured(payload)
			} else {
				// If no payload in result, copy from original message
				newMsg.SetStructured(jsMsg["payload"])
			}

			resultBatch = append(resultBatch, newMsg)
		}
	}

	return resultBatch, nil
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
		// Get message payload for context
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
func (p *TagProcessor) constructFinalMessage(msg *service.Message) (*service.Message, error) {
    newMsg := service.NewMessage(nil)

    // Copy all metadata from the original message.
    allMeta := map[string]string{}
    _ = msg.MetaWalkMut(func(k string, v any) error {
        if strVal, ok := v.(string); ok {
            allMeta[k] = strVal
        }
        return nil
    })

    // Construct the topic from UMH system fields.
    topic := p.constructTopic(msg)
    newMsg.MetaSet("topic", topic)

    // Retrieve the tag name (must be present).
    tagName, exists := allMeta["tag_name"]
    if !exists {
        return nil, fmt.Errorf("missing 'tag_name' in metadata")
    }

    // Get the payload value.
    structured, err := msg.AsStructured()
    if err != nil {
        return nil, fmt.Errorf("failed to get structured payload: %v", err)
    }
    value := p.convertValue(structured)

    // Check if advanced users want to include all meta fields.
    includeAll := allMeta["includeAll"] == "true"

    // Define system fields that are excluded from final metadata by default.
    systemFields := map[string]bool{
        "location_path": true,
        "data_contract": true,
        "tag_name":      true,
        "virtual_path":  true,
        "topic":         true,
        "includeAll":    true,
    }

    // Build the user metadata map.
    userMetadata := map[string]interface{}{}
    for k, v := range allMeta {
        if includeAll || !systemFields[k] {
            userMetadata[k] = v
        }
    }

    // Build the final payload.
    payload := map[string]interface{}{
        tagName:        value,
        "timestamp_ms": time.Now().UnixMilli(),
        "metadata":     userMetadata,
    }
    newMsg.SetStructured(payload)

    return newMsg, nil
}

// convertValue recursively converts values to their appropriate types
func (p *TagProcessor) convertValue(v interface{}) interface{} {
	switch val := v.(type) {
	case bool:
		return val
	case string:
		// Try to convert string to number if possible
		if num, err := strconv.ParseFloat(val, 64); err == nil {
			return json.Number(fmt.Sprintf("%v", num))
		}
		return val
	case float64, float32, int, int32, int64, uint, uint32, uint64:
		return json.Number(fmt.Sprintf("%v", val))
	case []interface{}:
		return fmt.Sprintf("%v", val)
	case map[string]interface{}:
		// Recursively convert values in the map
		converted := make(map[string]interface{})
		for k, v := range val {
			converted[k] = p.convertValue(v)
		}
		return converted
	default:
		// For any other type, try to convert to number if it's a string representation
		str := fmt.Sprintf("%v", val)
		if num, err := strconv.ParseFloat(str, 64); err == nil {
			return json.Number(fmt.Sprintf("%v", num))
		}
		return str
	}
}

// constructTopic creates the topic string from message metadata
func (p *TagProcessor) constructTopic(msg *service.Message) string {
	parts := []string{"umh", "v1"}

	// Add location path
	if value, exists := msg.MetaGet("location_path"); exists && value != "" {
		parts = append(parts, strings.Split(value, ".")...)
	}

	// Add data_contract
	if value, exists := msg.MetaGet("data_contract"); exists && value != "" {
		parts = append(parts, value)
	}

	// Add virtual_path if it exists
	if value, exists := msg.MetaGet("virtual_path"); exists && value != "" {
		parts = append(parts, strings.Split(value, ".")...)
	}

	// Add tag_name
	if value, exists := msg.MetaGet("tag_name"); exists && value != "" {
		parts = append(parts, value)
	}

	return strings.Join(parts, ".")
}

func (p *TagProcessor) Close(ctx context.Context) error {
	return nil
}
