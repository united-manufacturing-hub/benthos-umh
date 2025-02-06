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

func (p *TagProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var resultBatch service.MessageBatch

	// ─── NEW FEATURE: Store incoming metadata ────────────────────────────────
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
			vm := goja.New()

			jsMsg, err := nodered_js_plugin.ConvertMessageToJSObject(msg)
			if err != nil {
				p.logError(err, "message conversion", msg)
				continue
			}

			// Initialize meta if it doesn't exist
			if _, exists := jsMsg["meta"]; !exists {
				jsMsg["meta"] = make(map[string]interface{})
			}

			meta := jsMsg["meta"].(map[string]interface{})
			if err := msg.MetaWalkMut(func(key string, value any) error {
				meta[key] = value
				return nil
			}); err != nil {
				p.logError(err, "metadata extraction", msg)
				continue
			}

			if err := p.jsProcessor.SetupJSEnvironment(vm, jsMsg); err != nil {
				p.logError(err, "JS environment setup", jsMsg)
				continue
			}

			if err := vm.Set("msg", jsMsg); err != nil {
				p.logError(err, "JS message setup", jsMsg)
				continue
			}

			ifResult, err := vm.RunString(condition.If)
			if err != nil {
				p.logJSError(err, condition.If, jsMsg)
				continue
			}

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
		return nil, nil
	}

	return []service.MessageBatch{resultBatch}, nil
}

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

func (p *TagProcessor) logError(err error, stage string, msg interface{}) {
	p.logger.Errorf(`Processing failed at stage '%s':
Error: %v
Message content: %v`,
		stage,
		err,
		msg)
}

func (p *TagProcessor) executeJSCode(vm *goja.Runtime, code string, jsMsg map[string]interface{}) ([]map[string]interface{}, error) {
	wrappedCode := fmt.Sprintf(`(function(){%s})()`, code)
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

func (p *TagProcessor) processMessageBatch(batch service.MessageBatch, code string) (service.MessageBatch, error) {
	if code == "" {
		return batch, nil
	}

	var resultBatch service.MessageBatch

	for _, msg := range batch {
		vm := goja.New()

		jsMsg, err := nodered_js_plugin.ConvertMessageToJSObject(msg)
		if err != nil {
			p.logError(err, "message conversion", msg)
			return nil, fmt.Errorf("failed to convert message to JavaScript object: %v", err)
		}

		if _, exists := jsMsg["meta"]; !exists {
			jsMsg["meta"] = make(map[string]interface{})
		}

		meta := jsMsg["meta"].(map[string]interface{})
		if err := msg.MetaWalkMut(func(key string, value any) error {
			meta[key] = value
			return nil
		}); err != nil {
			p.logError(err, "metadata extraction", msg)
			return nil, fmt.Errorf("failed to extract metadata: %v", err)
		}

		if err := p.jsProcessor.SetupJSEnvironment(vm, jsMsg); err != nil {
			p.logError(err, "JS environment setup", jsMsg)
			return nil, fmt.Errorf("failed to setup JavaScript environment: %v", err)
		}

		if err := vm.Set("msg", jsMsg); err != nil {
			p.logError(err, "JS message setup", jsMsg)
			return nil, fmt.Errorf("failed to set message in JavaScript environment: %v", err)
		}

		messages, err := p.executeJSCode(vm, code, jsMsg)
		if err != nil {
			return nil, err
		}

		if messages == nil {
			continue
		}

		for _, resultMsg := range messages {
			newMsg := service.NewMessage(nil)
			if meta, ok := resultMsg["meta"].(map[string]interface{}); ok {
				for k, v := range meta {
					if str, ok := v.(string); ok {
						newMsg.MetaSet(k, str)
					}
				}
			}
			if payload, exists := resultMsg["payload"]; exists {
				newMsg.SetStructured(payload)
			} else {
				newMsg.SetStructured(jsMsg["payload"])
			}

			resultBatch = append(resultBatch, newMsg)
		}
	}

	return resultBatch, nil
}

func (p *TagProcessor) validateMessage(msg *service.Message) error {
	requiredFields := []string{"location_path", "data_contract", "tag_name"}
	missingFields := []string{}

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
			payloadJSON, err := json.MarshalIndent(payload, "", "  ")
			if err != nil {
				payloadStr = fmt.Sprintf("%v", payload)
			} else {
				payloadStr = string(payloadJSON)
			}
		}

		metadataJSON, err := json.MarshalIndent(metadata, "", "  ")
		if err != nil {
			metadataJSON = []byte("unable to format metadata as JSON")
		}

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

//
// ─── FINAL MESSAGE CONSTRUCTION WITH FILTERED METADATA ──────────────────────
//
// Here we remove from the output meta both the keys that were part of the original incoming
// metadata (as recorded in _incomingKeys) and those that were generated internally (e.g. tag_name, topic).
//
func (p *TagProcessor) constructFinalMessage(msg *service.Message) (*service.Message, error) {
    newMsg := service.NewMessage(nil)

    // Retrieve the original incoming metadata stored in _initialMetadata.
    originalMetaRaw, _ := msg.MetaGet("_initialMetadata")
    originalMeta := map[string]string{}
    if originalMetaRaw != "" {
        if err := json.Unmarshal([]byte(originalMetaRaw), &originalMeta); err != nil {
            p.logger.Errorf("failed to unmarshal _initialMetadata: %v", err)
        }
    }

    // Define a set of internal keys that should always be removed.
    // These keys are considered internal and are not to be part of the final output.
    internalKeys := map[string]bool{
        "tag_name":         true,
        "topic":            true,
        "_initialMetadata": true,
        "location_path":    true,
        "data_contract":    true,
        "virtual_path":     true,
    }

    // Build the filtered metadata object.
    filteredMeta := make(map[string]string)
    err := msg.MetaWalkMut(func(key string, value any) error {
        if internalKeys[key] {
            return nil // Skip internal keys.
        }
        // If the key existed originally and its value is unchanged, skip it.
        if origVal, exists := originalMeta[key]; exists {
            if strVal, ok := value.(string); ok && strVal == origVal {
                return nil
            }
        }
        if str, ok := value.(string); ok {
            filteredMeta[key] = str
        }
        return nil
    })
    if err != nil {
        return nil, fmt.Errorf("failed to filter metadata: %v", err)
    }

    // Retrieve tag_name from the message meta.
    tagName, exists := msg.MetaGet("tag_name")
    if !exists {
        return nil, fmt.Errorf("missing tag_name in metadata")
    }

    // Get the structured payload from the original message.
    structured, err := msg.AsStructured()
    if err != nil {
        return nil, fmt.Errorf("failed to get structured payload: %v", err)
    }
    value := p.convertValue(structured)

    // Build the final payload object.
    // Note that metadata is now placed under the key "meta" inside the payload.
    finalPayload := map[string]interface{}{
        tagName:        value,
        "timestamp_ms": time.Now().UnixMilli(),
        "meta":         filteredMeta,
    }
    newMsg.SetStructured(finalPayload)

    return newMsg, nil
}

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
		converted := make(map[string]interface{})
		for k, v := range val {
			converted[k] = p.convertValue(v)
		}
		return converted
	default:
		str := fmt.Sprintf("%v", val)
		if num, err := strconv.ParseFloat(str, 64); err == nil {
			return json.Number(fmt.Sprintf("%v", num))
		}
		return str
	}
}

func (p *TagProcessor) constructTopic(msg *service.Message) string {
	parts := []string{"umh", "v1"}

	if value, exists := msg.MetaGet("location_path"); exists && value != "" {
		parts = append(parts, strings.Split(value, ".")...)
	}

	if value, exists := msg.MetaGet("data_contract"); exists && value != "" {
		parts = append(parts, value)
	}

	if value, exists := msg.MetaGet("virtual_path"); exists && value != "" {
		parts = append(parts, strings.Split(value, ".")...)
	}

	if value, exists := msg.MetaGet("tag_name"); exists && value != "" {
		parts = append(parts, value)
	}

	return strings.Join(parts, ".")
}

func (p *TagProcessor) Close(ctx context.Context) error {
	return nil
}