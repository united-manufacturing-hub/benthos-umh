package tag_processor_plugin

import (
	"context"
	"encoding/json"
	"fmt"
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
- location0: Top-level hierarchy (e.g., "enterprise")
- datacontract: Data schema identifier (e.g., "_historian", "_analytics")
- tagName: Name of the tag/variable (e.g., "temperature")

Optional metadata fields:
- location1-5: Additional hierarchy levels (e.g., site, area, line, workcell, plc)
- path0-N: Logical, non-physical grouping paths (e.g., "axis", "x", "position")

The final topic will be constructed as:
umh.v1.<location0>.<location1>.<location2>.<location3>.<location4>.<location5>.<datacontract>.<path0>.<path1>.<path2>....<pathN>.<tagName>

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

			// Setup JS environment
			if err := p.jsProcessor.SetupJSEnvironment(vm, jsMsg); err != nil {
				p.logError(err, "JS environment setup", jsMsg)
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
				newBatch = append(newBatch, conditionBatch...)
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

	// Handle both single message and array returns
	switch v := result.Export().(type) {
	case []interface{}:
		// Handle array of messages
		messages := make([]map[string]interface{}, 0, len(v))
		for _, msg := range v {
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

		// Convert message to JS object
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

		// Execute the code
		messages, err := p.executeJSCode(vm, code, jsMsg)
		if err != nil {
			return nil, err
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
			}

			resultBatch = append(resultBatch, newMsg)
		}
	}

	return resultBatch, nil
}

// validateMessage checks if a message has all required metadata fields
func (p *TagProcessor) validateMessage(msg *service.Message) error {
	requiredFields := []string{"location0", "datacontract", "tagName"}
	for _, field := range requiredFields {
		if _, exists := msg.MetaGet(field); !exists {
			p.logError(fmt.Errorf("missing field: %s", field), "metadata validation", msg)
			return fmt.Errorf("missing required metadata field '%s'", field)
		}
	}
	return nil
}

// constructFinalMessage creates the final message with proper topic and payload structure
func (p *TagProcessor) constructFinalMessage(msg *service.Message) (*service.Message, error) {
	// Create new message
	newMsg := service.NewMessage(nil)

	// Copy all metadata
	err := msg.MetaWalkMut(func(key string, value any) error {
		if str, ok := value.(string); ok {
			newMsg.MetaSet(key, str)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to copy metadata: %v", err)
	}

	// Construct and set topic
	topic := p.constructTopic(msg)
	newMsg.MetaSet("topic", topic)

	// Get tagName from metadata
	tagName, exists := msg.MetaGet("tagName")
	if !exists {
		return nil, fmt.Errorf("missing tagName in metadata")
	}

	// Get payload value
	structured, err := msg.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("failed to get structured payload: %v", err)
	}

	// Structure payload
	payload := map[string]interface{}{
		tagName:        json.Number(fmt.Sprintf("%v", structured)),
		"timestamp_ms": time.Now().UnixMilli(),
	}
	newMsg.SetStructured(payload)

	return newMsg, nil
}

// constructTopic creates the topic string from message metadata
func (p *TagProcessor) constructTopic(msg *service.Message) string {
	parts := []string{"umh", "v1"}

	// Add location levels
	for i := 0; i <= 5; i++ {
		if value, exists := msg.MetaGet(fmt.Sprintf("location%d", i)); exists && value != "" {
			parts = append(parts, value)
		}
	}

	// Add datacontract
	if value, exists := msg.MetaGet("datacontract"); exists && value != "" {
		parts = append(parts, value)
	}

	// Add path components
	i := 0
	for {
		if value, exists := msg.MetaGet(fmt.Sprintf("path%d", i)); exists && value != "" {
			parts = append(parts, value)
		} else {
			break
		}
		i++
	}

	// Add tagName
	if value, exists := msg.MetaGet("tagName"); exists && value != "" {
		parts = append(parts, value)
	}

	return strings.Join(parts, ".")
}

func (p *TagProcessor) Close(ctx context.Context) error {
	return nil
}
