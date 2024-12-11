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

//TODO: change advanced processing to be applied at the end of processing the entire batch, to give the option to aggregate over the entire batch (e.g., merge into a single payload)

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
It applies defaults, conditional transformations, and optional advanced processing using a Node-RED style JavaScript environment.`).
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

	for _, msg := range batch {
		p.messagesProcessed.Incr(1)

		// Process the message
		newMsg, err := p.processMessage(msg)
		if err != nil {
			p.messagesErrored.Incr(1)
			p.logger.Errorf("Failed to process message: %v", err)
			continue
		}

		if newMsg != nil {
			resultBatch = append(resultBatch, newMsg)
		} else {
			p.messagesDropped.Incr(1)
		}
	}

	if len(resultBatch) == 0 {
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

func (p *TagProcessor) processMessage(msg *service.Message) (*service.Message, error) {
	// Create JavaScript runtime
	vm := goja.New()

	// Convert message to JS object using nodered_js_plugin
	jsMsg, err := nodered_js_plugin.ConvertMessageToJSObject(msg)
	if err != nil {
		p.logError(err, "message conversion", msg)
		return nil, fmt.Errorf("failed to convert Benthos message to JavaScript object: %v. Please check message format", err)
	}

	// Initialize meta if it doesn't exist
	if _, exists := jsMsg["meta"]; !exists {
		jsMsg["meta"] = make(map[string]interface{})
	}

	// Add existing metadata to the message wrapper
	meta := jsMsg["meta"].(map[string]interface{})
	if err := msg.MetaWalkMut(func(key string, value any) error {
		meta[key] = value
		return nil
	}); err != nil {
		p.logError(err, "metadata extraction", msg)
		return nil, fmt.Errorf("failed to extract metadata from message: %v. Please check message metadata format", err)
	}

	// Setup JS environment using nodered_js_plugin
	if err := p.jsProcessor.SetupJSEnvironment(vm, jsMsg); err != nil {
		p.logError(err, "JS environment setup", jsMsg)
		return nil, fmt.Errorf("failed to setup JavaScript environment: %v. This might be an internal error", err)
	}

	// Run defaults code
	if p.config.Defaults != "" {
		wrappedCode := fmt.Sprintf(`(function(){%s})()`, p.config.Defaults)
		result, err := vm.RunString(wrappedCode)
		if err != nil {
			p.logJSError(err, p.config.Defaults, jsMsg)
			return nil, fmt.Errorf("JavaScript error in defaults code: %v. Please check your defaults configuration", err)
		}

		// Get the returned message object
		returnedMsg, ok := result.Export().(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("defaults code must return a message object")
		}

		// Update jsMsg with the returned values
		jsMsg = returnedMsg

		// Update VM with new message state
		if err := vm.Set("msg", jsMsg); err != nil {
			p.logError(err, "JS environment update after defaults", jsMsg)
			return nil, fmt.Errorf("failed to update JavaScript environment after defaults: %v. This might be an internal error", err)
		}
	}

	// Run conditions
	for _, condition := range p.config.Conditions {
		// Evaluate if expression
		ifResult, err := vm.RunString(condition.If)
		if err != nil {
			p.logJSError(err, condition.If, jsMsg)
			return nil, fmt.Errorf("JavaScript error in condition expression: %v. Please check your condition 'if' statement", err)
		}

		// If condition is true, run then code
		if ifResult.ToBoolean() {
			wrappedCode := fmt.Sprintf(`(function(){%s})()`, condition.Then)
			result, err := vm.RunString(wrappedCode)
			if err != nil {
				p.logJSError(err, condition.Then, jsMsg)
				return nil, fmt.Errorf("JavaScript error in condition code: %v. Please check your condition 'then' code", err)
			}

			// Get the returned message object
			returnedMsg, ok := result.Export().(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("condition code must return a message object")
			}

			// Update jsMsg with the returned values
			jsMsg = returnedMsg

			// Update VM with new message state
			if err := vm.Set("msg", jsMsg); err != nil {
				p.logError(err, "JS environment update after condition", jsMsg)
				return nil, fmt.Errorf("failed to update JavaScript environment after condition: %v. This might be an internal error", err)
			}
		}
	}

	// Run advanced processing if configured
	if p.config.AdvancedProcessing != "" {
		wrappedCode := fmt.Sprintf(`(function(){%s})()`, p.config.AdvancedProcessing)
		result, err := vm.RunString(wrappedCode)
		if err != nil {
			p.logJSError(err, p.config.AdvancedProcessing, jsMsg)
			return nil, fmt.Errorf("JavaScript error in advanced processing code: %v. Please check your advanced processing configuration", err)
		}

		// Get the returned message object
		returnedMsg, ok := result.Export().(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("advanced processing code must return a message object")
		}

		// Update jsMsg with the returned values
		jsMsg = returnedMsg
	}

	// Validate required fields
	meta, ok := jsMsg["meta"].(map[string]interface{})
	if !ok {
		p.logError(fmt.Errorf("meta field is not a map"), "metadata validation", jsMsg)
		return nil, fmt.Errorf("message is missing the required 'meta' object. Your JavaScript code must set 'msg.meta' as an object with the required fields")
	}

	requiredFields := []string{"level0", "schema", "tagName"}
	for _, field := range requiredFields {
		if _, exists := meta[field]; !exists {
			p.logError(fmt.Errorf("missing field: %s", field), "metadata validation", meta)
			return nil, fmt.Errorf("missing required metadata field '%s'. Your JavaScript code must set 'msg.meta.%s'", field, field)
		}
	}

	// Construct topic
	topic := p.constructTopic(meta)

	// Create final message
	newMsg := service.NewMessage(nil)

	// Set all metadata from the final state
	for k, v := range meta {
		if str, ok := v.(string); ok {
			newMsg.MetaSet(k, str)
		}
	}

	// Set payload from the final state
	payload := map[string]interface{}{
		meta["tagName"].(string): json.Number(fmt.Sprintf("%v", jsMsg["payload"])),
		"timestamp_ms":           time.Now().UnixMilli(),
	}
	newMsg.SetStructured(payload)

	// Set topic
	newMsg.MetaSet("topic", topic)

	return newMsg, nil
}

func (p *TagProcessor) constructTopic(meta map[string]interface{}) string {
	levels := []string{"umh", "v1"}

	// Add hierarchical levels
	for i := 0; i <= 4; i++ {
		key := fmt.Sprintf("level%d", i)
		if value, exists := meta[key]; exists && value != "" {
			levels = append(levels, value.(string))
		}
	}

	// Add schema
	if schema, exists := meta["schema"]; exists && schema != "" {
		levels = append(levels, schema.(string))
	}

	// Add folder
	if folder, exists := meta["folder"]; exists && folder != "" {
		levels = append(levels, folder.(string))
	}

	// Add tagName
	if tagName, exists := meta["tagName"]; exists && tagName != "" {
		levels = append(levels, tagName.(string))
	}

	// Join with dots and normalize
	topic := strings.Join(levels, ".")
	topic = strings.ReplaceAll(topic, "..", ".")
	topic = strings.Trim(topic, ".")

	return topic
}

func (p *TagProcessor) Close(ctx context.Context) error {
	return nil
}
