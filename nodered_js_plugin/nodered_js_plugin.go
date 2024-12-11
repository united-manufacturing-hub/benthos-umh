package nodered_js_plugin

import (
	"context"
	"fmt"

	"github.com/dop251/goja"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// init registers the nodered_js processor.
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
					%s
				})()
			`, code)
			return newNodeREDJSProcessor(wrappedCode, mgr.Logger(), mgr.Metrics()), nil
		})
	if err != nil {
		panic(err)
	}
}

// NodeREDJSProcessor defines the processor that wraps the JavaScript processor.
type NodeREDJSProcessor struct {
	code              string
	logger            *service.Logger
	messagesProcessed *service.MetricCounter
	messagesErrored   *service.MetricCounter
	messagesDropped   *service.MetricCounter
}

func newNodeREDJSProcessor(code string, logger *service.Logger, metrics *service.Metrics) *NodeREDJSProcessor {
	return &NodeREDJSProcessor{
		code:              code,
		logger:            logger,
		messagesProcessed: metrics.NewCounter("messages_processed"),
		messagesErrored:   metrics.NewCounter("messages_errored"),
		messagesDropped:   metrics.NewCounter("messages_dropped"),
	}
}

// Helper function to convert a Benthos message to a JavaScript-compatible object
func convertMessageToJSObject(msg *service.Message) (map[string]interface{}, error) {
	jsMsg, err := msg.AsStructured()
	if err != nil {
		// If message can't be converted to structured format, wrap it in a payload field
		bytes, err := msg.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("failed to convert message to bytes: %v", err)
		}
		return map[string]interface{}{
			"payload": string(bytes),
		}, nil
	}

	// If it's not already a map, wrap it in a payload field
	if _, ok := jsMsg.(map[string]interface{}); !ok {
		return map[string]interface{}{
			"payload": jsMsg,
		}, nil
	}

	return jsMsg.(map[string]interface{}), nil
}

// Helper function to setup the JavaScript VM environment
func (u *NodeREDJSProcessor) setupJSEnvironment(vm *goja.Runtime, jsMsg map[string]interface{}) error {
	// Set up the msg variable in the JS environment
	if err := vm.Set("msg", jsMsg); err != nil {
		return fmt.Errorf("failed to set message in JS environment: %v", err)
	}

	// Set up console for logging that uses Benthos logger
	console := map[string]interface{}{
		"log":   func(msg string) { u.logger.Info(msg) },
		"warn":  func(msg string) { u.logger.Warn(msg) },
		"error": func(msg string) { u.logger.Error(msg) },
	}
	if err := vm.Set("console", console); err != nil {
		return fmt.Errorf("failed to set console in JS environment: %v", err)
	}

	return nil
}

// Helper function to handle JavaScript execution results
func (u *NodeREDJSProcessor) handleExecutionResult(result goja.Value) (*service.Message, bool, error) {
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

// ProcessBatch applies the JavaScript code to each message in the batch.
func (u *NodeREDJSProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var resultBatch service.MessageBatch

	for _, msg := range batch {
		u.messagesProcessed.Incr(1)
		vm := goja.New()

		// Convert message to JS object
		jsMsg, err := convertMessageToJSObject(msg)
		if err != nil {
			u.messagesErrored.Incr(1)
			u.logger.Errorf("%v\nOriginal message: %v", err, msg)
			continue
		}

		// Add metadata to the message wrapper
		meta := make(map[string]interface{})
		if err := msg.MetaWalkMut(func(key string, value any) error {
			meta[key] = value
			return nil
		}); err != nil {
			u.messagesErrored.Incr(1)
			u.logger.Errorf("Failed to walk message metadata: %v\nOriginal message: %v", err, msg)
			continue
		}
		jsMsg["meta"] = meta

		// Setup JS environment
		if err := u.setupJSEnvironment(vm, jsMsg); err != nil {
			u.messagesErrored.Incr(1)
			u.logger.Errorf("%v\nMessage content: %v", err, jsMsg)
			continue
		}

		// Execute the JavaScript code
		result, err := vm.RunString(u.code)
		if err != nil {
			u.messagesErrored.Incr(1)
			u.logJSError(err, jsMsg)
			continue
		}

		// Handle the execution result
		newMsg, shouldKeep, err := u.handleExecutionResult(result)
		if err != nil {
			u.messagesErrored.Incr(1)
			u.logger.Errorf("%v\nMessage content: %v\nReturned value: %v", err, jsMsg, result.Export())
			return nil, err
		}
		if shouldKeep {
			resultBatch = append(resultBatch, newMsg)
		}
	}

	if len(resultBatch) == 0 {
		return []service.MessageBatch{}, nil
	}

	return []service.MessageBatch{resultBatch}, nil
}

// Helper function to log JavaScript errors
func (u *NodeREDJSProcessor) logJSError(err error, jsMsg interface{}) {
	if jsErr, ok := err.(*goja.Exception); ok {
		stack := jsErr.String()
		u.logger.Errorf(`JavaScript execution failed:
Error: %v
Stack: %v
Code:
%v
Message content: %v`,
			jsErr.Error(),
			stack,
			u.code,
			jsMsg)
	} else {
		u.logger.Errorf(`JavaScript execution failed:
Error: %v
Code:
%v
Message content: %v`,
			err,
			u.code,
			jsMsg)
	}
}

// Close gracefully shuts down the processor.
func (u *NodeREDJSProcessor) Close(ctx context.Context) error {
	return nil
}
