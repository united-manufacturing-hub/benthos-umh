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
	code            string
	logger          *service.Logger
	executionCount  *service.MetricCounter
	errorCount      *service.MetricCounter
	nullReturnCount *service.MetricCounter
}

func newNodeREDJSProcessor(code string, logger *service.Logger, metrics *service.Metrics) *NodeREDJSProcessor {
	return &NodeREDJSProcessor{
		code:            code,
		logger:          logger,
		executionCount:  metrics.NewCounter("execution_count"),
		errorCount:      metrics.NewCounter("error_count"),
		nullReturnCount: metrics.NewCounter("null_return_count"),
	}
}

// ProcessBatch applies the JavaScript code to each message in the batch.
func (u *NodeREDJSProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var resultBatch service.MessageBatch

	for _, msg := range batch {
		u.executionCount.Incr(1)
		vm := goja.New()

		// Convert the Benthos message to a map
		jsMsg, err := msg.AsStructured()
		if err != nil {
			// If message can't be converted to structured format, use it as-is
			bytes, _ := msg.AsBytes()
			jsMsg = string(bytes)
		}

		// Create a wrapper object that contains both the message content and metadata
		meta := make(map[string]string)
		msg.MetaWalkMut(func(key string, value any) error {
			meta[key] = value.(string)
			return nil
		})
		msgWrapper := map[string]interface{}{
			"payload": jsMsg,
			"meta":    meta,
		}

		// Set up the msg variable in the JS environment
		if err := vm.Set("msg", msgWrapper); err != nil {
			u.errorCount.Incr(1)
			u.logger.Errorf("Failed to set message in JS environment: %v\nMessage content: %v", err, jsMsg)
			return []service.MessageBatch{}, nil
		}

		// Set up console for logging that uses Benthos logger
		console := map[string]interface{}{
			"log":   func(msg string) { u.logger.Info(msg) },
			"warn":  func(msg string) { u.logger.Warn(msg) },
			"error": func(msg string) { u.logger.Error(msg) },
		}
		if err := vm.Set("console", console); err != nil {
			u.errorCount.Incr(1)
			u.logger.Errorf("Failed to set console in JS environment: %v\nMessage content: %v", err, jsMsg)
			return []service.MessageBatch{}, nil
		}

		// Execute the user-provided JavaScript code
		result, err := vm.RunString(u.code)
		if err != nil {
			u.errorCount.Incr(1)
			// Check if it's a JavaScript error with stack trace
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
			return []service.MessageBatch{}, nil
		}

		// Handle the return value like Node-RED:
		// - If null/undefined is returned, stop processing this message
		// - If an object is returned, use it as the new message
		if result.Equals(goja.Undefined()) || result.Equals(goja.Null()) {
			u.nullReturnCount.Incr(1)
			u.logger.Debug("Message dropped due to null/undefined return")
			continue
		}

		returnedMsg, ok := result.Export().(map[string]interface{})
		if !ok {
			u.errorCount.Incr(1)
			u.logger.Error("Function must return a message object or null")
			return nil, fmt.Errorf("function must return a message object or null")
		}

		// Create a new message with the returned content
		newMsg := service.NewMessage(nil)
		if payload, exists := returnedMsg["payload"]; exists {
			newMsg.SetStructured(payload)
		}
		if meta, exists := returnedMsg["meta"].(map[string]string); exists {
			for k, v := range meta {
				newMsg.MetaSet(k, v)
			}
		}
		resultBatch = append(resultBatch, newMsg)
	}

	if len(resultBatch) == 0 {
		// If no messages were produced, return empty batch list
		return []service.MessageBatch{}, nil
	}

	return []service.MessageBatch{resultBatch}, nil
}

// Close gracefully shuts down the processor.
func (u *NodeREDJSProcessor) Close(ctx context.Context) error {
	return nil
}
