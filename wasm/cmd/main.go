package main

import (
	"context"
	"encoding/json"
	"fmt"
	"syscall/js"

	_ "github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/united-manufacturing-hub/benthos-umh/nodered_js_plugin"
)

func main() {
	c := make(chan struct{}, 0)

	js.Global().Set("blobl", js.FuncOf(blobl))
	js.Global().Set("nodered_js", js.FuncOf(nodered_js))

	<-c // Keep the program running
}

func blobl(_ js.Value, args []js.Value) any {
	if len(args) != 2 {
		return fmt.Sprintf("Expected two arguments, received %d instead", len(args))
	}
	for i, arg := range args {
		fmt.Printf("arg %d: %+v\n", i, arg)
	}

	mapping, err := bloblang.NewEnvironment().Parse(args[0].String())
	if err != nil {
		return fmt.Sprintf("Failed to parse mapping: %s", err)
	}

	msg, err := service.NewMessage([]byte(args[1].String())).BloblangQuery(mapping)
	if err != nil {
		return fmt.Sprintf("Failed to execute mapping: %s", err)
	}

	message, err := msg.AsStructured()
	if err != nil {
		return fmt.Sprintf("Failed to marshal message: %s", err)
	}

	var metadata map[string]any
	msg.MetaWalkMut(func(key string, value any) error {
		if metadata == nil {
			metadata = make(map[string]any)
		}
		metadata[key] = value
		return nil
	})

	var output []byte
	if output, err = json.MarshalIndent(struct {
		Msg  any            `json:"msg"`
		Meta map[string]any `json:"meta,omitempty"`
	}{
		Msg:  message,
		Meta: metadata,
	}, "", "  "); err != nil {
		return fmt.Sprintf("Failed to marshal output: %s", err)
	}

	return string(output)
}

func nodered_js(_ js.Value, args []js.Value) any {
	if len(args) != 2 {
		return fmt.Sprintf("Expected two arguments, received %d instead", len(args))
	}
	for i, arg := range args {
		fmt.Printf("arg %d: %+v\n", i, arg)
	}

	code := args[0].String()
	processor := nodered_js_plugin.NewNodeREDJSProcessor(code, nil, nil)

	// Create a Benthos message from the second argument.
	msg := service.NewMessage([]byte(args[1].String()))
	batch := service.MessageBatch{msg}

	// Process the message using the NodeRED JS processor.
	batches, err := processor.ProcessBatch(context.Background(), batch)
	if err != nil {
		return fmt.Sprintf("Failed to process message with NodeRED JS plugin: %s", err)
	}
	if len(batches) == 0 || len(batches[0]) == 0 {
		return "Message dropped by NodeRED JS plugin"
	}
	resultMsg := batches[0][0]

	// Retrieve the structured result.
	structured, err := resultMsg.AsStructured()
	if err != nil {
		return fmt.Sprintf("Failed to retrieve structured message: %s", err)
	}

	// Collect metadata, if any.
	var metadata map[string]any
	err = resultMsg.MetaWalkMut(func(key string, value any) error {
		if metadata == nil {
			metadata = make(map[string]any)
		}
		metadata[key] = value
		return nil
	})
	if err != nil {
		return fmt.Sprintf("Failed to walk metadata: %s", err)
	}

	// Marshal the output into a formatted JSON string.
	output, err := json.MarshalIndent(struct {
		Msg  any            `json:"msg"`
		Meta map[string]any `json:"meta,omitempty"`
	}{
		Msg:  structured,
		Meta: metadata,
	}, "", "  ")
	if err != nil {
		return fmt.Sprintf("Failed to marshal output: %s", err)
	}

	return string(output)
}
