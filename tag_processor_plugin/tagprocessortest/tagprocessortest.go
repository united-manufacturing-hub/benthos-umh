// Copyright 2026 UMH Systems GmbH
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

// Package tagprocessortest runs input plugin messages through tag_processor in
// tests, so each input plugin can assert that the type and value it decoded
// reach the UNS payload unchanged.
package tagprocessortest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	// pure registers the `none` tracer and metrics that ResourceBuilder.Build uses by default
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/united-manufacturing-hub/benthos-umh/tag_processor_plugin"
)

const tagProcessorLabel = "input_type_survival"

// Device values every input plugin's fixture list covers. A numeric string
// must stay a string, and SeventeenDigitString exceeds float64 precision, so
// a string that passes through a number loses its last digit.
const (
	NumericString        = "1234"
	NonNumericString     = "abc"
	SeventeenDigitString = "83280661010132726"
	NegativeInteger      = -2
	PositiveInteger      = 42
	FractionalNumber     = 23.5
)

// tagProcessorYAML runs payloadStatement before the defaults set the
// metadata tag_processor requires to build a UNS topic.
func tagProcessorYAML(payloadStatement string) string {
	return `
label: ` + tagProcessorLabel + `
tag_processor:
  defaults: |
    ` + payloadStatement + `
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_raw";
    msg.meta.tag_name = "tag";
    return msg;
`
}

type processedPayload struct {
	Value any `json:"value"`
}

// ExpectTagProcessorKeepsValue fails the spec unless tag_processor writes the
// value the input plugin decoded with the same JSON type and value.
// tag_processor writes an array as a JSON string, so a decoded slice must
// arrive as a string holding a JSON array of the same elements.
func ExpectTagProcessorKeepsValue(decoded any, message *service.Message) {
	ginkgo.GinkgoHelper()

	processed, err := processValue(message, "")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	expectSameTypeAndValue(decoded, processed)
}

// ExpectTagProcessorKeepsPayloadField is ExpectTagProcessorKeepsValue for a
// plugin that emits a JSON object: the defaults script replaces the payload
// with payloadField, as the plugin's documented tag_processor config does.
func ExpectTagProcessorKeepsPayloadField(decoded any, message *service.Message, payloadField string) {
	ginkgo.GinkgoHelper()

	payloadStatement := fmt.Sprintf("msg.payload = msg.payload[%q];", payloadField)
	processed, err := processValue(message, payloadStatement)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	expectSameTypeAndValue(decoded, processed)
}

// processValue runs message through tag_processor, with payloadStatement as the
// first line of the defaults script, and returns the `value` field of the
// output payload, with numbers decoded as json.Number.
func processValue(message *service.Message, payloadStatement string) (any, error) {
	ctx := context.Background()

	resourceBuilder := service.NewResourceBuilder()
	if err := resourceBuilder.AddProcessorYAML(tagProcessorYAML(payloadStatement)); err != nil {
		return nil, fmt.Errorf("add tag_processor: %w", err)
	}
	resources, closeResources, err := resourceBuilder.Build()
	if err != nil {
		return nil, fmt.Errorf("build tag_processor: %w", err)
	}
	defer func() { _ = closeResources(ctx) }()

	var outputBatch service.MessageBatch
	var processErr error
	accessErr := resources.AccessProcessor(ctx, tagProcessorLabel, func(processor *service.ResourceProcessor) {
		outputBatch, processErr = processor.Process(ctx, message)
	})
	if accessErr != nil {
		return nil, fmt.Errorf("access tag_processor: %w", accessErr)
	}
	if processErr != nil {
		return nil, fmt.Errorf("process message: %w", processErr)
	}
	if len(outputBatch) != 1 {
		return nil, fmt.Errorf("tag_processor returned %d messages, want 1", len(outputBatch))
	}

	payloadBytes, err := outputBatch[0].AsBytes()
	if err != nil {
		return nil, fmt.Errorf("read output payload: %w", err)
	}
	var payload processedPayload
	if err := decodeJSONWithNumbers(payloadBytes, &payload); err != nil {
		return nil, fmt.Errorf("decode output payload: %w", err)
	}
	return payload.Value, nil
}

// decodeJSONWithNumbers decodes numbers as json.Number, so a number that
// tag_processor wrote in exponent form (2.340925e+06) fails the integer check.
func decodeJSONWithNumbers(jsonBytes []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(jsonBytes))
	decoder.UseNumber()

	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode %q: %w", jsonBytes, err)
	}
	return nil
}

func expectSameTypeAndValue(decoded any, processed any) {
	ginkgo.GinkgoHelper()

	decodedValue := reflect.ValueOf(decoded)
	switch decodedValue.Kind() {
	case reflect.String:
		gomega.Expect(processed).To(gomega.Equal(decodedValue.String()))
	case reflect.Bool:
		gomega.Expect(processed).To(gomega.Equal(decodedValue.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		processedNumber := expectNumber(processed)
		gomega.Expect(strconv.ParseInt(processedNumber, 10, 64)).To(gomega.Equal(decodedValue.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		processedNumber := expectNumber(processed)
		gomega.Expect(strconv.ParseUint(processedNumber, 10, 64)).To(gomega.Equal(decodedValue.Uint()))
	case reflect.Float32, reflect.Float64:
		processedNumber := expectNumber(processed)
		gomega.Expect(strconv.ParseFloat(processedNumber, 64)).To(gomega.Equal(decodedValue.Float()))
	case reflect.Slice, reflect.Array:
		expectSameElements(decodedValue, processed)
	default:
		ginkgo.Fail(fmt.Sprintf("decoded value %v has type %T, which has no UNS equivalent", decoded, decoded))
	}
}

func expectNumber(processed any) string {
	ginkgo.GinkgoHelper()

	gomega.Expect(processed).To(gomega.BeAssignableToTypeOf(json.Number("")))
	return processed.(json.Number).String()
}

func expectSameElements(decodedValue reflect.Value, processed any) {
	ginkgo.GinkgoHelper()

	gomega.Expect(processed).To(gomega.BeAssignableToTypeOf(""))
	var processedElements []any
	err := decodeJSONWithNumbers([]byte(processed.(string)), &processedElements)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(processedElements).To(gomega.HaveLen(decodedValue.Len()))

	for index := range decodedValue.Len() {
		expectSameTypeAndValue(decodedValue.Index(index).Interface(), processedElements[index])
	}
}
