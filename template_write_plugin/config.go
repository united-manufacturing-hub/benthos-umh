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

package template_write_plugin

import (
	"errors"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Spec describes the protocol-specific parts of an output's config. The
// engine adds the shared fields.
type ProtocolSpec struct {
	Summary          string
	Description      string
	ConnectionFields []*service.ConfigField
	// AddressDescription explains the protocol's address format.
	AddressDescription string
	AddressExamples    []string
}

// newConfigSpec builds the full config spec: the protocol's connection
// fields, then mappings and readBack, which are identical for every protocol.
func newConfigSpec(spec ProtocolSpec) *service.ConfigSpec {
	addressExamples := make([]any, 0, len(spec.AddressExamples))
	for _, example := range spec.AddressExamples {
		addressExamples = append(addressExamples, example)
	}

	return service.NewConfigSpec().
		Summary(spec.Summary).
		Description(spec.Description + "\n\n" +
			"This output makes one attempt per message and does not retry. The write flow owns retry and backoff. " +
			"Every error starts with a stable reason (invalid_value, not_writable, unresolved, missing_field, " +
			"rejected, readback_mismatch), which the write flow reads from the `fallback_error` metadata. " +
			"Only rejected and readback_mismatch are worth retrying.").
		Fields(spec.ConnectionFields...).
		Field(service.NewObjectListField("mappings",
			service.NewInterpolatedStringField("address").
				Description(spec.AddressDescription+" Supports interpolation.").
				Examples(addressExamples...),
			service.NewStringField("valueFrom").
				Description("Field of the message to take the value from. Use dots for nested fields.").
				Examples("value", "recipe.speed")).
			Description("Which message fields to write to which device addresses. " +
				"All mappings of one message are written together, in the order listed.")).
		Field(service.NewBoolField("readBack").
			Description("Read every written address back and fail the write if the value differs.").
			Default(true).
			Advanced())
}

type mapping struct {
	address   *service.InterpolatedString
	valueFrom string
}

func parseMappings(conf *service.ParsedConfig) ([]mapping, error) {
	mappingConfs, err := conf.FieldObjectList("mappings")
	if err != nil {
		return nil, err
	}
	if len(mappingConfs) == 0 {
		return nil, errors.New("mappings needs at least one entry")
	}

	mappings := make([]mapping, 0, len(mappingConfs))
	for _, mappingConf := range mappingConfs {
		var parsed mapping
		parsed, err = parseMapping(mappingConf)
		if err != nil {
			return nil, err
		}
		mappings = append(mappings, parsed)
	}
	return mappings, nil
}

func parseMapping(conf *service.ParsedConfig) (mapping, error) {
	address, err := conf.FieldInterpolatedString("address")
	if err != nil {
		return mapping{}, err
	}
	valueFrom, err := conf.FieldString("valueFrom")
	if err != nil {
		return mapping{}, err
	}
	return mapping{address: address, valueFrom: valueFrom}, nil
}
