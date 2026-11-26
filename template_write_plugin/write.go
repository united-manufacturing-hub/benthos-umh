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
	"github.com/redpanda-data/benthos/v4/public/service"
)

// outputName is the output's name in a Benthos config. Use the same name as
// the protocol's input, so the read/write pairing in cmd/schema-export picks
// it up. The template has no input, and is not registered, so this name is
// reserved for nothing.
const outputName = "template_write"

// Spec is the output's config spec: the protocol's connection fields, then
// the mappings and readBack fields that the engine (config.go) adds for every protocol.
//
// Declare every field with .Default(), .Optional() and .Examples() as the
// comments on FieldSpec in cmd/schema-export/types.go describe, because the
// Management Console renders its form from this spec.
func Spec() *service.ConfigSpec {
	return newConfigSpec(ProtocolSpec{
		Summary:     "Template: writes message fields to an example device.",
		Description: "Copy-me template for device write outputs. It is not registered.",
		ConnectionFields: []*service.ConfigField{
			service.NewStringField("host").
				Description("Address of the device, as host:port.").
				Examples("192.168.1.10:1234"),
		},
		AddressDescription: "Address to write: INT, BOOL or FLOAT followed by an offset.",
		AddressExamples:    []string{"INT10", "BOOL3", "FLOAT20"},
	})
}

// build reads the protocol's own fields and returns its codec and dialer. It
// is the BuildFunc that Register and New expect.
func build(conf *service.ParsedConfig, _ *service.Resources) (Protocol[devicePoint], error) {
	// One simulated PLC per output, shared by every client the dialer makes.
	// A real adapter dials host instead.
	device := newSimulatedPLC()
	return buildWith(conf, func(string) Client { return &plcLibraryClient{device: device} })
}

// buildWith takes the client constructor so tests can inject a faulty one.
func buildWith(conf *service.ParsedConfig, newClient func(host string) Client) (Protocol[devicePoint], error) {
	host, err := conf.FieldString("host")
	if err != nil {
		return Protocol[devicePoint]{}, err
	}
	return Protocol[devicePoint]{
		Codec:  deviceCodec{},
		Dialer: &deviceDialer{host: host, newClient: newClient},
	}, nil
}

// Register registers the output with Benthos.
//
// Nothing calls it, on purpose: a template must not add an output to the
// binary. A real plugin calls its own Register from init() and panics on
// error, like the other plugins in this repository.
//
// The output runs with one message in flight: write order matters for
// recipes and handshakes, and most device libraries serialize requests anyway.
func Register() error {
	return service.RegisterOutput(outputName, Spec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			output, err := newOutput(conf, mgr, build)
			if err != nil {
				return nil, 0, err
			}
			return output, 1, nil
		})
}
