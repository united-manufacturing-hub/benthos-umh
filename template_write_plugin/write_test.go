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
	"context"
	"errors"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// faultyClient wraps the PLC library client to count requests and fail on demand.
type faultyClient struct {
	Client
	writeRequests int
	writeErr      error
	readOverride  []byte
}

func (c *faultyClient) WriteItems(ctx context.Context, items []Item) error {
	c.writeRequests++
	if c.writeErr != nil {
		return c.writeErr
	}
	return c.Client.WriteItems(ctx, items)
}

func (c *faultyClient) ReadItems(ctx context.Context, items []Item) ([]Item, error) {
	read, err := c.Client.ReadItems(ctx, items)
	if err == nil && c.readOverride != nil {
		for index := range read {
			read[index].Data = c.readOverride
		}
	}
	return read, err
}

var _ = Describe("example write output", func() {
	var (
		device *simulatedPLC
		client *faultyClient
		output service.Output
	)

	// newOutput builds the output from a YAML config, like Benthos does, with
	// the faulty client behind it.
	newOutput := func(yaml string) service.Output {
		conf, err := Spec().ParseYAML(yaml, nil)
		Expect(err).NotTo(HaveOccurred())

		build := func(conf *service.ParsedConfig, _ *service.Resources) (Protocol[devicePoint], error) {
			return buildWith(conf, func(string) Client {
				client = &faultyClient{Client: &plcLibraryClient{device: device}}
				return client
			})
		}
		built, err := newOutput(conf, service.MockResources(), build)
		Expect(err).NotTo(HaveOccurred())
		return built
	}

	message := func(fields map[string]any) *service.Message {
		msg := service.NewMessage(nil)
		msg.SetStructured(fields)
		return msg
	}

	const config = `
host: 192.168.1.10:1234
mappings:
  - { address: INT10, valueFrom: speed }
  - { address: BOOL3, valueFrom: ready }
  - { address: FLOAT20, valueFrom: temp }
`

	BeforeEach(func() {
		device = newSimulatedPLC()
		output = newOutput(config)
		Expect(output.Connect(context.Background())).To(Succeed())
	})

	It("writes every mapping and verifies it", func() {
		err := output.Write(context.Background(), message(map[string]any{"speed": 300, "ready": true, "temp": 1.5}))

		Expect(err).NotTo(HaveOccurred())
		Expect(device.memory).To(Equal(map[string][]byte{
			"INT10":   {0x01, 0x2c},
			"BOOL3":   {1},
			"FLOAT20": {0x3f, 0xc0, 0x00, 0x00},
		}))
	})

	It("reports a value that does not fit as invalid_value and writes nothing", func() {
		err := output.Write(context.Background(), message(map[string]any{"speed": 70000, "ready": true, "temp": 1.5}))

		var writeErr *WriteError
		Expect(errors.As(err, &writeErr)).To(BeTrue())
		Expect(writeErr.Reason).To(Equal(ReasonInvalidValue))
		Expect(strings.HasPrefix(err.Error(), "invalid_value")).To(BeTrue())
		Expect(device.memory).To(BeEmpty())
	})

	It("reports a read-only address as not_writable", func() {
		readOnly := newOutput(`
host: 192.168.1.10:1234
mappings:
  - { address: STATUS1, valueFrom: speed }
`)
		Expect(readOnly.Connect(context.Background())).To(Succeed())

		err := readOnly.Write(context.Background(), message(map[string]any{"speed": 1}))

		var writeErr *WriteError
		Expect(errors.As(err, &writeErr)).To(BeTrue())
		Expect(writeErr.Reason).To(Equal(ReasonNotWritable))
	})

	It("reports a device refusal as rejected and keeps the connection", func() {
		client.writeErr = fmt.Errorf("%w: area is protected", ErrRefused)

		err := output.Write(context.Background(), message(map[string]any{"speed": 1, "ready": true, "temp": 1.5}))

		var writeErr *WriteError
		Expect(errors.As(err, &writeErr)).To(BeTrue())
		Expect(writeErr.Reason).To(Equal(ReasonRejected))

		client.writeErr = nil
		Expect(output.Write(context.Background(), message(map[string]any{"speed": 1, "ready": true, "temp": 1.5}))).To(Succeed())
	})

	It("asks Benthos to reconnect after a transport failure", func() {
		client.writeErr = fmt.Errorf("%w: connection reset", ErrTransport)

		err := output.Write(context.Background(), message(map[string]any{"speed": 1, "ready": true, "temp": 1.5}))

		Expect(err).To(MatchError(service.ErrNotConnected))
		Expect(output.Write(context.Background(), message(map[string]any{"speed": 1, "ready": true, "temp": 1.5}))).
			To(MatchError(service.ErrNotConnected), "the session stays closed until Benthos calls Connect")
		Expect(output.Connect(context.Background())).To(Succeed())
		Expect(output.Write(context.Background(), message(map[string]any{"speed": 1, "ready": true, "temp": 1.5}))).To(Succeed())
	})

	It("fails the write when the device holds another value than written", func() {
		client.readOverride = []byte{0xff}

		err := output.Write(context.Background(), message(map[string]any{"speed": 1, "ready": true, "temp": 1.5}))

		var writeErr *WriteError
		Expect(errors.As(err, &writeErr)).To(BeTrue())
		Expect(writeErr.Reason).To(Equal(ReasonReadbackMismatch))
	})

	It("splits a message with more addresses than one request holds", func() {
		var mappings strings.Builder
		fields := map[string]any{}
		for offset := range 25 {
			fmt.Fprintf(&mappings, "  - { address: INT%d, valueFrom: v%d }\n", offset, offset)
			fields[fmt.Sprintf("v%d", offset)] = offset
		}
		many := newOutput("host: 192.168.1.10:1234\nmappings:\n" + mappings.String())
		Expect(many.Connect(context.Background())).To(Succeed())

		Expect(many.Write(context.Background(), message(fields))).To(Succeed())

		Expect(client.writeRequests).To(Equal(2), "25 items with a limit of 20 per request")
		Expect(device.memory).To(HaveLen(25))
	})
})

var _ = Describe("codec", func() {
	DescribeTable("Prepare",
		func(address string, value any, wantData []byte, wantErr error) {
			got, err := deviceCodec{}.Prepare(address, value)

			if wantErr != nil {
				Expect(err).To(MatchError(wantErr))
				return
			}
			Expect(err).NotTo(HaveOccurred())
			Expect(got.data).To(Equal(wantData))
		},
		Entry("int", "INT10", 300, []byte{0x01, 0x2c}, nil),
		Entry("negative int", "INT10", -1, []byte{0xff, 0xff}, nil),
		Entry("int above int16", "INT10", 32768, nil, ErrInvalidValue),
		Entry("int given a fraction", "INT10", 1.5, nil, ErrInvalidValue),
		Entry("bool true", "BOOL3", true, []byte{1}, nil),
		Entry("bool given a number", "BOOL3", 1, nil, ErrInvalidValue),
		Entry("float", "FLOAT20", 1.5, []byte{0x3f, 0xc0, 0x00, 0x00}, nil),
		Entry("float given a string", "FLOAT20", "1.5", nil, ErrInvalidValue),
		Entry("read-only address", "STATUS1", 1, nil, ErrNotWritable),
		Entry("unknown prefix", "WORD10", 1, nil, ErrInvalidValue),
		Entry("missing offset", "INT", 1, nil, ErrInvalidValue),
	)
})

var _ = Describe("template registration", func() {
	It("does not add an output to the binary", func() {
		var registered []string
		service.GlobalEnvironment().WalkOutputs(func(name string, _ *service.ConfigView) {
			registered = append(registered, name)
		})

		Expect(registered).NotTo(ContainElement(outputName))
	})
})
