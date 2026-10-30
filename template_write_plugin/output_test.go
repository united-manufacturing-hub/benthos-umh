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

// A fake protocol: integer registers 0..100 addressed by name. In resolving
// mode the device knows a symbol table and the codec range-checks by size.

type fakePoint struct {
	address string
	value   int64
}

func (p fakePoint) Address() string                { return p.address }
func (p fakePoint) SameValue(other fakePoint) bool { return p.value == other.value }

type fakeCodec struct{}

func (fakeCodec) Prepare(address string, value any) (fakePoint, error) {
	number, err := IntegerInRange(value, 0, 100)
	if err != nil {
		return fakePoint{}, err
	}
	return fakePoint{address: address, value: number}, nil
}

// PrepareResolved allows values up to the resolved size.
func (fakeCodec) PrepareResolved(address string, resolved Resolved, value any) (fakePoint, error) {
	number, err := IntegerInRange(value, 0, int64(resolved.Size))
	if err != nil {
		return fakePoint{}, err
	}
	return fakePoint{address: address, value: number}, nil
}

type fakeDevice struct {
	memory       map[string]int64
	symbols      map[string]Resolved // non-nil = resolving session
	resolveCalls [][]string
	writes       [][]fakePoint
	dials        int
	closes       int
	writeErr     error
	readOverride *int64
	batchSize    int
	noReadBack   bool
}

func (d *fakeDevice) Dial(context.Context) (Session[fakePoint], error) {
	d.dials++
	base := &fakeSession{device: d}
	switch {
	case d.noReadBack:
		return writeOnlySession{base: base}, nil
	case d.batchSize > 0:
		return batchingSession{fakeSession: base, size: d.batchSize}, nil
	case d.symbols != nil:
		return resolvingSession{fakeSession: base}, nil
	default:
		return base, nil
	}
}

type fakeSession struct{ device *fakeDevice }

func (s *fakeSession) Write(_ context.Context, points []fakePoint) error {
	if s.device.writeErr != nil {
		return s.device.writeErr
	}
	s.device.writes = append(s.device.writes, append([]fakePoint(nil), points...))
	for _, point := range points {
		s.device.memory[point.address] = point.value
	}
	return nil
}

func (s *fakeSession) ReadBack(_ context.Context, points []fakePoint) ([]fakePoint, error) {
	read := make([]fakePoint, len(points))
	for index, point := range points {
		value := s.device.memory[point.address]
		if s.device.readOverride != nil {
			value = *s.device.readOverride
		}
		read[index] = fakePoint{address: point.address, value: value}
	}
	return read, nil
}

func (s *fakeSession) Close() error {
	s.device.closes++
	return nil
}

type batchingSession struct {
	*fakeSession
	size int
}

func (s batchingSession) Batch(points []fakePoint) [][]fakePoint {
	var batches [][]fakePoint
	for start := 0; start < len(points); start += s.size {
		batches = append(batches, points[start:min(start+s.size, len(points))])
	}
	return batches
}

type resolvingSession struct{ *fakeSession }

func (s resolvingSession) Resolve(_ context.Context, addresses []string) (map[string]Resolved, error) {
	s.device.resolveCalls = append(s.device.resolveCalls, append([]string(nil), addresses...))
	found := map[string]Resolved{}
	for _, address := range addresses {
		if info, ok := s.device.symbols[address]; ok {
			found[address] = info
		}
	}
	return found, nil
}

// writeOnlySession hides ReadBack.
type writeOnlySession struct{ base *fakeSession }

func (s writeOnlySession) Write(ctx context.Context, points []fakePoint) error {
	return s.base.Write(ctx, points)
}
func (s writeOnlySession) Close() error { return s.base.Close() }

var fakeSpec = newConfigSpec(ProtocolSpec{
	Summary:            "fake",
	AddressDescription: "Register name.",
	AddressExamples:    []string{"speed"},
})

func newFakeOutput(yamlConfig string, device *fakeDevice) service.Output {
	conf, err := fakeSpec.ParseYAML(yamlConfig, nil)
	Expect(err).NotTo(HaveOccurred())
	output, err := newOutput(conf, service.MockResources(),
		func(*service.ParsedConfig, *service.Resources) (Protocol[fakePoint], error) {
			return Protocol[fakePoint]{Codec: fakeCodec{}, Dialer: device}, nil
		})
	Expect(err).NotTo(HaveOccurred())
	return output
}

const twoMappings = `
mappings:
  - address: speed
    valueFrom: recipe.speed
  - address: ready
    valueFrom: ready
`

func message(body string) *service.Message { return service.NewMessage([]byte(body)) }

func reasonOf(err error) Reason {
	var writeErr *WriteError
	if errors.As(err, &writeErr) {
		return writeErr.Reason
	}
	return ""
}

var _ = Describe("OT write engine", func() {
	var (
		device *fakeDevice
		ctx    context.Context
	)

	BeforeEach(func() {
		device = &fakeDevice{memory: map[string]int64{}}
		ctx = context.Background()
	})

	It("writes all mappings of a message together, in config order", func() {
		output := newFakeOutput(twoMappings, device)
		Expect(output.Connect(ctx)).To(Succeed())

		Expect(output.Write(ctx, message(`{"recipe": {"speed": 42}, "ready": 1}`))).To(Succeed())

		Expect(device.writes).To(Equal([][]fakePoint{{{"speed", 42}, {"ready", 1}}}))
	})

	It("splits the write when the session batches", func() {
		device.batchSize = 1
		output := newFakeOutput(twoMappings, device)
		Expect(output.Connect(ctx)).To(Succeed())

		Expect(output.Write(ctx, message(`{"recipe": {"speed": 42}, "ready": 1}`))).To(Succeed())

		Expect(device.writes).To(Equal([][]fakePoint{{{"speed", 42}}, {{"ready", 1}}}))
	})

	It("makes exactly one attempt and leaves retrying to Benthos", func() {
		other := int64(9)
		device.readOverride = &other
		output := newFakeOutput(twoMappings, device)
		Expect(output.Connect(ctx)).To(Succeed())

		err := output.Write(ctx, message(`{"recipe": {"speed": 42}, "ready": 1}`))

		Expect(reasonOf(err)).To(Equal(ReasonReadbackMismatch), "got %v", err)
		Expect(device.writes).To(HaveLen(1))
	})

	It("puts a stable reason first in the error text, for fallback_error", func() {
		output := newFakeOutput(twoMappings, device)
		Expect(output.Connect(ctx)).To(Succeed())

		err := output.Write(ctx, message(`{"recipe": {"speed": 4200}, "ready": 1}`))

		Expect(reasonOf(err)).To(Equal(ReasonInvalidValue))
		Expect(strings.HasPrefix(err.Error(), "invalid_value: speed: ")).To(BeTrue(), "got %q", err.Error())
		Expect(device.writes).To(BeEmpty())
	})

	It("reports a device refusal as rejected and keeps the session", func() {
		device.writeErr = fmt.Errorf("%w: busy", ErrRejected)
		output := newFakeOutput(twoMappings, device)
		Expect(output.Connect(ctx)).To(Succeed())

		err := output.Write(ctx, message(`{"recipe": {"speed": 42}, "ready": 1}`))

		Expect(reasonOf(err)).To(Equal(ReasonRejected), "got %v", err)
		Expect(device.closes).To(Equal(0))
	})

	It("drops the session on a transport error and reconnects on Connect", func() {
		device.writeErr = fmt.Errorf("%w: connection reset", ErrConnectionLost)
		output := newFakeOutput(twoMappings, device)
		Expect(output.Connect(ctx)).To(Succeed())

		err := output.Write(ctx, message(`{"recipe": {"speed": 42}, "ready": 1}`))

		Expect(err).To(MatchError(service.ErrNotConnected))
		Expect(device.closes).To(Equal(1))

		device.writeErr = nil
		Expect(output.Connect(ctx)).To(Succeed())
		Expect(device.dials).To(Equal(2))
		Expect(output.Write(ctx, message(`{"recipe": {"speed": 42}, "ready": 1}`))).To(Succeed())
	})

	It("treats an unclassified session error as a lost connection", func() {
		device.writeErr = errors.New("something the library made up")
		output := newFakeOutput(twoMappings, device)
		Expect(output.Connect(ctx)).To(Succeed())

		Expect(output.Write(ctx, message(`{"recipe": {"speed": 42}, "ready": 1}`))).To(MatchError(service.ErrNotConnected))
	})

	It("reports a missing field", func() {
		output := newFakeOutput(twoMappings, device)
		Expect(output.Connect(ctx)).To(Succeed())

		err := output.Write(ctx, message(`{"ready": 1}`))

		Expect(reasonOf(err)).To(Equal(ReasonMissingField), "got %v", err)
	})

	It("fails Connect when readBack is on but the protocol cannot read back", func() {
		device.noReadBack = true
		output := newFakeOutput(twoMappings, device)

		Expect(output.Connect(ctx)).To(MatchError(ContainSubstring("cannot read values back")))
		Expect(device.closes).To(Equal(1))
	})

	It("writes without read-back when readBack is off", func() {
		device.noReadBack = true
		output := newFakeOutput(`
mappings:
  - address: speed
    valueFrom: value
readBack: false
`, device)
		Expect(output.Connect(ctx)).To(Succeed())

		Expect(output.Write(ctx, message(`{"value": 5}`))).To(Succeed())
		Expect(device.writes).To(HaveLen(1))
	})

	Describe("resolving addresses (ADS symbols, S7+ tags)", func() {
		BeforeEach(func() {
			device.symbols = map[string]Resolved{
				"speed": {DataType: "INT", Size: 50},
				"ready": {DataType: "BOOL", Size: 1},
			}
		})

		It("resolves once per connection and encodes with the resolved type", func() {
			output := newFakeOutput(twoMappings, device)
			Expect(output.Connect(ctx)).To(Succeed())

			Expect(output.Write(ctx, message(`{"recipe": {"speed": 42}, "ready": 1}`))).To(Succeed())
			Expect(output.Write(ctx, message(`{"recipe": {"speed": 43}, "ready": 0}`))).To(Succeed())

			Expect(device.resolveCalls).To(Equal([][]string{{"speed", "ready"}}))
		})

		It("uses the resolved size to reject a value", func() {
			output := newFakeOutput(twoMappings, device)
			Expect(output.Connect(ctx)).To(Succeed())

			err := output.Write(ctx, message(`{"recipe": {"speed": 60}, "ready": 1}`))

			Expect(reasonOf(err)).To(Equal(ReasonInvalidValue), "got %v", err)
		})

		It("reports an address the device does not know", func() {
			delete(device.symbols, "ready")
			output := newFakeOutput(twoMappings, device)
			Expect(output.Connect(ctx)).To(Succeed())

			err := output.Write(ctx, message(`{"recipe": {"speed": 42}, "ready": 1}`))

			Expect(reasonOf(err)).To(Equal(ReasonUnresolved), "got %v", err)
		})

		It("resolves again after a reconnect, because the symbol table may have changed", func() {
			output := newFakeOutput(twoMappings, device)
			Expect(output.Connect(ctx)).To(Succeed())
			Expect(output.Write(ctx, message(`{"recipe": {"speed": 42}, "ready": 1}`))).To(Succeed())

			Expect(output.Close(ctx)).To(Succeed())
			Expect(output.Connect(ctx)).To(Succeed())
			Expect(output.Write(ctx, message(`{"recipe": {"speed": 42}, "ready": 1}`))).To(Succeed())

			Expect(device.resolveCalls).To(HaveLen(2))
		})
	})
})

var _ = Describe("value helpers", func() {
	DescribeTable("IntegerInRange",
		func(value any, wantErr bool) {
			_, err := IntegerInRange(value, -10, 10)
			Expect(err != nil).To(Equal(wantErr), "value %v: got err %v", value, err)
		},
		Entry("JSON integer", jsonNumber("7"), false),
		Entry("integral float64", float64(7), false),
		Entry("fraction", jsonNumber("7.5"), true),
		Entry("above range", jsonNumber("11"), true),
		Entry("bool", true, true),
	)

	It("does not accept numbers as bools", func() {
		_, err := Bool(jsonNumber("1"))
		Expect(errors.Is(err, ErrInvalidValue)).To(BeTrue())
	})
})

var _ = Describe("retry signal", func() {
	DescribeTable("Reason.Retryable",
		func(reason Reason, want bool) {
			Expect(reason.Retryable()).To(Equal(want))
		},
		Entry("invalid_value never succeeds on a second try", ReasonInvalidValue, false),
		Entry("not_writable never succeeds on a second try", ReasonNotWritable, false),
		Entry("missing_field never succeeds on a second try", ReasonMissingField, false),
		Entry("unresolved is left to a new message", ReasonUnresolved, false),
		Entry("rejected can clear when the device state changes", ReasonRejected, true),
		Entry("readback_mismatch can clear when the device state changes", ReasonReadbackMismatch, true),
	)

	DescribeTable("ParseReason reads what Error() wrote",
		func(text string, wantReason Reason, wantOK bool) {
			got, ok := ParseReason(text)

			Expect(ok).To(Equal(wantOK))
			Expect(got).To(Equal(wantReason))
		},
		Entry("rejected", newWriteError("INT10", ErrRejected).Error(), ReasonRejected, true),
		Entry("invalid_value", newWriteError("INT10", ErrInvalidValue).Error(), ReasonInvalidValue, true),
		Entry("text from another output", "connection refused", Reason(""), false),
		Entry("empty text", "", Reason(""), false),
	)

	It("treats a lost connection as retryable and a plain error as not", func() {
		Expect(IsRetryable(service.ErrNotConnected)).To(BeTrue())
		Expect(IsRetryable(newWriteError("INT10", ErrRejected))).To(BeTrue())
		Expect(IsRetryable(newWriteError("INT10", ErrInvalidValue))).To(BeFalse())
		Expect(IsRetryable(errors.New("something else"))).To(BeFalse())
	})
})
