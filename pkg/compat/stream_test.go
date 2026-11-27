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

package compat_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	// StreamBuilder's default tracer is the "none" tracer, which this
	// blank import registers. It is part of the same module as
	// public/service and adds no new dependency.
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"

	compat "github.com/united-manufacturing-hub/benthos-umh/pkg/compat"
)

// stubInput is an input that blocks until its context is cancelled, so a
// running stream stays healthy until the test cancels it.
type stubInput struct{}

func (stubInput) Connect(_ context.Context) error { return nil }

func (stubInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

func (stubInput) Close(_ context.Context) error { return nil }

// These specs go through the same public surface a real plugin goes through:
// RegisterInput on an Environment, then YAML through StreamBuilder, which is
// what the stream linter sees. This is the path that proves an AnyMap field
// keeps unknown compatibility keys valid.
var _ = Describe("through the real public config surface", func() {
	const pluginName = "throwaway_compat"

	var (
		env            *service.Environment
		compatibility  *compat.Flags
		correctTagPath *compat.Flag
		enabled        *bool
		// constructed receives the error Parse returned, once the input
		// constructor has run. Sending after writing enabled orders the two.
		constructed chan error
	)

	BeforeEach(func() {
		env = service.NewEnvironment()
		compatibility = compat.New()
		correctTagPath = compatibility.Declare("correct_tag_path",
			"Publish a tag under its browse path without its own name.")
		enabled = new(bool)
		constructed = make(chan error, 1)

		spec := service.NewConfigSpec().
			Field(service.NewStringField("endpoint")).
			Field(compatibility.Field())
		err := env.RegisterInput(pluginName, spec, func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			flags, err := compatibility.Parse(conf, mgr.Logger())
			if err == nil {
				*enabled = flags.Enabled(correctTagPath)
			}
			constructed <- err
			if err != nil {
				return nil, err
			}
			return stubInput{}, nil
		})
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("the stream linter", func() {
		It("accepts an unknown compatibility key", func() {
			sb := env.NewStreamBuilder()
			err := sb.AddInputYAML(pluginName + `:
  endpoint: localhost
  compatibility:
    retired_key: true
`)
			Expect(err).NotTo(HaveOccurred())
		})

		It("accepts a recognized compatibility key", func() {
			sb := env.NewStreamBuilder()
			err := sb.AddInputYAML(pluginName + `:
  endpoint: localhost
  compatibility:
    correct_tag_path: true
`)
			Expect(err).NotTo(HaveOccurred())
		})

		It("accepts the plugin config without a compatibility block", func() {
			sb := env.NewStreamBuilder()
			err := sb.AddInputYAML(pluginName + `:
  endpoint: localhost
`)
			Expect(err).NotTo(HaveOccurred())
		})

		It("still rejects an unknown key at the top level of the plugin config", func() {
			// Control proving the linter really ran on this YAML: if the
			// unknown compatibility key above slipped through because linting
			// was silently off, this would pass too.
			sb := env.NewStreamBuilder()
			err := sb.AddInputYAML(pluginName + `:
  endpoint: localhost
  no_such_field: 42
`)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("no_such_field"))
		})
	})

	Describe("building a stream from YAML", func() {
		var sb *service.StreamBuilder

		BeforeEach(func() {
			sb = env.NewStreamBuilder()
			// A consumer func stands in for an output, so the test does not
			// depend on any specific output component being registered.
			err := sb.AddConsumerFunc(func(_ context.Context, _ *service.Message) error {
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
		})

		// runFor runs the stream until the input constructor has run, then
		// shuts it down, and returns the error Parse returned and the error
		// Run returned. The constructor only runs once the stream is Run, not
		// at Build.
		runFor := func(strm *service.Stream) (error, error) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runDone := make(chan error, 1)
			go func() { runDone <- strm.Run(ctx) }()
			var parseErr error
			select {
			case parseErr = <-constructed:
			case err := <-runDone:
				Fail(fmt.Sprintf("Run returned before the input constructor ran: %v", err))
			case <-time.After(5 * time.Second):
				Fail("the input constructor did not run within 5s")
			}
			cancel()
			return parseErr, <-runDone
		}

		// runHealthy runs the stream and asserts that the constructor accepted
		// the config and the stream stopped only because it was cancelled.
		runHealthy := func(strm *service.Stream) {
			parseErr, runErr := runFor(strm)
			ExpectWithOffset(1, parseErr).NotTo(HaveOccurred())
			if runErr != nil {
				ExpectWithOffset(1, runErr).To(MatchError(context.Canceled))
			}
		}

		// What this file asserts about Parse's own output is only what only
		// this path can assert: that the flag reached the constructor. The
		// log content itself is pinned deterministically in compat_test.go.
		// (Reading a log buffer here raced against a Benthos goroutine that
		// outlives Run.)

		DescribeTable("passes an unknown compatibility key of any type all the way to the constructor",
			func(yml string) {
				Expect(sb.AddInputYAML(pluginName + ":\n  endpoint: localhost\n  compatibility:\n" + yml)).NotTo(HaveOccurred())

				strm, err := sb.Build()
				Expect(err).NotTo(HaveOccurred())
				runHealthy(strm)
				Expect(*enabled).To(BeFalse())
			},
			Entry("an unknown key holding a boolean", "    retired_key: true\n"),
			// A retired flag does not have to hold a boolean: this pins that
			// the type check cannot be hoisted above the key check.
			Entry("an unknown key holding a string", "    retired_key: Machine\n"),
			Entry("an unknown key holding an integer", "    retired_key: 42\n"),
			Entry("an unknown key holding a list", "    retired_key: [a, b]\n"),
			Entry("an unknown key holding a null", "    retired_key:\n"),
		)

		It("passes an enabled flag to the constructor", func() {
			Expect(sb.AddInputYAML(pluginName + `:
  endpoint: localhost
  compatibility:
    correct_tag_path: true
`)).NotTo(HaveOccurred())

			strm, err := sb.Build()
			Expect(err).NotTo(HaveOccurred())
			runHealthy(strm)
			Expect(*enabled).To(BeTrue())
		})

		It("rejects the YAML merge key in the compatibility block, surfacing the error from the constructor", func() {
			// The linter accepts any key inside the compatibility block, so
			// the stream builds; the plugin's constructor is where the merge
			// key is caught, and Run surfaces its error.
			Expect(sb.AddInputYAML(pluginName + `:
  endpoint: localhost
  compatibility:
    defaults: &d
      correct_tag_path: true
    <<: *d
`)).NotTo(HaveOccurred())

			strm, err := sb.Build()
			Expect(err).NotTo(HaveOccurred())
			parseErr, err := runFor(strm)
			Expect(parseErr).To(MatchError(ContainSubstring("<<")))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to init input"))
			Expect(err.Error()).To(ContainSubstring("<<"))
		})
	})
})
