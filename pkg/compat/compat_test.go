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
	"bytes"
	"fmt"
	"log/slog"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/redpanda-data/benthos/v4/public/service"

	compat "github.com/united-manufacturing-hub/benthos-umh/pkg/compat"
)

// newTestFlags declares two flags, as a plugin with a couple of compatibility
// flags would.
func newTestFlags() (*compat.Flags, *compat.Flag, *compat.Flag, *service.ConfigSpec) {
	c := compat.New()
	flagA := c.Declare("flag_a", "First flag description.")
	flagB := c.Declare("flag_b", "Second flag description.")
	spec := service.NewConfigSpec().Field(c.Field())
	return c, flagA, flagB, spec
}

// parseFlags parses YAML against spec and reads the compatibility block with
// c. It fails the test on any error except the one being tested, so callers
// that expect an error must use spec.ParseYAML directly.
func parseFlags(c *compat.Flags, spec *service.ConfigSpec, yml string) compat.Set {
	conf, err := spec.ParseYAML(yml, nil)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	set, err := c.Parse(conf, nil)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return set
}

// captureLogger returns a service.Logger writing levelled text output into a
// buffer, so tests can assert on what the plugin would log.
func captureLogger() (*service.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	l := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	return l, &buf
}

// inputConfigView finds the exported docs view of a plugin registered on an
// environment. GetInputConfig reads the global bundle, which does not see
// plugins registered on a service.NewEnvironment.
func inputConfigView(env *service.Environment, name string) (*service.ConfigView, bool) {
	var view *service.ConfigView
	found := false
	env.WalkInputs(func(n string, cv *service.ConfigView) {
		if n == name {
			view = cv
			found = true
		}
	})
	return view, found
}

var _ = Describe("compat", func() {
	var (
		c     *compat.Flags
		flagA *compat.Flag
		flagB *compat.Flag
		spec  *service.ConfigSpec
	)

	BeforeEach(func() {
		c, flagA, flagB, spec = newTestFlags()
	})

	Describe("reading flags from config", func() {
		DescribeTable("configurations where every flag stays off",
			func(yml string) {
				set := parseFlags(c, spec, yml)
				Expect(set.Enabled(flagA)).To(BeFalse())
				Expect(set.Enabled(flagB)).To(BeFalse())
			},
			Entry("no compatibility block at all", ``),
			Entry("an empty compatibility block", "compatibility: {}"),
			Entry("a recognised key that is absent (another key is present)", "compatibility:\n  flag_b: false\n"),
			Entry("a recognised key set to false", "compatibility:\n  flag_a: false\n"),
			Entry("a recognised key set to false next to an unknown key", "compatibility:\n  flag_a: false\n  retired_key: true\n"),
			// A retired flag does not have to hold a boolean: a config from
			// before the flag was retired may carry a string, a number, a list
			// or nothing at all. All of these must stay valid.
			Entry("an unknown key holding a string", "compatibility:\n  retired_key: Machine\n"),
			Entry("an unknown key holding an integer", "compatibility:\n  retired_key: 42\n"),
			Entry("an unknown key holding a list", "compatibility:\n  retired_key: [a, b]\n"),
			Entry("an unknown key holding a null", "compatibility:\n  retired_key:\n"),
			// Key lookup is exact, including case: a differently spelled key is
			// an unknown key, not the flag.
			Entry("a key spelled with different casing than the flag", "compatibility:\n  Flag_A: true\n"),
		)

		It("turns on a flag that is explicitly true", func() {
			set := parseFlags(c, spec, "compatibility:\n  flag_a: true\n")
			Expect(set.Enabled(flagA)).To(BeTrue())
			Expect(set.Enabled(flagB)).To(BeFalse())
		})

		It("sets several flags independently", func() {
			set := parseFlags(c, spec, "compatibility:\n  flag_a: true\n  flag_b: false\n")
			Expect(set.Enabled(flagA)).To(BeTrue())
			Expect(set.Enabled(flagB)).To(BeFalse())
		})

		It("keeps a live flag working when a retired key sits next to it", func() {
			set := parseFlags(c, spec, "compatibility:\n  retired_key: true\n  flag_a: true\n")
			Expect(set.Enabled(flagA)).To(BeTrue())
			Expect(set.Enabled(flagB)).To(BeFalse())
		})

		It("keeps a live flag working when a retired key of another type sits next to it", func() {
			set := parseFlags(c, spec, "compatibility:\n  retired_key: Machine\n  flag_a: true\n")
			Expect(set.Enabled(flagA)).To(BeTrue())
			Expect(set.Enabled(flagB)).To(BeFalse())
		})
	})

	Describe("rejecting values that are not booleans", func() {
		type entry struct {
			yamlValue string
			wantDesc  string
		}

		DescribeTable("fails Parse naming the key, what is accepted and what the customer wrote",
			func(e entry) {
				conf, err := spec.ParseYAML(fmt.Sprintf("compatibility:\n  flag_a: %s\n", e.yamlValue), nil)
				Expect(err).NotTo(HaveOccurred())
				_, err = c.Parse(conf, nil)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("flag_a"))
				Expect(err.Error()).To(ContainSubstring("must be true or false"))
				Expect(err.Error()).To(ContainSubstring(e.wantDesc))
				// The error reaches the operator wrapped as
				// "failed to init input ... : <message>", so it must not carry a
				// Go package name.
				Expect(err.Error()).NotTo(ContainSubstring("compat:"))
			},
			Entry("a string", entry{`"not a bool"`, `the string "not a bool"`}),
			Entry("a bool-looking string", entry{`"true"`, `the string "true"`}),
			Entry("a YAML 1.1 bool-looking string", entry{`yes`, `the string "yes"`}),
			Entry("an integer", entry{`1`, `the integer 1`}),
			Entry("a float", entry{`1.5`, `the number 1.5`}),
			Entry("a date", entry{`2020-01-01`, `the date 2020-01-01`}),
			Entry("a nested map", entry{`{inner: true}`, `an object`}),
			Entry("a list", entry{`[true]`, `a list`}),
			Entry("a null value", entry{"", `null`}),
		)
	})

	Describe("the YAML merge key", func() {
		It("is rejected instead of silently ignoring the merged flags", func() {
			// The realistic shape of the failure: a customer merges a shared
			// anchor into the compatibility block. Expanding it here would
			// mean reimplementing YAML merge semantics against a map Benthos
			// has already flattened, so the flags must be written out
			// explicitly instead.
			conf, err := spec.ParseYAML("compatibility:\n  defaults: &d\n    flag_a: true\n  <<: *d\n", nil)
			Expect(err).NotTo(HaveOccurred())
			_, err = c.Parse(conf, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("<<"))
			Expect(err.Error()).To(ContainSubstring("explicitly"))
		})

		It("is rejected when written as an inline mapping", func() {
			conf, err := spec.ParseYAML("compatibility:\n  <<: {flag_a: true}\n", nil)
			Expect(err).NotTo(HaveOccurred())
			_, err = c.Parse(conf, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("merge key"))
		})
	})

	Describe("logging", func() {
		It("logs an enabled flag at Info level", func() {
			log, buf := captureLogger()
			conf, err := spec.ParseYAML("compatibility:\n  flag_a: true\n", nil)
			Expect(err).NotTo(HaveOccurred())
			_, err = c.Parse(conf, log)
			Expect(err).NotTo(HaveOccurred())

			out := buf.String()
			Expect(out).To(ContainSubstring("level=INFO"))
			Expect(out).To(ContainSubstring("flag_a"))
			Expect(out).To(ContainSubstring("First flag description."))
			Expect(out).NotTo(ContainSubstring("level=WARN"))
			Expect(out).NotTo(ContainSubstring("level=ERROR"))
		})

		It("logs an unknown key at Info level, naming it and the recognised keys", func() {
			log, buf := captureLogger()
			conf, err := spec.ParseYAML("compatibility:\n  retired_key: true\n", nil)
			Expect(err).NotTo(HaveOccurred())
			_, err = c.Parse(conf, log)
			Expect(err).NotTo(HaveOccurred())

			out := buf.String()
			Expect(out).To(ContainSubstring("level=INFO"))
			Expect(out).To(ContainSubstring("retired_key"))
			Expect(out).To(ContainSubstring("flag_a"))
			Expect(out).To(ContainSubstring("flag_b"))
			Expect(out).NotTo(ContainSubstring("level=WARN"))
		})

		It("logs all unknown keys together in one Info line, in a stable order", func() {
			// One line per construction, not one line per key: retired keys
			// stay in customer configs forever, and a config with many of
			// them must not flood the log.
			log, buf := captureLogger()
			conf, err := spec.ParseYAML("compatibility:\n  retired_two: Machine\n  retired_one: true\n", nil)
			Expect(err).NotTo(HaveOccurred())
			_, err = c.Parse(conf, log)
			Expect(err).NotTo(HaveOccurred())

			out := buf.String()
			Expect(strings.Count(out, "level=INFO")).To(Equal(1))
			Expect(out).To(ContainSubstring("retired_one"))
			Expect(out).To(ContainSubstring("retired_two"))
			Expect(strings.Index(out, "retired_one")).To(BeNumerically("<", strings.Index(out, "retired_two")))
		})

		It("does not crash with a nil logger when a flag is enabled", func() {
			conf, err := spec.ParseYAML("compatibility:\n  flag_a: true\n", nil)
			Expect(err).NotTo(HaveOccurred())
			set, err := c.Parse(conf, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(set.Enabled(flagA)).To(BeTrue())
		})

		It("does not crash with a nil logger when a key is unknown", func() {
			conf, err := spec.ParseYAML("compatibility:\n  retired_key: true\n", nil)
			Expect(err).NotTo(HaveOccurred())
			set, err := c.Parse(conf, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(set.Enabled(flagA)).To(BeFalse())
		})
	})

	Describe("the zero value of Set", func() {
		It("reports every declared flag as off", func() {
			var set compat.Set
			Expect(set.Enabled(flagA)).To(BeFalse())
			Expect(set.Enabled(flagB)).To(BeFalse())
		})
	})

	Describe("separate Flags in one process", func() {
		It("do not recognise each other's keys", func() {
			other := compat.New()
			otherFlag := other.Declare("flag_a", "Some other plugin's flag of the same name.")

			set := parseFlags(c, spec, "compatibility:\n  flag_a: true\n")
			Expect(set.Enabled(flagA)).To(BeTrue())

			// The other Flags parses the same config: the key is recognised
			// there too, but only through its own handle.
			otherSet := parseFlags(other, spec, "compatibility:\n  flag_a: true\n")
			Expect(otherSet.Enabled(otherFlag)).To(BeTrue())

			// A key only c knows about is unknown to other, and vice versa.
			third := compat.New()
			thirdFlag := third.Declare("flag_c", "A key c does not declare.")
			thirdSet := parseFlags(third, spec, "compatibility:\n  flag_a: true\n")
			Expect(thirdSet.Enabled(thirdFlag)).To(BeFalse())
		})
	})

	Describe("programming errors", func() {
		It("panics when a flag is declared twice with the same name", func() {
			Expect(func() {
				c.Declare("flag_a", "Declared again.")
			}).To(Panic())

			// The panic must say what the caller did wrong.
			done := make(chan any)
			go func() {
				defer func() { done <- recover() }()
				c.Declare("flag_a", "Declared again.")
			}()
			r := <-done
			Expect(fmt.Sprint(r)).To(ContainSubstring("flag_a"))
			Expect(fmt.Sprint(r)).To(ContainSubstring("twice"))
		})

		It("panics when Declare is called with an empty name", func() {
			Expect(func() {
				c.Declare("", "Declared without a name.")
			}).To(Panic())
		})

		It("panics when Declare is called with an empty description", func() {
			// An empty description would render a dangling "- flag_a: " line
			// in the Management Console and "is enabled: " in the log.
			Expect(func() {
				c.Declare("flag_a", "")
			}).To(Panic())
		})

		It("panics when Enabled is called with a nil flag, even on the zero Set", func() {
			// A nil flag is a programming error regardless of the Set, so
			// the zero Set's short-circuit must not swallow it.
			var zero compat.Set
			Expect(func() {
				zero.Enabled(nil)
			}).To(Panic())

			set := parseFlags(c, spec, "compatibility:\n  flag_a: true\n")
			Expect(func() {
				set.Enabled(nil)
			}).To(Panic())
		})

		It("panics when Enabled is called with a flag from another Flags", func() {
			other := compat.New()
			foreignFlag := other.Declare("foreign_flag", "Declared on another Flags.")

			set := parseFlags(c, spec, "compatibility:\n  flag_a: true\n")
			Expect(func() {
				set.Enabled(foreignFlag)
			}).To(Panic())

			done := make(chan any)
			go func() {
				defer func() { done <- recover() }()
				_ = set.Enabled(foreignFlag)
			}()
			r := <-done
			Expect(fmt.Sprint(r)).To(ContainSubstring("foreign_flag"))
			Expect(fmt.Sprint(r)).To(ContainSubstring("not declared"))
		})
	})

	Describe("the config field", func() {
		It("is optional, so a config without the block still parses", func() {
			// A required field would make an otherwise empty config fail
			// ParseYAML; an optional one must not.
			conf, err := spec.ParseYAML("", nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(conf).NotTo(BeNil())
		})

		Describe("description copy", func() {
			It("mentions what a compatibility flag is and lists each flag", func() {
				// The description is user-facing copy rendered by the
				// Management Console; check it through the exported docs of a
				// registered plugin.
				env := service.NewEnvironment()
				err := env.RegisterInput("description_probe", spec, func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
					Fail("must not be constructed")
					return nil, nil
				})
				Expect(err).NotTo(HaveOccurred())

				view, ok := inputConfigView(env, "description_probe")
				Expect(ok).To(BeTrue())
				json, err := view.FormatJSON()
				Expect(err).NotTo(HaveOccurred())

				doc := string(json)
				Expect(doc).To(ContainSubstring("compatibility"))
				Expect(doc).To(ContainSubstring("flag_a"))
				Expect(doc).To(ContainSubstring("First flag description."))
				Expect(doc).To(ContainSubstring("flag_b"))
				Expect(doc).To(ContainSubstring("Second flag description."))
				Expect(doc).To(ContainSubstring("defaults to false"))
				// The one thing a customer with an old config most needs to
				// know: unknown keys do not fail validation.
				Expect(doc).To(ContainSubstring("are ignored"))
			})

			It("is a snapshot: a spec built before a Declare does not mention that flag, but Parse accepts it", func() {
				// This is the standard Go trap: a package-level spec variable
				// initialises before any init function that declares flags.
				// The behaviour is pinned here so the suite teaches it; the
				// docs on Field and Declare demand the same init function.
				env := service.NewEnvironment()
				fresh := compat.New()
				earlySpec := service.NewConfigSpec().Field(fresh.Field())
				lateFlag := fresh.Declare("late_flag", "Declared after the early spec was built.")
				lateSpec := service.NewConfigSpec().Field(fresh.Field())

				register := func(name string, spec *service.ConfigSpec) *service.ConfigView {
					err := env.RegisterInput(name, spec, func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
						Fail("must not be constructed")
						return nil, nil
					})
					Expect(err).NotTo(HaveOccurred())
					view, ok := inputConfigView(env, name)
					Expect(ok).To(BeTrue())
					return view
				}

				earlyJSON, err := register("early_spec_probe", earlySpec).FormatJSON()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(earlyJSON)).NotTo(ContainSubstring("late_flag"))
				Expect(string(earlyJSON)).NotTo(ContainSubstring("Declared after the early spec was built."))
				Expect(string(earlyJSON)).To(ContainSubstring("no compatibility flags"))

				lateJSON, err := register("late_spec_probe", lateSpec).FormatJSON()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(lateJSON)).To(ContainSubstring("late_flag"))
				Expect(string(lateJSON)).To(ContainSubstring("Declared after the early spec was built."))

				// The trap is only about the description: Parse accepts the
				// flag through the early spec regardless.
				conf, err := earlySpec.ParseYAML("compatibility:\n  late_flag: true\n", nil)
				Expect(err).NotTo(HaveOccurred())
				set, err := fresh.Parse(conf, nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(set.Enabled(lateFlag)).To(BeTrue())
			})

			It("is sensible for a Flags with no declared flags", func() {
				env := service.NewEnvironment()
				empty := compat.New()
				spec := service.NewConfigSpec().Field(empty.Field())
				err := env.RegisterInput("empty_description_probe", spec, func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
					Fail("must not be constructed")
					return nil, nil
				})
				Expect(err).NotTo(HaveOccurred())

				view, ok := inputConfigView(env, "empty_description_probe")
				Expect(ok).To(BeTrue())
				json, err := view.FormatJSON()
				Expect(err).NotTo(HaveOccurred())

				Expect(string(json)).To(ContainSubstring("compatibility"))
				Expect(string(json)).To(ContainSubstring("no compatibility flags"))
			})
		})
	})

})
