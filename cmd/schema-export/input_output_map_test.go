// Copyright 2025 UMH Systems GmbH
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

package main

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("buildMapping", func() {
	env := service.GlobalEnvironment()

	Context("with no overrides", func() {
		It("returns pairs sorted by input name", func() {
			pairs := buildMapping(env, nil)
			for i := 1; i < len(pairs); i++ {
				Expect(pairs[i-1].Input < pairs[i].Input).To(BeTrue(),
					"not sorted at %d: %q then %q", i, pairs[i-1].Input, pairs[i].Input)
			}
		})

		It("sets output to the input name when a same-named output exists, else empty", func() {
			pairs := buildMapping(env, nil)
			for _, p := range pairs {
				if p.Output != "" {
					Expect(p.Output).To(Equal(p.Input),
						"derived output must equal input for %q", p.Input)
				}
			}
		})

		It("includes UMH inputs with their expected outputs", func() {
			pairs := buildMapping(env, nil)
			byInput := make(map[string]string)
			for _, p := range pairs {
				byInput[p.Input] = p.Output
			}
			Expect(byInput).To(HaveKeyWithValue("opcua", "opcua"))
			Expect(byInput).To(HaveKeyWithValue("sparkplug_b", "sparkplug_b"))
			Expect(byInput).To(HaveKeyWithValue("uns", "uns"))
			Expect(byInput).To(HaveKeyWithValue("s7comm", ""))
			Expect(byInput).To(HaveKeyWithValue("modbus", ""))
		})

		It("includes upstream redpanda-connect inputs, not just UMH ones", func() {
			pairs := buildMapping(env, nil)
			inputs := make(map[string]bool)
			for _, p := range pairs {
				inputs[p.Input] = true
			}
			Expect(inputs).To(HaveKey("generate"))
			Expect(len(pairs)).To(BeNumerically(">", 20))
		})
	})

	Context("with an override", func() {
		It("replaces the derived output for the listed input", func() {
			overrides := []pluginPair{{Input: "modbus", Output: "uns"}}
			pairs := buildMapping(env, overrides)

			var modbus pluginPair
			for _, p := range pairs {
				if p.Input == "modbus" {
					modbus = p
				}
			}
			Expect(modbus.Output).To(Equal("uns"))
		})
	})
})
