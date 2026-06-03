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
		It("returns pairs sorted by plugin name", func() {
			pairs := buildMapping(env, nil)
			for i := 1; i < len(pairs); i++ {
				Expect(pairKey(pairs[i-1]) < pairKey(pairs[i])).To(BeTrue(),
					"not sorted at %d: %q then %q", i, pairKey(pairs[i-1]), pairKey(pairs[i]))
			}
		})

		It("derives read and write from the same plugin name", func() {
			pairs := buildMapping(env, nil)
			for _, p := range pairs {
				Expect(p.Read != "" || p.Write != "").To(BeTrue(), "pair with neither read nor write")
				if p.Read != "" && p.Write != "" {
					Expect(p.Write).To(Equal(p.Read),
						"derived write must equal read for %q", p.Read)
				}
			}
		})

		It("includes write-only plugins with empty read", func() {
			pairs := buildMapping(env, nil)
			byKey := make(map[string]pluginPair)
			for _, p := range pairs {
				byKey[pairKey(p)] = p
			}
			Expect(byKey).To(HaveKey("drop"))
			Expect(byKey["drop"].Read).To(Equal(""))
			Expect(byKey["drop"].Write).To(Equal("drop"))
		})

		It("includes UMH inputs with their expected writes", func() {
			pairs := buildMapping(env, nil)
			byRead := make(map[string]string)
			for _, p := range pairs {
				byRead[p.Read] = p.Write
			}
			Expect(byRead).To(HaveKeyWithValue("opcua", "opcua"))
			Expect(byRead).To(HaveKeyWithValue("sparkplug_b", "sparkplug_b"))
			Expect(byRead).To(HaveKeyWithValue("uns", "uns"))
			Expect(byRead).To(HaveKeyWithValue("s7comm", ""))
			Expect(byRead).To(HaveKeyWithValue("modbus", ""))
		})

		It("flags UMH plugins, not upstream ones", func() {
			pairs := buildMapping(env, nil)
			byRead := make(map[string]pluginPair)
			for _, p := range pairs {
				byRead[p.Read] = p
			}
			Expect(byRead["opcua"].IsUmhPlugin).To(BeTrue())
			Expect(byRead["modbus"].IsUmhPlugin).To(BeTrue())
			Expect(byRead["uns"].IsUmhPlugin).To(BeTrue())
			Expect(byRead["generate"].IsUmhPlugin).To(BeFalse())
			Expect(byRead["kafka"].IsUmhPlugin).To(BeFalse())
		})

		It("includes upstream redpanda-connect inputs, not just UMH ones", func() {
			pairs := buildMapping(env, nil)
			reads := make(map[string]bool)
			for _, p := range pairs {
				reads[p.Read] = true
			}
			Expect(reads).To(HaveKey("generate"))
			Expect(len(pairs)).To(BeNumerically(">", 20))
		})
	})

	Context("with an override", func() {
		It("replaces the derived write for the listed read", func() {
			overrides := []pluginPair{{Read: "modbus", Write: "uns"}}
			pairs := buildMapping(env, overrides)

			var modbus pluginPair
			for _, p := range pairs {
				if p.Read == "modbus" {
					modbus = p
				}
			}
			Expect(modbus.Write).To(Equal("uns"))
		})

		It("consumes both sides of an override", func() {
			overrides := []pluginPair{{Read: "stdin", Write: "stdout"}}
			pairs := buildMapping(env, overrides)

			var matches []pluginPair
			for _, p := range pairs {
				if p.Read == "stdin" || p.Write == "stdout" {
					matches = append(matches, p)
				}
			}
			Expect(matches).To(HaveLen(1), "override must replace both standalone entries")
			Expect(matches[0]).To(Equal(pluginPair{Read: "stdin", Write: "stdout"}))
		})
	})
})
