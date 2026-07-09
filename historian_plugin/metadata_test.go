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

package historian_plugin_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tsh "github.com/united-manufacturing-hub/benthos-umh/historian_plugin"
)

var _ = Describe("metadata", func() {
	meta := map[string]string{
		"serialNumber":           "abc",
		"location_path":          "acme.line1", // structural -> excluded in all-mode
		"opcua_source_timestamp": "x",          // high-churn -> excluded in all-mode
		"_ltree":                 "internal",   // underscore-prefixed -> excluded in all-mode
	}

	It("all-mode excludes structural, high-churn, and _-prefixed", func() {
		Expect(tsh.SelectMetaKeys(meta, true, nil, nil)).To(ConsistOf("serialNumber"))
	})
	It("allowlist-mode takes the list verbatim (blacklist NOT applied)", func() {
		Expect(tsh.SelectMetaKeys(meta, false, []string{"serialNumber", "opcua_source_timestamp"}, nil)).
			To(ConsistOf("serialNumber", "opcua_source_timestamp"))
	})
	It("build omits allowlisted-but-absent keys", func() {
		md := tsh.BuildMetadata(meta, []string{"serialNumber", "missing"})
		Expect(md).To(HaveKeyWithValue("serialNumber", "abc"))
		Expect(md).NotTo(HaveKey("missing"))
	})
	It("fingerprint is deterministic regardless of map order", func() {
		a := tsh.Fingerprint(map[string]string{"x": "1", "y": "2"})
		b := tsh.Fingerprint(map[string]string{"y": "2", "x": "1"})
		Expect(a).To(Equal(b))
	})
	It("fingerprint is a JSON object (matches the template's stored shape, not an array)", func() {
		// The attribute column and the read surface (attribute->>'serialNumber') depend on
		// this being {"k":"v"}, never [["k","v"]].
		Expect(tsh.Fingerprint(map[string]string{"serialNumber": "abc"})).To(Equal(`{"serialNumber":"abc"}`))
	})
	It("flags high-churn keys present in the built map", func() {
		md := map[string]string{"opcua_source_timestamp": "x", "serialNumber": "abc"}
		Expect(tsh.HighChurnKeys(md)).To(ConsistOf("opcua_source_timestamp"))
	})
})

var _ = Describe("metadata exclude blacklist", func() {
	Describe("MetaExcluder.Match", func() {
		It("matches exact key names", func() {
			e := tsh.NewMetaExcluder([]string{"serialNumber"})
			Expect(e.Match("serialNumber")).To(BeTrue())
			Expect(e.Match("serialnumber")).To(BeFalse()) // case-sensitive
			Expect(e.Match("other")).To(BeFalse())
		})
		It("matches a trailing-* prefix", func() {
			e := tsh.NewMetaExcluder([]string{"opcua_*"})
			Expect(e.Match("opcua_source_timestamp")).To(BeTrue())
			Expect(e.Match("opcua_")).To(BeTrue()) // empty remainder still matches the prefix
			Expect(e.Match("opcua")).To(BeFalse()) // shorter than the prefix
			Expect(e.Match("spb_sequence")).To(BeFalse())
		})
		It("treats a bare * as match-everything", func() {
			e := tsh.NewMetaExcluder([]string{"*"})
			Expect(e.Match("anything")).To(BeTrue())
			Expect(e.Match("")).To(BeTrue())
		})
		It("skips empty-string entries (never matches everything by accident)", func() {
			e := tsh.NewMetaExcluder([]string{""})
			Expect(e.Match("anything")).To(BeFalse())
			Expect(e.Match("")).To(BeFalse())
		})
		It("combines exact and prefix entries", func() {
			e := tsh.NewMetaExcluder([]string{"serialNumber", "opcua_*"})
			Expect(e.Match("serialNumber")).To(BeTrue())
			Expect(e.Match("opcua_x")).To(BeTrue())
			Expect(e.Match("keep")).To(BeFalse())
		})
		It("is nil-safe (no excluder configured)", func() {
			var e *tsh.MetaExcluder
			Expect(e.Match("anything")).To(BeFalse())
		})
	})

	Describe("SelectMetaKeys with an excluder", func() {
		meta := map[string]string{
			"serialNumber": "abc",
			"keep":         "1",
			"opcua_vendor": "siemens",
		}
		It("all-mode additionally drops blacklisted keys", func() {
			e := tsh.NewMetaExcluder([]string{"serialNumber", "opcua_*"})
			Expect(tsh.SelectMetaKeys(meta, true, nil, e)).To(ConsistOf("keep"))
		})
		It("allowlist-mode ignores the excluder (allowlist is explicit)", func() {
			e := tsh.NewMetaExcluder([]string{"serialNumber"})
			Expect(tsh.SelectMetaKeys(meta, false, []string{"serialNumber", "keep"}, e)).
				To(ConsistOf("serialNumber", "keep"))
		})
	})
})
