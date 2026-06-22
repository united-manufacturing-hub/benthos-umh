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
	"math"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tsh "github.com/united-manufacturing-hub/benthos-umh/historian_plugin"
)

var _ = Describe("contract helpers", func() {
	DescribeTable("NormalizeContract strips a trailing _vN",
		func(in, want string) { Expect(tsh.NormalizeContract(in)).To(Equal(want)) },
		Entry("plain", "_pump", "_pump"),
		Entry("v1", "_pump_v1", "_pump"),
		Entry("v12", "_pump_v12", "_pump"),
		Entry("bare with version", "pump_v1", "pump"),
		Entry("empty", "", ""),
	)

	DescribeTable("ValidateContract",
		func(in string, ok bool) {
			err := tsh.ValidateContract(in)
			if ok {
				Expect(err).NotTo(HaveOccurred())
			} else {
				Expect(err).To(HaveOccurred())
			}
		},
		Entry("good", "pump", true),
		Entry("digits + underscore", "pump_2", true),
		Entry("uppercase rejected", "Pump", false),
		Entry("leading underscore rejected", "_pump", false),
		Entry("version suffix rejected", "pump_v1", false),
		Entry("empty rejected", "", false),
	)
})

var _ = Describe("CanonicalLtreePath", func() {
	DescribeTable("mirrors the SQL to_ltree_path identity",
		func(in, want string) { Expect(tsh.CanonicalLtreePath(in)).To(Equal(want)) },
		Entry("plain", "acme.line1", "acme.line1"),
		Entry("non-word chars become _", "acme@line/1", "acme_line_1"),
		Entry("dash vs underscore alias", "acme.line-1", "acme.line_1"),
		Entry("empty segments dropped", "a...b", "a.b"),
		Entry("all dots -> empty", "...", ""),
	)
	It("collapses dash/underscore/at variants to one identity", func() {
		a := tsh.CanonicalLtreePath("enterprise.line-1")
		b := tsh.CanonicalLtreePath("enterprise.line_1")
		c := tsh.CanonicalLtreePath("enterprise.line@1")
		Expect(a).To(Equal(b))
		Expect(b).To(Equal(c))
	})
})

var _ = Describe("redact (password masking in connection errors)", func() {
	It("masks both the raw and the url-encoded password", func() {
		pw := "p@ss/w:rd %x"
		dsn, redacted := tsh.RedactDSN(pw)
		// sanity: the DSN really does carry the encoded password (the leak path)
		Expect(dsn).NotTo(ContainSubstring(pw))
		// neither the raw nor the encoded password survives redaction
		Expect(redacted).NotTo(ContainSubstring(pw))
		Expect(redacted).NotTo(ContainSubstring("p%40ss"))
		Expect(redacted).To(ContainSubstring("xxxxx"))
	})
})

var _ = Describe("LocationNormalizesToEmpty", func() {
	DescribeTable("matches the SQL to_ltree_path NULL condition",
		func(in string, empty bool) { Expect(tsh.LocationNormalizesToEmpty(in)).To(Equal(empty)) },
		Entry("normal path", "acme.line1", false),
		Entry("special chars keep content", "a@b/c", false),
		Entry("trailing dot keeps content", "a.", false),
		Entry("all dots", "...", true),
		Entry("single dot", ".", true),
		Entry("empty", "", true),
	)
})

var _ = Describe("ClassifyValue", func() {
	ptrF := func(f float64) *float64 { return &f }

	It("bool true -> numeric 1", func() {
		vt, num, text, ok := tsh.ClassifyValue(true)
		Expect(ok).To(BeTrue())
		Expect(vt).To(Equal(tsh.ValueNumeric))
		Expect(num).To(Equal(ptrF(1)))
		Expect(text).To(BeNil())
	})
	It("bool false -> numeric 0 (NOT dropped)", func() {
		_, num, _, ok := tsh.ClassifyValue(false)
		Expect(ok).To(BeTrue())
		Expect(num).To(Equal(ptrF(0)))
	})
	It("finite float -> numeric", func() {
		vt, num, _, ok := tsh.ClassifyValue(3.5)
		Expect(ok).To(BeTrue())
		Expect(vt).To(Equal(tsh.ValueNumeric))
		Expect(num).To(Equal(ptrF(3.5)))
	})
	It("NaN -> dropped", func() {
		_, _, _, ok := tsh.ClassifyValue(math.NaN())
		Expect(ok).To(BeFalse())
	})
	It("+Inf -> dropped", func() {
		_, _, _, ok := tsh.ClassifyValue(math.Inf(1))
		Expect(ok).To(BeFalse())
	})
	It("string -> text as-is", func() {
		vt, num, text, ok := tsh.ClassifyValue("hello")
		Expect(ok).To(BeTrue())
		Expect(vt).To(Equal(tsh.ValueText))
		Expect(num).To(BeNil())
		Expect(*text).To(Equal("hello"))
	})
	It("empty string -> text (NOT dropped)", func() {
		_, _, text, ok := tsh.ClassifyValue("")
		Expect(ok).To(BeTrue())
		Expect(*text).To(Equal(""))
	})
	It("object -> JSON-encoded text", func() {
		_, _, text, ok := tsh.ClassifyValue(map[string]any{"a": float64(1)})
		Expect(ok).To(BeTrue())
		Expect(*text).To(Equal(`{"a":1}`))
	})
	It("oversized text truncated to 8192 runes", func() {
		_, _, text, ok := tsh.ClassifyValue(strings.Repeat("x", 9000))
		Expect(ok).To(BeTrue())
		Expect([]rune(*text)).To(HaveLen(8192))
	})
})

var _ = Describe("ParseTimestampMs", func() {
	It("epoch -> 1970", func() {
		got, ok := tsh.ParseTimestampMs(float64(0))
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal("1970-01-01T00:00:00.000Z"))
	})
	It("keeps milliseconds", func() {
		got, ok := tsh.ParseTimestampMs(float64(1500))
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal("1970-01-01T00:00:01.500Z"))
	})
	It("parses a numeric string", func() {
		got, ok := tsh.ParseTimestampMs("1500")
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal("1970-01-01T00:00:01.500Z"))
	})
	It("drops NaN", func() {
		_, ok := tsh.ParseTimestampMs(math.NaN())
		Expect(ok).To(BeFalse())
	})
	It("drops out-of-range", func() {
		_, ok := tsh.ParseTimestampMs(float64(9e15))
		Expect(ok).To(BeFalse())
	})
	It("drops non-numeric string", func() {
		_, ok := tsh.ParseTimestampMs("not-a-number")
		Expect(ok).To(BeFalse())
	})
	It("floors negative epoch ms like JS new Date (pre-1970)", func() {
		// -1500 ms is 1.5s before the epoch. Integer-modulo truncation would give
		// 1969-12-31T23:59:59.-500 and round the wrong way; UnixMilli floors correctly.
		got, ok := tsh.ParseTimestampMs(float64(-1500))
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal("1969-12-31T23:59:58.500Z"))
	})
})

var _ = Describe("Transform", func() {
	base := func() (map[string]any, map[string]string) {
		return map[string]any{"value": 3.5, "timestamp_ms": float64(0)},
			map[string]string{"data_contract": "_pump_v1", "location_path": "acme.line1", "tag_name": "x", "virtual_path": "vibration"}
	}
	tr := func(p map[string]any, m map[string]string) (*tsh.Row, tsh.DropReason) {
		return tsh.Transform(p, m, "pump", true, nil, tsh.NewDedupCache().NewBatch())
	}

	It("maps a good message to a row", func() {
		p, m := base()
		row, reason := tr(p, m)
		Expect(reason).To(Equal(tsh.DropNone))
		Expect(row.RawLocation).To(Equal("acme.line1"))
		Expect(row.ContractName).To(Equal("_pump"))
		Expect(row.ValueType).To(Equal(tsh.ValueNumeric))
		Expect(*row.ValueNum).To(Equal(3.5))
		Expect(row.ValueText).To(BeNil())
		Expect(row.TS).To(Equal("1970-01-01T00:00:00.000Z"))
		// base() carries only structural metadata (all stripped), so no attribute row.
		Expect(row.EmitMeta).To(BeFalse())
	})
	It("emits an object-shaped attribute when an eligible metadata key is present", func() {
		p, m := base()
		m["serialNumber"] = "abc"
		row, reason := tr(p, m)
		Expect(reason).To(Equal(tsh.DropNone))
		Expect(row.EmitMeta).To(BeTrue())
		Expect(row.MetadataJSON).To(Equal(`{"serialNumber":"abc"}`))
	})
	It("does not emit an attribute row when there is no eligible metadata", func() {
		p, m := base() // only structural keys -> BuildMetadata returns {}
		row, reason := tr(p, m)
		Expect(reason).To(Equal(tsh.DropNone))
		Expect(row.EmitMeta).To(BeFalse())
		Expect(row.MetadataJSON).To(BeEmpty())
	})
	It("keeps a boolean false value", func() {
		p, m := base()
		p["value"] = false
		row, reason := tr(p, m)
		Expect(reason).To(Equal(tsh.DropNone))
		Expect(*row.ValueNum).To(Equal(0.0))
	})

	DescribeTable("drops with the right reason",
		func(mutate func(map[string]any, map[string]string), want tsh.DropReason) {
			p, m := base()
			mutate(p, m)
			row, reason := tr(p, m)
			Expect(row).To(BeNil())
			Expect(reason).To(Equal(want))
		},
		Entry("non-matching contract", func(_ map[string]any, m map[string]string) { m["data_contract"] = "_other_v1" }, tsh.DropContractMismatch),
		Entry("missing tag_name", func(_ map[string]any, m map[string]string) { delete(m, "tag_name") }, tsh.DropMissingLocationOrTag),
		Entry("Root.Objects.Server virtual_path", func(_ map[string]any, m map[string]string) { m["virtual_path"] = "Root.Objects.Server.foo" }, tsh.DropServerVirtualPath),
		Entry("absent value", func(p map[string]any, _ map[string]string) { delete(p, "value") }, tsh.DropMissingValueOrTimestamp),
		Entry("empty location", func(_ map[string]any, m map[string]string) { m["location_path"] = "..." }, tsh.DropEmptyLocation),
		Entry("non-finite value", func(p map[string]any, _ map[string]string) { p["value"] = math.Inf(1) }, tsh.DropUnclassifiableValue),
		Entry("bad timestamp", func(p map[string]any, _ map[string]string) { p["timestamp_ms"] = "not-a-number" }, tsh.DropBadTimestamp),
	)

	It("suppresses EmitMeta on the second identical-metadata message (shared view)", func() {
		view := tsh.NewDedupCache().NewBatch()
		p1, m1 := base()
		m1["serialNumber"] = "abc"
		row1, reason1 := tsh.Transform(p1, m1, "pump", true, nil, view)
		Expect(reason1).To(Equal(tsh.DropNone))
		Expect(row1.EmitMeta).To(BeTrue())

		p2, m2 := base()
		p2["timestamp_ms"] = float64(1) // distinct value row, same metadata
		m2["serialNumber"] = "abc"
		row2, reason2 := tsh.Transform(p2, m2, "pump", true, nil, view)
		Expect(reason2).To(Equal(tsh.DropNone))
		Expect(row2.EmitMeta).To(BeFalse())
	})
})
