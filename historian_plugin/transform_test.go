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
	"encoding/json"
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
		Entry("53 characters accepted, the longest that keeps umh.attribute_<name> under 64 bytes", strings.Repeat("p", 53), true),
		Entry("54 characters rejected, since the server would truncate the attribute table", strings.Repeat("p", 54), false),
	)
})

var _ = Describe("CanonicalLtreePath", func() {
	DescribeTable("mirrors the SQL to_ltree_path identity",
		func(in, want string) { Expect(tsh.CanonicalLtreePath(in)).To(Equal(want)) },
		Entry("plain", "acme.line1", "acme.line1"),
		Entry("non-word chars become _", "acme@line/1", "acme_line_1"),
		Entry("hyphen is preserved (PG16+ ltree)", "acme.line-1", "acme.line-1"),
		Entry("empty segments dropped", "a...b", "a.b"),
		Entry("all dots -> empty", "...", ""),
	)
	It("keeps hyphen and underscore distinct but folds other punctuation to _", func() {
		dash := tsh.CanonicalLtreePath("enterprise.line-1")
		under := tsh.CanonicalLtreePath("enterprise.line_1")
		at := tsh.CanonicalLtreePath("enterprise.line@1")
		Expect(dash).NotTo(Equal(under), "PG16+ ltree accepts hyphens; they are no longer aliased to _")
		Expect(at).To(Equal(under), "characters outside [A-Za-z0-9_-] still fold to _")
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

var _ = Describe("ClassifyValue", func() {
	ptrF := func(f float64) *float64 { return &f }

	It("bool true -> numeric 1", func() {
		vt, num, text, ok, truncated := tsh.ClassifyValue(true)
		Expect(ok).To(BeTrue())
		Expect(vt).To(Equal(tsh.ValueNumeric))
		Expect(num).To(Equal(ptrF(1)))
		Expect(text).To(BeNil())
		Expect(truncated).To(BeFalse())
	})
	It("bool false -> numeric 0 (NOT dropped)", func() {
		_, num, _, ok, _ := tsh.ClassifyValue(false)
		Expect(ok).To(BeTrue())
		Expect(num).To(Equal(ptrF(0)))
	})
	It("finite float -> numeric", func() {
		vt, num, _, ok, _ := tsh.ClassifyValue(3.5)
		Expect(ok).To(BeTrue())
		Expect(vt).To(Equal(tsh.ValueNumeric))
		Expect(num).To(Equal(ptrF(3.5)))
	})
	It("int64 -> numeric (not JSON-marshaled text)", func() {
		vt, num, text, ok, _ := tsh.ClassifyValue(int64(42))
		Expect(ok).To(BeTrue())
		Expect(vt).To(Equal(tsh.ValueNumeric))
		Expect(num).To(Equal(ptrF(42)))
		Expect(text).To(BeNil())
	})
	It("int -> numeric", func() {
		vt, num, _, ok, _ := tsh.ClassifyValue(7)
		Expect(ok).To(BeTrue())
		Expect(vt).To(Equal(tsh.ValueNumeric))
		Expect(num).To(Equal(ptrF(7)))
	})
	It("NaN -> dropped", func() {
		_, _, _, ok, _ := tsh.ClassifyValue(math.NaN())
		Expect(ok).To(BeFalse())
	})
	It("+Inf -> dropped", func() {
		_, _, _, ok, _ := tsh.ClassifyValue(math.Inf(1))
		Expect(ok).To(BeFalse())
	})
	It("string -> text as-is (not truncated)", func() {
		vt, num, text, ok, truncated := tsh.ClassifyValue("hello")
		Expect(ok).To(BeTrue())
		Expect(vt).To(Equal(tsh.ValueText))
		Expect(num).To(BeNil())
		Expect(*text).To(Equal("hello"))
		Expect(truncated).To(BeFalse())
	})
	It("empty string -> text (NOT dropped)", func() {
		_, _, text, ok, _ := tsh.ClassifyValue("")
		Expect(ok).To(BeTrue())
		Expect(*text).To(Equal(""))
	})
	It("object -> JSON-encoded text", func() {
		_, _, text, ok, _ := tsh.ClassifyValue(map[string]any{"a": float64(1)})
		Expect(ok).To(BeTrue())
		Expect(*text).To(Equal(`{"a":1}`))
	})
	It("oversized text truncated to 8192 runes and flagged", func() {
		_, _, text, ok, truncated := tsh.ClassifyValue(strings.Repeat("x", 9000))
		Expect(ok).To(BeTrue())
		Expect([]rune(*text)).To(HaveLen(8192))
		Expect(truncated).To(BeTrue())
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
	It("accepts a json.Number (AsStructured yields these for JSON integers)", func() {
		got, ok := tsh.ParseTimestampMs(json.Number("1500"))
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal("1970-01-01T00:00:01.500Z"))
	})
	// A bloblang integer literal (e.g. `root.timestamp_ms = 1782983057000`) reaches an output as
	// a Go int64, not a float64; ts_unix_milli() also returns an integer. These must be accepted.
	It("accepts an int64 (bloblang integer literal / ts_unix_milli output)", func() {
		got, ok := tsh.ParseTimestampMs(int64(1782983057000))
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal("2026-07-02T09:04:17.000Z"))
	})
	It("accepts a plain int", func() {
		got, ok := tsh.ParseTimestampMs(1500)
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal("1970-01-01T00:00:01.500Z"))
	})
	It("trims surrounding whitespace on a numeric string", func() {
		got, ok := tsh.ParseTimestampMs("  1500  ")
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal("1970-01-01T00:00:01.500Z"))
	})
	It("truncates a fractional millisecond toward zero", func() {
		got, ok := tsh.ParseTimestampMs(float64(1500.9))
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal("1970-01-01T00:00:01.500Z"))
	})
	It("accepts the in-range boundaries", func() {
		_, ok := tsh.ParseTimestampMs(float64(8.64e15))
		Expect(ok).To(BeTrue())
		_, ok = tsh.ParseTimestampMs(float64(-8.64e15))
		Expect(ok).To(BeTrue())
	})
	It("drops an empty string", func() {
		_, ok := tsh.ParseTimestampMs("")
		Expect(ok).To(BeFalse())
	})
	It("drops a numeric string carrying a unit suffix", func() {
		_, ok := tsh.ParseTimestampMs("1782983057ms")
		Expect(ok).To(BeFalse())
	})
	It("drops an unsupported type", func() {
		_, ok := tsh.ParseTimestampMs(true)
		Expect(ok).To(BeFalse())
	})
	// The field is milliseconds. A 10-digit epoch-seconds value is a valid, in-range number,
	// so it is not rejected -- it is read as ~1970. A "bad timestamp" seen downstream therefore
	// comes from the mapping, not from a value of this magnitude.
	It("reads a 10-digit epoch-seconds value as ms (silently ~1970, not dropped)", func() {
		got, ok := tsh.ParseTimestampMs(float64(1782983057))
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal("1970-01-21T15:16:23.057Z"))
	})
	It("reads the same instant correctly once scaled to 13-digit ms", func() {
		got, ok := tsh.ParseTimestampMs(float64(1782983057000))
		Expect(ok).To(BeTrue())
		Expect(got).To(Equal("2026-07-02T09:04:17.000Z"))
	})
})

var _ = Describe("Transform", func() {
	base := func() (map[string]any, map[string]string) {
		// Topic carries location=acme.line1, contract=_pump_v1, virtual_path=vibration, name=x;
		// the historian parses it via the canonical parser instead of separate meta fields.
		return map[string]any{"value": 3.5, "timestamp_ms": float64(0)},
			map[string]string{"umh_topic": "umh.v1.acme.line1._pump_v1.vibration.x"}
	}
	tr := func(p map[string]any, m map[string]string) (*tsh.Row, tsh.DropReason) {
		row, drop := tsh.Transform(p, m, "pump", true, nil, nil, tsh.NewDedupCache().NewBatch())
		return row, drop
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
	It("accepts a 10-digit epoch-seconds timestamp but records ~1970 (not a drop)", func() {
		p, m := base()
		p["timestamp_ms"] = float64(1782983057) // seconds mistaken for ms
		row, reason := tr(p, m)
		Expect(reason).To(Equal(tsh.DropNone))
		Expect(row.TS).To(Equal("1970-01-21T15:16:23.057Z"))
	})
	It("records the intended instant when the timestamp is 13-digit ms", func() {
		p, m := base()
		p["timestamp_ms"] = float64(1782983057000)
		row, reason := tr(p, m)
		Expect(reason).To(Equal(tsh.DropNone))
		Expect(row.TS).To(Equal("2026-07-02T09:04:17.000Z"))
	})
	It("maps an int64 value and int64 timestamp (a bloblang integer pipeline)", func() {
		p, m := base()
		p["value"] = int64(42)
		p["timestamp_ms"] = int64(1782983057000)
		row, reason := tr(p, m)
		Expect(reason).To(Equal(tsh.DropNone))
		Expect(row.ValueType).To(Equal(tsh.ValueNumeric))
		Expect(*row.ValueNum).To(Equal(42.0))
		Expect(row.TS).To(Equal("2026-07-02T09:04:17.000Z"))
	})

	DescribeTable("drops with the right reason",
		func(mutate func(map[string]any, map[string]string), want tsh.DropReason) {
			p, m := base()
			mutate(p, m)
			row, reason := tr(p, m)
			Expect(row).To(BeNil())
			Expect(reason).To(Equal(want))
		},
		Entry("missing umh_topic", func(_ map[string]any, m map[string]string) { delete(m, "umh_topic") }, tsh.DropInvalidTopic),
		Entry("malformed umh_topic (consecutive dots)", func(_ map[string]any, m map[string]string) { m["umh_topic"] = "umh.v1.acme..line1._pump.x" }, tsh.DropInvalidTopic),
		Entry("non-matching contract", func(_ map[string]any, m map[string]string) {
			m["umh_topic"] = "umh.v1.acme.line1._other_v1.vibration.x"
		}, tsh.DropContractMismatch),
		Entry("Root.Objects.Server virtual_path", func(_ map[string]any, m map[string]string) {
			m["umh_topic"] = "umh.v1.acme.line1._pump_v1.Root.Objects.Server.foo.x"
		}, tsh.DropServerVirtualPath),
		Entry("the diagnostics subtree with no further virtual path", func(_ map[string]any, m map[string]string) {
			m["umh_topic"] = "umh.v1.acme.line1._pump_v1.Root.Objects.Server.CurrentTime"
		}, tsh.DropServerVirtualPath),
		Entry("absent value", func(p map[string]any, _ map[string]string) { delete(p, "value") }, tsh.DropMissingValue),
		Entry("nil value", func(p map[string]any, _ map[string]string) { p["value"] = nil }, tsh.DropMissingValue),
		Entry("absent timestamp_ms", func(p map[string]any, _ map[string]string) { delete(p, "timestamp_ms") }, tsh.DropMissingTimestamp),
		Entry("nil timestamp_ms", func(p map[string]any, _ map[string]string) { p["timestamp_ms"] = nil }, tsh.DropMissingTimestamp),
		Entry("non-finite value", func(p map[string]any, _ map[string]string) { p["value"] = math.Inf(1) }, tsh.DropUnclassifiableValue),
		Entry("bad timestamp", func(p map[string]any, _ map[string]string) { p["timestamp_ms"] = "not-a-number" }, tsh.DropBadTimestamp),
	)

	DescribeTable("keeps a tag whose virtual path only starts with the diagnostics prefix",
		func(topic string) {
			p, m := base()
			m["umh_topic"] = topic
			row, drop := tsh.Transform(p, m, "pump", true, nil, nil, tsh.NewDedupCache().NewBatch())
			Expect(drop).To(Equal(tsh.DropNone), "only the Root.Objects.Server subtree is OPC UA diagnostics; a sibling that shares its prefix is real data")
			Expect(row).NotTo(BeNil())
		},
		Entry("ServerRoom", "umh.v1.acme.line1._pump_v1.Root.Objects.ServerRoom.temperature"),
		Entry("Servers", "umh.v1.acme.line1._pump_v1.Root.Objects.Servers.count"),
		Entry("nested under ServerRoom", "umh.v1.acme.line1._pump_v1.Root.Objects.ServerRoom.rack1.temperature"),
	)

	It("suppresses EmitMeta on the second identical-metadata message (shared view)", func() {
		view := tsh.NewDedupCache().NewBatch()
		p1, m1 := base()
		m1["serialNumber"] = "abc"
		row1, drop1 := tsh.Transform(p1, m1, "pump", true, nil, nil, view)
		Expect(drop1).To(Equal(tsh.DropNone))
		Expect(row1.EmitMeta).To(BeTrue())

		p2, m2 := base()
		p2["timestamp_ms"] = float64(1) // distinct value row, same metadata
		m2["serialNumber"] = "abc"
		row2, drop2 := tsh.Transform(p2, m2, "pump", true, nil, nil, view)
		Expect(drop2).To(Equal(tsh.DropNone))
		Expect(row2.EmitMeta).To(BeFalse())
	})
})

var _ = Describe("Transform validation guards", func() {
	ts := func(contract string) (map[string]any, map[string]string) {
		return map[string]any{"value": 3.5, "timestamp_ms": float64(0)},
			map[string]string{"umh_topic": "umh.v1.acme.line1." + contract + ".x"}
	}
	run := func(p map[string]any, m map[string]string) (*tsh.Row, tsh.DropReason) {
		return tsh.Transform(p, m, "pump", true, nil, nil, tsh.NewDedupCache().NewBatch())
	}

	It("rejects a versioned contract whose schema was bypassed", func() {
		p, m := ts("_pump_v1")
		m["data_contract_bypassed"] = "true"
		row, drop := run(p, m)
		Expect(row).To(BeNil())
		Expect(drop).To(Equal(tsh.DropContractBypassed))
	})

	It("accepts a versioned contract that was validated", func() {
		p, m := ts("_pump_v1")
		row, drop := run(p, m)
		Expect(drop).To(Equal(tsh.DropNone))
		Expect(row).NotTo(BeNil())
	})

	It("accepts an unversioned contract, which is never schema-validated", func() {
		p, m := ts("_pump")
		row, drop := run(p, m)
		Expect(drop).To(Equal(tsh.DropNone), "an unversioned contract is stored; only a datatype change on it needs a config opt-in")
		Expect(row).NotTo(BeNil())
	})

	It("accepts an unversioned contract carrying the bypass flag", func() {
		p, m := ts("_pump")
		m["data_contract_bypassed"] = "true"
		row, drop := run(p, m)
		Expect(drop).To(Equal(tsh.DropNone), "the uns output stamps this flag on EVERY unversioned message, so honoring it here would drop all _historian traffic")
		Expect(row).NotTo(BeNil())
	})

	It("rejects a relational payload that happens to carry value and timestamp_ms", func() {
		p, m := ts("_pump_v1")
		p["orderId"] = "WO-42"
		row, drop := run(p, m)
		Expect(row).To(BeNil())
		Expect(drop).To(Equal(tsh.DropNotTimeseries))
	})

	It("names a relational payload as such even when it carries neither value nor timestamp_ms", func() {
		p, m := ts("_pump_v1")
		delete(p, "value")
		delete(p, "timestamp_ms")
		p["orderId"] = "WO-42"
		p["quantity"] = 5.0
		row, drop := run(p, m)
		Expect(row).To(BeNil())
		Expect(drop).To(Equal(tsh.DropNotTimeseries), "reporting missing_value here sends the operator to add a value field, which converges on a payload the historian stores as JSON text")
	})

	It("accepts a payload carrying exactly value and timestamp_ms", func() {
		p, m := ts("_pump_v1")
		_, drop := run(p, m)
		Expect(drop).To(Equal(tsh.DropNone))
	})
})
