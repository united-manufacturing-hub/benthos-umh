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
	"regexp"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tsh "github.com/united-manufacturing-hub/benthos-umh/historian_plugin"
)

var _ = Describe("suggestedTopicPattern", func() {
	matcher := func(contract string) *regexp.Regexp {
		re, err := regexp.Compile(tsh.SuggestedTopicPatternForTest(contract))
		Expect(err).NotTo(HaveOccurred())
		return re
	}

	It("emits the exact documented pattern", func() {
		Expect(tsh.SuggestedTopicPatternForTest("historian")).
			To(Equal(`^umh\.v1(?:\.[^._][^.]*)+\._historian(_v\d+)?\..+$`))
	})

	DescribeTable("selects every topic that really carries the contract",
		func(topic string) {
			Expect(matcher("historian").MatchString(topic)).To(BeTrue(), topic)
		},
		Entry("one location level", "umh.v1.acme._historian.temperature"),
		Entry("twelve location levels", "umh.v1.a.b.c.d.e.f.g.h.i.j.k.l._historian.temperature"),
		Entry("versioned contract", "umh.v1.acme.berlin._historian_v3.temperature"),
		Entry("virtual path before the name", "umh.v1.acme.berlin._historian.motor.electrical.temperature"),
	)

	DescribeTable("rejects a topic that does not carry the contract",
		func(topic string) {
			Expect(matcher("historian").MatchString(topic)).To(BeFalse(), topic)
		},
		Entry("a different contract", "umh.v1.acme.berlin._raw.temperature"),
		Entry("the name appears only as a virtual path segment", "umh.v1.acme.berlin._raw._historian.temperature"),
		Entry("no tag name after the contract", "umh.v1.acme.berlin._historian"),
	)
})

var _ = Describe("mismatchMessage", func() {
	It("names the wrong contract, the refused share and both fixes when nothing has been stored", func() {
		got := tsh.MismatchMessageForTest("historian", false, 4, 4)
		Expect(got).To(ContainSubstring("no message carries data contract _historian"))
		Expect(got).To(ContainSubstring("reason=contract_mismatch"))
		Expect(got).To(ContainSubstring("4 of 4"))
		Expect(got).To(ContainSubstring("set data_contract_name"))
		Expect(got).To(ContainSubstring(`^umh\.v1(?:\.[^._][^.]*)+\._historian(_v\d+)?\..+$`))
	})

	It("reports the over-broad subscription and the refused share once rows are landing", func() {
		got := tsh.MismatchMessageForTest("historian", true, 12084, 12000)
		Expect(got).To(ContainSubstring("subscription is over-broad"))
		Expect(got).To(ContainSubstring("reason=contract_mismatch"))
		Expect(got).To(ContainSubstring("12000 of 12084"))
		Expect(got).To(ContainSubstring(`^umh\.v1(?:\.[^._][^.]*)+\._historian(_v\d+)?\..+$`))
		Expect(got).NotTo(ContainSubstring("no message carries"))
	})

	DescribeTable("carries neither an example topic nor the contracts that arrived",
		func(contractIsPublished bool) {
			got := tsh.MismatchMessageForTest("historian", contractIsPublished, 4, 4)
			Expect(got).NotTo(ContainSubstring("example"))
			Expect(got).NotTo(ContainSubstring("--"))
		},
		Entry("contract never seen", false),
		Entry("over-broad subscription", true),
	)

	DescribeTable("stays a single line so it survives being rendered as a status reason",
		func(contractIsPublished bool) {
			got := tsh.MismatchMessageForTest("historian", contractIsPublished, 4, 4)
			Expect(got).NotTo(ContainSubstring("\n"))
			Expect(got).To(HavePrefix("TimescaleDB historian: "))
		},
		Entry("contract never seen", false),
		Entry("over-broad subscription", true),
	)
})

var _ = Describe("contract-mismatch notification", func() {
	var (
		h    *tsh.HistorianTestHandle
		logs func() string
		now  time.Time
	)
	BeforeEach(func() {
		h = tsh.NewHistorianTestHandle("", "historian")
		now = time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
		logs = h.CaptureLogs()
	})

	It("errors on the very first mismatch", func() {
		h.NoteContractMismatch(now, 4, 4, false)
		Expect(logs()).To(ContainSubstring("level=error msg=TimescaleDB historian: no message carries data contract _historian"), "the batch is NACKed from the first message, so the reason has to arrive with it")
	})

	It("throttles repeat mismatches instead of logging every batch", func() {
		now = now.Add(31 * time.Second)
		h.NoteContractMismatch(now, 4, 4, false)
		h.NoteContractMismatch(now, 4, 4, false)
		now = now.Add(1 * time.Minute)
		h.NoteContractMismatch(now, 4, 4, false)
		Expect(strings.Count(logs(), "level=error")).To(Equal(1))
	})

	It("re-logs after the interval so the fault stays visible", func() {
		now = now.Add(31 * time.Second)
		h.NoteContractMismatch(now, 4, 4, false)
		now = now.Add(tsh.MismatchLogIntervalForTest() + time.Second)
		h.NoteContractMismatch(now, 4, 4, false)
		Expect(strings.Count(logs(), "level=error")).To(Equal(2))
	})

	It("blames the subscription, not data_contract_name, when the batch also carries the configured contract", func() {
		h.NoteContractMismatch(now, 2, 1, true)
		Expect(logs()).To(ContainSubstring("subscription is over-broad"))
		Expect(logs()).To(ContainSubstring("1 of 2"))
		Expect(logs()).NotTo(ContainSubstring("no message carries"), "the contract is published, so narrowing is the fix and changing data_contract_name would match nothing")
	})
})

var _ = Describe("dropHint", func() {
	It("tells the user how to supply a missing timestamp_ms", func() {
		got := tsh.DropHintForTest(tsh.DropMissingTimestamp)
		Expect(got).To(ContainSubstring("timestamp_ms"))
		Expect(got).To(ContainSubstring("tag processor"))
		Expect(got).NotTo(ContainSubstring("Node-RED"), "the hint names the processor that supplies the field, not the ones that do not")
	})

	It("tells the user how to supply a missing value", func() {
		got := tsh.DropHintForTest(tsh.DropMissingValue)
		Expect(got).To(ContainSubstring("no value field"))
		Expect(got).NotTo(ContainSubstring("msg.payload"), "the value comes from the input, and msg.payload is syntax only some processors have")
	})

	DescribeTable("stays empty for reasons with no actionable fix",
		func(reason tsh.DropReason) {
			Expect(tsh.DropHintForTest(reason)).To(BeEmpty())
		},
		Entry("invalid topic", tsh.DropInvalidTopic),
		Entry("contract mismatch", tsh.DropContractMismatch),
		Entry("bad timestamp", tsh.DropBadTimestamp),
		Entry("unclassifiable value", tsh.DropUnclassifiableValue),
	)

	It("keeps the hint on one line so it survives as a status reason", func() {
		Expect(tsh.DropHintForTest(tsh.DropMissingTimestamp)).NotTo(ContainSubstring("\n"))
	})

	DescribeTable("joins the log line with a sentence break rather than a dash",
		func(reason tsh.DropReason) {
			got := tsh.DropHintForTest(reason)
			Expect(got).NotTo(ContainSubstring("--"))
			Expect(got).To(HavePrefix(". "))
		},
		Entry("missing value", tsh.DropMissingValue),
		Entry("missing timestamp", tsh.DropMissingTimestamp),
		Entry("contract bypassed", tsh.DropContractBypassed),
		Entry("contract unvalidated", tsh.DropContractUnvalidated),
		Entry("not timeseries", tsh.DropNotTimeseries),
	)
})

var _ = Describe("dropHint for the validation guards", func() {
	It("names the flag and the alternative for an unvalidated contract", func() {
		got := tsh.DropHintForTest(tsh.DropContractUnvalidated)
		Expect(got).To(ContainSubstring("allow_unvalidated_data: true"))
		Expect(got).To(ContainSubstring("versioned contract"))
	})

	It("says a bypassed versioned contract cannot be overridden", func() {
		got := tsh.DropHintForTest(tsh.DropContractBypassed)
		Expect(got).To(ContainSubstring("cannot be overridden"))
		Expect(got).To(ContainSubstring("register the schema"))
	})

	It("points relational data elsewhere", func() {
		Expect(tsh.DropHintForTest(tsh.DropNotTimeseries)).To(ContainSubstring("relational data"))
	})
})

var _ = Describe("drop logging", func() {
	It("reports the first dropped batch immediately, with no startup hold", func() {
		h := tsh.NewHistorianTestHandle("", "historian")
		logs := h.CaptureLogs()

		h.ReportDropForTest(4, "missing_timestamp", 4, "umh.v1.acme._historian_v1.t")
		Expect(logs()).To(ContainSubstring("level=error msg=TimescaleDB historian: dropped 4 of 4 message(s) (reason=missing_timestamp"), "a plugin usable outside umh-core cannot defer its errors for a host's startup gate")
	})

	It("keeps an OPC UA server-diagnostics drop at debug so it cannot degrade the bridge", func() {
		h := tsh.NewHistorianTestHandle("", "historian")
		logs := h.CaptureLogs()

		h.ReportDropForTest(4, "server_virtual_path", 4, "umh.v1.acme._historian_v1.Root.Objects.Server.ServerStatus.CurrentTime")
		Expect(logs()).To(ContainSubstring("level=debug msg=TimescaleDB historian: dropped 4 of 4 message(s) (reason=server_virtual_path"), "a broad OPC UA browse picks these up by accident; they are expected noise, not an operator error")
		Expect(logs()).NotTo(ContainSubstring("level=error"))
	})

	It("reports one line per reason, naming each reason's share and an example topic", func() {
		h := tsh.NewHistorianTestHandle("", "historian")
		logs := h.CaptureLogs()

		h.ReportDropsForTest(10, map[string]tsh.DropSummaryForTest{
			"missing_value":     {Count: 7, Example: "umh.v1.acme._historian_v1.a"},
			"missing_timestamp": {Count: 3, Example: "umh.v1.acme._historian_v1.b"},
		})

		Expect(logs()).To(ContainSubstring(`dropped 7 of 10 message(s) (reason=missing_value, example umh_topic="umh.v1.acme._historian_v1.a")`))
		Expect(logs()).To(ContainSubstring(`dropped 3 of 10 message(s) (reason=missing_timestamp, example umh_topic="umh.v1.acme._historian_v1.b")`))
		Expect(strings.Count(logs(), "level=error")).To(Equal(2), "one line per reason, not one per message")
		Expect(logs()).To(ContainSubstring("no value field"), "each line keeps its own hint")
		Expect(logs()).To(ContainSubstring("tag processor sets timestamp_ms"))
	})
})
