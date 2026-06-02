//go:build !integration

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

// Tests for the alias-recovery rebirth path: when DATA references aliases that aren't
// in the in-memory cache (e.g. after a bridge restart with no retained BIRTH on the
// broker), the input plugin must publish NCMD/Node Control/Rebirth. Without this,
// the cache stays empty forever and Topic Browser shows tags as `…/_historian/alias_<n>`.
//
// Verification: tests assert against GetAliasRebirthsCount (the in-memory testCounter
// wired in the test helper). The counter increments only after sendRebirthRequest
// with rebirthReasonUnresolvedAliases passes its role and throttle gates, so it
// answers "did the recovery path execute?". The actual MQTT publish is unreachable
// in unit tests (no broker client); broker-side NCMD assertion lives in integration.

package sparkplug_plugin_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	sparkplugplugin "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

// aliasOnlyDataPayload builds a DATA-style payload where metrics carry only an
// alias (no name) -- the wire format Ignition emits once aliases are negotiated.
func aliasOnlyDataPayload(seq uint64, aliases ...uint64) *sparkplugb.Payload {
	s := seq
	ts := uint64(1730986400000)
	metrics := make([]*sparkplugb.Payload_Metric, 0, len(aliases))
	for _, a := range aliases {
		alias := a
		metrics = append(metrics, &sparkplugb.Payload_Metric{
			Alias: &alias,
			Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 1.23},
		})
	}
	return &sparkplugb.Payload{Seq: &s, Timestamp: &ts, Metrics: metrics}
}

// namedDataPayload builds a DATA where metrics carry names but no aliases.
// Nothing to resolve, so the rebirth path must not fire.
func namedDataPayload(seq uint64, names ...string) *sparkplugb.Payload {
	s := seq
	ts := uint64(1730986400000)
	metrics := make([]*sparkplugb.Payload_Metric, 0, len(names))
	for _, n := range names {
		name := n
		metrics = append(metrics, &sparkplugb.Payload_Metric{
			Name:  &name,
			Value: &sparkplugb.Payload_Metric_DoubleValue{DoubleValue: 1.23},
		})
	}
	return &sparkplugb.Payload{Seq: &s, Timestamp: &ts, Metrics: metrics}
}

var _ = Describe("Rebirth on unresolved aliases", func() {
	var (
		topicInfo *sparkplugplugin.TopicInfo
		nodeKey   string
	)

	BeforeEach(func() {
		topicInfo = &sparkplugplugin.TopicInfo{
			Group:    "Factory",
			EdgeNode: "Edge1",
		}
		nodeKey = topicInfo.NodeKey()
	})

	Context("when role permits rebirth and DATA aliases are not in cache", func() {
		It("triggers a rebirth under secondary_active", func() {
			wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryActive)

			wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(0, 101, 102), topicInfo)

			Expect(wrapper.GetAliasRebirthsCount()).To(Equal(int64(1)),
				"alias-recovery rebirth must execute exactly once")
			_, ok := wrapper.GetBirthRequestedAt(nodeKey)
			Expect(ok).To(BeTrue(), "throttle map must record the nodeKey")
		})

		It("triggers a rebirth under primary host", func() {
			// The CHANGELOG explicitly promises primary publishes rebirths. The role
			// gate in sendRebirthRequest is an allowlist of
			// {primary, secondary_active}, so pin both branches with distinct tests.
			wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RolePrimaryHost)

			wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(0, 101), topicInfo)

			Expect(wrapper.GetAliasRebirthsCount()).To(Equal(int64(1)))
		})

		It("triggers a rebirth on DDATA (device-level) at the node-key scope", func() {
			// The alias cache is keyed by deviceKey (group/node/device); the rebirth
			// throttle is keyed by nodeKey (group/node). On DDATA those differ, and
			// the rebirth must be issued at the node level so the edge republishes
			// NBIRTH plus DBIRTH for every device beneath it.
			wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryActive)
			dTopic := &sparkplugplugin.TopicInfo{Group: "Factory", EdgeNode: "Edge1", Device: "Device1"}

			wrapper.ProcessDataMessage("DDATA", aliasOnlyDataPayload(0, 101), dTopic)

			Expect(wrapper.GetAliasRebirthsCount()).To(Equal(int64(1)))
			_, ok := wrapper.GetBirthRequestedAt(dTopic.NodeKey())
			Expect(ok).To(BeTrue(), "throttle entry must be at nodeKey, not deviceKey")
			_, ok = wrapper.GetBirthRequestedAt(dTopic.DeviceKey())
			Expect(ok).To(BeFalse(), "throttle must NOT be keyed by deviceKey")
		})

		It("triggers a rebirth when only some aliases are unresolved (partial cache hit)", func() {
			// Realistic case after a partial cache loss: one alias resolves, one
			// doesn't. The trigger is `unresolved > 0`, not "all unresolved".
			wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryActive)
			a101, name101 := uint64(101), "Temperature"
			wrapper.SeedAliasCache(topicInfo.DeviceKey(), []*sparkplugb.Payload_Metric{
				{Name: &name101, Alias: &a101},
			})

			wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(0, 101, 102), topicInfo)

			Expect(wrapper.GetAliasRebirthsCount()).To(Equal(int64(1)))
		})
	})

	Context("when role=secondary_passive and DATA aliases are not in cache", func() {
		It("does NOT trigger a rebirth (passive bridges stay silent)", func() {
			wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryPassive)

			wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(0, 101), topicInfo)

			Expect(wrapper.GetAliasRebirthsCount()).To(BeZero())
			_, ok := wrapper.GetBirthRequestedAt(nodeKey)
			Expect(ok).To(BeFalse(),
				"secondary_passive must short-circuit before touching the throttle map")
		})
	})

	Context("when DATA aliases resolve from a populated cache", func() {
		It("does NOT trigger a rebirth", func() {
			wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryActive)
			alias1, alias2 := uint64(101), uint64(102)
			name1, name2 := "Temperature", "Pressure"
			wrapper.SeedAliasCache(topicInfo.DeviceKey(), []*sparkplugb.Payload_Metric{
				{Name: &name1, Alias: &alias1},
				{Name: &name2, Alias: &alias2},
			})

			wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(0, 101, 102), topicInfo)

			Expect(wrapper.GetAliasRebirthsCount()).To(BeZero())
		})
	})

	Context("when DATA contains only named metrics (no aliases at all)", func() {
		It("does NOT trigger a rebirth", func() {
			wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryActive)

			wrapper.ProcessDataMessage("NDATA", namedDataPayload(0, "Temperature"), topicInfo)

			Expect(wrapper.GetAliasRebirthsCount()).To(BeZero())
		})
	})

	Context("when DATA carries alias=0 (Sparkplug-reserved sentinel)", func() {
		It("does NOT trigger a rebirth (alias 0 never lives in the cache)", func() {
			// AliasCache treats *Alias==0 as "no real alias" and skips it in both
			// CacheAliases and ResolveAliases. The input-side counter must agree;
			// otherwise alias=0 metrics would re-trigger a rebirth on every NDATA
			// without ever populating the cache.
			wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryActive)

			wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(0, 0), topicInfo)

			Expect(wrapper.GetAliasRebirthsCount()).To(BeZero())
		})
	})

	Context("throttling", func() {
		It("rate-limits two rapid alias-recovery rebirths to a single request", func() {
			wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryActive)
			wrapper.SetBirthRequestThrottle(500 * time.Millisecond)

			wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(0, 101), topicInfo)
			wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(1, 101), topicInfo)

			Expect(wrapper.GetAliasRebirthsCount()).To(Equal(int64(1)),
				"second call inside the throttle window must NOT increment the counter")
		})

		It("allows another rebirth after the throttle window elapses", func() {
			// 20ms throttle with a 150ms gap. Tight but well above OS scheduler
			// jitter on macOS/Linux CI; smaller than the original 1s+100ms pair.
			wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryActive)
			wrapper.SetBirthRequestThrottle(20 * time.Millisecond)

			wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(0, 101), topicInfo)
			time.Sleep(150 * time.Millisecond)
			wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(1, 101), topicInfo)

			Expect(wrapper.GetAliasRebirthsCount()).To(Equal(int64(2)))
		})
	})

	Context("when sequence gap and unresolved aliases occur on the same DATA", func() {
		It("collapses both signals into a single rebirth via the shared throttle", func() {
			// The call sites in processDataMessage are independent flat ifs.
			// There is no in-code precedence between sequence-gap and alias-miss; the shared
			// birthRequested throttle is what prevents redundant broker commands:
			// whichever reason fires first stamps the map, the next finds the
			// entry within the throttle window, and returns silently. A positive
			// throttle is required to demonstrate the suppression because the
			// default is zero (never throttles) in the test wrapper.
			wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryActive)
			wrapper.SetBirthRequestThrottle(500 * time.Millisecond)

			// Warm-up: seq=0 establishes state, no gap.
			wrapper.ProcessDataMessage("NDATA", namedDataPayload(0, "warmup"), topicInfo)
			Expect(wrapper.GetNodeState(topicInfo.DeviceKey()).LastSeq).To(Equal(uint8(0)))

			// Trigger: seq=10 (gap) with alias-only metrics that won't resolve.
			wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(10, 101), topicInfo)

			// Gap branch ran -- UpdateNodeState flips IsOnline to false on a gap.
			state := wrapper.GetNodeState(topicInfo.DeviceKey())
			Expect(state.IsOnline).To(BeFalse(),
				"sequence-gap path must have executed (UpdateNodeState sets IsOnline=false on gap)")
			// Seq-gap path stamped the throttle map first; the alias-miss call
			// on the same DATA hit the throttle and never reached the counter.
			_, ok := wrapper.GetBirthRequestedAt(topicInfo.NodeKey())
			Expect(ok).To(BeTrue(),
				"seq-gap path must have stamped the throttle map (prerequisite for the alias-miss suppression)")
			Expect(wrapper.GetAliasRebirthsCount()).To(BeZero(),
				"shared throttle must suppress the alias-recovery rebirth when seq-gap already fired in the same window")
		})
	})
})

// The role gate inside sendRebirthRequest is reason-agnostic: RoleSecondaryPassive
// must suppress every reason (discovery, sequence-gap, unresolved-aliases) before
// the throttle map is touched. The alias-recovery branch is covered by the passive
// context inside the "Rebirth on unresolved aliases" Describe above; these tests
// pin the same property for the other two reasons so a future regression that
// drifts one reason away from the unified gate gets caught here.
var _ = Describe("Rebirth role gate suppresses every reason under secondary_passive", func() {
	var topicInfo *sparkplugplugin.TopicInfo

	BeforeEach(func() {
		topicInfo = &sparkplugplugin.TopicInfo{Group: "Factory", EdgeNode: "Edge1"}
	})

	It("does NOT send a discovery rebirth for a newly seen node", func() {
		// Set RequestBirthOnConnect=true so the feature flag would otherwise
		// permit the rebirth; only the role gate is left to block it.
		wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryPassive)
		wrapper.SetRequestBirthOnConnect(true)

		// First DATA from this node => IsNewNode=true => discovery reason fires.
		wrapper.ProcessDataMessage("NDATA", namedDataPayload(0, "warmup"), topicInfo)

		// Node was discovered (state created), but no throttle entry was made.
		Expect(wrapper.GetNodeState(topicInfo.DeviceKey())).NotTo(BeNil(),
			"UpdateNodeState must still record the new node even under passive role")
		_, ok := wrapper.GetBirthRequestedAt(topicInfo.NodeKey())
		Expect(ok).To(BeFalse(),
			"secondary_passive must short-circuit the discovery rebirth before touching the throttle map")
	})

	It("does NOT send a sequence-gap rebirth", func() {
		wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryPassive)

		// Warm-up establishes state (seq=0), no rebirth path runs.
		wrapper.ProcessDataMessage("NDATA", namedDataPayload(0, "warmup"), topicInfo)
		// Trigger sequence gap (seq=10).
		wrapper.ProcessDataMessage("NDATA", namedDataPayload(10, "warmup"), topicInfo)

		// Gap was detected (UpdateNodeState ran and flipped IsOnline) but the
		// rebirth path was suppressed by the role gate.
		state := wrapper.GetNodeState(topicInfo.DeviceKey())
		Expect(state).NotTo(BeNil())
		Expect(state.IsOnline).To(BeFalse(),
			"sequence gap must still be recorded by UpdateNodeState (state mutation is independent of role)")
		_, ok := wrapper.GetBirthRequestedAt(topicInfo.NodeKey())
		Expect(ok).To(BeFalse(),
			"secondary_passive must short-circuit the seq-gap rebirth before touching the throttle map")
	})
})

// Positive coverage for the discovery rebirth path. The CHANGELOG headlines the
// default flip of request_birth_on_connect to true; a regression that swaps the
// gate order or accidentally flips the default back to false would otherwise
// not be caught (the existing discovery-related tests are all negative).
var _ = Describe("Discovery rebirth on newly seen node", func() {
	var topicInfo *sparkplugplugin.TopicInfo

	BeforeEach(func() {
		topicInfo = &sparkplugplugin.TopicInfo{Group: "Factory", EdgeNode: "Edge1"}
	})

	It("stamps the throttle map on the first DATA from an unknown node", func() {
		wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryActive)
		wrapper.SetRequestBirthOnConnect(true)

		// First DATA: IsNewNode=true, named metric (no alias-miss), no gap.
		// Only the discovery reason should fire.
		wrapper.ProcessDataMessage("NDATA", namedDataPayload(0, "warmup"), topicInfo)

		_, ok := wrapper.GetBirthRequestedAt(topicInfo.NodeKey())
		Expect(ok).To(BeTrue(),
			"discovery rebirth must stamp the throttle map under secondary_active + RequestBirthOnConnect=true")
		Expect(wrapper.GetAliasRebirthsCount()).To(BeZero(),
			"alias-recovery counter must NOT tick when only the discovery reason was triggered")
	})

	It("does NOT stamp the throttle map when request_birth_on_connect is false", func() {
		wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryActive)
		wrapper.SetRequestBirthOnConnect(false)

		wrapper.ProcessDataMessage("NDATA", namedDataPayload(0, "warmup"), topicInfo)

		_, ok := wrapper.GetBirthRequestedAt(topicInfo.NodeKey())
		Expect(ok).To(BeFalse(),
			"discovery feature flag must gate the rebirth before the throttle stamp")
	})
})

// Coverage for the throttle:0 = "no throttling" contract documented in the
// CHANGELOG and field description. Before the coercion fix at sparkplug_b_input.go:393
// was removed, the production parser silently rewrote 0 to 1s; this test
// would have caught that drift if it had existed earlier.
var _ = Describe("birth_request_throttle = 0 disables throttling", func() {
	It("allows back-to-back alias-recovery rebirths with no throttle", func() {
		topicInfo := &sparkplugplugin.TopicInfo{Group: "Factory", EdgeNode: "Edge1"}
		wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryActive)
		wrapper.SetBirthRequestThrottle(0)

		// Two rapid alias-recovery dispatches must both fire when the throttle
		// is zero. The "elapsed < BirthRequestThrottle" check is false for any
		// non-negative elapsed when BirthRequestThrottle is 0, so no suppression.
		wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(0, 101), topicInfo)
		wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(1, 101), topicInfo)

		Expect(wrapper.GetAliasRebirthsCount()).To(Equal(int64(2)),
			"throttle=0 must allow every alias-recovery dispatch through")
	})
})

// Coverage for the same-dispatch co-fire log-level distinction. When two reasons
// fire on the same DATA (e.g. IsNewNode=true AND unresolvedAliases>0 on a cold
// start), the second reason's suppression should be silent at info level so
// operator logs aren't polluted with "throttled, 0s ago" lines on every restart.
// We can't directly assert log levels from the test layer, but we can assert
// the observable side-effect: rebirthsSuppressed ticks via the role gate path
// at line 1335, but co-firing within sameDispatchThreshold doesn't add to the
// throttle-bookkeeping side effect any test wrapper observes today.
//
// What we DO assert below: the elapsed-threshold behavior produces exactly one
// rebirth NCMD per processDataMessage call, even when two reasons would fire.
var _ = Describe("Same-dispatch co-firing reasons do not produce extra rebirths", func() {
	It("only the first reason in the call dispatch stamps the throttle", func() {
		topicInfo := &sparkplugplugin.TopicInfo{Group: "Factory", EdgeNode: "Edge1"}
		wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryActive)
		wrapper.SetRequestBirthOnConnect(true)
		wrapper.SetBirthRequestThrottle(500 * time.Millisecond)

		// Single DATA triggers both discovery (IsNewNode=true) and unresolved-aliases.
		// Discovery fires first, stamps the throttle; alias-recovery hits the
		// throttle in the same call and is suppressed.
		wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(0, 101, 102), topicInfo)

		_, ok := wrapper.GetBirthRequestedAt(topicInfo.NodeKey())
		Expect(ok).To(BeTrue(), "discovery must stamp the throttle map")
		Expect(wrapper.GetAliasRebirthsCount()).To(BeZero(),
			"alias-recovery must be suppressed by the same-dispatch throttle hit")
	})
})
