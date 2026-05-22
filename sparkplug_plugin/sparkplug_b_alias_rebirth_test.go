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
// wired in the test helper). The counter increments only after
// requestRebirthForUnresolvedAliases passes its role and throttle gates, so it is a
// load-bearing observable for "did the recovery path execute?". The actual MQTT
// publish is unreachable in unit tests (no broker client), so this is the strongest
// assertion the unit layer can make; broker-side NCMD assertion lives in integration.

package sparkplug_plugin_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	sparkplugplugin "github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin"
	"github.com/united-manufacturing-hub/benthos-umh/sparkplug_plugin/sparkplugb"
)

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

	// aliasOnlyDataPayload builds a DATA-style payload where metrics carry only an
	// alias (no name) — the wire format Ignition emits once aliases are negotiated.
	aliasOnlyDataPayload := func(seq uint64, aliases ...uint64) *sparkplugb.Payload {
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

	// namedDataPayload builds a DATA where metrics carry names but no aliases —
	// nothing to resolve, so the rebirth path must not fire.
	namedDataPayload := func(seq uint64, names ...string) *sparkplugb.Payload {
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
			// gate in requestRebirthForUnresolvedAliases only suppresses
			// secondary_passive, so primary works by default — pin it.
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
			// CacheAliases and ResolveAliases. The input-side counter must agree —
			// otherwise alias=0 metrics produce a self-perpetuating rebirth storm
			// (each NDATA triggers a rebirth that can never refill the cache).
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
			// Use a 100ms throttle and a 1s gap to absorb CI jitter and monotonic-clock
			// granularity on macOS/Linux runners.
			wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryActive)
			wrapper.SetBirthRequestThrottle(100 * time.Millisecond)

			wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(0, 101), topicInfo)
			time.Sleep(1 * time.Second)
			wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(1, 101), topicInfo)

			Expect(wrapper.GetAliasRebirthsCount()).To(Equal(int64(2)))
		})
	})

	Context("when sequence gap and unresolved aliases occur on the same DATA", func() {
		It("takes the sequence-gap rebirth path and skips the alias-recovery path", func() {
			// Sequence-gap reports a definite protocol violation, so it takes
			// precedence over cache-miss in the processDataMessage switch.
			wrapper := sparkplugplugin.NewSparkplugInputForTestingWithRole(sparkplugplugin.RoleSecondaryActive)

			// Warm-up: seq=0 establishes state, no gap.
			wrapper.ProcessDataMessage("NDATA", namedDataPayload(0, "warmup"), topicInfo)
			Expect(wrapper.GetNodeState(topicInfo.DeviceKey()).LastSeq).To(Equal(uint8(0)))

			// Trigger: seq=10 (gap) with alias-only metrics that won't resolve.
			wrapper.ProcessDataMessage("NDATA", aliasOnlyDataPayload(10, 101), topicInfo)

			// Gap branch ran — UpdateNodeState flips IsOnline to false on a gap.
			state := wrapper.GetNodeState(topicInfo.DeviceKey())
			Expect(state.IsOnline).To(BeFalse(),
				"sequence-gap path must have executed (UpdateNodeState sets IsOnline=false on gap)")
			// Alias-recovery branch did NOT run — counter stays 0.
			Expect(wrapper.GetAliasRebirthsCount()).To(BeZero(),
				"alias-recovery branch must be skipped when the sequence-gap branch fires")
		})
	})
})
