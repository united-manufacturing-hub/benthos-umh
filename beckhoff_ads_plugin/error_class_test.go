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

package beckhoff_ads_plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	adsLib "github.com/RuneRoven/go-ads/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/errclass"
)

// udpTimeout is what an unanswered identify or route-registration exchange on
// UDP 48899 fails with.
var udpTimeout = &net.OpError{Op: "read", Net: "udp4", Err: os.ErrDeadlineExceeded}

// udpRefused is what an identify or route-registration exchange fails with
// when the host answers with ICMP port unreachable.
var udpRefused = &net.OpError{Op: "read", Net: "udp4", Err: &os.SyscallError{Syscall: "recvfrom", Err: syscall.ECONNREFUSED}}

// goADSRecord sends one record through the go-ads log bridge, as go-ads would.
func goADSRecord(level slog.Level, msg string, args ...any) capturedLog {
	log, recs := capturingLogger()
	classified(log).Slog().Log(context.Background(), level, msg, args...)
	Expect(*recs).To(HaveLen(1))
	return (*recs)[0]
}

// haveKind checks that a line carries the error_kind and error_owner of kind.
func haveKind(kind *errclass.Kind) OmegaMatcher {
	return HaveField("Attrs", SatisfyAll(
		HaveKeyWithValue("error_kind", kind.String()),
		HaveKeyWithValue("error_owner", kind.Owner()),
	))
}

var _ = Describe("classifier", func() {
	It("classifies no error as ads::unclassified", func() {
		Expect(kindUnclassified.String()).To(Equal("ads::unclassified"))
	})

	// umh-core groups and routes the lines of this input by these names and
	// owners, so a renamed Kind or a flipped owner fails here.
	DescribeTable("pins every Kind and its owner",
		func(kind *errclass.Kind, name, owner string) {
			Expect(kind.String()).To(Equal("ads::" + name))
			Expect(kind.Owner()).To(Equal(owner))
		},
		Entry(nil, kindConnectionDropped, "connection_dropped", "user"),
		Entry(nil, kindSessionDisconnected, "session_disconnected", "user"),
		Entry(nil, kindHeartbeatResubscribe, "heartbeat_resubscribe", "user"),
		Entry(nil, kindResubscribeFailed, "resubscribe_failed", "user"),
		Entry(nil, kindHeartbeatObserveOnly, "heartbeat_observe_only", "user"),
		Entry(nil, kindTargetUnreachable, "target_unreachable", "user"),
		Entry(nil, kindTargetPortNotFound, "target_port_not_found", "user"),
		Entry(nil, kindReconnectFailing, "reconnect_failing", "user"),
		Entry(nil, kindRouteFault, "route_fault", "user"),
		Entry(nil, kindPLCNotRunning, "plc_not_running", "user"),
		Entry(nil, kindSymbolConfig, "symbol_config", "user"),
		Entry(nil, kindInvalidHostAMS, "invalid_host_ams", "user"),
		Entry(nil, kindInvalidTargetAMS, "invalid_target_ams", "user"),
		Entry(nil, kindInvalidConfig, "invalid_config", "user"),
		Entry(nil, kindSessionCreateFailed, "session_create_failed", "umh"),
		Entry(nil, kindSymbolCacheMiss, "symbol_cache_miss", "umh"),
		Entry(nil, kindNoInitialSamples, "no_initial_samples", "umh"),
		Entry(nil, kindReadGap, "read_gap", "umh"),
		Entry(nil, kindBatchReadFallback, "batch_read_fallback", "user"),
		Entry(nil, kindReadTimeout, "read_timeout", "user"),
		Entry(nil, kindCloseFailed, "close_failed", "user"),
		Entry(nil, kindProtocolDecode, "protocol_decode", "umh"),
		Entry(nil, kindInternalBug, "internal_bug", "umh"),
		Entry(nil, kindUnclassified, "unclassified", "unknown"),
	)

	DescribeTable("maps an error onto the contract by type",
		func(err error, want *errclass.Kind) {
			Expect(classifier.Classify(err)).To(Equal(want))
		},
		Entry("every notification symbol failed", kindSymbolConfig.Errorf("no symbols registered for notifications (2 symbols all failed to resolve)"), kindSymbolConfig),
		Entry("hostAMS does not parse", kindInvalidHostAMS.Errorf("hostAMS %q is not a valid AMS NetID: %w", "1.2", errors.New("bad")), kindInvalidHostAMS),
		Entry("targetAMS does not parse", kindInvalidTargetAMS.Errorf("targetAMS %q is not a valid AMS NetID: %w", "1.2", errors.New("bad")), kindInvalidTargetAMS),
		Entry("go-ads rejected the session options", sessionCreateError(errors.New("remote.IP must be set")), kindSessionCreateFailed),
		Entry("router answered nothing", fmt.Errorf("connect: %w", adsLib.ErrRouterUnresponsive), kindTargetUnreachable),
		Entry("TCP dial failed", fmt.Errorf("connect: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}), kindTargetUnreachable),
		Entry("UDP exchange timed out", fmt.Errorf("identify: %w", udpTimeout), kindTargetUnreachable),
		Entry("target discovery timed out while creating the session",
			sessionCreateError(fmt.Errorf("discovery failed: identify: %w", udpTimeout)), kindTargetUnreachable),
		Entry("target discovery refused while creating the session",
			sessionCreateError(fmt.Errorf("discovery failed: identify: %w", udpRefused)), kindTargetUnreachable),
		// The text of go-ads v2.3.4 session.go:443.
		Entry("a device that reports no TwinCAT version, with runtimePort 0",
			sessionCreateError(errors.New("ads: NewSession: 10.13.37.52 (CX-1234) reported no TwinCAT version, so the runtime "+
				"AMS port cannot be inferred; set remote.AMS.Port explicitly (801 on TwinCAT 2, 851 on TwinCAT 3)")),
			kindTargetPortNotFound),
		Entry("a refused TCP read is not an unanswered UDP exchange",
			&net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNREFUSED}}, kindUnclassified),
		Entry("TCP socket timed out on a live session", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}, kindReadTimeout),
		Entry("subscribe stranded by a cache reload", fmt.Errorf("symbol %q: %w", "MAIN.a", adsLib.ErrNotificationStrandedByReload), kindInternalBug),
		Entry("route not served, though the transport closed too", fmt.Errorf("%w: %w", adsLib.ErrRouteNotServed, adsLib.ErrTransportClosed), kindRouteFault),
		Entry("runtime not in RUN", fmt.Errorf("route is served but %w (ADS state 15)", adsLib.ErrRuntimeNotRunning), kindPLCNotRunning),
		Entry("router rejected the port", adsLib.AMSError{Code: adsLib.ReturnCodeGlobalTargetPortNotFound}, kindTargetPortNotFound),
		Entry("device rejected the port", fmt.Errorf("read: %w", adsLib.ReturnCodeGlobalTargetPortNotFound), kindTargetPortNotFound),
		Entry("symbol not found", fmt.Errorf("GetSymbolInfoByName failed: %w", adsLib.ReturnCodeDeviceSymbolNoFound), kindSymbolConfig),
		Entry("batch symbol unresolved", fmt.Errorf("item: %w", adsLib.ErrBatchSymbolUnresolved), kindSymbolConfig),
		Entry("batch read without a result for the item", adsLib.ErrBatchNoResult, kindReadGap),
		Entry("batch read value that did not parse", fmt.Errorf("item: %w", adsLib.ErrBatchValueUnparsable), kindReadGap),
		Entry("batch read symbol removed from the cache", adsLib.ErrBatchSymbolVanished, kindReadGap),
		Entry("notification symbol subscribed twice", fmt.Errorf("symbol %q: %w", "MAIN.a", adsLib.ErrNotificationDuplicate), kindSymbolConfig),
		Entry("notification symbol removed by an online change", adsLib.ErrNotificationSymbolVanished, kindSymbolConfig),
		Entry("notification on a second update channel", adsLib.ErrNotificationChannelMismatch, kindInternalBug),
		Entry("batch subscribe aborted after a transport failure",
			fmt.Errorf("%w: batch aborted at item %d", adsLib.ErrNotificationTransportFailure, 3), kindConnectionDropped),
		Entry("batch subscribe whose transport closed", fmt.Errorf("%w: %w", adsLib.ErrNotificationTransportFailure, adsLib.ErrTransportClosed), kindConnectionDropped),
		Entry("transport closed", fmt.Errorf("read: %w", adsLib.ErrTransportClosed), kindConnectionDropped),
		Entry("established connection dropped", adsLib.ErrEstablishedDropped, kindConnectionDropped),
		Entry("session disconnected", adsLib.ErrDisconnected, kindConnectionDropped),
		Entry("request deadline", fmt.Errorf("read: %w", context.DeadlineExceeded), kindReadTimeout),
		Entry("a socket read that did not time out", &net.OpError{Op: "read", Net: "tcp", Err: io.EOF}, kindUnclassified),
		Entry("anything else", errors.New("boom"), kindUnclassified),
	)

	It("keeps the Kind an error was created with, whatever it wraps", func() {
		Expect(classifier.Classify(kindSymbolConfig.Errorf("boom"))).To(Equal(kindSymbolConfig))
		Expect(classifier.Classify(kindSymbolConfig.Errorf("x: %w", adsLib.ErrTransportClosed))).To(Equal(kindSymbolConfig))
	})

	DescribeTable("matches the route faults go-ads returns untyped by their text, and only those",
		func(err error, want *errclass.Kind) {
			Expect(classifier.Classify(err)).To(Equal(want))
		},
		Entry("PLC refused the registration", errors.New("route registration failed during connect: route registration failed with error code: 1796"), kindRouteFault),
		Entry("route registered but never served, though the probe saw a closed transport",
			fmt.Errorf("route registration during connect: %w", fmt.Errorf(`route "benthosADS-1.2.3.4" was registered but the PLC did not serve it within 10s (3 redials): %w`, adsLib.ErrTransportClosed)),
			kindRouteFault),
		Entry("a deaf router is not a route fault", fmt.Errorf("route registration failed during connect: %w", adsLib.ErrRouterUnresponsive), kindTargetUnreachable),
		Entry("a redial after registration that was refused",
			fmt.Errorf("TCP reconnect after route registration failed: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}),
			kindTargetUnreachable),
		Entry("a registration whose probe connection dropped", fmt.Errorf("route registration failed during connect: %w", adsLib.ErrTransportClosed), kindConnectionDropped),
		Entry("a registration wrapper around nothing typed", errors.New("route registration failed during connect: boom"), kindUnclassified),
		Entry("typed errors still classify", fmt.Errorf("connect: %w", adsLib.ErrRuntimeNotRunning), kindPLCNotRunning),
	)

	// The predicates decide retries and log levels; the classifier decides the
	// logged Kind. Where a predicate holds, in the order logConnectFailure tries
	// them, the classifier has to agree, or a line would say one cause and the
	// code act on another.
	DescribeTable("agrees with the control-flow predicates where they hold",
		func(err error, want *errclass.Kind) {
			var got *errclass.Kind
			switch {
			case isRouteFault(err):
				got = kindRouteFault
			case isTransportGone(err):
				got = kindConnectionDropped
			case isReadTimeout(err):
				got = kindReadTimeout
			}
			Expect(got).To(Equal(want), "predicate verdict")
			if want != nil {
				Expect(classifier.Classify(err)).To(Equal(want))
			}
		},
		Entry("route not served, though the transport closed too",
			fmt.Errorf("%w: %w", adsLib.ErrRouteNotServed, adsLib.ErrTransportClosed), kindRouteFault),
		Entry("PLC refused the registration",
			errors.New("route registration failed during connect: route registration failed with error code: 1796"), kindRouteFault),
		Entry("route registered but never served, though the probe saw a closed transport",
			fmt.Errorf(`route "benthosADS-1.2.3.4" was registered but the PLC did not serve it within 10s (3 redials): %w`, adsLib.ErrTransportClosed),
			kindRouteFault),
		Entry("a deaf router wrapped in route-registration text",
			fmt.Errorf("route registration failed with error code: %w", adsLib.ErrRouterUnresponsive), nil),
		Entry("transport closed", fmt.Errorf("read: %w", adsLib.ErrTransportClosed), kindConnectionDropped),
		Entry("session disconnected", fmt.Errorf("read: %w", adsLib.ErrDisconnected), kindConnectionDropped),
		Entry("established connection dropped", adsLib.ErrEstablishedDropped, kindConnectionDropped),
		Entry("request deadline", fmt.Errorf("read: %w", context.DeadlineExceeded), kindReadTimeout),
		Entry("TCP socket deadline", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}, kindReadTimeout),
	)

	DescribeTable("reasonKinds maps go-ads session-event reasons",
		func(reason adsLib.Reason, want *errclass.Kind) {
			Expect(reasonKinds[string(reason)]).To(Equal(want))
		},
		Entry("symbol not found", adsLib.ReasonSymbolNotFound, kindSymbolConfig),
		Entry("symbol not active", adsLib.ReasonSymbolNotActive, kindSymbolConfig),
		Entry("heartbeat silent", adsLib.ReasonHeartbeatSilent, kindHeartbeatObserveOnly),
		Entry("an unmapped reason", adsLib.ReasonReloadCapExhausted, nil),
	)
})

var _ = Describe("go-ads record classification", func() {
	decisive, fallbacks := messageRules()

	It("has an expected Kind for every MessageIs rule, and a rule for every expected Kind", func() {
		Expect(classifier.Messages()).To(ConsistOf(slices.Collect(maps.Keys(messageKinds))))
		Expect(classifier.Messages()).To(ContainElements(slices.Collect(maps.Keys(fallbacks))))
	})

	for _, msg := range decisive {
		want := messageKinds[msg]
		It("tags go-ads message "+strconv.Quote(msg)+" as "+want.String()+", whatever its error", func() {
			Expect(goADSRecord(slog.LevelError, msg)).To(haveKind(want))
			// An error that would classify differently must not win over the message.
			Expect(goADSRecord(slog.LevelError, msg, "error", adsLib.ErrRouterUnresponsive)).To(haveKind(want))
		})
	}

	for msg, kind := range fallbacks {
		It("tags "+strconv.Quote(msg)+" by its error first, then by its message", func() {
			Expect(goADSRecord(slog.LevelError, msg, "error", adsLib.ErrRuntimeNotRunning)).To(haveKind(kindPLCNotRunning))
			Expect(goADSRecord(slog.LevelError, msg, "error", adsLib.ErrTransportClosed)).To(haveKind(kindConnectionDropped))
			Expect(goADSRecord(slog.LevelError, msg, "error", errors.New("boom"))).To(haveKind(kind))
		})
	}

	It("classifies an unlisted record by its error value", func() {
		rec := goADSRecord(slog.LevelWarn, "send request failed",
			"error", adsLib.AMSError{Code: adsLib.ReturnCodeGlobalTargetPortNotFound})
		Expect(rec).To(haveKind(kindTargetPortNotFound))
	})

	It("finds the error among the attributes a logger was built with", func() {
		log, recs := capturingLogger()
		classified(log).Slog().With("err", adsLib.ErrTransportClosed).Warn("some go-ads warning")
		Expect((*recs)[0]).To(haveKind(kindConnectionDropped))
	})

	It("tags a record it cannot classify as unclassified", func() {
		Expect(goADSRecord(slog.LevelWarn, "something go-ads never said before", "count", 3)).
			To(haveKind(kindUnclassified))
	})

	It("leaves info and debug records untagged", func() {
		for _, level := range []slog.Level{slog.LevelInfo, slog.LevelDebug} {
			rec := goADSRecord(level, "session disconnected, reconnecting; no data is read until it succeeds")
			Expect(rec.Attrs).NotTo(HaveKey("error_kind"))
			Expect(rec.Attrs).NotTo(HaveKey("error_owner"))
		}
	})
})

var _ = Describe("go-ads message table guard", func() {
	It("finds every classified message in the pinned go-ads source", func() {
		// A go-ads bump that rewords a message fails here instead of silently
		// turning that message into ads::unclassified.
		src, version := goADSSource()

		var missing []string
		for _, msg := range classifier.Messages() {
			if !strings.Contains(src, strconv.Quote(msg)) {
				missing = append(missing, msg)
			}
		}
		Expect(missing).To(BeEmpty(), "messages not found as string literals in go-ads %s", version)

		// The untyped route faults and the missing TwinCAT version are matched
		// by a fragment of a format string, not by a whole literal.
		for _, text := range append(slices.Clone(untypedRouteFaults), noTwinCATVersion) {
			Expect(src).To(ContainSubstring(text), "error text not found in go-ads %s", version)
		}
	})

	It("has a rule or a reason for every warning and error message in the pinned go-ads source", func() {
		// A go-ads bump that adds a warning or an error fails here, so that it
		// gets a rule or an entry in deliberatelyUnclassified.
		src, version := goADSSource()
		found := goADSLogMessages(src)
		// The extraction finds a message of each call form.
		Expect(found).To(HaveKey("header too short"), "Error")
		Expect(found).To(HaveKey("PLC closed connection, transport down"), "Log")
		Expect(found).To(HaveKey("reconnect step failed, retrying"), "logAttempt")
		Expect(found).To(HaveKey(
			"no notification heartbeat within the allowed window; treating this session's subscriptions as dead and re-subscribing"), "msg :=")

		var uncovered, stale []string
		for msg := range found {
			_, ruled := messageKinds[msg]
			_, listed := deliberatelyUnclassified[msg]
			if !ruled && !listed {
				uncovered = append(uncovered, msg)
			}
		}
		for msg := range deliberatelyUnclassified {
			if !found[msg] {
				stale = append(stale, msg)
			}
		}
		Expect(uncovered).To(BeEmpty(), "go-ads %s messages with neither a MessageIs rule nor an entry in deliberatelyUnclassified", version)
		Expect(stale).To(BeEmpty(), "deliberatelyUnclassified entries that go-ads %s no longer logs", version)
	})
})

// goADSSource returns the non-test Go source of the pinned go-ads module and
// its version. It fails the spec if the module is not in the module cache.
func goADSSource() (string, string) {
	out, err := exec.Command("go", "list", "-m", "-json", "github.com/RuneRoven/go-ads/v2").Output()
	Expect(err).NotTo(HaveOccurred())
	var mod struct{ Dir, Version string }
	Expect(json.Unmarshal(out, &mod)).To(Succeed())
	Expect(mod.Dir).NotTo(BeEmpty(), "go-ads %s is not in the module cache; run go mod download", mod.Version)

	files, err := filepath.Glob(filepath.Join(mod.Dir, "*.go"))
	Expect(err).NotTo(HaveOccurred())
	Expect(files).NotTo(BeEmpty(), "no Go files in %s", mod.Dir)
	var src strings.Builder
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		Expect(err).NotTo(HaveOccurred())
		src.Write(b)
	}
	return src.String(), mod.Version
}

// goLiteral matches a Go interpreted string literal.
const goLiteral = `("(?:[^"\\\n]|\\.)*")`

var (
	// goADSLogCalls match the message of a go-ads warning or error that is a
	// string literal: the first argument of Warn, Error and the go-ads helpers
	// that pass their msg on to them, the second of WarnContext and
	// ErrorContext, and a literal assigned to msg, which go-ads passes to Warn
	// or Error later.
	goADSLogCalls = []*regexp.Regexp{
		regexp.MustCompile(`\.(?:Warn|Error|logAttempt|warnInferenceOnce)\(\s*` + goLiteral),
		regexp.MustCompile(`\.(?:WarnContext|ErrorContext)\([^,()]+,\s*` + goLiteral),
		regexp.MustCompile(`\bmsg :?= ` + goLiteral),
	}
	// goADSLevelLog matches Log and LogAttrs, whose level is a variable or
	// function call as often as a constant. Only trace, debug and info
	// constants are skipped.
	goADSLevelLog = regexp.MustCompile(`\.Log(?:Attrs)?\([^,()]+(?:\(\))?,\s*([^,]+?),\s*` + goLiteral)
	belowWarn     = regexp.MustCompile(`Level(?:Trace|Debug|Info)\b`)
)

// goADSLogMessages returns the literal messages of the warnings and errors in
// src.
func goADSLogMessages(src string) map[string]bool {
	found := map[string]bool{}
	add := func(lit string) {
		msg, err := strconv.Unquote(lit)
		Expect(err).NotTo(HaveOccurred(), lit)
		found[msg] = true
	}
	for _, re := range goADSLogCalls {
		for _, m := range re.FindAllStringSubmatch(src, -1) {
			add(m[1])
		}
	}
	for _, m := range goADSLevelLog.FindAllStringSubmatch(src, -1) {
		if !belowWarn.MatchString(m[1]) {
			add(m[2])
		}
	}
	return found
}

var _ = Describe("plugin log lines carry their class", func() {
	It("tags a symbol go-ads skipped in a batch read as a read gap", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classified(log)}

		a.logBatchFailures(&BatchReadError{Requested: 3, Failed: []NotifyResult{
			{SymbolName: "MAIN.gap", Skipped: adsLib.ErrBatchNoResult},
			{SymbolName: "MAIN.typo", Code: 0x710},
		}})

		Expect(*recs).To(ConsistOf(
			SatisfyAll(HaveField("Msg", ContainSubstring("MAIN.gap")), haveKind(kindReadGap)),
			SatisfyAll(HaveField("Msg", ContainSubstring("MAIN.typo")), haveKind(kindSymbolConfig)),
		))
	})

	It("tags a batch-read symbol go-ads could not resolve as symbol configuration, not a read gap", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classified(log)}

		a.logBatchFailures(&BatchReadError{Requested: 2, Failed: []NotifyResult{{
			SymbolName: "MAIN.typo",
			Skipped:    fmt.Errorf("%w: %w", adsLib.ErrBatchSymbolUnresolved, adsLib.ReturnCodeDeviceSymbolNoFound),
		}}})

		Expect(*recs).To(ConsistOf(haveKind(kindSymbolConfig)))
	})

	It("tags a notification stranded by a cache reload as a bug, with the symbol in the message", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{
			Log:              classified(log),
			Symbols:          []PlcSymbol{{Name: "MAIN.ok"}, {Name: "MAIN.a"}},
			NotificationChan: make(chan *Update, 8),
			client: &fakeClient{notifyResults: []NotifyResult{
				{SymbolName: "MAIN.ok", Registered: true},
				{SymbolName: "MAIN.a", Skipped: fmt.Errorf("symbol %q: %w", "MAIN.a", adsLib.ErrNotificationStrandedByReload)},
			}},
		}
		a.NotificationChan <- &Update{Variable: "MAIN.ok", Value: "1"}

		Expect(a.setupNotifications(context.Background())).To(Succeed())

		Expect(*recs).To(ContainElement(SatisfyAll(
			HaveField("Msg", HavePrefix(`notification symbol "MAIN.a" skipped (check symbol name): `)),
			haveKind(kindInternalBug),
		)))
	})

	DescribeTable("logReadFailure",
		func(err error, want *errclass.Kind) {
			log, recs := capturingLogger()
			a := &AdsCommInput{Log: classified(log), client: &fakeClient{}}

			a.logReadFailure(2, err)

			Expect(*recs).To(ConsistOf(haveKind(want)))
		},
		Entry("PLC did not answer in time", fmt.Errorf("read: %w", context.DeadlineExceeded), kindReadTimeout),
		Entry("connection dropped", adsLib.ErrTransportClosed, kindConnectionDropped),
		Entry("anything else", errors.New("boom"), kindUnclassified),
	)

	It("logs a timeout whose error also wraps a closed transport as the timeout warning", func() {
		// isReadTimeout decides the message and the level; the classifier, which
		// tries the transport rules first, decides the Kind.
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classified(log), client: &fakeClient{}}

		a.logReadFailure(2, fmt.Errorf("read: %w: %w", context.DeadlineExceeded, adsLib.ErrTransportClosed))

		Expect(*recs).To(ConsistOf(SatisfyAll(
			HaveField("Level", slog.LevelWarn),
			HaveField("Msg", HavePrefix("the PLC did not answer the read of 2 symbols within requestTimeout")),
			haveKind(kindConnectionDropped),
		)))
	})

	DescribeTable("logConnectFailure",
		func(err error, want *errclass.Kind) {
			log, recs := capturingLogger()
			a := &AdsCommInput{Log: classified(log), TargetIP: "1.2.3.4", TargetPort: 48898, RuntimePort: 851}

			a.logConnectFailure(err)

			Expect(*recs).To(ConsistOf(haveKind(want)))
		},
		Entry("route not served", fmt.Errorf("connect: %w", adsLib.ErrRouteNotServed), kindRouteFault),
		Entry("route registration refused", errors.New("route registration failed during connect: route registration failed with error code: 1796"), kindRouteFault),
		Entry("transport dropped", adsLib.ErrTransportClosed, kindConnectionDropped),
		Entry("dial failed", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, kindTargetUnreachable),
		Entry("unrecognized", errors.New("ADS error 0x706"), kindUnclassified),
		Entry("a dropped transport around a stopped runtime", fmt.Errorf("%w: %w", adsLib.ErrTransportClosed, adsLib.ErrRuntimeNotRunning), kindPLCNotRunning),
	)

	It("gives the plugin line and the returned error the same error_kind", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classified(log), TargetIP: "1.2.3.4", TargetPort: 48898, RuntimePort: 851}
		err := fmt.Errorf("%w: %w", adsLib.ErrTransportClosed, adsLib.ErrRuntimeNotRunning)

		a.logConnectFailure(err)
		returned := classifier.EnrichForLog(err)

		Expect(*recs).To(ConsistOf(haveKind(kindPLCNotRunning)))
		Expect(returned.Error()).To(HavePrefix("[error_kind=ads::plc_not_running error_owner=user] "))
	})

	It("tags notification symbols the PLC did not register as symbol configuration", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{
			Log:              classified(log),
			Symbols:          []PlcSymbol{{Name: "MAIN.ok"}, {Name: "MAIN.typo"}, {Name: "MAIN.refused"}},
			NotificationChan: make(chan *Update, 8),
			client: &fakeClient{notifyResults: []NotifyResult{
				{SymbolName: "MAIN.ok", Registered: true},
				{SymbolName: "MAIN.typo", Skipped: fmt.Errorf("resolve symbol %q: %w", "MAIN.typo", adsLib.ReturnCodeDeviceSymbolNoFound)},
				{SymbolName: "MAIN.refused", Code: 0x710},
			}},
		}
		a.NotificationChan <- &Update{Variable: "MAIN.ok", Value: "1"}

		Expect(a.setupNotifications(context.Background())).To(Succeed())

		warns := 0
		for _, r := range *recs {
			if r.Level >= slog.LevelWarn {
				warns++
				Expect(r).To(haveKind(kindSymbolConfig))
			}
		}
		Expect(warns).To(Equal(2))
	})

	It("fails the connect as symbol configuration when no notification symbol registered, after logging each cause", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{
			Log:     classified(log),
			Symbols: []PlcSymbol{{Name: "MAIN.typo"}, {Name: "MAIN.stranded"}},
			client: &fakeClient{notifyResults: []NotifyResult{
				{SymbolName: "MAIN.typo", Skipped: fmt.Errorf("resolve symbol %q: %w", "MAIN.typo", adsLib.ReturnCodeDeviceSymbolNoFound)},
				{SymbolName: "MAIN.stranded", Skipped: fmt.Errorf("symbol %q: %w", "MAIN.stranded", adsLib.ErrNotificationStrandedByReload)},
			}},
		}

		err := classifier.EnrichForLog(a.setupNotifications(context.Background()))

		Expect(classifier.Classify(err)).To(Equal(kindSymbolConfig))
		Expect(err.Error()).To(HavePrefix("[error_kind=ads::symbol_config error_owner=user] "))
		Expect(*recs).To(ContainElements(
			SatisfyAll(HaveField("Msg", ContainSubstring("MAIN.typo")), haveKind(kindSymbolConfig)),
			SatisfyAll(HaveField("Msg", ContainSubstring("MAIN.stranded")), haveKind(kindInternalBug)),
		))
	})

	DescribeTable("returns a configuration error from the constructor with its Kind in the text",
		func(yaml string, want *errclass.Kind) {
			conf, err := adsConf.ParseYAML(yaml, nil)
			Expect(err).NotTo(HaveOccurred())

			_, err = NewAdsCommInput(conf, service.MockResources())

			Expect(err).To(HaveOccurred())
			Expect(classifier.Classify(err)).To(Equal(want))
			Expect(err.Error()).To(HavePrefix(fmt.Sprintf("[error_kind=%s error_owner=%s] ", want, want.Owner())))
			Expect(strings.Count(err.Error(), "[error_kind=")).To(Equal(1))
		},
		Entry("hostAMS that is not a NetID", "targetAddress: 1.2.3.4\nhostAMS: 1.2\nunifiedAddress: [MAIN.a]\n", kindInvalidHostAMS),
		Entry("targetAMS that is not a NetID", "targetAddress: 1.2.3.4\ntargetAMS: 1.2\nunifiedAddress: [MAIN.a]\n", kindInvalidTargetAMS),
		Entry("no symbols", "targetAddress: 1.2.3.4\nunifiedAddress: []\n", kindSymbolConfig),
		Entry("targetAddress that is not an IP", "targetAddress: plc\nunifiedAddress: [MAIN.a]\n", kindInvalidConfig),
		Entry("runtimePort out of range", "targetAddress: 1.2.3.4\nruntimePort: 70000\nunifiedAddress: [MAIN.a]\n", kindInvalidConfig),
		Entry("hostPort out of range", "targetAddress: 1.2.3.4\nhostPort: 70000\nunifiedAddress: [MAIN.a]\n", kindInvalidConfig),
		Entry("hostIP that is not an IP", "targetAddress: 1.2.3.4\nhostIP: host\nunifiedAddress: [MAIN.a]\n", kindInvalidConfig),
		Entry("negative duration", "targetAddress: 1.2.3.4\nmaxReconnectInterval: -5s\nunifiedAddress: [MAIN.a]\n", kindInvalidConfig),
		Entry("unsupported heartbeatRecovery", "targetAddress: 1.2.3.4\nheartbeatRecovery: restart\nunifiedAddress: [MAIN.a]\n", kindInvalidConfig),
	)

	It("tags a symbol whose metadata did not resolve by its error", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classified(log), Symbols: []PlcSymbol{{Name: "MAIN.typo"}}, client: &fakeClient{}}

		a.initSymbolIndex(context.Background())

		Expect(*recs).To(ConsistOf(haveKind(kindSymbolConfig)))
	})

	// Each reason in reasonKinds, since a rebuild line carrying one fixed Kind
	// would pass for any single reason.
	DescribeTable("tags the rebuild line with the class of the go-ads reason",
		func(reason adsLib.Reason, want *errclass.Kind) {
			log, recs := capturingLogger()
			a := &AdsCommInput{Log: classified(log)}

			a.onSessionEvent(SessionEventSubscriptionsDead, string(reason))

			Expect(*recs).To(ConsistOf(SatisfyAll(
				HaveField("Level", slog.LevelError),
				haveKind(want),
			)))
		},
		Entry("symbol not found", adsLib.ReasonSymbolNotFound, kindSymbolConfig),
		Entry("symbol not active", adsLib.ReasonSymbolNotActive, kindSymbolConfig),
		Entry("heartbeat silent", adsLib.ReasonHeartbeatSilent, kindHeartbeatObserveOnly),
		Entry("an unmapped reason", adsLib.ReasonReloadCapExhausted, kindUnclassified),
	)

	It("tags an unhandled session event as an internal bug", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classified(log)}

		a.onSessionEvent(SessionEvent(99), "new-reason")

		Expect(*recs).To(ConsistOf(haveKind(kindInternalBug)))
	})

	It("tags a failed close", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classified(log), client: &fakeClient{closeErr: errors.New("boom")}}

		Expect(a.Close(context.Background())).To(HaveOccurred())

		Expect(*recs).To(ContainElement(SatisfyAll(HaveField("Level", slog.LevelError), haveKind(kindCloseFailed))))
	})

	It("prefixes a failed Connect with its class for the Benthos core line", func() {
		// Port 1 on loopback refuses at once, so this needs no PLC.
		a := &AdsCommInput{
			Log: classified(testLog()), TargetIP: "127.0.0.1", TargetPort: 1, TargetAMS: "127.0.0.1.1.1",
			RuntimePort: 851, HostAMS: "auto", HostIP: "auto", RequestTimeout: time.Second,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		err := a.Connect(ctx)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(HavePrefix("[error_kind=ads::target_unreachable error_owner=user] "))
		Expect(isRouteFault(err)).To(BeFalse(), "the connect gate still sees the wrapped error")
	})

	It("prefixes a Connect error only once", func() {
		err := classifier.EnrichForLog(classifier.EnrichForLog(adsLib.ErrTransportClosed))
		Expect(strings.Count(err.Error(), "[error_kind=")).To(Equal(1))
		Expect(err).To(MatchError(adsLib.ErrTransportClosed))
	})
})

// The eight message forms of Sentry issue UMH-CORE-45M, rebuilt from the raw
// lines. None is a fault only UMH can fix, so none may be bug or unknown.
var _ = Describe("UMH-CORE-45M message forms", func() {
	notReported := HaveField("Attrs", HaveKeyWithValue("error_owner", "user"))

	It("discovery timeout: Failed to connect to ads (identify got no answer)", func() {
		err := classifier.EnrichForLog(fmt.Errorf("creating the ADS session failed: %w",
			fmt.Errorf("ads: NewSession: target AMS address incomplete and discovery failed "+
				"(set remote.AMS explicitly if the device does not answer the identify service): %w",
				fmt.Errorf("identify 10.13.37.52: no answer after 3 attempts: %w", udpTimeout))))
		Expect(err.Error()).To(HavePrefix("[error_kind=ads::target_unreachable error_owner=user] "))
	})

	It("heartbeat", func() {
		Expect(goADSRecord(slog.LevelError,
			"no notification heartbeat within the allowed window; treating this session's subscriptions as dead and re-subscribing",
			"cycle", 2*time.Second, "missedTicks", 5)).To(SatisfyAll(notReported, haveKind(kindHeartbeatResubscribe)))
	})

	It("connection dropped", func() {
		Expect(goADSRecord(slog.LevelError, "PLC dropped an established connection, transport down",
			"error", io.EOF, "framesPeer", 0)).To(SatisfyAll(notReported, haveKind(kindConnectionDropped)))
	})

	It("session disconnected", func() {
		Expect(goADSRecord(slog.LevelError, "session disconnected, reconnecting; no data is read until it succeeds",
			"flapCount", 2)).To(SatisfyAll(notReported, haveKind(kindSessionDisconnected)))
	})

	It("target port not found", func() {
		Expect(goADSRecord(slog.LevelError, "send request failed",
			"error", adsLib.AMSError{Code: adsLib.ReturnCodeGlobalTargetPortNotFound})).
			To(SatisfyAll(notReported, haveKind(kindTargetPortNotFound)))
	})

	It("reconnect failing", func() {
		err := fmt.Errorf("route registration failed after probe: %w",
			fmt.Errorf("route registration got no usable answer in 6s over 3 attempts: %w", udpTimeout))
		Expect(goADSRecord(slog.LevelError, "reconnect step failed, retrying",
			"stage", "route", "error", err, "attempt", 1)).To(notReported)
	})

	It("symbol not found", func() {
		err := fmt.Errorf(`symbol ".SCADA.PD.Hubzahl_Soll" not found and on-demand lookup failed: %w`,
			fmt.Errorf("GetSymbolInfoByName(.SCADA.PD.Hubzahl_Soll) failed: ADS error in WriteRead: %w",
				adsLib.ReturnCodeDeviceSymbolNoFound))
		Expect(goADSRecord(slog.LevelError, "error getting symbol for batch notification",
			"error", err, "symbol", ".SCADA.PD.Hubzahl_Soll")).To(SatisfyAll(notReported, haveKind(kindSymbolConfig)))
	})

	It("route registration", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classified(log), TargetIP: "192.168.40.25", TargetPort: 48898, RuntimePort: 801}
		err := errors.New("route registration failed during connect: route registration failed with error code: 1796")

		a.logConnectFailure(err)
		returned := classifier.EnrichForLog(err)

		Expect(*recs).To(ConsistOf(SatisfyAll(notReported, haveKind(kindRouteFault))))
		Expect(returned.Error()).To(HavePrefix("[error_kind=ads::route_fault error_owner=user] "))
	})
})

func testLog() *service.Logger {
	log, _ := capturingLogger()
	return log
}

// messageKinds is the Kind that every MessageIs rule in classifier must give.
// It is part of the contract with umh-core, so a rule that changes its Kind
// fails a test.
var messageKinds = map[string]*errclass.Kind{
	"PLC dropped an established connection, transport down":                                                      kindConnectionDropped,
	"PLC closed connection, transport down":                                                                      kindConnectionDropped,
	"listen read error, transport down":                                                                          kindConnectionDropped,
	"no frame has arrived on either socket; treating the transport as dead and reconnecting":                     kindConnectionDropped,
	"listen body read error, transport down":                                                                     kindConnectionDropped,
	"error setting the write deadline, transport down":                                                           kindConnectionDropped,
	"error sending data on conn, transport down":                                                                 kindConnectionDropped,
	"error flushing data on conn, transport down":                                                                kindConnectionDropped,
	"error clearing the write deadline, transport down":                                                          kindConnectionDropped,
	"session disconnected, reconnecting; no data is read until it succeeds":                                      kindSessionDisconnected,
	"connection flapping, applying cross-cycle cooldown before reconnect":                                        kindSessionDisconnected,
	"subscriptions are missing; recovering":                                                                      kindHeartbeatResubscribe,
	"heartbeat silent; not re-subscribing (WithHeartbeatRecovery(Observe))":                                      kindHeartbeatObserveOnly,
	"notification target symbol no longer in cache; skipping parse":                                              kindSymbolCacheMiss,
	"batch read result for symbol no longer in cache; skipping":                                                  kindSymbolCacheMiss,
	"no batch read result for symbol":                                                                            kindReadGap,
	"error parsing symbol in batch read":                                                                         kindReadGap,
	"symbol read error in batch":                                                                                 kindSymbolConfig,
	"error adding notification in batch":                                                                         kindSymbolConfig,
	"duplicate notification rejected (already subscribed)":                                                       kindSymbolConfig,
	"duplicate notification rejected (within batch)":                                                             kindSymbolConfig,
	"notification symbol gone after reconnect, dropping subscription":                                            kindSymbolConfig,
	"cannot resolve the base type of a user-defined type; no datatype table is loaded":                           kindSymbolConfig,
	"inferring base type from size (no datatype table loaded; LoadSymbols() recommended for user-defined types)": kindSymbolConfig,
	"target NetID differs from the NetID this device reports for itself":                                         kindInvalidTargetAMS,
	"resubscribe: dropping configs after max retries":                                                            kindResubscribeFailed,
	"SumReadEx not supported, using individual reads":                                                            kindBatchReadFallback,
	"sendRequest aborted due to timeout":                                                                         kindReadTimeout,
	"max reconnect attempts exhausted, closing session":                                                          kindReconnectFailing,
	"header too short":                                       kindProtocolDecode,
	"Error parsing header":                                   kindProtocolDecode,
	"Error parsing body":                                     kindProtocolDecode,
	"device notification decode failed":                      kindProtocolDecode,
	"failed to parse notification response":                  kindProtocolDecode,
	"error during parse of notification":                     kindProtocolDecode,
	"listen header decode error, transport down":             kindProtocolDecode,
	"AMS packet length exceeds sanity limit, transport down": kindProtocolDecode,
	"SumRead errored-item declared length exceeds remaining bytes — protocol drift":  kindProtocolDecode,
	"SumRead per-item undersize — protocol drift":                                    kindProtocolDecode,
	"SumRead per-item oversize — protocol drift or wire corruption":                  kindProtocolDecode,
	"SumRead truncated — likely protocol corruption":                                 kindProtocolDecode,
	"error during binary read":                                                       kindProtocolDecode,
	"makeArrayChildren: array Elements exceeds sanity cap, refusing to allocate":     kindProtocolDecode,
	"makeArrayChildren: LBound + Elements overflows uint32, refusing":                kindProtocolDecode,
	"collectSubtree hit depth cap; possible Children cycle or malformed symbol tree": kindProtocolDecode,
	"addOffset hit depth cap; possible datatype self-cycle in PLC response":          kindProtocolDecode,
	"binary.Write failed": kindInternalBug,
	"notification batch callback got an out-of-range index":                                                                 kindInternalBug,
	"notification batch: success with a zero handle":                                                                        kindInternalBug,
	"notification send panicked — caller closed the update channel?":                                                        kindInternalBug,
	"orphan delete goroutine panic recovered":                                                                               kindInternalBug,
	"Error during sendRequest encode":                                                                                       kindInternalBug,
	"endHandshake without a matching beginHandshake; clamping":                                                              kindInternalBug,
	"FSM invalid transition (ignoring)":                                                                                     kindInternalBug,
	"WithBackoff: invalid config, keeping current value":                                                                    kindInternalBug,
	"WithHeartbeatRecovery: unrecognised mode, keeping the default":                                                         kindInternalBug, //nolint:misspell // go-ads spelling
	"parseTree: invalid BOOL value, defaulting to false":                                                                    kindInternalBug,
	"parseTree: invalid signed integer value, defaulting to 0":                                                              kindInternalBug,
	"parseTree: invalid unsigned integer value, defaulting to 0":                                                            kindInternalBug,
	"parseTree: invalid float value, defaulting to 0":                                                                       kindInternalBug,
	"parseTree: invalid numeric value, defaulting to 0":                                                                     kindInternalBug,
	"no notification heartbeat within the allowed window; treating this session's subscriptions as dead and re-subscribing": kindHeartbeatResubscribe,
	"connected, but the PLC runtime is not in RUN: symbol and subscription calls will refuse until it returns":              kindPLCNotRunning,
	"route is active but the PLC runtime is not in RUN; connecting anyway and waiting for it":                               kindPLCNotRunning,
	"re-subscribe after a heartbeat timeout failed; keeping the subscriptions on file and retrying in the next window":      kindResubscribeFailed,
	"re-subscribe bound nothing (PLC not serving yet); keeping the subscriptions on file and retrying in the next window":   kindResubscribeFailed,
	"reconnect dial/start failed, retrying":                                                                                 kindReconnectFailing,
	"reconnect step failed, retrying":                                                                                       kindReconnectFailing,
	"error getting symbol for batch notification":                                                                           kindSymbolConfig,
	"error getting symbol for batch read":                                                                                   kindSymbolConfig,
	"on-demand symbol unavailable after reconnect, skipping":                                                                kindSymbolConfig,
	"could not dial the PLC":                                                                                                kindTargetUnreachable,
	"send aborted due to timeout":                                                                                           kindReadTimeout,
	"AddDeviceNotification fallback aborted: transport failed mid-batch":                                                    kindConnectionDropped,
}

// fallbackMessages are the MessageIs rules that come after the error rules in
// classifier, so they apply only if no error rule matches the record's error.
var fallbackMessages = []string{
	"reconnect dial/start failed, retrying",
	"reconnect step failed, retrying",
	"error getting symbol for batch notification",
	"error getting symbol for batch read",
	"on-demand symbol unavailable after reconnect, skipping",
	"could not dial the PLC",
	"send aborted due to timeout",
	"AddDeviceNotification fallback aborted: transport failed mid-batch",
}

// messageRules returns the messages of the MessageIs rules in classifier:
// those that decide on their own, and fallbackMessages with their Kinds.
func messageRules() ([]string, map[string]*errclass.Kind) {
	fallbacks := make(map[string]*errclass.Kind, len(fallbackMessages))
	for _, msg := range fallbackMessages {
		fallbacks[msg] = messageKinds[msg]
	}
	decisive := slices.DeleteFunc(classifier.Messages(), func(msg string) bool {
		_, ok := fallbacks[msg]
		return ok
	})
	return decisive, fallbacks
}

// deliberatelyUnclassified are the warning and error messages of go-ads that
// have no MessageIs rule, each with the reason. A record with one of them gets
// the Kind of its error, if an error rule matches, and ads::unclassified
// otherwise.
var deliberatelyUnclassified = map[string]string{
	// The error decides the Kind.
	"send request failed":                                                                          "classified by its error",
	"error during read state":                                                                      "classified by its error",
	"individual read failed in SumRead fallback":                                                   "classified by its error",
	"individual AddDeviceNotification failed in fallback":                                          "classified by its error",
	"route activation gave up: redial budget spent and the transport is gone":                      "classified by its error; the route fault that go-ads returns next is logged as route_fault",
	"PLC accepted the connection but answered nothing; backing off completely before trying again": "classified by its error",
	"could not establish the notification heartbeat; retrying, and until it succeeds a silent subscription death will go unnoticed": "classified by its error",
	"auto-reload failed":                                         "classified by its error",
	"Close error during stale-detection shutdown":                "classified by its error",
	"failed to read symbol version, continuing with symbol load": "classified by its error; go-ads continues",
	"failed to read symbol version during slow discovery":        "classified by its error; go-ads continues",
	"failed to read symbol version during LoadSymbolList":        "classified by its error; go-ads continues",
	"failed to add notification handler":                         "only an ADS code, which means a runtime in CONFIG as often as a bad symbol; setupNotifications logs the result with its Kind",
	"sum command not supported by PLC, using fallback":           "covers sum reads, writes and notification commands alike; go-ads falls back and delivers",

	// go-ads releases a PLC handle on a best-effort basis, after a cause
	// that is logged with its own Kind. No Kind exists yet for a handle left
	// on the PLC.
	"error deleting handle":                                                           "best-effort handle release",
	"orphan delete RPC failed":                                                        "best-effort handle release",
	"bestEffortDelete: SumDeleteDeviceNotification failed":                            "best-effort handle release",
	"bestEffortDelete: some handles not cleaned up":                                   "best-effort handle release",
	"individual DeleteDeviceNotification: transport-level failure, aborting fallback": "best-effort handle release",
	"individual DeleteDeviceNotification failed in fallback":                          "best-effort handle release",
	"failed to release PLC handle after symbol vanished":                              "best-effort handle release",
	"failed to release PLC handle after cache reload during subscribe":                "best-effort handle release",
	"failed to release PLC handle after channel-mismatch reject":                      "best-effort handle release",
	"failed to release PLC handle after duplicate-subscribe reject":                   "best-effort handle release",
	"batch entry not committed; releasing the PLC handle":                             "best-effort handle release",
	"released PLC notification handles the batch did not bind":                        "best-effort handle release",
	"failed to release symbol handle":                                                 "best-effort handle release",
	"failed to release orphan symbol handle after cache swap":                         "best-effort handle release",
	"failed to release duplicate symbol handle":                                       "best-effort handle release",
	"reconnect: giving up on releasing pre-reconnect notification handles":            "best-effort handle release",
	"reconnect: keeping unreleased notification handles for the next attempt":         "best-effort handle release",
	"resubscribe: released PLC handles for Skipped+Handle entries":                    "best-effort handle release",
	"resubscribe rollback: deleted partial-success handles":                           "best-effort handle release",
	"received notification for unknown handle":                                        "subscriptions of an earlier session, which go-ads deletes itself",

	// Values dropped because this input reads them too slowly. No Kind
	// exists yet for that.
	"recvQueue full, dropping inbound packet (PLC overrun or slow handler)": "data dropped under load; no Kind yet",
	"notification dropped (channel full, receiver too slow)":                "data dropped under load; no Kind yet",
	"early notification sample dropped (buffer full)":                       "data dropped under load; no Kind yet",
	"getJSON marshal error": "a struct value that JSON cannot encode, such as a NaN float; no Kind yet",

	// go-ads works around the cause and goes on.
	"send aborted due to shutdown": "logged only while this input closes",
	"PLC returned a heartbeat handle that is already in use by a subscription; not using it":       "go-ads works around the PLC",
	"InContext mode not available for symbol (ContextMask=0), falling back":                        "go-ads falls back to the plain transmission mode and delivers",
	"notifications swept mid-batch; committed entries reported as stranded":                        "the stranded entries carry ErrNotificationStrandedByReload, which setupNotifications logs as internal_bug",
	"TCP and UDP source IPs differ — possible NAT/Docker/VPN":                                      "a hint for hostIP; the connect may still succeed",
	"could not bind the inbound AMS port for a device known to need it; requests may all time out": "a local port conflict; no Kind yet",
	"route probe was silent, but the PLC answers on a connection it opens to us; continuing":       "go-ads recovered the connection",
	"PLC answers on a connection it opens to us, not on ours; using that connection":               "go-ads recovered the connection",
	"stale-cache detection":                                           "go-ads reloads the symbols itself; a failed reload is logged on its own",
	"reload cap exhausted - degrading to Ignore":                      "go-ads reports ReasonReloadCapExhausted as a session event, which onSessionEvent logs",
	"Close proceeded without waiting out the in-flight reconnect":     "go-ads lifecycle detail; the reconnect exits by itself",
	"taking over a reconnect state left behind by an earlier attempt": "go-ads lifecycle detail",

	// Code this input does not reach.
	"error during send request for write":                                                           "this input never writes",
	"SumWrite not supported by PLC, using individual writes":                                        "this input never writes",
	"individual write failed in SumWrite fallback":                                                  "this input never writes",
	"error getting symbol for batch write":                                                          "this input never writes",
	"error serializing symbol for batch write":                                                      "this input never writes",
	"no batch write result for symbol":                                                              "this input never writes",
	"inferring base type from size for write (no datatype table loaded; LoadSymbols() recommended)": "this input never writes",
	"GetJSON marshal error":                                                                         "this input does not call SymbolView.GetJSON",
	"WithLocalBindIP: invalid IP, ignoring (using OS-default routing)":                              "this input does not set the option",
	"invalid SymbolVersionStrategy, using AutoReload":                                               "this input does not set the option",
	"WithMaxSymbolVersionReloadAttempts: n<1 rejected, keeping current value":                       "this input does not set the option",
	"WithSymbolVersionReloadWindow: d<=0 rejected, keeping current value":                           "this input does not set the option",
	"heartbeat interval exceeds what ADS can express; clamping":                                     "this input does not set WithNotificationHeartbeat",
}
