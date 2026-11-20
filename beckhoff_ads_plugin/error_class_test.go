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
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
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

// goADSRecord sends one record through the go-ads log bridge, as go-ads would.
func goADSRecord(level slog.Level, msg string, args ...any) capturedLog {
	log, recs := capturingLogger()
	classifier.Slog(log).Log(context.Background(), level, msg, args...)
	Expect(*recs).To(HaveLen(1))
	return (*recs)[0]
}

// haveKind checks the error_kind. The errclass tests check that a line carries
// the class its Kind was defined with.
func haveKind(kind *errclass.Kind) OmegaMatcher {
	return HaveField("Attrs", SatisfyAll(
		HaveKeyWithValue("error_kind", kind.String()),
		HaveKey("error_owner"),
	))
}

var _ = Describe("classifier", func() {
	It("classifies no error as ads::unclassified", func() {
		Expect(kindUnclassified.String()).To(Equal("ads::unclassified"))
	})

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

	It("finds the Message rules in the classifier", func() {
		Expect(decisive).To(HaveLen(32))
		Expect(fallbacks).To(ConsistOf("reconnect dial/start failed, retrying", "reconnect step failed, retrying"))
	})

	for _, msg := range decisive {
		It("tags go-ads message "+strconv.Quote(msg)+" by its message, whatever its error", func() {
			alone := goADSRecord(slog.LevelError, msg)
			Expect(alone.Attrs).NotTo(HaveKeyWithValue("error_kind", kindUnclassified.String()))
			// An error that would classify differently must not win over the message.
			withErr := goADSRecord(slog.LevelError, msg, "error", adsLib.ErrRouterUnresponsive)
			Expect(withErr.Attrs["error_kind"]).To(Equal(alone.Attrs["error_kind"]))
		})
	}

	for _, msg := range fallbacks {
		It("tags "+strconv.Quote(msg)+" by its error first, then by its message", func() {
			Expect(goADSRecord(slog.LevelError, msg, "error", adsLib.ErrRuntimeNotRunning)).To(haveKind(kindPLCNotRunning))
			Expect(goADSRecord(slog.LevelError, msg, "error", errors.New("boom"))).To(haveKind(kindReconnectFailing))
		})
	}

	It("classifies an unlisted record by its error value", func() {
		rec := goADSRecord(slog.LevelWarn, "send request failed",
			"error", adsLib.AMSError{Code: adsLib.ReturnCodeGlobalTargetPortNotFound})
		Expect(rec).To(haveKind(kindTargetPortNotFound))
	})

	It("finds the error among the attributes a logger was built with", func() {
		log, recs := capturingLogger()
		classifier.Slog(log).With("err", adsLib.ErrTransportClosed).Warn("some go-ads warning")
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
		out, err := exec.Command("go", "list", "-m", "-json", "github.com/RuneRoven/go-ads/v2").Output()
		Expect(err).NotTo(HaveOccurred())
		var mod struct{ Dir, Version string }
		Expect(json.Unmarshal(out, &mod)).To(Succeed())
		Expect(mod.Dir).NotTo(BeEmpty(), "go-ads %s is not in the module cache; run go mod download", mod.Version)

		files, err := filepath.Glob(filepath.Join(mod.Dir, "*.go"))
		Expect(err).NotTo(HaveOccurred())
		var src strings.Builder
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			b, err := os.ReadFile(f)
			Expect(err).NotTo(HaveOccurred())
			src.Write(b)
		}

		decisive, fallbacks := messageRules()
		var missing []string
		for _, msg := range append(decisive, fallbacks...) {
			if !strings.Contains(src.String(), strconv.Quote(msg)) {
				missing = append(missing, msg)
			}
		}
		Expect(missing).To(BeEmpty(), "messages not found as string literals in go-ads %s", mod.Version)

		// The untyped route faults are matched by a fragment of a format string,
		// not by a whole literal.
		for _, text := range untypedRouteFaults {
			Expect(src.String()).To(ContainSubstring(text), "route fault text not found in go-ads %s", mod.Version)
		}
	})
})

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

	It("tags a notification stranded by a cache reload as a bug, with the message unchanged", func() {
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
			HaveField("Msg", `Notification symbol "MAIN.a" skipped (check symbol name)`),
			haveKind(kindInternalBug),
		)))
	})

	DescribeTable("logReadFailure",
		func(closed bool, err error, want *errclass.Kind) {
			log, recs := capturingLogger()
			a := &AdsCommInput{Log: classified(log), client: &fakeClient{closed: closed}}

			a.logReadFailure(context.Background(), 2, err)

			Expect(*recs).To(ConsistOf(haveKind(want)))
		},
		Entry("PLC did not answer in time", false, fmt.Errorf("read: %w", context.DeadlineExceeded), kindReadTimeout),
		Entry("connection dropped", false, adsLib.ErrTransportClosed, kindConnectionDropped),
		Entry("session gone", true, errors.New("closed"), kindConnectionDropped),
		Entry("anything else", false, errors.New("boom"), kindUnclassified),
	)

	DescribeTable("logConnectFailure",
		func(err error, want *errclass.Kind) {
			log, recs := capturingLogger()
			a := &AdsCommInput{Log: classified(log), TargetIP: "1.2.3.4", TargetPort: 48898, RuntimePort: 851}

			Expect(a.logConnectFailure(context.Background(), "Connecting to PLC failed", err)).To(MatchError(err))

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

		returned := classifier.EnrichForLog(a.logConnectFailure(context.Background(), "Connecting to PLC failed", err))

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

	It("fails the connect as symbol configuration when no notification symbol registered", func() {
		a := &AdsCommInput{
			Log:     classified(testLog()),
			Symbols: []PlcSymbol{{Name: "MAIN.typo"}},
			client:  &fakeClient{notifyResults: []NotifyResult{{SymbolName: "MAIN.typo", Skipped: fmt.Errorf("resolve symbol %q: %w", "MAIN.typo", adsLib.ReturnCodeDeviceSymbolNoFound)}}},
		}

		err := classifier.EnrichForLog(a.setupNotifications(context.Background()))

		Expect(classifier.Classify(err)).To(Equal(kindSymbolConfig))
		Expect(err.Error()).To(HavePrefix("[error_kind=ads::symbol_config error_owner=user] "))
	})

	It("tags a symbol whose metadata did not resolve by its error", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classified(log), Symbols: []PlcSymbol{{Name: "MAIN.typo"}}, client: &fakeClient{}}

		a.initSymbolIndex(context.Background())

		Expect(*recs).To(ConsistOf(haveKind(kindSymbolConfig)))
	})

	It("tags the rebuild line with the class of the go-ads reason", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classified(log)}

		a.onSessionEvent(SessionEventSubscriptionsDead, string(adsLib.ReasonHeartbeatSilent))

		Expect(*recs).To(ConsistOf(SatisfyAll(
			HaveField("Level", slog.LevelError),
			haveKind(kindHeartbeatObserveOnly),
		)))
	})

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
	notReported := HaveField("Attrs", HaveKeyWithValue("error_owner", string(errclass.User)))

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

		returned := classifier.EnrichForLog(a.logConnectFailure(context.Background(), "Connecting to PLC failed", err))

		Expect(*recs).To(ConsistOf(SatisfyAll(notReported, haveKind(kindRouteFault))))
		Expect(returned.Error()).To(HavePrefix("[error_kind=ads::route_fault error_owner=user] "))
	})
})

func testLog() *service.Logger {
	log, _ := capturingLogger()
	return log
}

// messageRules returns the texts of the errclass.Message rules in classifier,
// read from error_class.go: those listed before the first error rule, which
// decide on their own, and those after the error rules, which only decide when
// no error rule matched.
func messageRules() ([]string, []string) {
	var decisive, fallbacks []string
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "error_class.go", nil, 0)
	Expect(err).NotTo(HaveOccurred())

	var call *ast.CallExpr
	ast.Inspect(file, func(n ast.Node) bool {
		spec, ok := n.(*ast.ValueSpec)
		if ok && len(spec.Names) == 1 && spec.Names[0].Name == "classifier" {
			call = spec.Values[0].(*ast.CallExpr)
		}
		return call == nil
	})
	Expect(call).NotTo(BeNil(), "var classifier not found in error_class.go")

	afterErrorRules := false
	for _, arg := range call.Args {
		rule := arg.(*ast.CallExpr)
		switch rule.Fun.(*ast.SelectorExpr).Sel.Name {
		case "Message":
			msg, err := strconv.Unquote(rule.Args[0].(*ast.BasicLit).Value)
			Expect(err).NotTo(HaveOccurred())
			if afterErrorRules {
				fallbacks = append(fallbacks, msg)
			} else {
				decisive = append(decisive, msg)
			}
		default:
			afterErrorRules = true
		}
	}
	return decisive, fallbacks
}
