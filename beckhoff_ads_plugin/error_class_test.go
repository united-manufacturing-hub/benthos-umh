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
	slog.New(&benthosLogHandler{logger: classifier.Logger(log)}).Log(context.Background(), level, msg, args...)
	Expect(*recs).To(HaveLen(1))
	return (*recs)[0]
}

func haveID(id errclass.ID) OmegaMatcher {
	return HaveField("Attrs", SatisfyAll(
		HaveKeyWithValue("error_id", id.String()),
		HaveKeyWithValue("error_class", string(id.Class())),
	))
}

var _ = Describe("classifier", func() {
	DescribeTable("maps an error onto the contract by type",
		func(err error, want errclass.ID) {
			Expect(classifier.Classify(err, idUnclassified)).To(Equal(want))
		},
		Entry("no error", nil, idUnclassified),
		Entry("every notification symbol failed", fmt.Errorf("%w (2 symbols all failed to resolve)", errNoSymbolsRegistered), idSymbolConfig),
		Entry("hostAMS does not parse", fmt.Errorf("%w (%q): %w", errInvalidHostAMS, "1.2", errors.New("bad")), idInvalidHostAMS),
		Entry("targetAMS does not parse", fmt.Errorf("%w (%q): %w", errInvalidTargetAMS, "1.2", errors.New("bad")), idInvalidTargetAMS),
		Entry("go-ads rejected the session options", fmt.Errorf("%w: %w", errSessionCreate, errors.New("remote.IP must be set")), idSessionCreateFailed),
		Entry("router answered nothing", fmt.Errorf("connect: %w", adsLib.ErrRouterUnresponsive), idTargetUnreachable),
		Entry("TCP dial failed", fmt.Errorf("connect: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}), idTargetUnreachable),
		Entry("UDP exchange timed out", fmt.Errorf("identify: %w", udpTimeout), idTargetUnreachable),
		Entry("TCP socket timed out on a live session", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}, idReadTimeout),
		Entry("subscribe stranded by a cache reload", fmt.Errorf("symbol %q: %w", "MAIN.a", adsLib.ErrNotificationStrandedByReload), idInternalBug),
		Entry("route not served, though the transport closed too", fmt.Errorf("%w: %w", adsLib.ErrRouteNotServed, adsLib.ErrTransportClosed), idRouteFault),
		Entry("runtime not in RUN", fmt.Errorf("route is served but %w (ADS state 15)", adsLib.ErrRuntimeNotRunning), idPLCNotRunning),
		Entry("router rejected the port", adsLib.AMSError{Code: adsLib.ReturnCodeGlobalTargetPortNotFound}, idTargetPortNotFound),
		Entry("device rejected the port", fmt.Errorf("read: %w", adsLib.ReturnCodeGlobalTargetPortNotFound), idTargetPortNotFound),
		Entry("symbol not found", fmt.Errorf("GetSymbolInfoByName failed: %w", adsLib.ReturnCodeDeviceSymbolNoFound), idSymbolConfig),
		Entry("batch symbol unresolved", fmt.Errorf("item: %w", adsLib.ErrBatchSymbolUnresolved), idSymbolConfig),
		Entry("transport closed", fmt.Errorf("read: %w", adsLib.ErrTransportClosed), idConnectionDropped),
		Entry("established connection dropped", adsLib.ErrEstablishedDropped, idConnectionDropped),
		Entry("session disconnected", adsLib.ErrDisconnected, idConnectionDropped),
		Entry("request deadline", fmt.Errorf("read: %w", context.DeadlineExceeded), idReadTimeout),
		Entry("a socket read that did not time out", &net.OpError{Op: "read", Net: "tcp", Err: io.EOF}, idUnclassified),
		Entry("anything else", errors.New("boom"), idUnclassified),
	)

	It("falls back to the call site's class only when no type matches", func() {
		Expect(classifier.Classify(nil, idSymbolConfig)).To(Equal(idSymbolConfig))
		Expect(classifier.Classify(errors.New("boom"), idSymbolConfig)).To(Equal(idSymbolConfig))
		Expect(classifier.Classify(adsLib.ErrTransportClosed, idSymbolConfig)).To(Equal(idConnectionDropped))
	})

	It("treats a zero fallback, such as a missed table lookup, as unclassified", func() {
		Expect(classifier.Classify(nil, reasonIDs["no-such-reason"])).To(Equal(idUnclassified))
	})

	DescribeTable("matches the route faults go-ads returns untyped by their text, and only those",
		func(err error, want errclass.ID) {
			Expect(classifier.Classify(err, idUnclassified)).To(Equal(want))
		},
		Entry("PLC refused the registration", errors.New("route registration failed during connect: route registration failed with error code: 1796"), idRouteFault),
		Entry("route registered but never served, though the probe saw a closed transport",
			fmt.Errorf("route registration during connect: %w", fmt.Errorf(`route "benthosADS-1.2.3.4" was registered but the PLC did not serve it within 10s (3 redials): %w`, adsLib.ErrTransportClosed)),
			idRouteFault),
		Entry("a deaf router is not a route fault", fmt.Errorf("route registration failed during connect: %w", adsLib.ErrRouterUnresponsive), idTargetUnreachable),
		Entry("a redial after registration that was refused",
			fmt.Errorf("TCP reconnect after route registration failed: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}),
			idTargetUnreachable),
		Entry("a registration whose probe connection dropped", fmt.Errorf("route registration failed during connect: %w", adsLib.ErrTransportClosed), idConnectionDropped),
		Entry("a registration wrapper around nothing typed", errors.New("route registration failed during connect: boom"), idUnclassified),
		Entry("typed errors still classify", fmt.Errorf("connect: %w", adsLib.ErrRuntimeNotRunning), idPLCNotRunning),
	)

	DescribeTable("reasonIDs maps go-ads session-event reasons",
		func(reason adsLib.Reason, want errclass.ID) {
			Expect(classifier.Classify(nil, reasonIDs[string(reason)])).To(Equal(want))
		},
		Entry("symbol not found", adsLib.ReasonSymbolNotFound, idSymbolConfig),
		Entry("symbol not active", adsLib.ReasonSymbolNotActive, idSymbolConfig),
		Entry("heartbeat silent", adsLib.ReasonHeartbeatSilent, idHeartbeatObserveOnly),
		Entry("an unmapped reason", adsLib.ReasonReloadCapExhausted, idUnclassified),
	)
})

var _ = Describe("benthosLogHandler classification", func() {
	for msg, want := range goADSMessages {
		It("tags go-ads message "+strconv.Quote(msg), func() {
			// An error that would classify differently must not win over the message.
			rec := goADSRecord(slog.LevelError, msg, "error", adsLib.ErrRouterUnresponsive)
			Expect(rec).To(haveID(want))
		})
	}

	for msg, want := range goADSMessageFallbacks {
		It("tags "+strconv.Quote(msg)+" by its error first, then by its message", func() {
			Expect(goADSRecord(slog.LevelError, msg, "error", adsLib.ErrRuntimeNotRunning)).To(haveID(idPLCNotRunning))
			Expect(goADSRecord(slog.LevelError, msg, "error", errors.New("boom"))).To(haveID(want))
		})
	}

	It("classifies an unlisted record by its error value", func() {
		rec := goADSRecord(slog.LevelWarn, "send request failed",
			"error", adsLib.AMSError{Code: adsLib.ReturnCodeGlobalTargetPortNotFound})
		Expect(rec).To(haveID(idTargetPortNotFound))
	})

	It("finds the error among the attributes a logger was built with", func() {
		log, recs := capturingLogger()
		slog.New(&benthosLogHandler{logger: classifier.Logger(log)}).With("err", adsLib.ErrTransportClosed).Warn("some go-ads warning")
		Expect((*recs)[0]).To(haveID(idConnectionDropped))
	})

	It("tags a record it cannot classify as unclassified", func() {
		Expect(goADSRecord(slog.LevelWarn, "something go-ads never said before", "count", 3)).
			To(haveID(idUnclassified))
	})

	It("leaves info and debug records untagged", func() {
		for _, level := range []slog.Level{slog.LevelInfo, slog.LevelDebug} {
			rec := goADSRecord(level, "session disconnected, reconnecting; no data is read until it succeeds")
			Expect(rec.Attrs).NotTo(HaveKey("error_id"))
			Expect(rec.Attrs).NotTo(HaveKey("error_class"))
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

		var missing []string
		for _, table := range []map[string]errclass.ID{goADSMessages, goADSMessageFallbacks} {
			for msg := range table {
				if !strings.Contains(src.String(), strconv.Quote(msg)) {
					missing = append(missing, msg)
				}
			}
		}
		Expect(missing).To(BeEmpty(), "messages not found as string literals in go-ads %s", mod.Version)
	})
})

var _ = Describe("plugin log lines carry their class", func() {
	It("tags a symbol go-ads skipped in a batch read as a read gap", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classifier.Logger(log)}

		a.logBatchFailures(&BatchReadError{Requested: 3, Failed: []NotifyResult{
			{SymbolName: "MAIN.gap", Skipped: adsLib.ErrBatchNoResult},
			{SymbolName: "MAIN.typo", Code: 0x710},
		}})

		Expect(*recs).To(ConsistOf(
			SatisfyAll(HaveField("Msg", ContainSubstring("MAIN.gap")), haveID(idReadGap)),
			SatisfyAll(HaveField("Msg", ContainSubstring("MAIN.typo")), haveID(idSymbolConfig)),
		))
	})

	It("tags a batch-read symbol go-ads could not resolve as symbol configuration, not a read gap", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classifier.Logger(log)}

		a.logBatchFailures(&BatchReadError{Requested: 2, Failed: []NotifyResult{{
			SymbolName: "MAIN.typo",
			Skipped:    fmt.Errorf("%w: %w", adsLib.ErrBatchSymbolUnresolved, adsLib.ReturnCodeDeviceSymbolNoFound),
		}}})

		Expect(*recs).To(ConsistOf(haveID(idSymbolConfig)))
	})

	It("tags a notification stranded by a cache reload as a bug, with the message unchanged", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{
			Log:              classifier.Logger(log),
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
			haveID(idInternalBug),
		)))
	})

	DescribeTable("logReadFailure",
		func(closed bool, err error, want errclass.ID) {
			log, recs := capturingLogger()
			a := &AdsCommInput{Log: classifier.Logger(log), client: &fakeClient{closed: closed}}

			a.logReadFailure(context.Background(), 2, err)

			Expect(*recs).To(ConsistOf(haveID(want)))
		},
		Entry("PLC did not answer in time", false, fmt.Errorf("read: %w", context.DeadlineExceeded), idReadTimeout),
		Entry("connection dropped", false, adsLib.ErrTransportClosed, idConnectionDropped),
		Entry("session gone", true, errors.New("closed"), idConnectionDropped),
		Entry("anything else", false, errors.New("boom"), idUnclassified),
	)

	DescribeTable("logConnectFailure",
		func(err error, want errclass.ID) {
			log, recs := capturingLogger()
			a := &AdsCommInput{Log: classifier.Logger(log), TargetIP: "1.2.3.4", TargetPort: 48898, RuntimePort: 851}

			Expect(a.logConnectFailure(context.Background(), "Connecting to PLC failed", err)).To(MatchError(err))

			Expect(*recs).To(ConsistOf(haveID(want)))
		},
		Entry("route not served", fmt.Errorf("connect: %w", adsLib.ErrRouteNotServed), idRouteFault),
		Entry("route registration refused", errors.New("route registration failed during connect: route registration failed with error code: 1796"), idRouteFault),
		Entry("transport dropped", adsLib.ErrTransportClosed, idConnectionDropped),
		Entry("dial failed", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, idTargetUnreachable),
		Entry("unrecognized", errors.New("ADS error 0x706"), idUnclassified),
		Entry("a dropped transport around a stopped runtime", fmt.Errorf("%w: %w", adsLib.ErrTransportClosed, adsLib.ErrRuntimeNotRunning), idPLCNotRunning),
	)

	It("gives the plugin line and the returned error the same error_id", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classifier.Logger(log), TargetIP: "1.2.3.4", TargetPort: 48898, RuntimePort: 851}
		err := fmt.Errorf("%w: %w", adsLib.ErrTransportClosed, adsLib.ErrRuntimeNotRunning)

		returned := classifier.WithPrefix(a.logConnectFailure(context.Background(), "Connecting to PLC failed", err))

		Expect(*recs).To(ConsistOf(haveID(idPLCNotRunning)))
		Expect(returned.Error()).To(HavePrefix("[error_id=ads::plc_not_running error_class=known] "))
	})

	It("tags notification symbols the PLC did not register as symbol configuration", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{
			Log:              classifier.Logger(log),
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
				Expect(r).To(haveID(idSymbolConfig))
			}
		}
		Expect(warns).To(Equal(2))
	})

	It("fails the connect as symbol configuration when no notification symbol registered", func() {
		a := &AdsCommInput{
			Log:     classifier.Logger(testLog()),
			Symbols: []PlcSymbol{{Name: "MAIN.typo"}},
			client:  &fakeClient{notifyResults: []NotifyResult{{SymbolName: "MAIN.typo", Skipped: fmt.Errorf("resolve symbol %q: %w", "MAIN.typo", adsLib.ReturnCodeDeviceSymbolNoFound)}}},
		}

		err := classifier.WithPrefix(a.setupNotifications(context.Background()))

		Expect(err).To(MatchError(errNoSymbolsRegistered))
		Expect(err.Error()).To(HavePrefix("[error_id=ads::symbol_config error_class=known] "))
	})

	It("tags a symbol whose metadata did not resolve, by its error when it has a type", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classifier.Logger(log), Symbols: []PlcSymbol{{Name: "MAIN.typo"}}, client: &fakeClient{}}

		a.initSymbolIndex(context.Background())

		Expect(*recs).To(ConsistOf(haveID(idSymbolConfig)))
	})

	It("tags the rebuild line with the class of the go-ads reason", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classifier.Logger(log)}

		a.onSessionEvent(SessionEventSubscriptionsDead, string(adsLib.ReasonHeartbeatSilent))

		Expect(*recs).To(ConsistOf(SatisfyAll(
			HaveField("Level", slog.LevelError),
			haveID(idHeartbeatObserveOnly),
		)))
	})

	It("tags an unhandled session event as an internal bug", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classifier.Logger(log)}

		a.onSessionEvent(SessionEvent(99), "new-reason")

		Expect(*recs).To(ConsistOf(haveID(idInternalBug)))
	})

	It("tags a failed close", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classifier.Logger(log), client: &fakeClient{closeErr: errors.New("boom")}}

		Expect(a.Close(context.Background())).To(HaveOccurred())

		Expect(*recs).To(ContainElement(SatisfyAll(HaveField("Level", slog.LevelError), haveID(idCloseFailed))))
	})

	It("prefixes a failed Connect with its class for the Benthos core line", func() {
		// Port 1 on loopback refuses at once, so this needs no PLC.
		a := &AdsCommInput{
			Log: classifier.Logger(testLog()), TargetIP: "127.0.0.1", TargetPort: 1, TargetAMS: "127.0.0.1.1.1",
			RuntimePort: 851, HostAMS: "auto", HostIP: "auto", RequestTimeout: time.Second,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		err := a.Connect(ctx)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(HavePrefix("[error_id=ads::target_unreachable error_class=known] "))
		Expect(isRouteFault(err)).To(BeFalse(), "the connect gate still sees the wrapped error")
	})

	It("prefixes a Connect error only once", func() {
		err := classifier.WithPrefix(classifier.WithPrefix(adsLib.ErrTransportClosed))
		Expect(strings.Count(err.Error(), "[error_id=")).To(Equal(1))
		Expect(err).To(MatchError(adsLib.ErrTransportClosed))
	})
})

// The eight message forms of Sentry issue UMH-CORE-45M, rebuilt from the raw
// lines. None is a fault only UMH can fix, so none may be bug or unknown.
var _ = Describe("UMH-CORE-45M message forms", func() {
	notReported := HaveField("Attrs", HaveKeyWithValue("error_class", BeElementOf(string(errclass.Known), string(errclass.Operational))))

	It("discovery timeout: Failed to connect to ads (identify got no answer)", func() {
		err := classifier.WithPrefix(fmt.Errorf("creating the ADS session failed: %w",
			fmt.Errorf("ads: NewSession: target AMS address incomplete and discovery failed "+
				"(set remote.AMS explicitly if the device does not answer the identify service): %w",
				fmt.Errorf("identify 10.13.37.52: no answer after 3 attempts: %w", udpTimeout))))
		Expect(err.Error()).To(HavePrefix("[error_id=ads::target_unreachable error_class=known] "))
	})

	It("heartbeat", func() {
		Expect(goADSRecord(slog.LevelError,
			"no notification heartbeat within the allowed window; treating this session's subscriptions as dead and re-subscribing",
			"cycle", 2*time.Second, "missedTicks", 5)).To(SatisfyAll(notReported, haveID(idHeartbeatResubscribe)))
	})

	It("connection dropped", func() {
		Expect(goADSRecord(slog.LevelError, "PLC dropped an established connection, transport down",
			"error", io.EOF, "framesPeer", 0)).To(SatisfyAll(notReported, haveID(idConnectionDropped)))
	})

	It("session disconnected", func() {
		Expect(goADSRecord(slog.LevelError, "session disconnected, reconnecting; no data is read until it succeeds",
			"flapCount", 2)).To(SatisfyAll(notReported, haveID(idSessionDisconnected)))
	})

	It("target port not found", func() {
		Expect(goADSRecord(slog.LevelError, "send request failed",
			"error", adsLib.AMSError{Code: adsLib.ReturnCodeGlobalTargetPortNotFound})).
			To(SatisfyAll(notReported, haveID(idTargetPortNotFound)))
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
			"error", err, "symbol", ".SCADA.PD.Hubzahl_Soll")).To(SatisfyAll(notReported, haveID(idSymbolConfig)))
	})

	It("route registration", func() {
		log, recs := capturingLogger()
		a := &AdsCommInput{Log: classifier.Logger(log), TargetIP: "192.168.40.25", TargetPort: 48898, RuntimePort: 801}
		err := errors.New("route registration failed during connect: route registration failed with error code: 1796")

		returned := classifier.WithPrefix(a.logConnectFailure(context.Background(), "Connecting to PLC failed", err))

		Expect(*recs).To(ConsistOf(SatisfyAll(notReported, haveID(idRouteFault))))
		Expect(returned.Error()).To(HavePrefix("[error_id=ads::route_fault error_class=known] "))
	})
})

func testLog() *service.Logger {
	log, _ := capturingLogger()
	return log
}
