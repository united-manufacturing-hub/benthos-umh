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
	"fmt"
	"strings"
	"time"
)

// initSymbolIndex builds the lower-cased symbol lookup and resolves each
// symbol's DataType/BaseType/Size via the PLC symbol table.
func (a *AdsCommInput) initSymbolIndex(ctx context.Context) {
	// Index first: it needs no PLC round-trip, so a half-built one would be a
	// pointless second failure mode when the lookups below are cut short.
	a.symbolByName = make(map[string]*PlcSymbol, len(a.Symbols))
	for i := range a.Symbols {
		a.symbolByName[strings.ToLower(a.Symbols[i].Name)] = &a.Symbols[i]
	}
	for i := range a.Symbols {
		sym := &a.Symbols[i]
		// Stop rather than continue: on shutdown every remaining lookup would
		// fail the same way, one log line each.
		if shuttingDown(ctx) {
			a.Log.Debugf("Symbol metadata resolution abandoned during shutdown at %q", sym.Name)
			return
		}
		info, err := a.client.GetSymbol(ctx, sym.Name)
		if err != nil {
			a.Log.Warnf("Failed to resolve metadata for ADS symbol %q: %v", sym.Name, err)
			continue
		}
		sym.DataType = info.DataType
		sym.BaseType = info.BaseType
		sym.Size = info.Length
	}
}

// loadSymbolTable downloads the full symbol and datatype table from the PLC;
// required for struct/array symbols.
func (a *AdsCommInput) loadSymbolTable(ctx context.Context) error {
	a.Log.Debugf("Loading symbol and datatype table from PLC")
	// finishConnect logs the failure; it knows whether a retry is coming.
	if err := a.client.LoadSymbols(ctx); err != nil {
		return err
	}
	a.Log.Debugf("Loading symbol and datatype table from PLC succeeded")
	return nil
}

// setupNotifications registers batch notifications and waits for an initial
// sample from each registered symbol.
func (a *AdsCommInput) setupNotifications(ctx context.Context) error {
	cfgs := make([]NotifyConfig, len(a.Symbols))
	for i, sym := range a.Symbols {
		cfgs[i] = NotifyConfig{
			SymbolName:       sym.Name,
			MaxDelay:         sym.MaxDelay,
			CycleTime:        sym.CycleTime,
			TransmissionMode: a.TransmissionMode,
		}
	}

	// Connect() already ensures session is stable; no retry needed here.
	// If this fails, return error and let Benthos retry Connect().
	a.Log.Debugf("Registering notifications for %d symbols", len(cfgs))
	results, err := a.client.AddNotifications(ctx, cfgs, a.NotificationChan)
	if err != nil {
		return fmt.Errorf("registering notifications for %d symbols: %w", len(cfgs), err)
	}

	// AddNotifications returns nil errors even when all symbols fail to
	// resolve (e.g. PLC not yet ready); detect that here so Benthos retries Connect().
	registered := 0
	var failed []NotifyResult
	for _, r := range results {
		if r.Registered {
			registered++
			continue
		}
		failed = append(failed, r)
	}
	if registered == 0 && len(cfgs) > 0 {
		return fmt.Errorf("no symbols registered for notifications (%d symbols all failed to resolve)", len(cfgs))
	}
	for _, r := range failed {
		if r.Skipped {
			a.Log.Warnf("Notification symbol %q skipped (check symbol name)", r.SymbolName)
			continue
		}
		a.Log.Warnf("Notification symbol %q rejected by PLC: ADS error 0x%X", r.SymbolName, r.Code)
	}
	a.Log.Infof("Registering notifications succeeded for %d/%d symbols", registered, len(cfgs))

	return a.waitForInitialSamples(ctx, results)
}

// waitForInitialSamples buffers the initial sample from each registered symbol
// into pendingInitial (readiness gate); the first ReadBatch flushes them.
// initialSampleTimeout bounds the wait for first samples. The PLC may hold a
// notification for up to cycleTime + maxDelay, so the wait has to exceed the
// slowest configured symbol or it would expire before delivery is even due.
func (a *AdsCommInput) initialSampleTimeout() time.Duration {
	timeout := initialSampleWait
	for _, sym := range a.Symbols {
		if d := sym.CycleTime + sym.MaxDelay + initialSampleWait; d > timeout {
			timeout = d
		}
	}
	return timeout
}

func (a *AdsCommInput) waitForInitialSamples(ctx context.Context, results []NotifyResult) error {
	a.pendingInitial = nil
	needed := make(map[string]bool, len(results))
	for _, r := range results {
		if r.Registered {
			needed[strings.ToLower(r.SymbolName)] = true
		}
	}
	initialCtx, initialCancel := context.WithTimeout(context.Background(), a.initialSampleTimeout())
	defer initialCancel()
	for len(needed) > 0 {
		select {
		case update := <-a.NotificationChan:
			if update != nil {
				a.pendingInitial = append(a.pendingInitial, update)
				delete(needed, strings.ToLower(sampleName(update)))
			}
		case <-initialCtx.Done():
			// select picks at random when ctx.Done() is ready too.
			if shuttingDown(ctx) {
				return ctx.Err()
			}
			a.Log.Warnf("Timed out waiting for initial samples; %d symbols not yet received: %v", len(needed), needed)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
