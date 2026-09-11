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
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

var (
	// TC2 addresses GVL globals as ".varName"; the dot is a namespace marker,
	// not part of the name.
	symbolGlobalPrefix = regexp.MustCompile(`^\.`)
	// An array index is structure, so it keeps its own separator instead of
	// becoming the two underscores that "[0]." would otherwise produce.
	symbolArrayIndex = regexp.MustCompile(`\[(-?\d+)\]`)
	symbolInvalid    = regexp.MustCompile(`[^a-zA-Z0-9_-]`)
	symbolUnderscore = regexp.MustCompile(`_{2,}`)
)

// sanitize turns a PLC symbol name into a UMH topic segment: dots separate
// topic levels, so nothing but [a-zA-Z0-9_-] can survive here.
func sanitize(s string) string {
	out := symbolGlobalPrefix.ReplaceAllString(s, "")
	out = symbolArrayIndex.ReplaceAllString(out, "_$1")
	out = symbolInvalid.ReplaceAllString(out, "_")
	out = symbolUnderscore.ReplaceAllString(out, "_")
	// Only the tail: a leading underscore may be the PLC's own naming.
	if trimmed := strings.TrimRight(out, "_"); trimmed != "" {
		return trimmed
	}
	// Nothing legal survived; an empty segment would make the topic invalid.
	return "_"
}

// closeHandler async-closes a dead client session; nil-safe and non-blocking
// so callers can invoke it right after detecting IsClosed().
func (a *AdsCommInput) closeHandler() {
	if a.client != nil {
		c := a.client
		a.client = nil
		go func() { _ = c.Close() }()
	}
}

// newSymbolMessage is the single message-build site: payload is typed via parse.go
// (base type preferred, data type fallback); metadata carries symbol/type/size/timestamp.
// logReadFailure logs a failed poll once, at the severity that matches what has
// to happen next. Mirrors logConnectFailure: a PLC that did not answer in time
// is retried on the next poll, anything unrecognized keeps ERROR.
func (a *AdsCommInput) logReadFailure(ctx context.Context, symbols int, err error) {
	log := a.Log.With("symbols", symbols, "requestTimeout", a.RequestTimeout.String())
	switch {
	case shuttingDown(ctx):
		a.Log.Debugf("Read aborted during shutdown: %v", err)
	case a.client != nil && a.client.IsClosed():
		log.Warnf("Read failed because the session is gone; reconnecting: %v", err)
	case errors.Is(err, context.DeadlineExceeded):
		// The PLC was reachable but did not reply within requestTimeout, so the
		// poll is lost and the next one is due immediately.
		log.Warnf("The PLC did not answer this read within requestTimeout; retrying on the next poll: %v", err)
	case isTransportGone(err):
		log.Warnf("Read failed because the connection dropped; retrying on the next poll: %v", err)
	default:
		log.Errorf("Read failed: %v", err)
	}
}

func (a *AdsCommInput) newSymbolMessage(sym *PlcSymbol, value string, ts time.Time) *service.Message {
	typeName := sym.BaseType
	if typeName == "" {
		typeName = sym.DataType
	}
	payload, tagType := adsValueBytes(typeName, value)
	msg := service.NewMessage(payload)
	// ads_ prefix per repo convention (modbus_tag_*, opcua_tag_*);
	// timestamp_ms stays unprefixed as the UMH-wide payload timestamp.
	msg.MetaSet("ads_tag_type", tagType)
	msg.MetaSet("ads_symbol_name", sanitize(sym.Name))
	msg.MetaSet("ads_symbol_name_original", sym.Name)
	if sym.DataType != "" {
		msg.MetaSet("ads_datatype", sym.DataType)
	}
	if sym.BaseType != "" {
		msg.MetaSet("ads_base_type", sym.BaseType)
	}
	if sym.Size != 0 {
		msg.MetaSet("ads_data_size", strconv.FormatUint(uint64(sym.Size), 10))
	}
	if !ts.IsZero() {
		msg.MetaSet("timestamp_ms", strconv.FormatInt(ts.UnixMilli(), 10))
	}
	return msg
}

// logBatchFailures names each failed symbol once per session; a misspelled name
// is permanent, so warning per poll would bury every other line.
func (a *AdsCommInput) logBatchFailures(e *BatchReadError) {
	if a.warnedBatchFailures == nil {
		a.warnedBatchFailures = make(map[string]bool, len(e.Failed))
	}
	for _, f := range e.Failed {
		if a.warnedBatchFailures[f.SymbolName] {
			continue
		}
		a.warnedBatchFailures[f.SymbolName] = true
		if f.Skipped {
			a.Log.Warnf("Batch read produced no value for symbol %q; the other %d symbols are unaffected",
				f.SymbolName, e.Requested-len(e.Failed))
			continue
		}
		a.Log.Warnf("PLC refused symbol %q with ADS error 0x%X (check the symbol name); the other %d symbols are unaffected",
			f.SymbolName, f.Code, e.Requested-len(e.Failed))
	}
}

// makeNotificationMessage resolves the enriched symbol for a notification
// update, falling back to a bare symbol (name only) if it isn't tracked.
func (a *AdsCommInput) makeNotificationMessage(u *Update) *service.Message {
	sym, ok := a.symbolByName[strings.ToLower(sampleName(u))]
	if !ok || sym == nil {
		sym = &PlcSymbol{Name: sampleName(u)}
	}
	return a.newSymbolMessage(sym, sampleValue(u), sampleTime(u))
}

func (a *AdsCommInput) ReadBatchNotification(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	a.Log.Debugf("ReadBatchNotification called")

	// Flush any initial samples captured during Connect before blocking on the channel,
	// so the first values (e.g. static serverOnChange symbols) are delivered.
	if len(a.pendingInitial) > 0 {
		msgs := make(service.MessageBatch, 0, len(a.pendingInitial))
		for _, u := range a.pendingInitial {
			if u != nil {
				msgs = append(msgs, a.makeNotificationMessage(u))
			}
		}
		a.pendingInitial = nil
		return msgs, func(_ context.Context, _ error) error { return nil }, nil
	}

	// Use a short-lived context so ReadBatch returns periodically even when no
	// notifications arrive (e.g. slow-changing symbols). Caller loops immediately.
	waitCtx, cancel := context.WithTimeout(ctx, notificationWait)
	defer cancel()

	var first *Update
	select {
	case first = <-a.NotificationChan:
		if first == nil {
			a.Log.Warnf("Received nil update from ADS library, skipping")
			return nil, func(_ context.Context, _ error) error { return nil }, nil
		}
	case <-waitCtx.Done():
		if a.client != nil && a.client.IsClosed() {
			a.closeHandler()
			return nil, nil, service.ErrNotConnected
		}
		// No data within timeout — normal for slow-changing symbols or mid-reconnect.
		return nil, func(_ context.Context, _ error) error { return nil }, nil
	}

	msgs := service.MessageBatch{a.makeNotificationMessage(first)}

	// Drain buffered notifications, bounded to a channel-depth snapshot so a sustained
	// producer can't grow this batch unboundedly (go-ads drops when the channel is full).
	pending := len(a.NotificationChan)
	for i := 0; i < pending; i++ {
		select {
		case update := <-a.NotificationChan:
			if update != nil {
				msgs = append(msgs, a.makeNotificationMessage(update))
			}
		default:
			return msgs, func(_ context.Context, _ error) error { return nil }, nil
		}
	}
	return msgs, func(_ context.Context, _ error) error { return nil }, nil
}

func (a *AdsCommInput) ReadBatchPull(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	a.Log.Debugf("ReadBatchPull called")
	start := time.Now()
	if a.client == nil {
		return nil, nil, service.ErrNotConnected
	}

	names := make([]string, len(a.Symbols))
	for i, symbol := range a.Symbols {
		names[i] = symbol.Name
	}

	values, err := a.client.ReadMultipleSymbols(ctx, names)
	// A partial batch is not a failed poll: the map holds every symbol that
	// worked, so emit those and name the ones that did not.
	var batchErr *BatchReadError
	if errors.As(err, &batchErr) {
		a.logBatchFailures(batchErr)
		err = nil
	}
	if err != nil {
		if a.client.IsClosed() {
			// Session permanently dead — async close to avoid blocking.
			a.logReadFailure(ctx, len(names), err)
			a.closeHandler()
			return nil, nil, service.ErrNotConnected
		}
		if shuttingDown(ctx) {
			a.Log.Debugf("Read aborted during shutdown: %v", err)
			return nil, nil, ctx.Err()
		}
		// Transient: reconnecting or PLC not ready. Return empty batch immediately so
		// the caller controls the retry rate; small sleep avoids spinning in production.
		a.logReadFailure(ctx, len(names), err)
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(pullRetryBackoff):
		}
		return service.MessageBatch{}, func(_ context.Context, _ error) error { return nil }, nil
	}

	now := time.Now()
	msgs := service.MessageBatch{}
	for i := range a.Symbols {
		val, ok := values[a.Symbols[i].Name]
		if !ok {
			continue
		}
		// Symbol type unresolved at connect (e.g. PLC not ready yet): retry here
		// so it gets typed on a later poll instead of staying string-typed forever.
		if a.Symbols[i].BaseType == "" && a.Symbols[i].DataType == "" {
			if info, err := a.client.GetSymbol(ctx, a.Symbols[i].Name); err == nil {
				a.Symbols[i].DataType = info.DataType
				a.Symbols[i].BaseType = info.BaseType
				a.Symbols[i].Size = info.Length
			}
		}
		msgs = append(msgs, a.newSymbolMessage(&a.Symbols[i], val, now))
	}

	// Fall back to individual reads if batch returned no results: some PLCs
	// don't support ADS sum read commands, silently skipping all symbols.
	if len(msgs) == 0 && len(a.Symbols) > 0 {
		a.Log.Warnf("Batch read returned no results for %d symbols, falling back to individual reads", len(a.Symbols))
		for i := range a.Symbols {
			if shuttingDown(ctx) {
				a.Log.Debugf("Individual reads abandoned during shutdown at %q", a.Symbols[i].Name)
				return nil, nil, ctx.Err()
			}
			val, readErr := a.client.ReadFromSymbol(ctx, a.Symbols[i].Name)
			if readErr != nil {
				a.Log.Errorf("Individual read failed for %s: %v", a.Symbols[i].Name, readErr)
				continue
			}
			msgs = append(msgs, a.newSymbolMessage(&a.Symbols[i], val, time.Now()))
		}
	}

	// Sleep the remaining interval so the poll period matches IntervalTime; on
	// ctx cancellation, discard the collected batch and return the error.
	if remaining := a.IntervalTime - time.Since(start); remaining > 0 {
		select {
		case <-time.After(remaining):
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
	return msgs, func(_ context.Context, _ error) error { return nil }, nil
}
