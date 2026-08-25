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
	"time"

	adsLib "github.com/RuneRoven/go-ads/v2"
)

// Update is the go-ads notification sample type; the one deliberate library leak.
type Update = adsLib.Update

// Client is the library-agnostic ADS session contract. Methods return values
// pre-decoded to string form (typing is parse.go's job, not the adapter's).
type Client interface {
	Connect(ctx context.Context) error
	Close() error
	IsClosed() bool
	LoadSymbols(ctx context.Context) error
	GetSymbol(ctx context.Context, name string) (SymbolInfo, error)
	AddNotifications(ctx context.Context, cfgs []NotifyConfig, ch chan *adsLib.Update) ([]NotifyResult, error)
	ReadMultipleSymbols(ctx context.Context, names []string) (map[string]string, error)
	ReadFromSymbol(ctx context.Context, name string) (string, error) // fallback: PLCs without sum-read
}

// SessionConfig is the library-agnostic connection spec.
type SessionConfig struct {
	TargetIP, TargetAMS, HostIP, HostAMS string
	TargetPort, RuntimePort, HostPort    int
	Username, Password                   string
	RequestTimeout                       time.Duration
}

type NotifyConfig struct {
	SymbolName       string
	MaxDelay         time.Duration
	CycleTime        time.Duration
	TransmissionMode int
}

type SymbolInfo struct {
	DataType string
	BaseType string
	Length   uint32
}

// NotifyResult normalises per-symbol registration outcome.
type NotifyResult struct {
	SymbolName string
	Registered bool   // Skipped==false && Error==NoErrors
	Skipped    bool   // symbol not resolvable (bad name)
	Code       uint32 // raw ADS return code; 0 when Registered
}

// BatchReadError reports the symbols a batch read produced no value for. The
// map returned alongside it still holds every symbol that did succeed, so one
// bad name costs that one tag rather than the whole poll.
type BatchReadError struct {
	Requested int
	Failed    []NotifyResult // same three-state shape: Skipped, else Code
}

func (e *BatchReadError) Error() string {
	return fmt.Sprintf("%d of %d symbols failed", len(e.Failed), e.Requested)
}
