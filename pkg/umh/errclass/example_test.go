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

package errclass_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/errclass"
)

// A plugin declares its Plugin, Kinds and Classifier once, as package
// variables.
var (
	example = errclass.NewPlugin("example")

	kindDeviceUnreachable = example.User("device_unreachable")
	kindSymbolConfig      = example.User("symbol_config")
	kindCloseFailed       = example.User("close_failed")

	// errDeviceUnreachable stands for a sentinel error of a protocol library.
	errDeviceUnreachable = errors.New("device unreachable")

	exampleClassifier = example.Classifier(
		errclass.Is(errDeviceUnreachable, kindDeviceUnreachable),
	)
)

// exampleLog returns the plugin's logger, printing each line as its level,
// message and fields, with the fields sorted, so the output below is stable.
func exampleLog() *service.Logger {
	out := service.NewLoggerFromSlog(slog.New(printHandler{}))
	return exampleClassifier.Logger(service.MockResources(service.MockResourcesOptUseLogger(out)))
}

type printHandler struct{ attrs []slog.Attr }

func (printHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h printHandler) WithGroup(string) slog.Handler          { return h }

func (h printHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return printHandler{attrs: append(slices.Clone(h.attrs), attrs...)}
}

func (h printHandler) Handle(_ context.Context, r slog.Record) error {
	fields := make([]string, 0, len(h.attrs))
	for _, a := range h.attrs {
		fields = append(fields, a.String())
	}
	r.Attrs(func(a slog.Attr) bool {
		fields = append(fields, a.String())
		return true
	})
	slices.Sort(fields)
	fmt.Println(strings.Join(append([]string{r.Level.String(), strconv.Quote(r.Message)}, fields...), " "))
	return nil
}

func Example() {
	log := exampleLog()

	// The call site says what the line means.
	exampleClassifier.With(log, kindSymbolConfig).Warnf("Symbol %q not found", "MAIN.speed")

	// The line reports an error, and the error says what it means.
	err := fmt.Errorf("connect: %w", errDeviceUnreachable)
	exampleClassifier.WithErr(log, err).Errorf("Connect failed: %v", err)

	// The line is about the call site, whatever the error.
	closeErr := errors.New("socket busy")
	exampleClassifier.With(log, kindCloseFailed).Errorf("Close failed: %v", closeErr)

	// Nobody said what it means.
	log.Warn("Something odd happened")

	// Info and Debug lines carry no fields.
	log.Info("Connected")

	// The error returned from Connect carries the same error_kind in its text.
	fmt.Println(exampleClassifier.EnrichForLog(err))
	// Output:
	// WARN "Symbol \"MAIN.speed\" not found" error_kind=example::symbol_config error_owner=user
	// ERROR "Connect failed: connect: device unreachable" error_kind=example::device_unreachable error_owner=user
	// ERROR "Close failed: socket busy" error_kind=example::close_failed error_owner=user
	// WARN "Something odd happened" error_kind=example::unclassified error_owner=unknown
	// INFO "Connected"
	// [error_kind=example::device_unreachable error_owner=user] connect: device unreachable
}

// Classify picks the Kind in this order: the Kind the error was created with,
// then the first rule that matches the error, then "<plugin>::unclassified".
// Where the Kind decides what the code does next, ask IsKind.
func ExampleClassifier_Classify() {
	fmt.Println(exampleClassifier.Classify(fmt.Errorf("connect: %w", errDeviceUnreachable)))
	fmt.Println(exampleClassifier.Classify(kindSymbolConfig.Errorf("symbol %q is not in the PLC", "MAIN.speed")))
	fmt.Println(exampleClassifier.Classify(errors.New("boom")))

	if exampleClassifier.IsKind(errDeviceUnreachable, kindDeviceUnreachable) {
		fmt.Println("backing off")
	}
	// Output:
	// example::device_unreachable
	// example::symbol_config
	// example::unclassified
	// backing off
}

// A library that logs through log/slog gets Slog. Its records are classified
// by the error among their fields.
func ExampleClassifier_Slog() {
	lib := exampleClassifier.Slog(exampleLog())

	lib.Error("dial failed", "error", fmt.Errorf("dial: %w", errDeviceUnreachable))
	// Output:
	// ERROR "dial failed" error=dial: device unreachable error_kind=example::device_unreachable error_owner=user
}

func ExampleClassifier_EnrichForLog() {
	err := exampleClassifier.EnrichForLog(errors.New("boom"))

	fmt.Println(err)
	// Output:
	// [error_kind=example::unclassified error_owner=unknown] boom
}
