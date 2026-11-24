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

// A component declares its Component, Kinds and Classifier once, as package
// variables.
var (
	example = errclass.NewComponent("example")

	kindDeviceUnreachable = example.UserKind("device_unreachable")
	kindSymbolConfig      = example.UserKind("symbol_config")
	kindCloseFailed       = example.UserKind("close_failed")

	// errDeviceUnreachable stands for a sentinel error of a protocol library.
	errDeviceUnreachable = errors.New("device unreachable")

	exampleClassifier = example.Classifier(
		errclass.ErrorIs(errDeviceUnreachable, kindDeviceUnreachable),
	)
)

// exampleLog returns the component's Logger, printing each line as its level,
// message and fields, with the fields sorted, so the output below is stable.
func exampleLog() *errclass.Logger {
	out := service.NewLoggerFromSlog(slog.New(printHandler{}))
	return errclass.NewLogger(service.MockResources(service.MockResourcesOptUseLogger(out)), exampleClassifier)
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

	// The error matches a rule. Wrapping it with %w keeps its Kind.
	err := fmt.Errorf("connect: %w", errDeviceUnreachable)
	log.Error(err)

	// To warn without an error at hand, create one with its Kind.
	log.With("symbol", "MAIN.speed").Warn(kindSymbolConfig.Errorf("symbol %q not found", "MAIN.speed"))

	// Kind.Errorf sets the Kind, even though the wrapped error matches a rule.
	log.Error(kindCloseFailed.Errorf("close: %w", errDeviceUnreachable))

	// No rule matches this error, so it is unclassified.
	log.Warn(errors.New("something odd happened"))

	// Info and Debug lines carry no fields.
	log.Info("Connected")

	// An error returned to Benthos, e.g. from Connect, carries the same fields
	// in its text.
	fmt.Println(exampleClassifier.EnrichForLog(err))
	// Output:
	// ERROR "connect: device unreachable" error_kind=example::device_unreachable error_owner=user
	// WARN "symbol \"MAIN.speed\" not found" error_kind=example::symbol_config error_owner=user symbol=MAIN.speed
	// ERROR "close: device unreachable" error_kind=example::close_failed error_owner=user
	// WARN "something odd happened" error_kind=example::unclassified error_owner=unknown
	// INFO "Connected"
	// [error_kind=example::device_unreachable error_owner=user] connect: device unreachable
}

// Classify returns the first of: the Kind the error was created with, the
// Kind of the first rule that matches the error, or
// "<component>::unclassified".
func ExampleClassifier_Classify() {
	fmt.Println(exampleClassifier.Classify(fmt.Errorf("connect: %w", errDeviceUnreachable)))
	fmt.Println(exampleClassifier.Classify(kindSymbolConfig.Errorf("symbol %q is not in the PLC", "MAIN.speed")))
	fmt.Println(exampleClassifier.Classify(errors.New("boom")))
	// Output:
	// example::device_unreachable
	// example::symbol_config
	// example::unclassified
}

// A library that logs through log/slog gets the *slog.Logger from Slog. Its
// error record here is classified by the error among its fields.
func ExampleLogger_Slog() {
	lib := exampleLog().Slog()

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
