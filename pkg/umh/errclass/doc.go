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

// Package errclass adds two fields to every warning and error a Benthos
// component logs: error_kind names the cause, e.g. "ads::route_fault", and
// error_owner says who has to act on it: "user" (configuration, device,
// network), "umh" (a bug or gap in UMH's code) or "unknown" (no rule matched).
// umh-core groups log lines by these fields without parsing their text.
//
// A component declares its Kinds and its Classifier once, as package
// variables, and logs every warning and error through a Logger:
//
//	var (
//		ads = errclass.NewComponent("ads")
//
//		kindRouteFault        = ads.UserKind("route_fault")
//		kindConnectionDropped = ads.UserKind("connection_dropped")
//
//		classifier = ads.Classifier(
//			errclass.MessageIs("PLC closed connection, transport down", kindConnectionDropped),
//			errclass.ErrorIs(adsLib.ErrRouteNotServed, kindRouteFault),
//		)
//	)
//
//	log := errclass.NewLogger(mgr, classifier) // in the constructor
//	log.Error(fmt.Errorf("connect to %s: %w", addr, err))
//
// An error the component creates itself gets its Kind from Kind.Errorf and
// needs no rule. An error from a library gets its Kind from the first
// matching rule. See Classifier.Classify for the full order.
//
// Logger.Warn and Logger.Error take an error, not a message, so there is no
// Warnf or Errorf: wrap the error with fmt.Errorf and %w instead. The
// forbidigo rule in .golangci.yaml forbids mgr.Logger() outside this package,
// so a component cannot log a warning or error without a Kind.
//
// Kinds are for logs only. Code that decides whether to retry checks the
// error itself, for example with errors.Is.
package errclass
