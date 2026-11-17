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

// Package errclass tags every Warn and Error log line of a plugin with two
// fields, so that whoever reads the log can tell causes apart without parsing
// message text:
//
//   - error_kind names the cause, e.g. "ads::route_fault": the plugin name,
//     "::", and the name of the cause within that plugin. In Go it is a Kind.
//   - error_class says where the cause lies: Known (outside UMH's code, such as
//     configuration, device state, the network, or a fault that recovers by
//     itself), Bug (only UMH can fix it) or Unknown (no Kind matched).
//
// The list of every plugin's Kinds is the contract with umh-core:
// https://linear.app/united-manufacturing-hub/document/plugin-error-classes-contract-between-benthos-umh-and-umh-core-e51f0744258b
//
// These classes are separate from the retry classes of the engineering
// handbook (transient, persistent transient, persistent). They say whether
// UMH has to act, not whether a retry helps.
//
// A plugin declares its Plugin, its Kinds and its Classifier once, as package
// variables:
//
//	var (
//		ads = errclass.NewPlugin("ads")
//
//		kindConnectionDropped = ads.Known("connection_dropped")
//		kindRouteFault        = ads.Known("route_fault")
//		kindInvalidHostAMS    = ads.Known("invalid_host_ams")
//		kindReadGap           = ads.Bug("read_gap")
//
//		classifier = ads.Classifier(
//			errclass.Message("PLC closed connection, transport down", kindConnectionDropped),
//			errclass.Is(adsLib.ErrRouteNotServed, kindRouteFault),
//			errclass.Match(func(err error) bool { return errors.Is(err, io.EOF) }, kindConnectionDropped),
//		)
//	)
//
// The plugin logs through classifier.Logger(mgr), a plain *service.Logger.
// Every Warn and Error line it writes carries error_kind and error_class; a
// line nothing classified is "<plugin>::unclassified". The call site names the
// Kind with With, or lets the error decide with WithErr:
//
//	log := classifier.Logger(mgr)
//	classifier.With(log, kindReadGap).Warnf("No value for %q", name)
//	classifier.WithErr(log, err).Errorf("Read failed: %v", err)
//	log.Warn("Something nobody classified yet") // error_kind=ads::unclassified
//
// Errors the plugin creates itself carry their Kind, so they need no Rule:
//
//	return kindInvalidHostAMS.Errorf("hostAMS %q is not a valid AMS NetID: %w", host, err)
//
// A Kind named at the call site, with With or Kind.Errorf, always wins over
// the Rules.
//
// A library that logs through log/slog gets classifier.Slog(log). Its records
// are classified by the Message rules and by the error among their fields.
//
// An error returned to Benthos is logged again by Benthos itself, without
// fields. Classifier.EnrichForLog puts error_kind and error_class into its text.
//
// The forbidigo rule in .golangci.yaml forbids mgr.Logger() outside this
// package, so a plugin cannot log through a logger that skips classification.
package errclass
