// Copyright 2026 UMH Systems GmbH
//
// Package handshake delivers one write request to a machine by following the
// machine's own procedure, the handshake, and reports what happened. The
// procedure differs for every machine, so its owner writes it as a script; a
// [Binding] supplies the reads, writes and calls. The package does not retry;
// the flow around it does.
package handshake
