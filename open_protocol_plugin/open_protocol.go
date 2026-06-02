// Copyright 2025 UMH Systems GmbH
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

package open_protocol_plugin

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	fieldEndpoint          = "endpoint"
	fieldSubscribe         = "subscribe"
	fieldGenericSubscribe  = "generic_subscribe"
	fieldRevision          = "revision"
	fieldKeepAliveInterval = "keepalive_interval"
	fieldRequestTimeout    = "request_timeout"
	fieldTimezone          = "timezone"
	fieldReadTimeout       = "read_timeout"
)

func configSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("1.0.0").
		Summary("Reads data from an Atlas Copco Open Protocol tightening controller.").
		Description(`Connects to an industrial tightening controller (Atlas Copco, Bosch Rexroth Nexo, Desoutter, Stanley and others) that speaks the Atlas Copco Open Protocol over TCP.

The controller acts as the server (default port 4545); this input is the client. It establishes a long-lived session: it logs in (MID 0001), subscribes to the configured event streams, receives pushed result telegrams, acknowledges them, and keeps the link alive (MID 9999). It transparently reconnects and replays the login + subscriptions if the connection drops.

For each received telegram the input emits one or more messages:
- MID 0061 rev-1 (last tightening result) is fanned out into 18 messages — one per measurement tag (torque, angle, statuses, IDs, etc.).
- All other MIDs, or MID 0061 with a revision other than 1, are emitted as one raw message containing the ASCII data field.

Routing metadata is attached to every message: ` + "`open_protocol_mid`" + ` (4-digit), ` + "`open_protocol_revision`" + `, ` + "`open_protocol_station_id`" + `, ` + "`open_protocol_spindle_id`" + `, and ` + "`open_protocol_endpoint`" + `. For fanned-out MID 0061 messages, ` + "`open_protocol_tag_name`" + ` identifies the measurement and ` + "`timestamp_ms`" + ` carries the controller result timestamp as Unix milliseconds. Route on ` + "`open_protocol_mid`" + ` to apply the right decoder per message type.`).
		Field(service.NewStringField(fieldEndpoint).
			Description("The controller's TCP endpoint as host:port.").
			Example("10.0.0.42:4545")).
		Field(service.NewIntListField(fieldSubscribe).
			Description("Subscription MIDs to open — the MID the plugin sends to the controller. 0060 = last tightening (pushes MID 0061), 0070 = alarms (pushes MID 0071). The plugin forwards the pushed data MID (subscribe MID + 1) and acknowledges it (subscribe MID + 2).").
			Default([]any{60}).
			Example([]any{60}).
			Example([]any{60, 70})).
		Field(service.NewIntListField(fieldGenericSubscribe).
			Description("Additional MIDs to subscribe to via the generic subscription mechanism (MID 0008). Experimental; validate against your controller.").
			Default([]any{}).
			Advanced()).
		Field(service.NewIntField(fieldRevision).
			Description("MID revision requested at login (sent as the 3-digit revision field of MID 0001). Pinned to 1; the plugin natively decodes only MID 0061 revision 1.").
			Default(1).
			Examples(1).
			Advanced()).
		Field(service.NewDurationField(fieldKeepAliveInterval).
			Description("How often to send keep-alive telegrams (MID 9999).").
			Default("10s").
			Advanced()).
		Field(service.NewDurationField(fieldRequestTimeout).
			Description("How long to wait for the login/handshake reply before treating the connection as failed.").
			Default("5s").
			Advanced()).
		Field(service.NewStringField(fieldTimezone).
			Description("IANA timezone (e.g. Europe/Berlin) used to interpret the controller's zone-less result timestamp. Defaults to 'Local' (the host/VM timezone); set 'UTC' or an explicit IANA zone to override.").
			Default("Local").
			Example("Local").
			Example("UTC").
			Example("Europe/Berlin").
			Advanced()).
		Field(service.NewDurationField(fieldReadTimeout).
			Description("Max idle awaiting a telegram before the connection is treated as lost and Benthos reconnects. Must be >= 2x keepalive_interval.").
			Default("30s").
			Advanced())
}

func init() {
	err := service.RegisterBatchInput("open_protocol", configSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newOpenProtocolInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type openProtocolInput struct {
	endpoint string
	loc      *time.Location
	session  *Session
	logger   *service.Logger
}

func newOpenProtocolInput(conf *service.ParsedConfig, mgr *service.Resources) (*openProtocolInput, error) {
	endpoint, err := conf.FieldString(fieldEndpoint)
	if err != nil {
		return nil, err
	}
	subscribe, err := conf.FieldIntList(fieldSubscribe)
	if err != nil {
		return nil, err
	}
	genericMIDs, err := conf.FieldIntList(fieldGenericSubscribe)
	if err != nil {
		return nil, err
	}
	revision, err := conf.FieldInt(fieldRevision)
	if err != nil {
		return nil, err
	}
	keepAlive, err := conf.FieldDuration(fieldKeepAliveInterval)
	if err != nil {
		return nil, err
	}
	requestTimeout, err := conf.FieldDuration(fieldRequestTimeout)
	if err != nil {
		return nil, err
	}
	tz, err := conf.FieldString(fieldTimezone)
	if err != nil {
		return nil, err
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, fmt.Errorf("open protocol: invalid timezone %q: %w", tz, err)
	}
	readTimeout, err := conf.FieldDuration(fieldReadTimeout)
	if err != nil {
		return nil, err
	}
	if readTimeout < 2*keepAlive {
		return nil, fmt.Errorf("open protocol: read_timeout (%s) must be >= 2x keepalive_interval (%s)", readTimeout, keepAlive)
	}

	logger := mgr.Logger()
	sess := NewSession(SessionConfig{
		Endpoint:          endpoint,
		SubscribeMIDs:     subscribe,
		GenericMIDs:       genericMIDs,
		Revision:          revision,
		KeepAliveInterval: keepAlive,
		RequestTimeout:    requestTimeout,
		ReadTimeout:       readTimeout,
	}, logger)

	return &openProtocolInput{
		endpoint: endpoint,
		loc:      loc,
		session:  sess,
		logger:   logger,
	}, nil
}

func (in *openProtocolInput) Connect(ctx context.Context) error {
	return in.session.Connect(ctx)
}

func (in *openProtocolInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	res, err := in.session.Read(ctx)
	if err != nil {
		if errors.Is(err, errSessionClosed) {
			return nil, nil, service.ErrEndOfInput
		}
		return nil, nil, service.ErrNotConnected
	}
	batch := in.buildBatch(res.Telegram)
	return batch, func(_ context.Context, e error) error {
		if e == nil {
			res.Ack()
		} else {
			in.logger.Warnf("open protocol: downstream delivery failed; MID 0062 withheld, result left un-acked for controller re-push: %v", e)
		}
		return nil
	}, nil
}

func (in *openProtocolInput) Close(_ context.Context) error {
	return in.session.Close()
}

// buildBatch converts a received telegram into a Benthos MessageBatch.
//
// Rev-1 MID 0061 is fanned out into 18 messages — one per measurement tag.
// Everything else (non-0061, or 0061 with revision != 1, or a malformed 0061)
// is passed through as a single raw message.
func (in *openProtocolInput) buildBatch(t Telegram) service.MessageBatch {
	midStr := fmt.Sprintf("%04d", t.Header.MID)
	revStr := strconv.Itoa(t.Header.Revision)

	// Rev-1 MID 0061 fan-out path.
	if t.Header.MID == MIDLastTightening && t.Header.Revision == 1 {
		lt, derr := ParseLastTightening(t)
		if derr == nil {
			// Compute the shared timestamp_ms.
			tsRaw := lt.Timestamp
			var tsMs string
			if instant, perr := ParseControllerTime(tsRaw, in.loc); perr == nil {
				tsMs = strconv.FormatInt(instant.UnixMilli(), 10)
			} else {
				in.logger.Warnf("open protocol: MID 0061 timestamp parse failed (%v), tag_processor will use ingest time", perr)
			}

			commonMeta := map[string]string{
				"open_protocol_mid":                   midStr,
				"open_protocol_revision":              revStr,
				"open_protocol_timestamp":             tsRaw,
				"open_protocol_pset_change_timestamp": lt.PsetChangeTime,
				"open_protocol_cell_id":               strconv.Itoa(lt.CellID),
				"open_protocol_channel_id":            strconv.Itoa(lt.ChannelID),
				"open_protocol_station_id":            strconv.Itoa(t.Header.StationID),
				"open_protocol_spindle_id":            strconv.Itoa(t.Header.SpindleID),
				"open_protocol_controller_name":       strings.TrimSpace(lt.ControllerName),
				"open_protocol_endpoint":              in.endpoint,
			}

			tags := FanOut(lt)
			batch := make(service.MessageBatch, 0, len(tags))
			for _, tag := range tags {
				msg := service.NewMessage(nil)
				msg.SetStructured(tag.Value)
				for k, v := range commonMeta {
					msg.MetaSet(k, v)
				}
				if tsMs != "" {
					msg.MetaSet("timestamp_ms", tsMs)
				}
				msg.MetaSet("open_protocol_tag_name", tag.Name)
				batch = append(batch, msg)
			}
			return batch
		}
		in.logger.Warnf("open protocol: MID 0061 parse failed, emitting raw: %v", derr)
		// Fall through to the raw path below.
	}

	// Raw passthrough: non-0061, rev != 1, or malformed 0061.
	// Edge #9: warn when MID 0061 is present but with an unsupported revision.
	// The malformed-rev-1 path already logged above; only the non-rev-1 path is
	// silent without this guard.
	if t.Header.MID == MIDLastTightening && t.Header.Revision != 1 {
		in.logger.Warnf("open protocol: MID 0061 revision %d != 1, emitting raw (native decode skipped)", t.Header.Revision)
	}
	msg := service.NewMessage(append([]byte{}, t.Data...))
	msg.MetaSet("open_protocol_mid", midStr)
	msg.MetaSet("open_protocol_revision", revStr)
	msg.MetaSet("open_protocol_station_id", strconv.Itoa(t.Header.StationID))
	msg.MetaSet("open_protocol_spindle_id", strconv.Itoa(t.Header.SpindleID))
	msg.MetaSet("open_protocol_endpoint", in.endpoint)
	return service.MessageBatch{msg}
}
