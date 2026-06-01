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
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	fieldEndpoint          = "endpoint"
	fieldSubscribe         = "subscribe"
	fieldGenericSubscribe  = "generic_subscribe"
	fieldRevision          = "revision"
	fieldKeepAliveInterval = "keepalive_interval"
	fieldRequestTimeout    = "request_timeout"
	fieldReconnect         = "reconnect"
	fieldMaxBackoff        = "max_backoff"
)

func configSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("1.0.0").
		Summary("Reads data from an Atlas Copco Open Protocol tightening controller.").
		Description(`Connects to an industrial tightening controller (Atlas Copco, Bosch Rexroth Nexo, Desoutter, Stanley and others) that speaks the Atlas Copco Open Protocol over TCP.

The controller acts as the server (default port 4545); this input is the client. It establishes a long-lived session: it logs in (MID 0001), subscribes to the configured event streams, receives pushed result telegrams, acknowledges them, and keeps the link alive (MID 9999). It transparently reconnects and replays the login + subscriptions if the connection drops.

For each received telegram the input emits one message:
- MID 0061 (last tightening result) is natively decoded into a structured JSON object (torque, angle, status, IDs, timestamps).
- All other MIDs are emitted as the raw ASCII data field, to be decoded downstream in bloblang.

Routing metadata is attached to every message: ` + "`op_mid`" + `, ` + "`op_revision`" + `, ` + "`op_station_id`" + `, ` + "`op_spindle_id`" + ` and ` + "`op_endpoint`" + `. Route on ` + "`op_mid`" + ` to apply the right decoder per message type.`).
		Field(service.NewStringField(fieldEndpoint).
			Description("The controller's TCP endpoint as host:port.").
			Example("10.0.0.42:4545")).
		Field(service.NewStringListField(fieldSubscribe).
			Description("Event streams to subscribe to. 'last_tightening' opens MID 0060/0061/0062; 'alarms' opens MID 0070/0071/0072.").
			Default([]string{"last_tightening"}).
			Example([]string{"last_tightening"}).
			Example([]string{"last_tightening", "alarms"})).
		Field(service.NewIntListField(fieldGenericSubscribe).
			Description("Additional MIDs to subscribe to via the generic subscription mechanism (MID 0008). Experimental; validate against your controller.").
			Default([]any{}).
			Advanced()).
		Field(service.NewIntField(fieldRevision).
			Description("Requested MID revision during login. 0 lets the controller choose (treated as revision 1).").
			Default(0).
			Examples(0, 1).
			Advanced()).
		Field(service.NewDurationField(fieldKeepAliveInterval).
			Description("How often to send keep-alive telegrams (MID 9999).").
			Default("10s").
			Advanced()).
		Field(service.NewDurationField(fieldRequestTimeout).
			Description("How long to wait for the login/handshake reply before treating the connection as failed.").
			Default("5s").
			Advanced()).
		Field(service.NewObjectField(fieldReconnect,
			service.NewDurationField(fieldMaxBackoff).
				Description("Maximum backoff between reconnection attempts.").
				Default("30s"),
		).
			Description("Reconnection behaviour.").
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
	session  *Session
	logger   *service.Logger
	done     chan struct{}
}

func newOpenProtocolInput(conf *service.ParsedConfig, mgr *service.Resources) (*openProtocolInput, error) {
	endpoint, err := conf.FieldString(fieldEndpoint)
	if err != nil {
		return nil, err
	}
	subscribe, err := conf.FieldStringList(fieldSubscribe)
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
	maxBackoff, err := conf.FieldDuration(fieldReconnect, fieldMaxBackoff)
	if err != nil {
		return nil, err
	}

	logger := mgr.Logger()
	sess := NewSession(SessionConfig{
		Endpoint:          endpoint,
		Subscriptions:     subscribe,
		GenericMIDs:       genericMIDs,
		Revision:          revision,
		KeepAliveInterval: keepAlive,
		RequestTimeout:    requestTimeout,
		MaxBackoff:        maxBackoff,
	}, logger)

	return &openProtocolInput{
		endpoint: endpoint,
		session:  sess,
		logger:   logger,
		done:     make(chan struct{}),
	}, nil
}

func (in *openProtocolInput) Connect(ctx context.Context) error {
	return in.session.Start(ctx)
}

func (in *openProtocolInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-in.done:
		return nil, nil, service.ErrEndOfInput
	case tel := <-in.session.Telegrams():
		msg := in.telegramToMessage(tel)
		return service.MessageBatch{msg},
			func(context.Context, error) error { return nil }, // at-most-once
			nil
	}
}

func (in *openProtocolInput) Close(_ context.Context) error {
	select {
	case <-in.done:
	default:
		close(in.done)
	}
	in.session.Stop()
	return nil
}

// telegramToMessage converts a received telegram into a Benthos message. MID
// 0061 is decoded into structured JSON; every other MID carries the raw data
// field. Routing metadata is attached in both cases.
func (in *openProtocolInput) telegramToMessage(t Telegram) *service.Message {
	var msg *service.Message
	if decoded, native := Decode(t); native {
		if b, err := json.Marshal(decoded); err == nil {
			msg = service.NewMessage(b)
		} else {
			in.logger.Warnf("open protocol: failed to encode MID %04d, emitting raw: %v", t.Header.MID, err)
			msg = service.NewMessage(append([]byte{}, t.Data...))
		}
	} else {
		msg = service.NewMessage(append([]byte{}, t.Data...))
	}

	msg.MetaSet("op_mid", fmt.Sprintf("%04d", t.Header.MID))
	msg.MetaSet("op_revision", strconv.Itoa(t.Header.Revision))
	msg.MetaSet("op_station_id", strconv.Itoa(t.Header.StationID))
	msg.MetaSet("op_spindle_id", strconv.Itoa(t.Header.SpindleID))
	msg.MetaSet("op_endpoint", in.endpoint)
	return msg
}
