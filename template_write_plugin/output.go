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

package template_write_plugin

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// BuildFunc reads the protocol's own fields from conf and returns its codec
// and dialer.
type BuildFunc[P Point[P]] func(conf *service.ParsedConfig, mgr *service.Resources) (Protocol[P], error)

// newOutput builds the output from a parsed config. Register in write.go uses
// it; tests call it directly with a fake protocol.
func newOutput[P Point[P]](conf *service.ParsedConfig, mgr *service.Resources, build BuildFunc[P]) (service.Output, error) {
	mappings, err := parseMappings(conf)
	if err != nil {
		return nil, err
	}
	readBack, err := conf.FieldBool("readBack")
	if err != nil {
		return nil, err
	}
	protocol, err := build(conf, mgr)
	if err != nil {
		return nil, err
	}
	if protocol.Codec == nil || protocol.Dialer == nil {
		return nil, errors.New("protocol needs both a codec and a dialer")
	}

	return &output[P]{
		mappings: mappings,
		readBack: readBack,
		protocol: protocol,
		log:      mgr.Logger(),
	}, nil
}

type output[P Point[P]] struct {
	mappings []mapping
	readBack bool
	protocol Protocol[P]
	log      *service.Logger

	mutex    sync.Mutex
	session  Session[P]
	resolved map[string]Resolved // per connection; symbol tables can change on a PLC download
}

// Connect opens a session. Benthos calls it again after Write returns
// service.ErrNotConnected.
func (o *output[P]) Connect(ctx context.Context) error {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.session != nil {
		return nil
	}
	session, err := o.protocol.Dialer.Dial(ctx)
	if err != nil {
		return err
	}
	if _, canReadBack := session.(ReadBacker[P]); o.readBack && !canReadBack {
		_ = session.Close()
		return errors.New("readBack is enabled but this protocol cannot read values back; set readBack to false")
	}
	o.session = session
	o.resolved = map[string]Resolved{}
	return nil
}

// Write makes one attempt to write all mappings of msg. It does not retry;
// a returned error nacks the message, and the write flow decides from the
// error's Reason whether to try again.
func (o *output[P]) Write(ctx context.Context, msg *service.Message) error {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if o.session == nil {
		return service.ErrNotConnected
	}

	points, err := o.preparePoints(ctx, msg)
	if err != nil {
		return err
	}

	batches := [][]P{points}
	if batcher, ok := o.session.(Batcher[P]); ok {
		batches = batcher.Batch(points)
	}
	for _, batch := range batches {
		if err := o.writeBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

// Close ends the session.
func (o *output[P]) Close(_ context.Context) error {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	return o.disconnect()
}

type target struct {
	address string
	value   any
}

func (o *output[P]) preparePoints(ctx context.Context, msg *service.Message) ([]P, error) {
	targets, err := o.targets(msg)
	if err != nil {
		return nil, err
	}

	resolver, needsResolving := o.session.(Resolver)
	resolvingCodec, codecResolves := o.protocol.Codec.(ResolvingCodec[P])
	if needsResolving && !codecResolves {
		return nil, errors.New("the session resolves addresses but the codec does not implement ResolvingCodec")
	}
	if needsResolving {
		if err = o.resolve(ctx, resolver, targets); err != nil {
			return nil, err
		}
	}

	points := make([]P, 0, len(targets))
	for _, target := range targets {
		var point P
		if needsResolving {
			point, err = resolvingCodec.PrepareResolved(target.address, o.resolved[target.address], target.value)
		} else {
			point, err = o.protocol.Codec.Prepare(target.address, target.value)
		}
		if err != nil {
			return nil, newWriteError(target.address, err)
		}
		points = append(points, point)
	}
	return points, nil
}

func (o *output[P]) targets(msg *service.Message) ([]target, error) {
	structured, err := msg.AsStructured()
	if err != nil {
		return nil, newWriteError("", fmt.Errorf("%w: message is not JSON: %w", ErrInvalidValue, err))
	}

	targets := make([]target, 0, len(o.mappings))
	for _, mapping := range o.mappings {
		address, err := mapping.address.TryString(msg)
		if err != nil {
			return nil, newWriteError("", fmt.Errorf("%w: resolving address: %w", ErrInvalidValue, err))
		}
		value, err := Field(structured, mapping.valueFrom)
		if err != nil {
			return nil, newWriteError(address, err)
		}
		targets = append(targets, target{address: address, value: value})
	}
	return targets, nil
}

// resolve asks the device about every address not yet resolved on this
// connection, in one call.
func (o *output[P]) resolve(ctx context.Context, resolver Resolver, targets []target) error {
	var missing []string
	for _, target := range targets {
		if _, ok := o.resolved[target.address]; !ok {
			missing = append(missing, target.address)
		}
	}
	if len(missing) == 0 {
		return nil
	}

	resolved, err := resolver.Resolve(ctx, missing)
	if err != nil {
		return o.classify("resolve", "", err)
	}
	for _, address := range missing {
		info, ok := resolved[address]
		if !ok {
			return newWriteError(address, ErrUnresolved)
		}
		o.resolved[address] = info
	}
	return nil
}

// writeBatch writes one batch and, with readBack on, verifies it.
func (o *output[P]) writeBatch(ctx context.Context, batch []P) error {
	if err := o.session.Write(ctx, batch); err != nil {
		return o.classify("write", batchAddress(batch), err)
	}
	if !o.readBack {
		return nil
	}

	readBacker, ok := o.session.(ReadBacker[P])
	if !ok {
		return errors.New("readBack is enabled but the session cannot read values back")
	}
	readBack, err := readBacker.ReadBack(ctx, batch)
	if err != nil {
		return o.classify("read-back", batchAddress(batch), err)
	}
	if len(readBack) != len(batch) {
		return o.classify("read-back", batchAddress(batch), fmt.Errorf("got %d values for %d points", len(readBack), len(batch)))
	}
	for index, point := range batch {
		if !point.SameValue(readBack[index]) {
			return newWriteError(point.Address(), ErrReadbackMismatch)
		}
	}
	return nil
}

// classify keeps device refusals as WriteErrors and turns everything else
// into a reconnect, so an unclassified library error never leaves a broken
// session in use.
func (o *output[P]) classify(operation string, address string, err error) error {
	if errors.Is(err, ErrRejected) {
		return newWriteError(address, err)
	}
	o.log.Errorf("Device %s failed, reconnecting: %v", operation, err)
	_ = o.disconnect()
	return service.ErrNotConnected
}

// disconnect must be called with the mutex held.
func (o *output[P]) disconnect() error {
	if o.session == nil {
		return nil
	}
	session := o.session
	o.session = nil
	o.resolved = nil
	return session.Close()
}

// batchAddress names a batch in errors. Sessions should name the exact
// address in their own error; this is the fallback.
func batchAddress[P Point[P]](batch []P) string {
	if len(batch) == 1 {
		return batch[0].Address()
	}
	return fmt.Sprintf("%d addresses", len(batch))
}
