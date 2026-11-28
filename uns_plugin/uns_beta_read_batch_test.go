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

package uns_plugin

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// fakeBatchInput returns the queued batches in order and records each ack.
type fakeBatchInput struct {
	batches []service.MessageBatch
	acks    []error
	ackErr  error
}

func (f *fakeBatchInput) Connect(context.Context) error { return nil }

func (f *fakeBatchInput) ReadBatch(context.Context) (service.MessageBatch, service.AckFunc, error) {
	if len(f.batches) == 0 {
		return nil, nil, service.ErrEndOfInput
	}
	batch := f.batches[0]
	f.batches = f.batches[1:]
	return batch, func(_ context.Context, err error) error {
		f.acks = append(f.acks, err)
		return f.ackErr
	}, nil
}

func (f *fakeBatchInput) Close(context.Context) error { return nil }

// keyedMessage is a message as connect's redpanda input emits it.
func keyedMessage(key string) *service.Message {
	msg := service.NewMessage([]byte("v"))
	msg.MetaSetMut("kafka_key", key)
	msg.MetaSetMut(rpcnKafkaHeadersKey, "raw")
	return msg
}

func meta(msg *service.Message, key string) string {
	v, _ := msg.MetaGet(key)
	return v
}

// Needs no broker and no connect patches, so it runs in the unpatched lane.
var _ = Describe("uns_beta ReadBatch", func() {
	var fake *fakeBatchInput
	var input *unsBetaInput

	BeforeEach(func() {
		filter, err := newBetaKeyFilter([]string{`^umh\.v1\.acme\..+`})
		Expect(err).NotTo(HaveOccurred())
		fake = &fakeBatchInput{}
		input = &unsBetaInput{inner: fake, keyFilter: filter}
	})

	It("delivers matching records with umh_topic set and connect's header slice removed", func() {
		fake.batches = []service.MessageBatch{{keyedMessage("umh.v1.acme._raw.temp")}}

		batch, _, err := input.ReadBatch(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(batch).To(HaveLen(1))
		Expect(meta(batch[0], "umh_topic")).To(Equal("umh.v1.acme._raw.temp"))
		Expect(meta(batch[0], "kafka_msg_key")).To(Equal("umh.v1.acme._raw.temp"))
		_, hasHeaders := batch[0].MetaGet(rpcnKafkaHeadersKey)
		Expect(hasHeaders).To(BeFalse())
	})

	It("drops the placeholder, keyless and non-matching records", func() {
		fake.batches = []service.MessageBatch{{
			keyedMessage(""),
			keyedMessage("umh.v1.other._raw.temp"),
			keyedMessage("umh.v1.acme._raw.temp"),
			service.NewMessage(nil),
		}}

		batch, _, err := input.ReadBatch(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(batch).To(HaveLen(1))
		Expect(meta(batch[0], "umh_topic")).To(Equal("umh.v1.acme._raw.temp"))
		Expect(fake.acks).To(BeEmpty(), "a delivered batch is acked by the pipeline, not by uns_beta")
	})

	It("acks a fully filtered batch itself and returns the next batch", func() {
		fake.batches = []service.MessageBatch{
			{keyedMessage("umh.v1.other._raw.temp"), service.NewMessage(nil)},
			{keyedMessage("umh.v1.acme._raw.temp")},
		}

		batch, _, err := input.ReadBatch(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(batch).To(HaveLen(1))
		Expect(fake.acks).To(Equal([]error{nil}))
	})

	It("returns the error when acking a fully filtered batch fails", func() {
		ackErr := errors.New("commit failed")
		fake.ackErr = ackErr
		fake.batches = []service.MessageBatch{{service.NewMessage(nil)}}

		_, _, err := input.ReadBatch(context.Background())
		Expect(err).To(MatchError(ackErr))
	})

	It("stops reading after a fully filtered batch once the context is cancelled", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		fake.batches = []service.MessageBatch{{service.NewMessage(nil)}, {keyedMessage("umh.v1.acme._raw.temp")}}

		_, _, err := input.ReadBatch(ctx)
		Expect(err).To(MatchError(context.Canceled))
		Expect(fake.batches).To(HaveLen(1), "the next batch must not be read")
	})

	It("passes the inner reader's error through", func() {
		_, _, err := input.ReadBatch(context.Background())
		Expect(err).To(MatchError(service.ErrEndOfInput))
	})
})
