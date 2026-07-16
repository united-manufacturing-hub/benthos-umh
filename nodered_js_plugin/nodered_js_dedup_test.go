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

package nodered_js_plugin_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Unique names prevent cross-spec cache pollution.
var dedupTestSeq uint64

func dedupTestCacheName() string {
	return fmt.Sprintf("dedup-test-%d-%d", time.Now().UnixNano(), atomic.AddUint64(&dedupTestSeq, 1))
}

func withUniqueCacheName(yaml string) string {
	name := dedupTestCacheName()
	nameLine := "    name: " + fmt.Sprintf("%q", name) + "\n"
	if strings.Contains(yaml, "cache:") {
		return strings.Replace(yaml, "cache:\n", "cache:\n"+nameLine, 1)
	}
	return strings.Replace(yaml, "nodered_js:", "nodered_js:\n  cache:\n"+nameLine[:len(nameLine)-1], 1)
}

var _ = Describe("Replay dedup", func() {
	// Fresh pointer, identical payload+meta — mirrors what a Kafka input hands the pipeline on retry.
	replayMsg := func(payload string, meta map[string]string) *service.Message {
		m := service.NewMessage([]byte(payload))
		for k, v := range meta {
			m.MetaSet(k, v)
		}
		return m
	}

	BeforeEach(func() {
		if os.Getenv("TEST_NODERED_JS") == "" {
			Skip("Skipping Node-RED JS tests: TEST_NODERED_JS not set")
		}
	})

	buildStreamWithConfig := func(procYAML string) (service.MessageHandlerFunc, *[]*service.Message, context.CancelFunc) {
		builder := service.NewStreamBuilder()

		handler, err := builder.AddProducerFunc()
		Expect(err).NotTo(HaveOccurred())

		err = builder.AddProcessorYAML(withUniqueCacheName(procYAML))
		Expect(err).NotTo(HaveOccurred())

		var msgs []*service.Message
		err = builder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
			msgs = append(msgs, m)
			return nil
		})
		Expect(err).NotTo(HaveOccurred())

		stream, err := builder.Build()
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		go func() { _ = stream.Run(ctx) }()
		return handler, &msgs, cancel
	}

	counterCode := `
var n = cache.exists("count") ? cache.get("count") : 0;
n = n + 1;
cache.set("count", n);
msg.payload = n;
return msg;
`

	newCounterStream := func() (service.MessageHandlerFunc, *[]*service.Message, context.CancelFunc) {
		return buildStreamWithConfig("nodered_js:\n  code: |\n" + indentLines(counterCode, "    "))
	}

	It("suppresses cache.set on identical-content replay so the counter doesn't double", func() {
		handler, msgs, cancel := newCounterStream()
		defer cancel()

		ctx := context.Background()
		Expect(handler(ctx, newMsg("A"))).To(Succeed())
		Expect(handler(ctx, newMsg("A"))).To(Succeed())
		Expect(handler(ctx, newMsg("B"))).To(Succeed())

		Eventually(func() int { return len(*msgs) }).Should(Equal(3))
		Expect(payloadFloat(*msgs, 0)).To(Equal(float64(1)))
		Expect(payloadFloat(*msgs, 1)).To(Equal(float64(2)))
		// B's write finds cache at 1 not 2 — proves A's replay was suppressed.
		Expect(payloadFloat(*msgs, 2)).To(Equal(float64(2)))
	})

	It("does not false-positive on genuinely different messages", func() {
		handler, msgs, cancel := newCounterStream()
		defer cancel()

		ctx := context.Background()
		Expect(handler(ctx, newMsg("A"))).To(Succeed())
		Expect(handler(ctx, newMsg("B"))).To(Succeed())
		Expect(handler(ctx, newMsg("C"))).To(Succeed())

		Eventually(func() int { return len(*msgs) }).Should(Equal(3))
		Expect(payloadFloat(*msgs, 0)).To(Equal(float64(1)))
		Expect(payloadFloat(*msgs, 1)).To(Equal(float64(2)))
		Expect(payloadFloat(*msgs, 2)).To(Equal(float64(3)))
	})

	It("catches a Benthos redelivery (fresh pointer, identical payload+meta)", func() {
		handler, msgs, cancel := newCounterStream()
		defer cancel()

		ctx := context.Background()
		// Same offset = same source record. A Kafka retry looks like this.
		Expect(handler(ctx, replayMsg("X", map[string]string{"kafka_offset": "42"}))).To(Succeed())
		Expect(handler(ctx, replayMsg("X", map[string]string{"kafka_offset": "42"}))).To(Succeed())
		// Different offset = genuinely new record.
		Expect(handler(ctx, replayMsg("X", map[string]string{"kafka_offset": "43"}))).To(Succeed())

		Eventually(func() int { return len(*msgs) }).Should(Equal(3))
		Expect(payloadFloat(*msgs, 0)).To(Equal(float64(1)))
		Expect(payloadFloat(*msgs, 1)).To(Equal(float64(2)))
		// Offset-43 sees cache=1 not 2 — replay never wrote.
		Expect(payloadFloat(*msgs, 2)).To(Equal(float64(2)))
	})

	It("distinguishes messages that differ only in meta", func() {
		handler, msgs, cancel := newCounterStream()
		defer cancel()

		ctx := context.Background()
		// Marker covers meta, not just payload — three distinct node ids, three writes.
		Expect(handler(ctx, replayMsg("val", map[string]string{"opcua_node_id": "ns=2;s=A"}))).To(Succeed())
		Expect(handler(ctx, replayMsg("val", map[string]string{"opcua_node_id": "ns=2;s=B"}))).To(Succeed())
		Expect(handler(ctx, replayMsg("val", map[string]string{"opcua_node_id": "ns=2;s=C"}))).To(Succeed())

		Eventually(func() int { return len(*msgs) }).Should(Equal(3))
		Expect(payloadFloat(*msgs, 0)).To(Equal(float64(1)))
		Expect(payloadFloat(*msgs, 1)).To(Equal(float64(2)))
		Expect(payloadFloat(*msgs, 2)).To(Equal(float64(3)))
	})
})
