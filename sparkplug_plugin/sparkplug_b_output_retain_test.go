//go:build !integration

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

// Unit tests for sparkplug_b_output retain-flag behavior (ENG-5003).
// Sparkplug B v3.0 §5.4 requires all non-STATE messages (NBIRTH, DBIRTH,
// NDEATH, DDEATH, LWT) to be published with retain=false; only Primary Host
// STATE messages may be retained.

package sparkplug_plugin

import (
	"context"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// recordedPublish captures the arguments passed to mqtt.Client.Publish.
type recordedPublish struct {
	topic    string
	qos      byte
	retained bool
	payload  []byte
}

// fakeMQTTToken is a no-op mqtt.Token used by fakeMQTTClient. All flows
// "succeed" immediately so the output's WaitTimeout/Error checks pass.
type fakeMQTTToken struct{}

func (t *fakeMQTTToken) Wait() bool                     { return true }
func (t *fakeMQTTToken) WaitTimeout(time.Duration) bool { return true }
func (t *fakeMQTTToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (t *fakeMQTTToken) Error() error { return nil }

// fakeMQTTClient implements paho mqtt.Client for unit-testing publish behavior.
// Publish calls are recorded; all other methods are no-ops.
type fakeMQTTClient struct {
	mu          sync.Mutex
	publishes   []recordedPublish
	isConnected bool
}

// Compile-time assertion that fakeMQTTClient satisfies mqtt.Client.
// Catches signature drift if paho.mqtt.golang changes the interface.
var _ mqtt.Client = (*fakeMQTTClient)(nil)

func (c *fakeMQTTClient) IsConnected() bool      { return c.isConnected }
func (c *fakeMQTTClient) IsConnectionOpen() bool { return c.isConnected }
func (c *fakeMQTTClient) Connect() mqtt.Token    { return &fakeMQTTToken{} }
func (c *fakeMQTTClient) Disconnect(uint)        {}

func (c *fakeMQTTClient) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, _ := payload.([]byte)
	c.publishes = append(c.publishes, recordedPublish{
		topic:    topic,
		qos:      qos,
		retained: retained,
		payload:  b,
	})
	return &fakeMQTTToken{}
}

func (c *fakeMQTTClient) Subscribe(string, byte, mqtt.MessageHandler) mqtt.Token {
	return &fakeMQTTToken{}
}

func (c *fakeMQTTClient) SubscribeMultiple(map[string]byte, mqtt.MessageHandler) mqtt.Token {
	return &fakeMQTTToken{}
}
func (c *fakeMQTTClient) Unsubscribe(...string) mqtt.Token        { return &fakeMQTTToken{} }
func (c *fakeMQTTClient) AddRoute(string, mqtt.MessageHandler)    {}
func (c *fakeMQTTClient) OptionsReader() mqtt.ClientOptionsReader { return mqtt.ClientOptionsReader{} }

func (c *fakeMQTTClient) records() []recordedPublish {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]recordedPublish, len(c.publishes))
	copy(out, c.publishes)
	return out
}

// newOutputForRetainTest builds a minimal *sparkplugOutput wired to a fake
// mqtt.Client so unit tests can exercise publishBirthMessage(), publishDBIRTH(),
// and Close() without a broker.
func newOutputForRetainTest(fake *fakeMQTTClient, groupID, edgeNodeID, deviceID string) *sparkplugOutput {
	res := service.MockResources()
	m := res.Metrics()
	return &sparkplugOutput{
		config: Config{
			MQTT: MQTT{QoS: 1},
			Identity: Identity{
				GroupID:    groupID,
				EdgeNodeID: edgeNodeID,
				DeviceID:   deviceID,
			},
		},
		logger:            res.Logger(),
		client:            fake,
		metrics:           []MetricConfig{},
		metricAliases:     map[string]uint64{},
		metricTypes:       map[string]string{},
		lastValues:        map[string]interface{}{},
		deviceLastValues:  map[string]map[string]interface{}{},
		seenDevices:       map[string]bool{},
		deviceMetrics:     map[string]map[string]uint64{},
		messagesPublished: m.NewCounter("messages_published"),
		birthsPublished:   m.NewCounter("births_published"),
		deathsPublished:   m.NewCounter("deaths_published"),
		sequenceWraps:     m.NewCounter("sequence_wraps"),
		publishErrors:     m.NewCounter("publish_errors"),
	}
}

var _ = Describe("Sparkplug B edge_node output retain flag (ENG-5003)", func() {
	It("publishes NBIRTH with retain=false per Sparkplug v3.0 §5.4", func() {
		fake := &fakeMQTTClient{}
		out := newOutputForRetainTest(fake, "FactoryA", "Line3", "")

		Expect(out.publishBirthMessage()).To(Succeed())

		records := fake.records()
		Expect(records).To(HaveLen(1))
		Expect(records[0].topic).To(Equal("spBv1.0/FactoryA/NBIRTH/Line3"))
		Expect(records[0].retained).To(BeFalse(), "NBIRTH MUST NOT be retained per Sparkplug B v3.0 §5.4")
	})

	It("publishes DBIRTH with retain=false per Sparkplug v3.0 §5.4", func() {
		fake := &fakeMQTTClient{}
		out := newOutputForRetainTest(fake, "FactoryA", "Line3", "")

		Expect(out.publishDBIRTH("dev01", map[string]interface{}{})).To(Succeed())

		records := fake.records()
		Expect(records).To(HaveLen(1))
		Expect(records[0].topic).To(Equal("spBv1.0/FactoryA/DBIRTH/Line3/dev01"))
		Expect(records[0].retained).To(BeFalse(), "DBIRTH MUST NOT be retained per Sparkplug B v3.0 §5.4")
	})

	It("publishes NDEATH with retain=false on Close per Sparkplug v3.0 §5.4", func() {
		fake := &fakeMQTTClient{isConnected: true}
		out := newOutputForRetainTest(fake, "FactoryA", "Line3", "")

		Expect(out.Close(context.Background())).To(Succeed())

		records := fake.records()
		Expect(records).To(HaveLen(1))
		Expect(records[0].topic).To(Equal("spBv1.0/FactoryA/NDEATH/Line3"))
		Expect(records[0].retained).To(BeFalse(), "NDEATH MUST NOT be retained per Sparkplug B v3.0 §5.4")
	})

	It("publishes DDEATH with retain=false on Close when DeviceID is set", func() {
		fake := &fakeMQTTClient{isConnected: true}
		out := newOutputForRetainTest(fake, "FactoryA", "Line3", "dev01")

		Expect(out.Close(context.Background())).To(Succeed())

		records := fake.records()
		Expect(records).To(HaveLen(1))
		Expect(records[0].topic).To(Equal("spBv1.0/FactoryA/DDEATH/Line3/dev01"))
		Expect(records[0].retained).To(BeFalse(), "DDEATH MUST NOT be retained per Sparkplug B v3.0 §5.4")
	})
})
