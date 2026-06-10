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

// The startBroker/produce/rec/committedOffset helpers used here live in
// uns_input_nack_commit_repro_test.go (same package).

package uns_plugin

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// runUnsBetaStream starts a StreamBuilder pipeline: uns_beta input -> consumerFn.
// consumerFn's returned error NACKs the batch. Returns a stop func.
func runUnsBetaStream(t *testing.T, unsBetaYAML string, consumerFn func(context.Context, service.MessageBatch) error) (stop func()) {
	t.Helper()
	sb := service.NewStreamBuilder()
	if err := sb.AddInputYAML(unsBetaYAML); err != nil {
		t.Fatalf("input yaml: %v", err)
	}
	if err := sb.AddBatchConsumerFunc(consumerFn); err != nil {
		t.Fatalf("consumer: %v", err)
	}
	stream, err := sb.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error // written before close(done), read after <-done
	go func() { defer close(done); runErr = stream.Run(ctx) }()
	return func() {
		cancel()
		<-done
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			t.Fatalf("stream run: %v", runErr)
		}
	}
}

func TestUnsBeta_DeliversOneMessage(t *testing.T) {
	addr := startBroker(t)
	const group = "uns-beta-delivery"
	produce(t, addr, rec("umh.v1.acme.berlin.temp", `{"v":1}`))

	var mu sync.Mutex
	var got []string
	stop := runUnsBetaStream(t, `
uns_beta:
  broker_address: "`+addr+`"
  consumer_group: "`+group+`"
`, func(_ context.Context, b service.MessageBatch) error {
		mu.Lock()
		defer mu.Unlock()
		for _, m := range b {
			bs, _ := m.AsBytes()
			got = append(got, string(bs))
		}
		return nil
	})
	defer stop()

	deadline := time.Now().Add(15 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("message never arrived through uns_beta")
		}
		time.Sleep(100 * time.Millisecond)
	}
	mu.Lock()
	first := got[0]
	mu.Unlock()
	if first != `{"v":1}` {
		t.Fatalf("payload = %q", first)
	}

	// Stop the stream, then verify the ack actually committed the offset. A
	// mis-wired ack path (or a consumer_group dropped from the innerYAML
	// redpanda config built in newUnsBetaInput) would deliver fine here but
	// replay the full topic on every restart in production.
	stop()
	deadline = time.Now().Add(10 * time.Second)
	for {
		off, ok := committedOffset(t, addr, group)
		if ok && off == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("committed offset never reached 1 (off=%d, committed=%v)", off, ok)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestUnsBeta_EmptyFieldValidation(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "empty broker_address",
			yaml:    "broker_address: \"\"\nconsumer_group: \"g\"",
			wantErr: "broker_address must not be empty",
		},
		{
			name:    "empty consumer_group",
			yaml:    "broker_address: \"localhost:9092\"\nconsumer_group: \"\"",
			wantErr: "consumer_group must not be empty",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := unsBetaConfigSpec().ParseYAML(tc.yaml, nil)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			_, err = newUnsBetaInput(parsed, nil)
			if err == nil || err.Error() != tc.wantErr {
				t.Fatalf("constructor error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}
