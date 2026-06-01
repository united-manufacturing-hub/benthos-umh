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

//go:build integration

package open_protocol_plugin_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	_ "github.com/united-manufacturing-hub/benthos-umh/open_protocol_plugin"
)

// These tests run the real (GPL) Atlas Copco Open Protocol emulator inside a
// container and drive the plugin against it, validating the wire format against
// code we did not write. They are gated behind TEST_OPEN_PROTOCOL=true and
// require Docker (the image clones the emulator at build time).
var _ = Describe("open_protocol integration (dockerized emulator)", Serial, func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		container testcontainers.Container
		endpoint  string
	)

	BeforeEach(func() {
		if os.Getenv("TEST_OPEN_PROTOCOL") == "" {
			Skip("Skipping integration test: set TEST_OPEN_PROTOCOL=true to run")
		}

		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)

		req := testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context:    "testdata/emulator",
				Dockerfile: "Dockerfile",
				KeepImage:  true,
			},
			ExposedPorts: []string{"4545/tcp"},
			WaitingFor:   wait.ForLog("listening on").WithStartupTimeout(60 * time.Second),
		}
		var err error
		container, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		Expect(err).NotTo(HaveOccurred())

		host, err := container.Host(ctx)
		Expect(err).NotTo(HaveOccurred())
		port, err := container.MappedPort(ctx, "4545")
		Expect(err).NotTo(HaveOccurred())
		endpoint = fmt.Sprintf("%s:%s", host, port.Port())
	})

	AfterEach(func() {
		if container != nil {
			_ = container.Terminate(context.Background())
			container = nil
		}
		if cancel != nil {
			cancel()
		}
	})

	It("receives and natively decodes a real MID 0061 tightening result", func() {
		var mu sync.Mutex
		var got []map[string]any

		builder := service.NewStreamBuilder()
		Expect(builder.AddInputYAML(fmt.Sprintf(`
open_protocol:
  endpoint: "%s"
  subscribe: [last_tightening]
  keepalive_interval: 5s
  request_timeout: 10s
`, endpoint))).To(Succeed())

		Expect(builder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
			mid, _ := m.MetaGet("op_mid")
			if mid != "0061" {
				return nil
			}
			structured, err := m.AsStructured()
			if err != nil {
				return nil
			}
			if obj, ok := structured.(map[string]any); ok {
				mu.Lock()
				got = append(got, obj)
				mu.Unlock()
			}
			return nil
		})).To(Succeed())

		stream, err := builder.Build()
		Expect(err).NotTo(HaveOccurred())
		go func() {
			defer GinkgoRecover()
			_ = stream.Run(ctx)
		}()

		// The emulator pushes a 0061 on a timer once subscribed (default ~5s).
		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return len(got)
		}, 90*time.Second, 250*time.Millisecond).Should(BeNumerically(">=", 1))

		mu.Lock()
		first := got[0]
		mu.Unlock()
		// Sanity-check that the native decode produced the expected fields.
		Expect(first).To(HaveKey("tightening_ok"))
		Expect(first).To(HaveKey("torque_actual"))
		Expect(first).To(HaveKey("tightening_id"))

		_ = stream.StopWithin(5 * time.Second)
	})
})
