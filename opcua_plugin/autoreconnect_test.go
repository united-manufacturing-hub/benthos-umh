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

package opcua_plugin

import (
	"context"
	"time"

	"github.com/gopcua/opcua/ua"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/opcua_plugin/internal/faultopcua"
)

// A reconnect rebuilds the session without logging anything, so the only
// evidence of one is a new dial on the server.
var _ = Describe("autoReconnect: false", func() {
	It("does not redial after the server drops the session", func(ctx SpecContext) {
		srv, err := faultopcua.New()
		Expect(err).NotTo(HaveOccurred())
		srv.AddNode("Flag", true)
		Expect(srv.Start(ctx)).To(Succeed())
		DeferCleanup(srv.Close)

		conn := &OPCUAConnection{
			Endpoint:      srv.Endpoint(),
			Insecure:      true,
			DirectConnect: true,
			AutoReconnect: false,
			Log:           service.MockResources().Logger(),
		}
		Expect(conn.connect(ctx)).To(Succeed())
		DeferCleanup(func() { _ = conn.Close(context.Background()) })

		dialsAfterConnect := srv.Dials()
		Expect(dialsAfterConnect).To(BeNumerically(">", 0), "the server saw no dial")

		srv.Inject(faultopcua.Fault{Node: "Flag", Kind: faultopcua.DropSession})
		id, err := ua.ParseNodeID(srv.NodeID("Flag"))
		Expect(err).NotTo(HaveOccurred())
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, _ = conn.Client.Read(readCtx, &ua.ReadRequest{
			NodesToRead: []*ua.ReadValueID{{NodeID: id, AttributeID: ua.AttributeIDValue}},
		})
		cancel()
		Expect(srv.Cuts()).To(BeNumerically(">", 0), "the server dropped no connection")

		// gopcua's first redial after a drop is immediate.
		Consistently(srv.Dials, 1500*time.Millisecond, 100*time.Millisecond).
			Should(Equal(dialsAfterConnect), "the client redialled")
	})
})
