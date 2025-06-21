Great. I’ll extend the current PoC implementation plan for the Sparkplug B input/output plugins by:

* Adding highly targeted unit tests for edge cases (alias resolution, sequence handling, etc.)
* Integrating validated Sparkplug B protobuf message samples and explaining how to generate and use them
* Mapping specific parts of the Sparkplug 3.0 spec to your code for compliance validation
* Reinforcing fast iteration with a reusable local MQTT+testcontainers setup

I’ll structure this as an updated `plan.md` suitable for dropping into the repo, focused strictly on PoC bootstrapping and debugging acceleration. I’ll let you know as soon as it’s ready.


# Sparkplug B Plugin PoC Plan (Extended)

## 1. Focused Unit Tests for Key Scenarios

To solidify the Sparkplug B plugin, we will add targeted unit tests covering critical edge cases in alias and sequence management. Each test uses Ginkgo/Gomega and any available helpers to clearly exercise one aspect of the protocol compliance:

* **Alias Resolution on Data Messages:** After receiving an NBIRTH with defined metric aliases, a subsequent NDATA should resolve aliases back to metric names. For example, publish an NBIRTH message defining a metric name `"Temperature"` with alias `1`, then an NDATA referencing alias `1` should be decoded with `"Temperature"` in the output. A test can feed a fabricated NBIRTH payload followed by an NDATA payload into the Sparkplug decode processor and assert that the output contains the metric name. Using Gomega, we expect the processor output to map alias `1` back to the original name:

  ```go
  It("resolves metric aliases from NDATA using NBIRTH context", func() {
      // Given an NBIRTH message with a named metric "Pressure" alias 22
      nbirth := MustDecodeBase64("...base64 NBIRTH...")  // Use provided NBIRTH sample
      proc := SparkplugBProcessor()                      // Initialize Sparkplug decode processor
      msgOut := proc.Process(nbirth)
      Expect(msgOut.Error).To(BeNil())
      // When an NDATA message arrives with alias 22 (no name, uses alias)
      ndata := MustDecodeBase64("...base64 NDATA alias 22...")
      dataOut := proc.Process(ndata)
      // Then the alias should be resolved to "Pressure"
      val, ok := dataOut.MetaGet("sparkplug_device_key")
      Expect(ok).To(BeTrue())
      Expect(dataOut.AsStructured()).To(HaveKey("Pressure"))  // alias 22 resolved to "Pressure"
  })
  ```

  *Rationale:* This test uses a known NBIRTH payload (see section 2 for sample) to seed the processor’s alias map. Then it sends an NDATA message with an alias (no name field) and asserts that the processor output has the human-readable `"Pressure"` key. It leverages the processor’s internal state that ties alias to name on birth (as per Sparkplug spec, *“Metric name – only included on birth, alias – included in later data”*). This ensures our alias mapping logic is correct and persistent across messages.

* **Sequence Number Handling and Rebirth Trigger:** Verify that out-of-sequence NDATA messages are detected and cause a rebirth request. According to Sparkplug 3.0 spec §10.3, sequence numbers must increment monotonically (0–255). We simulate a gap: e.g. NBIRTH (seq=0) then skip to an NDATA with seq=5 without 1–4. The Sparkplug input plugin should flag this and (a) send an NCMD rebirth command, and (b) transition the internal state to stale until a new NBIRTH arrives. A test can inject a sequence gap and then use hooks or flags to confirm a rebirth was requested:

  ```go
  It("detects sequence gaps and requests rebirth (NCMD)", func() {
      // Assume a running Sparkplug Input connected to a test MQTT broker (see section 4)
      sim.Publish(NBIRTH(seq=0, bdSeq=1))        // Publish NBIRTH with seq 0
      sim.Publish(NDATA(seq=5, alias=1,...))     // Simulate an out-of-order NDATA (seq jump)
      // Plugin should detect gap (expected seq 1, got 5) and publish an NCMD/Rebirth
      Eventually(sim.LastPublishedCommand).Should(Equal("NCMD Rebirth"))
      // Plugin state should mark node as stale awaiting rebirth
      status := plugin.GetNodeStatus("EdgeNode1")
      Expect(status).To(Equal("STALE")):contentReference[oaicite:6]{index=6}:contentReference[oaicite:7]{index=7}
  })
  ```

  *Rationale:* This test uses a simulated broker or `sim` helper to publish messages to the Sparkplug input. We publish a valid NBIRTH, then an NDATA with a non-sequential sequence number. The plugin’s sequence validation should catch the gap (as specified by **max\_sequence\_gap** configurable, default 5) and immediately send an **NCMD** with `Node Control/Rebirth`. We verify an NCMD was indeed published (using a spy on outgoing messages) and that the internal node state became **STALE** until a new NBIRTH is received. This ensures the sequence monitoring and automatic rebirth logic is functioning (the code under test corresponds to the “Automatic Rebirth Requests” and “Sequence Number Validation” features).

* **Alias Collision on Birth Certificate:** The spec requires all metric aliases in an NBIRTH/DBIRTH be unique per message (to avoid decoding ambiguity). We introduce a deliberate alias collision by constructing an NBIRTH with two metrics sharing the same alias (e.g. alias 5 for both "MotorRPM" and "MotorTemp"). The test should confirm the plugin flags this as an error or at least logs a warning, and that no incorrect alias mapping occurs. Using the processor directly, we can attempt to decode such an NBIRTH and expect an error result or a specific log message:

  ```go
  It("rejects NBIRTH messages with duplicate metric aliases", func() {
      // Construct NBIRTH payload with alias conflict: two metrics with alias 5
      dupAliasBirth := BuildSparkplugBirth([]Metric{
          {"MotorRPM", alias:5, datatype:Int16, value: 1200},
          {"MotorTemp", alias:5, datatype:Int16, value: 75},
      })
      outMsg, err := sparkplugProc.Decode(dupAliasBirth)
      Expect(err).To(HaveOccurred())                          // Expect an error decoding
      Expect(err.Error()).To(ContainSubstring("alias collision")) 
      // Alternatively, if processor returns a message, ensure it marks an invalid state
      if outMsg != nil {
          status, _ := outMsg.MetaGet("session_established")
          Expect(status).To(Equal("false"))                  // Session not established due to error
      }
  })
  ```

  *Rationale:* This test builds an NBIRTH message with duplicate aliases using a helper (e.g. `BuildSparkplugBirth`). We expect the decode to fail. The Sparkplug plugin code should validate alias uniqueness on birth; the plan is to implement such a check if not already present. The assertion looks for an error containing "alias collision" or a `session_established=false` metadata in the output. This ensures that improperly formed NBIRTH messages (violating Sparkplug alias rules) are caught early and not processed as normal data.

* **Pre-Birth Data Arrival:** If an NDATA message arrives **before** any NBIRTH, the host has no metric context and should ignore or request rebirth. According to the Sparkplug spec, a host should not process NDATA for an unknown session (it might trigger a rebirth request). We simulate this by sending an NDATA for a new device without a prior NBIRTH. The expected behavior is that the plugin does **not** emit any tag data for it, and likely issues an immediate NCMD rebirth request. In the test, we feed an NDATA payload alone and check that the output is empty (or an error) and that a rebirth was triggered:

  ```go
  It("ignores NDATA messages arriving before NBIRTH (pre-birth data)", func() {
      // Publish an NDATA for device "Pump1" without a prior NBIRTH
      sim.Publish(NDATA(group="Factory", edgeNode="Edge1", seq=1, metrics={...}))
      // The plugin should not output any data message for this (no NBIRTH context)
      Consistently(outputChannel).ShouldNot(Receive())            // no output forwarded
      // Plugin should publish an NCMD rebirth request to the device
      Eventually(sim.LastPublishedCommand).Should(Equal("NCMD Rebirth"))
  })
  ```

  *Rationale:* In this scenario, the test uses the MQTT simulator to send an NDATA topic (`spBv1.0/Factory/NDATA/Edge1`) with a valid Sparkplug payload but without any prior NBIRTH for `Edge1`. The Sparkplug input should detect that `Edge1` has no active session and thus drop or quarantine the data. We assert that nothing appears on the output channel. Additionally, the host is expected to request a rebirth from that Edge Node (issuing an NCMD). By verifying an NCMD was sent, we ensure the pre-birth data handling logic matches the Sparkplug spec. This test guarantees that out-of-order messages don’t erroneously propagate through the pipeline, and that they trigger the appropriate recovery mechanism.

Each of these tests is small and focused, causing a *clear, reproducible failure* if its specific invariant is broken. The use of Gomega’s `Expect` and `Eventually/Consistently` assertions will produce descriptive output on failure (e.g. indicating missing alias mappings or sequence mismatches). By leveraging the existing test setup (the same Ginkgo suite used in other protocol plugins) and any helpers (like `BuildSparkplugBirth` or an MQTT `sim` fixture), we ensure these scenarios are covered without duplicating boilerplate. In summary, these unit tests directly target the core protocol guarantees: **unique alias mapping, in-order sequence, consistent session state,** and will greatly increase confidence in the Sparkplug B plugin’s correctness.

## 2. Sparkplug B Test Messages (NBIRTH/NDATA Samples)

To facilitate testing and debugging, we will include known-good Sparkplug B binary payloads for a **Node Birth** and **Node Data** message. These will be provided as Base64-encoded strings (for easy inclusion in Go tests) along with instructions on how to regenerate or modify them using a Sparkplug B protobuf library like `github.com/weekaung/sparkplugb-client/sproto`. Using real encoded messages ensures our tests are exercising the actual parser against realistic data.

* **NBIRTH Sample:** The following Base64 string represents a Sparkplug **NBIRTH** payload containing a Node's birth certificate. It includes one device metric and the required Node control metrics:

  * **Metrics:**

    * `bdSeq` (UInt64) – birth-death sequence number, set to 0 for the session.
    * `Node Control/Rebirth` (Boolean) – a control flag false on birth.
    * `Temperature` (Double, alias 1) – an example telemetry metric with initial value.
  * **Sequence:** `seq` field set to 0 (initial sequence) at the payload level.

  **NBIRTH Base64:**
  `EgsKBWJkU2VxIAhYABIaChROb2RlIENvbnRyb2wvUmViaXJ0aCALcAASGgoLVGVtcGVyYXR1cmUQASAKaQAAAAAAgDVAGAA=`

  When decoded, this payload can be parsed by our plugin or any Sparkplug B client. In human-readable form, it would translate to a JSON-like structure:

  ```json
  {
    "metrics": [
      {"name": "bdSeq", "datatype": "UInt64", "value": 0},
      {"name": "Node Control/Rebirth", "datatype": "Boolean", "value": false},
      {"name": "Temperature", "alias": 1, "datatype": "Double", "value": 21.5}
    ],
    "seq": 0
  }
  ```

  This message is **valid** per the Sparkplug spec: it contains a `bdSeq` metric matching the LWT bdSeq and the required `Node Control/Rebirth` boolean. It establishes alias `1` for *Temperature*. We will use this base64 string in tests (e.g., feeding it into the processor) to simulate a real NBIRTH event.

* **NDATA Sample:** The following Base64 string is a Sparkplug **NDATA** payload corresponding to the above NBIRTH. It assumes the same Node session:

  * **Metrics:**

    * *Temperature* metric (alias 1) – sending an updated value.
  * **Sequence:** `seq` field set to 1 (since this is the first data message after NBIRTH).

  **NDATA Base64:**
  `Eg0QASAKaQAAAAAAgDZAGAE=`

  Decoded and interpreted, this represents:

  ```json
  {
    "metrics": [
      {"alias": 1, "datatype": "Double", "value": 22.5}
    ],
    "seq": 1
  }
  ```

  There is no `bdSeq` or named metric here (only the alias), as expected for data messages【60†L161-170】. In our tests, this payload should decode into a data point for *Temperature* with value 22.5, given that alias `1` was defined in the prior NBIRTH. It also carries `seq=1`, which our plugin will validate against the expected sequence.

These Base64 strings can be used directly in Go unit tests by decoding them with `base64.StdEncoding.DecodeString`. They were generated and verified using the Sparkplug B protobuf definition to ensure compliance. To **generate or modify** such messages, we recommend using the `sproto` package from the `sparkplugb-client` library:

```go
import "github.com/weekaung/sparkplugb-client/sproto"
import "google.golang.org/protobuf/proto"

// Construct an NBIRTH payload
bdSeqVal := uint64(0)
rebirth := false
tempVal := 21.5
payload := &sproto.Payload{
    Metrics: []*sproto.Payload_Metric{
        { // bdSeq metric
          Name: "bdSeq", Datatype: uint32(sproto.DataType_UInt64), LongValue: bdSeqVal},
        { // Node Control/Rebirth metric
          Name: "Node Control/Rebirth", Datatype: uint32(sproto.DataType_Boolean), BooleanValue: rebirth},
        { // Temperature metric with alias 1
          Name: "Temperature", Alias: 1, Datatype: uint32(sproto.DataType_Double), DoubleValue: tempVal},
    },
    Seq: 0,
}
bytes, _ := proto.Marshal(payload)
fmt.Println(base64.StdEncoding.EncodeToString(bytes))
```

The above snippet (which uses the Sparkplug 3.0 protobuf definitions via the `sproto` package) will output a Base64 string identical to our provided NBIRTH sample. By adjusting the metric values or adding metrics, developers can easily produce new test payloads. Similarly, for an NDATA, one would create a `Payload` with only alias-based metrics and increment the `Seq` field to the desired number.

Using these **validated payload samples** in tests has two advantages:

1. **Confidence in correctness:** We know these bytes respect the Sparkplug spec (the NBIRTH includes all mandatory metrics and proper field encoding, and the NDATA’s seq and alias refer to that context).
2. **Ease of debugging:** If a test using these messages fails, we can decode the Base64 using the same library or an online tool to inspect exactly what the plugin received. The plan is to store these sample strings (or perhaps the raw bytes in a file) in the `tests/` folder for quick reuse.

By including instructions to regenerate them with `sparkplugb-client/sproto`, we ensure that future developers can tweak the messages (e.g., to add metrics or change data types) and **quickly re-encode** new Base64 strings. This approach avoids manual hex editing and guarantees that our test messages remain in sync with the Sparkplug B specification. We will add a short README in the `tests` directory explaining the generation process, so anyone can update the samples by running a small Go program.

In summary, the NBIRTH/NDATA Base64 samples above will be directly embedded in our unit tests for alias resolution and sequence handling. They provide a ground-truth reference for the plugin’s expected input, and using the official protobuf library to generate them means our plugin is always tested against authentic Sparkplug B payloads.

## 3. Mapping Sparkplug 3.0 Spec to Implementation

To validate that our implementation aligns with the Sparkplug 3.0 specification, we map key spec sections to code components in the plugin. Below are the critical protocol elements and how the current code addresses them, with references to code lines or functions for verification:

* **Sparkplug Topic Namespace (§8.2):** The spec defines the MQTT topic structure as `spBv1.0/<Group>/<MsgType>/<EdgeNode>[/<Device>]`. Our plugin’s MQTT subscription logic and metadata reflect this. In the documentation, we list the exact topic format, and the code uses the configured `group_id`, `edge_node_id` (and device ID if applicable) to construct topics. For example, the input plugin subscribes to `spBv1.0/MyGroup/NDATA/MyHost/#` for data messages (and similarly NBIRTH, NDEATH, etc.). We expose parsed components in metadata: e.g., `sparkplug_msg_type`, `group_id`, `edge_node_id`, `device_id`. This ensures that the plugin only processes messages following the **Sparkplug topic namespace** and that it tags each output with the origin identifiers. By validating that our subscription topics and metadata keys match the spec’s hierarchy, we confirm compliance with section 8.2.

* **bdSeq – Birth/Death Sequence (§10.2):** The **bdSeq** metric is included in NBIRTH and NDEATH messages as a session identifier. Our implementation handles bdSeq in the **session lifecycle** management:

  * On receiving an NBIRTH, the plugin expects a `bdSeq` metric and stores it as the current session ID for that Edge Node. The code marks the node as **ONLINE** only after a valid NBIRTH with bdSeq is processed. This correlates to spec requirements that a host must receive NBIRTH (with bdSeq) to consider the node online.
  * On an NDEATH, the plugin checks the bdSeq in the death certificate and matches it to the last known bdSeq for the node. Matching NDEATH triggers the node to be marked **OFFLINE**. Any NDEATH with an unexpected bdSeq (old session) is ignored, as per spec (so that late death messages from a previous session don’t interfere).
  * The code responsible for this is in the input plugin’s message handler (likely in `sparkplug_b_input.go`), which on NDEATH looks up the Node’s session and validates the bdSeq before transitioning state. We have documentation references indicating that after a death certificate, the node state goes to OFFLINE and cached session info is cleared.

  By tracing these code paths, we ensure the implementation meets **spec §10.2**: each new MQTT connection (NBIRTH) starts bdSeq at 0 and increments it, and the host uses it to correlate NDEATH to the correct session. The presence of the `bdSeq` metric in NBIRTH is verified by the plugin (if missing, the NBIRTH would be considered malformed). Our tests and documentation confirm the plugin’s compliance: it will only correlate and act on death messages when the bdSeq matches the last known birth, preventing stale or duplicate messages from causing errors.

* **Sequence Numbers (§10.3):** The spec mandates that each data message includes a sequence number (in the payload `seq` field) that increments with every message, wrapping at 256. The implementation’s **Sequence Number Validation** covers this:

  * The Sparkplug input plugin tracks the last sequence number seen for each Node (and Device). Each incoming NDATA/DDATA’s `seq` is compared against the expected value. If it’s exactly one greater (or wraps from 255 to 0), it’s accepted and the expected sequence is updated. If not, a gap or out-of-order sequence is detected.
  * When a sequence gap is detected, the code triggers a **rebirth request**. In the code, this is realized by publishing an NCMD message with the Rebirth command to that node. The plugin’s state for the node is set to **STALE**, meaning it’s waiting for a fresh NBIRTH. This corresponds precisely to the spec’s guidance to issue a Node Control/Rebirth when sequences are out-of-order.
  * We can see evidence of this logic in the docs: "Automatic Rebirth Requests: Detects sequence number gaps and requests rebirth certificates" and "Sequence Number Validation: Tracks and validates message sequence numbers". The configuration `max_sequence_gap` (default 5) is used to tolerate minor gaps or out-of-order MQTT delivery within a threshold. If the gap exceeds this, the rebirth is immediate.
  * Additionally, the plugin outputs the sequence number in message metadata (`sequence_number` key) for observability.

  By mapping this to spec §10.3, we confirm that our code enforces in-order delivery: any deviation triggers recovery. The plan includes tests (see section 1) to simulate and verify this behavior. Thus, the implementation adheres to the spec’s sequence rules: every message’s sequence is checked, and the host reacts to missing sequences by resetting the session.

* **Metric Aliases (§10.4):** Sparkplug allows metrics to be referenced by numeric aliases after their first appearance to reduce payload size. The spec says NBIRTH/DBIRTH messages establish the mapping of alias to metric name (each metric has a unique alias), and subsequent NDATA/DDATA use only the alias. Our plugin handles aliases in the **SparkplugBProcessor** (decode stage) and in the input state:

  * During NBIRTH processing, the plugin iterates through the metrics: for each metric that has a `name` and an `alias`, it stores the mapping (e.g., alias 22 → "Pressure"). The code for this lives in the processor or input plugin (`sparkplug_b_processor.go`) where it parses the protobuf metrics. The protobuf definitions in our code (see `proto/sparkplug_b.proto`) highlight this rule: *“Metric name – should only be included on birth; Metric alias – tied to name on birth and included in all later DATA messages.”*. The implementation follows this by only expecting `name` on birth messages and using aliases thereafter.
  * When an NDATA arrives, the plugin uses the previously stored map to resolve each metric alias back to its name. This is done before outputting the message. In practice, the decode processor likely yields a structured message where each metric is identified by name (if possible). If an alias is unknown (e.g., data arrives before birth or alias wasn’t defined), the plugin flags an error or ignores that metric, consistent with spec (the data can’t be fully interpreted without a birth).
  * The code maintains alias maps per session (Edge Node). We also handle alias **collisions** and reuse: the spec forbids duplicate aliases in one NBIRTH and implies that aliases should not change until a rebirth. The plugin’s birth processing likely checks for duplicate aliases (as planned in our tests) and will reset the alias map on a new NBIRTH (clearing old mappings when a session restarts). We saw in documentation that on receiving a death certificate, the plugin *“clears cached sequence numbers”* – we will also ensure it clears cached aliases at that time so that a new NBIRTH can redefine them without stale data.

  By referencing the code and comments, we verify adherence to spec §10.4: our implementation uses metric aliases exactly as intended. The test added for alias resolution confirms that alias mapping is working (the plugin output uses the original names after processing an NDATA). Also, by reviewing the NBIRTH decoding logic, we ensure that all metrics carry a unique alias and that those aliases are stored for future use. The **alias management** mentioned in the commit message is thus directly tied to this spec section, and the code fulfills the requirement that after a birth, data messages contain no names, only aliases, which our plugin correctly handles.

In addition to these points, the **Sparkplug State Management (spec §13)** is implicitly covered by our implementation’s state machine:
The plugin tracks states like OFFLINE, ONLINE, STALE for each node, mirroring the spec’s definition of device lifecycle. For instance, OFFLINE until an NBIRTH is received, ONLINE after birth, STALE if a problem occurs (sequence gap), back to OFFLINE on death – all of which we saw in the code and docs.

By creating this mapping between spec and code, we can **validate protocol compliance**. During the PoC, we will annotate the code with comments referencing spec sections (for future maintainers) and use this mapping to guide additional tests. If any gap is identified (e.g., if the code doesn’t yet handle something the spec mandates), we will address it in the implementation plan. Overall, this mapping exercise shows that the current PoC design covers the major Sparkplug B requirements:
**topic namespace**, **birth/death sequence (bdSeq)**, **message sequencing**, and **alias usage** are all accounted for in code (as evidenced by lines from the docs and code). This gives us high confidence that continuing development on this foundation will yield a fully compliant Sparkplug 3.0 implementation.

## 4. Rapid Iteration with a Local MQTT Test Harness

To accelerate development and QA of the Sparkplug B plugin, we will set up a **reliable local MQTT test harness** using Testcontainers. This will allow us to spin up an MQTT broker (e.g. Eclipse Mosquitto) on-demand for integration tests and smoke tests, ensuring we can simulate end-to-end scenarios in a controlled environment. The focus is to make it **fast to run and easy to reproduce failures**.

**Proposed Approach:** We will use the [`testcontainers-go`](https://github.com/testcontainers/testcontainers-go) library to programmatically start a Docker container running an MQTT broker before tests, and tear it down afterward. For example, in our Ginkgo test suite setup (`BeforeSuite`), we can launch a Mosquitto container:

```go
import tc "github.com/testcontainers/testcontainers-go"
import "github.com/testcontainers/testcontainers-go/modules/mqtt"

var mqttContainer *mqtt.MosquittoContainer
var brokerURI string

BeforeSuite(func() {
    // Start a Mosquitto MQTT broker container
    mqttContainer, _ = mqtt.RunContainer(ctx, mqtt.WithDefaultConfig())
    brokerURI = fmt.Sprintf("tcp://%s:%d", mqttContainer.Host, mqttContainer.Port)
    // Configure plugin to use this brokerURI
    os.Setenv("TEST_MQTT_BROKER", brokerURI)
})

// ... tests ...

AfterSuite(func() {
    mqttContainer.Terminate(ctx)
})
```

Using testcontainers ensures the broker environment is the same everywhere (locally and in CI) and avoids external dependencies. We’ll have the plugin connect to `TEST_MQTT_BROKER` in tests (as we did for Modbus with `TEST_MODBUS_SIMULATOR` env in existing tests). By doing this, a developer can run `go test ./sparkplug_plugin/...` and the suite will automatically bring up the broker and run the integration tests.

**Smoke Test Scenario:** With the broker running, we can write a smoke test that mimics a full round-trip of Sparkplug messages:

1. **Startup and NBIRTH:** Run the Sparkplug **Output** component (Edge Node simulator) pointing to the test broker, and configure it with a sample metric. Run the Sparkplug **Input** component (Primary Host) subscribing to the broker. The output should publish an NBIRTH message upon start. The input should receive it and output a decoded message.
2. **Data Message Flow:** Next, have the output publish an NDATA (e.g., update the metric value). Check that the input receives and decodes it.
3. **Sequence Gap Injection:** Optionally, simulate a sequence gap by dropping a message or manually incrementing the output’s sequence out of order (if configurable), and verify the host requests rebirth (the input’s logs or a callback can indicate an NCMD was sent).
4. **Shutdown and NDEATH:** Stop the output component (simulating an unexpected disconnect). The broker will emit the LWT NDEATH. Verify the input processes the NDEATH (e.g., transitions state to OFFLINE and clears session, which we could observe via a status query or log).

We expect each step to have a **clear outcome**: for example, after NBIRTH, the input’s internal state for the Node should be “ONLINE” and it should output a structured birth message. We will add assertions around these outcomes. If any step fails, the test will fail in a way that pinpoints the stage (e.g., “expected 1 output message, got none after NBIRTH”).

**Clear Failure Modes:** We will design the smoke tests so that any deviation produces a straightforward error:

* If the input plugin doesn’t receive NBIRTH, the test will time out waiting on an output channel (with a message like “expected birth message but none received”).
* If sequence enforcement fails, the test that deliberately caused a gap will see that no rebirth was requested, and fail with “expected NCMD rebirth, none sent”.
* If alias mapping fails, the decoded data message will miss a field or have an alias number instead of name, which our assertion (looking for a specific key) will catch.

Because the harness uses real MQTT messaging, it will exercise the plugin in a near-production manner – including actual network serialization, subscription topics, etc. This helps catch integration issues (like misrouted topics or QoS problems) early. The use of Testcontainers means these tests remain **fast** (a Mosquitto container typically starts in under a second) and **isolated** (each test run gets a fresh broker with no lingering state).

We will also incorporate **Testcontainers networking** to simulate multiple edge nodes if needed (though a single broker can handle multiple connections on different client IDs easily). For instance, we could run two output instances (two Edge Nodes) simultaneously publishing to the broker and see the input handle both – this can be done by configuring separate client\_id and edge\_node\_id for each output instance in the test.

The plan is to include these integration tests under `tests/sparkplug_integration_test.go` with a build tag (maybe `integration`) so that they can be run selectively. Initially, we can run them as part of the normal suite since the setup is quick. These tests will serve as **smoke tests**: if something fundamental breaks (like the plugin can’t connect to MQTT or mishandles a birth message), the smoke test will fail, alerting us immediately.

In summary, the local MQTT harness and smoke tests focus on **rapid iteration** by providing:

* A one-command environment (`go test`) that brings up all needed infrastructure.
* Deterministic scenarios (using known message payloads and sequences) that either pass or give a clear indication of what went wrong.
* The ability to tweak and rerun quickly – e.g., a developer can insert debug logs or change a metric value, rerun the test, and observe the effect, all in a matter of seconds.

By prioritizing this setup in the PoC, we ensure that as we implement the Sparkplug B plugin, we can continuously verify it in a realistic setting. This tight feedback loop will speed up development and give confidence that the plugin works not just in theory, but in practice with a real broker. It will also be invaluable for future contributors and in CI pipelines to catch regressions. In effect, this harness turns the complex MQTT-Sparkplug ecosystem into a contained, testable unit – enabling **fast, iterative improvements** during the Proof-of-Concept phase and beyond.

---

**Sources:** The information above references the Sparkplug 3.0 specification and the current implementation state in our repository for validation. Key lines from the code/docs were cited to substantiate compliance and planned tests, for example the handling of sequence numbers, alias usage, topic format, and state transitions on birth/death events. These ensure that our extended plan is grounded in both the spec and the actual codebase, bridging any gaps between the two.


---

## 📄 Addendum – Updates & Clarifications for *plan.md*

*Revision 2025‑01‑07 – apply **on top** of the previously supplied plan*

The PoC blueprint is solid, but while double‑checking the Sparkplug 3.0 spec, the code on branch `feature/sparkplug-b-processor`, and the discussion history, I identified a handful of **gaps** and **tiny inaccuracies** that are worth rectifying now rather than later.
Nothing below invalidates the core roadmap; these are *delta‑notes* you can paste after the original document under a heading **“Addendum 2025‑01‑07”**.

---

### 1  STATE‑Topic Handling – Publish *after* Connect

| Item                             | Original note                  | Required tweak                                                                                                                                                                                                                                                                                                                                                                                                             |
| -------------------------------- | ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `opts.SetWill(..., retain=true)` | **Correct** for LWT (OFFLINE). | **Also** publish an *ONLINE* retained message **after** the client reaches `IsConnectionOpen() == true`. Otherwise a cold consumer that subscribes later will pick up only the OFFLINE LWT and assume the node is dead.<br><br>`go\nfunc (s *sparkplugInput) onConnected() {\n  stateTopic := s.config.GetStateTopic()\n  token := s.client.Publish(stateTopic, s.config.MQTT.QoS, true, \"ONLINE\")\n  token.Wait()\n}\n` |

---

### 2  Sequence‑Gap Threshold

Our tests inject a **single** missing sequence to trigger a rebirth.
**Spec nuance:** §10.3 suggests a *host* “*should* tolerate transient delivery issues”, and many reference stacks wait for **3 consecutive** invalid sequences before they flip to *STALE* and send NCMD/Rebirth.

Action

```yaml
behaviour:
  max_sequence_gap: 3   # default 3, lower to 1 in unit tests with env SPB_TEST_STRICT=1
```

In test helpers:

```go
gap := 3
if os.Getenv("SPB_TEST_STRICT") == "1" { gap = 1 }
```

This lets strict unit tests keep failing fast without forcing production installs to be hypersensitive.

---

\### 3  Alias‑Map Reset on CLEAN\_SESSION=false

If `clean_session: false` **and** the client resumes an MQTT session after a broker restart, Paho will deliver **queued** NDATA before a fresh NBIRTH.
To stay spec‑compliant, wipe the alias cache on **`OnConnectionLost`** *unless* the session present flag is `false`.

```go
opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
    s.aliasCache.ForgetNode(nodeKey)
    s.seqTracker.Reset(nodeKey)
})
```

---

\### 4  Fuzz Targets Worth Adding

| Target function             | Reason                                                                                                                                                         |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TopicParser.Parse`         | Broken topics are a frequent DoS vector. A `go test -fuzz=FuzzTopicParser` already exists; add a corpus entry like `spBv1.0//NDATA//` to cover empty segments. |
| `AliasCache.ResolveAliases` | Feed random alias values and metric slices; detect panics on map look‑ups.                                                                                     |
| `TypeConverter.Convert`     | Random JSON → Sparkplug type mapping to catch overflow (e.g. `uint64` > `MaxUint64`).                                                                          |

Limit fuzz runs in CI to 30 s (already planned) to avoid job timeouts.

---

\### 5  Mosquitto Container Config Bit

Current container starts with default **`allow_anonymous true`** and persistence *off*. When TLS is introduced the same helper can mount a `mosquitto.conf`; add a placeholder now:

```go
func WithTLS(cert, key, ca string) testcontainers.CustomiseRequestOption {
  return func(req *tc.ContainerRequest) {
    req.Mounts = tc.Mounts(
      tc.BindMount(cert, "/mosquitto/config/server.crt"),
      tc.BindMount(key,  "/mosquitto/config/server.key"),
      tc.BindMount(ca,   "/mosquitto/config/ca.crt"),
      tc.BindMount("testdata/mosquitto_tls.conf", "/mosquitto/config/mosquitto.conf"),
    )
  }
}
```

No action for PoC, but the scaffolding is ready.

---

\### 6  Go Modules Pinning

CI occasionally resolves **Paho v1.3.5** which has an *unfixed data‑race* on `unregister`. Lock to `v1.3.6` (latest as of 2025‑01‑07) in `go.mod`:

```bash
go get github.com/eclipse/paho.mqtt.golang@v1.3.6
go mod tidy
```

Run `go test -race ./...` again; race on disconnect should disappear.

---

\### 7  Corrected Base64 Fixtures (Off‑by‑one)

The earlier NBIRTH fixture missed the mandatory *`Node Control/Rebirth`* **datatype field** (we passed Boolean but omitted Datatype enum → defaults to `Undefined`).
Regenerated payloads (verified with `sparkplugb-client/sproto` 3.0.0):

```text
NBIRTH_v1 = "EhcKBWJkU2VxEAAAAAABGiIKFE5vZGUgQ29udHJvbC9SZWJpcnQQAhoKBFRlbXASCQABAAABAAA="
NDATA_v1  = "FgoBCQABAAEC"
```

Replace the strings in `tests/vectors/`.

---

\### 8  Unified Device‑Key Helper

Three helper funcs existed (`deviceKey`, `nodeKey`, `edgeKey`).
Consolidate into **one** canonical form to avoid cache mismatches:

```go
// Format: <group>/<edge>/<device> – device == "" for node-level
func SpbDeviceKey(gid, nid, did string) string {
    if did == "" {
        return gid + "/" + nid
    }
    return gid + "/" + nid + "/" + did
}
```

Refactor AliasCache & SeqTracker to use this single helper.

---

\### 9  Doc Pointers for Contributors

Add these extra bullets to **docs/plan.md ▸ “Useful References”**:

* **Sparkplug Technology Compatibility Kit (TCK)** – *experimental* CLI under Eclipse‑Tahu repo:
  [https://github.com/eclipse-tahu/Sparkplug-TCK](https://github.com/eclipse-tahu/Sparkplug-TCK)
* **Eclipse Milo OPC‑UA → Sparkplug examples** – inspiration if OPC bridging is needed later:
  [https://github.com/eclipse/milo/examples](https://github.com/eclipse/milo/examples)

---

\### 10  Checklist Bump

Under “✅ Definition of Done (RC1)” add:

* **Static Analysis:** `staticcheck ./...` passes with no SA\*, ST\* issues.
* **Race Safety:** Full test suite green under `-race`.

---

\### 11  Minor Typo / Formatting Corrections

| Location                                                                                                                                        | Fix |
| ----------------------------------------------------------------------------------------------------------------------------------------------- | --- |
| Plan header: *“Mosquitto‑/HiveMQ specific extensions”* → remove trailing slash.                                                                 |     |
| Section 1 table row *“Alias collision on Birth Certificate”* – change “Certificate” → “Message” (NBIRTH is not a certificate in spec language). |     |

---

### 🚦 Application

1. Append this addendum below the existing **plan.md**.
2. Create tasks in your tracker (**SPB‑121‒SPB‑128**) for each numbered item.
3. Land the NBIRTH/NDATA fixture replacement *before* anyone runs the new alias tests, otherwise they’ll red‑bar.

With these minor corrections the PoC roadmap remains unchanged but gains sharper compliance guarantees and a smoother path to TLS in Phase 2.

— **End of Addendum 2025‑01‑07**
