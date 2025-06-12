## Downsampler in a Nutshell

The UMH **Downsampler** sits between the field bus and your persistence layer and performs one task with provable bounds: **it discards samples that do not contribute new information**.

### Supported compression modes

* **Dead-band** – forwards every *value change*, suppresses byte-for-byte repeats, and—if `max_time` is set—adds an audit heartbeat.
* **Swinging-Door Trending (SDT)** – maintains a slope-aware envelope; a sample is forwarded only when it would exceed a fixed vertical error bound.

Wildcard overrides let you apply a conservative baseline (`deadband {threshold: 0}`) and refine it only where tighter compression is beneficial.

### Recommended deployment points

1. **First hop into the Unified Namespace**
   *Configuration:* `deadband {threshold: 0, max_time: 30m}`
   *Effect:* Removes pure duplicates while guaranteeing one confirmatory message every 30 min. No latency is introduced.

2. **Noise suppression anywhere in the stream**
   *Configuration:* `deadband {threshold: 2 × σ_noise, max_time: 1h}`
   *Effect:* Removes fluctuations beneath the sensor’s specified noise floor yet retains all legitimate steps.

3. **Final stage before the historian**
   *Configuration:* `swinging_door {threshold: 2 × σ_noise, min_time: physics_limit, max_time: 1h}`
   *Effect:* Achieves 95–99 % volume reduction while preserving slope integrity and audit heartbeat. Introduces latency of up to `max_time` as the algorithm needs to buffer the last point before it can be emitted.

Discrete counters and alarms should remain on `deadband {threshold: 0}`; SDT’s emit-previous logic could delay a critical state change.

> **Compliance note** – Both algorithms enforce an absolute error bound and maintain a heartbeat, but they still remove data. If your GxP / 21 CFR Part 11 process mandates a full raw stream, archive the unfiltered UMH-core topic in parallel.


---

## 1  Quick-start Configuration

```yaml
processors:
  - downsampler:
      default:                     # safe baseline for every topic
        deadband:
          threshold: 0             # keep every change
          max_time: 30m            # 30-minute heartbeat
      overrides:                   # finer control
        - pattern: "*.temperature"
          deadband:
            threshold: 0.1         # 2 × 0.05 °C noise
        - pattern: "*.furnace*"
          swinging_door:
            threshold: 0.1
            min_time: 5s
            max_time: 1h
```

*Nothing else is required.*
Messages that aren’t strict UMH-core time-series (`value` + `timestamp_ms`) pass straight through.

---

## Algorithm & Parameter Deep Dive

The Downsampler exposes just four parameters; once you understand their purpose you can configure the system with confidence.


| Parameter     | Purpose                                                                                          | Typical setting                                                                            | Issue it prevents                                                               |
| ------------- | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------- |
| `threshold`   | Maximum absolute deviation allowed before a new point must be stored (same units as the signal). | **2 × sensor-noise σ** · If σ is unknown, inspect a steady period and take ± peak spread   | Removes pure measurement noise without masking genuine step changes.            |
| `min_time`    | Smallest physically realistic interval between meaningful changes. Available only on `swinging_door`                               | Fastest credible process period (e.g. 1 s for furnaces, 50 ms for servo torque). `0` = off | Suppresses transients caused by bursty drivers or unstable links.               |
| `max_time`    | Heart-beat that forces an output even during flat periods; also flushes any SDT buffer.          | 15 min – 1 h (aligns with 21 CFR §11 “system liveness”).                                   | Ensures line-flat sensors remain visible and internal buffers stay bounded.     |
| `late_policy` | Action for out-of-order samples.                                                                 | `passthrough` (default) or `drop`.                                                         | Lets you balance historical accuracy against traffic volume on skewed networks. |     |

### Dead-band

* **Rule** Emit when `|v − last| ≥ threshold` or when `max_time` expires.
* **Best suited to** Duplicate removal, discrete states, counters, Boolean flags.
* **Tuning notes**

  * `threshold = 2 × σ_noise` removes sensor noise yet preserves real transitions.
  * `threshold = 0` provides safe, universal de-duplication.
  * Keep `max_time` within 15 – 60 min to satisfy audit requirements.

### Swinging-Door Trending (SDT)

* **Rule** Maintain rotating upper and lower “doors” that bound the current slope; emit the **previous** point when a new sample would violate the envelope.
* **Additional parameters**

  * `threshold` – vertical tolerance (same intuition as for dead-band).
  * `min_time` – enforces physical plausibility for fast-changing signals.
  * `max_time` – still provides the audit heart-beat and buffer flush.
* **Why choose SDT** Captures long ramps with just two points, eliminating the stair-step pattern created by naïve dead-band on slow trends, while mathematically guaranteeing the reconstruction error remains ≤ `threshold`.

## Edge-case Handling & Internal Behaviour

The Downsampler runs each series in its own finite-state machine and guards two invariants:

1. **No silent loss** A point is acknowledged only after it—or the predecessor that still sits in the buffer—has been forwarded downstream.
2. **Bounded memory** At most **one** candidate is stored per series; an idle-flush watchdog empties it after `max_time`.

---

### How incoming records are routed

| Payload type            | Processing path                                    | Reasoning                                                                                 |
| ----------------------- | -------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| *Numeric* (int / float) | Selected algorithm (`deadband` or `swinging_door`) | These are the signals where compression counts.                                           |
| *Boolean, string*       | Change detector (`value ≠ last_value`)             | For states and text, only transitions matter; thresholds are irrelevant.                  |
| Other JSON structures   | **Bypass** (fail-open)                             | Complex objects could hide multiple semantics—forward untouched rather than risk pruning. |

---

### Late-arrival strategy

*The timestamp of each processed sample is compared with the most recent one already seen for that series.*

* `passthrough` (default) Forward the late sample raw, flagging it with `meta:late_oos=true`.
* `drop` Discard and increment a metric counter—useful when stale data has no value but bandwidth is critical.

Both modes preserve at-least-once delivery for in-order traffic; only you decide what to do with stragglers.

---

### Buffered-emit logic (needed by SDT)

1. A candidate point is retained while the doors stay open.
2. When a violation occurs, the **previous** candidate is released and the new sample becomes the fresh candidate.
3. If traffic stops before the doors close, the watchdog flushes the last candidate after `max_time` to guarantee visibility.

The same mechanism ensures a graceful shutdown: during a Benthos drain, every buffered point is emitted before the plug-in confirms closure.

---

### Numerical edge cases covered in tests

| Scenario                              | Guard rail in code                                                     |
| ------------------------------------- | ---------------------------------------------------------------------- |
| `threshold < 0`                       | Configuration rejected at startup.                                     |
| `threshold = 0`                       | Legal—drops exact repeats, keeps any change.                           |
| `max_time = 0`                        | Treated as *unset* (no heart-beat, but still at-least-once buffering). |
| `min_time > max_time`                 | Validation error to prevent deadlocks.                                 |
| IEEE-754 extremes (`±Inf`, `NaN`)     | Sample is bypassed with a warning; counting metrics record the event.  |
| Clock skew (non-monotonic timestamps) | Logged and routed through late-arrival policy; compression continues.  |

With these guards the Downsampler behaves deterministically across PLC glitches, network jitter and even deliberate fuzz-test assaults—yet still errs on the side of passing the data through rather than dropping it.

## Further Reading & Learning Materials for Swinging-Door Trending

| Topic                                                             | Resource                                                                                                                       | Why it’s worth your time                                                                         |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------ |
| Step-by-step animations of the envelope closing and segment flush | **emrumo/swingingdoor** – GIF-heavy GitHub repo that visualises each decision the algorithm makes. ([pisquare.osisoft.com][1]) | Watch the “doors” rotate in real time and see exactly why a point is (not) kept.                 |                                                   |
| Original historian context & glossary                             | AVEVA (OSIsoft) PI documentation page on *swinging-door compression*. ([docs.aveva.com][4])                                    | Shows how SDT is wired into a production-grade historian and why `max_time` exists.              |
| Academic analysis & tuning guidance                               | “Swinging Door Trending Compression Algorithm for IoT Environments,” IEEE/ResearchGate. ([researchgate.net][5])                | Explains error bounds, calibration of `threshold`, and benchmark results on real sensor streams. |
| Historical patent background                                      | US Patent 5,437,030 – the original 1990s PI Swinging-Door patent (expired). ([github.com][7])                                  | Good trivia and helps you understand why older systems look the way they do.                     |

[1]: https://pisquare.osisoft.com/0D51I00004UHdq2SAD?utm_source=chatgpt.com "swinging door compression algorithm - PI Square"
[4]: https://docs.aveva.com/bundle/glossary/page/1457222.html?utm_source=chatgpt.com "swinging-door compression - AVEVA™ Documentation"
[5]: https://www.researchgate.net/publication/337361831_Swinging_Door_Trending_Compression_Algorithm_for_IoT_Environments?utm_source=chatgpt.com "Swinging Door Trending Compression Algorithm for IoT Environments"
[6]: https://github.com/apache/streampipes/discussions/1285?utm_source=chatgpt.com "The Swinging Door Trending (SDT) Filter Processor #1285 - GitHub"
[7]: https://github.com/gfoidl/DataCompression/blob/master/api-doc/articles/SwingingDoor.md?utm_source=chatgpt.com "DataCompression/api-doc/articles/SwingingDoor.md at master"
