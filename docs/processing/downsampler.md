## Downsampler in a Nutshell

The UMH **Downsampler** sits between raw field traffic and your storage layer and does one thing only: **drops points that add no information**.
It offers two proven modes:

* **Dead-band** – keeps every *change*, drops byte-for-byte repeats, and (optionally) emits a watchdog heartbeat after `max_time`.
* **Swinging-Door Trending (SDT)** – follows the *trend* itself; a sample is forwarded only if it would violate an error-bounded, slope-aware “door”.

Because the plug-in lets you stack wildcard overrides on top of a safe default, you can start with *zero-risk* settings—`deadband {threshold: 0}`—and tighten the rules where it pays off.

**Where it makes sense to deploy**

1. **First hop into the Unified Namespace** – use dead-band with `threshold: 0` and a 30 min heartbeat. You simply de-bounce duplicates while proving the pipe is alive; no data are ever delayed.
2. **Sensor-noise cleanup anywhere in the pipe** – raise the dead-band to `2 × σ_noise` (twice the sensor manufacturer’s stated noise) and keep the 1 h heartbeat
3. **Last stop before the historian** – switch to SDT with the same `2 × σ_noise` error band, add a physics-based `min_time` (fastest plausible change), and keep the 1 h heartbeat. You retain slope fidelity yet routinely see 95–99 % compression.

Counters, binary alarms and similar discrete tags should *stay* on dead-band (`threshold: 0`); SDT’s internal buffering could hide a state flip too long for real-time alerting.

Why this beats the usual “stick a dead-band in Node-RED” trick?
*Per-tag state*, safe ACK-buffering, slope-aware compression, audit heartbeats and pattern-based overrides come baked in—no custom code, no hidden duplicates, no stair-step artefacts on slow ramps.

> **Compliance footnote** – SDT and dead-band guarantee an absolute error bound and an audit heartbeat, but they do discard data.  If your GxP / 21 CFR Part 11 regime demands the unfiltered stream, archive the raw UMH-core topic in parallel.

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