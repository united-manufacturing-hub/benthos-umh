---
paths:
  - "tag_processor_plugin/**"
  - "stream_processor_plugin/**"
  - "nodered_js_plugin/**"
  - "downsampler_plugin/**"
  - "topic_browser_plugin/**"
  - "classic_to_core_plugin/**"
---

# Processor plugins

## JavaScript runs in goja

`tag_processor`, `nodered_js` and `stream_processor` run JavaScript in goja, a pure-Go engine. goja supports ES5.1 and some ES6. There are no Node.js APIs: no `require()`, no `fetch`, no `setTimeout`.

A processor drops a message by returning `null` or `undefined`. It emits several messages by returning an array.

## Stream processor state

The stream processor keeps the latest value of each configured source in memory. It evaluates single JavaScript expressions over those source variables, such as `press / temp`, not functions. The state is lost on restart. See `docs/processing/stream-processor.md`.

## Output context is restored by the engine, not the plugin

Every processor registered with `service.RegisterBatchProcessor` runs inside the engine's `v2BatchedToV1Processor` wrapper (`internal/component/processor/auto_observed.go` in the module `github.com/redpanda-data/benthos/v4`). The wrapper saves each input part's `context.Context` before the plugin runs. After the plugin returns, it puts the saved context back onto every output part. The SortGroup tag and the OpenTelemetry trace parent therefore reach the output however the plugin builds its messages.

So `service.NewMessage(nil)` is safe in a processor. A plugin-side `msg.Copy()` or `WithContext(input.Context())` has no effect, because the wrapper overwrites it.

The wrapper maps contexts by output index:

```go
ctxIdx := partIdx
if ctxIdx >= len(origCtxs) {
    ctxIdx = 0
}
```

One input fanned out to N outputs gives every output the right context. A batch of several inputs that fan out can attach an output to the wrong input's context. That is engine behaviour, and a fix belongs upstream, not in the plugin.

Verify any claim about context, lineage or SortGroup acknowledgement against a full `service.NewStreamBuilder` pipeline, not against `ProcessBatch` alone.
