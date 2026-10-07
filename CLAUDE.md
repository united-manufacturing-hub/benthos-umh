# CLAUDE.md

benthos-umh extends Benthos (Redpanda Connect) with industrial protocol inputs, processors and the UNS output. UNS means Unified Namespace: the topic tree under `umh.v1.` that all UMH data is written to. benthos-umh does not run on its own in production. umh-core generates each benthos config from a template and runs the process under S6 (see umh-core's `CLAUDE.md`).

## Branches and pull requests

- PRs target `staging`, the default branch.
- A release is a `main ← staging` PR titled with the bare version (`v0.13.0`), merged as a **merge commit** (not a squash). Then push a lightweight `vX.Y.Z` tag on the `main` tip. The tag triggers `release.yml` and the umh-core and ManagementConsole version bumps. The cross-repo pattern is in umh-core's [`RELEASING.md`](https://github.com/united-manufacturing-hub/united-manufacturing-hub/blob/staging/umh-core/RELEASING.md).
- Every source file needs the Apache 2.0 license header (`make license-check`, `make license-fix`).

## Changelog (changie)

Do not edit `CHANGELOG.md` by hand. It is generated from per-PR fragments in `.changelog/unreleased/`.

```bash
make changelog   # installs changie into .tools/ on first use, prompts for kind and body, writes a fragment
./.tools/changie new --kind fixes --body "OPC UA browse no longer stalls on large address spaces (ENG-1234)"
```

- Kinds: `breaking`, `new`, `improvements`, `fixes`.
- End the line with the uppercase Linear id in parentheses, e.g. `(ENG-1234)`. It is stripped when entries propagate to the umh-core changelog.
- Describe the user-visible change in product language. No "Added"/"Fixed" lead-ins.
- The release PR runs `.github/workflows/changelog.yml`, which batches the fragments into `.changelog/<version>.md` and regenerates `CHANGELOG.md`.

## Layout

| Path | Contents |
|---|---|
| `<name>_plugin/` | One package per plugin (16). Each registers itself in `init()` with `service.RegisterBatchInput`, `RegisterBatchProcessor`, `RegisterBatchOutput` or `RegisterOutput`. |
| `cmd/benthos/` | Main binary. `bundle/package.go` blank-imports every plugin package. |
| `cmd/schema-export/` | Exports plugin config schemas for the ManagementConsole (`make generate-schema VERSION=…`). |
| `pkg/umh/topic/` | UMH topic builder and parser. |
| `docs/` | User docs (GitBook, published at [docs.umh.app/benthos-umh](https://docs.umh.app/benthos-umh)). `docs/SUMMARY.md` is the index. |
| `config/` | Example configs. |
| `templates/` | `umh_input`, `umh_processor`, `umh_output` Benthos templates. |
| `proto/` | Sparkplug B protobuf definitions. |
| `tests/` | Docker Compose setups for protocol tests. |
| `.changelog/` | changie fragments and released versions. |

Plugins by role:

- **Inputs:** `opcua_plugin` (also an output), `modbus_plugin`, `s7comm_plugin`, `sparkplug_plugin` (also an output), `eip_plugin`, `sensorconnect_plugin`, `beckhoff_ads_plugin`, `uns_plugin` (`uns` input).
- **Processors:** `tag_processor_plugin`, `stream_processor_plugin`, `downsampler_plugin`, `topic_browser_plugin`, `classic_to_core_plugin`, `nodered_js_plugin`.
- **Outputs:** `uns_plugin` (`uns` output), `historian_plugin`, `snowflake_put_plugin`.

Plugin internals load as path-scoped rules from `.claude/rules/` when you open a file in the matching directory.

## Build, run and test

```bash
make build                    # binary at tmp/bin/benthos
make run CONFIG=config/stdout.yaml LOG_LEVEL=debug
tmp/bin/benthos lint config/stdout.yaml
tmp/bin/benthos list inputs   # also: processors, outputs
make lint                     # golangci-lint
make fmt
make test                     # all Ginkgo suites
make serve-pprof              # profiling server
```

Per-plugin targets: `test-unit-opc`, `test-integration-opc`, `test-modbus`, `test-s7comm`, `test-sparkplug`, `test-eip`, `test-sensorconnect`, `test-ads`, `test-noderedjs`, `test-tag-processor`, `test-stream-processor`, `test-topic-browser`, `test-downsampler`, `test-classic-to-core`, `test-uns`, `test-historian`, `test-snowflake-put`, `test-pkg-umh-topic`, `test-schema-export`. Benchmarks: `bench-stream-processor`, `bench-pkg-umh-topic`.

Protocol integration tests need a device or simulator. The plugin's CI workflow in `.github/workflows/test-*.yml` shows how it is started.

## Tests

- Ginkgo v2 with Gomega. Each package has a `*_suite_test.go` and `*_test.go` files.
- Ginkgo runs specs in parallel and in random order. Specs must not depend on each other.
- Never commit a focused spec (`FIt`, `FDescribe`, `FContext`).

## Adding a plugin

1. Create `<name>_plugin/` and register the component in `init()`.
2. Add the blank import to `cmd/benthos/bundle/package.go`.
3. Add docs under `docs/input/`, `docs/processing/` or `docs/output/`, and an entry in `docs/SUMMARY.md`.
4. Add a `test-<name>` Make target and a CI workflow in `.github/workflows/`.
5. Classify the config fields as described in `.claude/rules/plugin-fields.md`.

## Engineering Handbook

Our shared standards live at https://engineering.umh.app. Start with:

- Go: https://engineering.umh.app/engineering/development-process/how-to-build/coding-standards/go
- Error management: https://engineering.umh.app/engineering/development-process/how-to-build/coding-standards/error-management
- Product standards: https://engineering.umh.app/product/product-standards ([Opinionated simplicity](https://engineering.umh.app/product/product-standards/opinionated-simplicity), [Code and UI](https://engineering.umh.app/product/product-standards/code-and-ui), [Immediate trust](https://engineering.umh.app/product/product-standards/immediate-trust))
- How to build: https://engineering.umh.app/engineering/development-process/how-to-build
- Testing: https://engineering.umh.app/engineering/development-process/how-to-build/testing
- How to ship: https://engineering.umh.app/engineering/development-process/how-to-ship

Rules to apply while writing code here:

- benthos-umh moves customer data, so a change to it is a one-way door. Agree the approach with a Code Owner before you code.
- Every change in behaviour comes with a test that fails without the change.
- Every error is either retried, shown to the user (the bridge goes degraded and says why), or sent to Sentry.
- The YAML is limited to what the UI can render. Do not add a config field when one value works for nearly every user.
- Behaviour changes ship behind a feature flag.
