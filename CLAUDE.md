# CLAUDE.md

benthos-umh extends Benthos (Redpanda Connect) with industrial protocol inputs, processors and the UNS output. UNS means Unified Namespace: the topic tree under `umh.v1.` that all UMH data is written to. benthos-umh does not run on its own in production. umh-core generates each benthos config from a template and runs the process under S6 (see the root `CLAUDE.md` of the united-manufacturing-hub repo).

## Branches and pull requests

- PRs target `staging`, the default branch.
- Releases are cut from `staging`; `main` is no longer used. First run Actions > Changelog > Run workflow with the version (`v0.17.0`) and merge the changelog PR it opens. Then push a lightweight tag on the `staging` tip (`git tag vX.Y.Z origin/staging && git push origin vX.Y.Z`). Do not create the GitHub Release by hand: `release.yml` creates it with the changelog as its body, and it keeps the body of a release that already exists. The tag also triggers the umh-core and ManagementConsole version bumps.
- Every source file needs the Apache 2.0 license header (`make license-check`, `make license-fix`).

## Changelog (changie)

Do not edit `CHANGELOG.md` by hand. It is generated from per-PR fragments in `.changelog/unreleased/`.

```bash
make changelog   # installs changie into .tools/ on first use, prompts for kind and body, writes a fragment
./.tools/changie new --kind fixes --body "OPC UA browse no longer stalls on large address spaces (ENG-1234)"
```

- Kinds: `breaking`, `new`, `improvements`, `fixes`.
- End the line with the uppercase Linear id in parentheses, e.g. `(ENG-1234)`. Leave the id out when you copy the entry into the umh-core changelog. No workflow removes it.
- Describe the user-visible change in product language. No "Added"/"Fixed" lead-ins.
- The Changelog workflow (`.github/workflows/changelog.yml`, run by hand at release time) batches the fragments into `.changelog/<version>.md` and regenerates `CHANGELOG.md`.

## Layout

| Path | Contents |
|---|---|
| `<name>_plugin/` | One package per plugin. Each registers itself in `init()` with `service.RegisterBatchInput`, `RegisterBatchProcessor`, `RegisterBatchOutput` or `RegisterOutput`. |
| `cmd/benthos/` | Main binary. `bundle/package.go` blank-imports every plugin package. |
| `cmd/schema-export/` | Exports plugin config schemas for the ManagementConsole (`make generate-schema VERSION=…`). |
| `pkg/umh/topic/` | UMH topic builder and parser. |
| `docs/` | User docs (GitBook, published at [docs.umh.app/benthos-umh](https://docs.umh.app/benthos-umh)). `docs/SUMMARY.md` is the index. |
| `config/` | Example configs. |
| `templates/` | The `umh_*` Benthos templates. |
| `proto/` | Sparkplug B protobuf definitions. |
| `tests/` | Docker Compose setups for protocol tests. |
| `.changelog/` | changie fragments and released versions. |

Plugins by role:

- **Inputs:** `opcua_plugin` (also an output), `modbus_plugin`, `s7comm_plugin`, `sparkplug_plugin` (also an output), `eip_plugin`, `sensorconnect_plugin`, `beckhoff_ads_plugin`, `uns_plugin` (`uns` input).
- **Processors:** `tag_processor_plugin`, `stream_processor_plugin`, `downsampler_plugin`, `topic_browser_plugin`, `classic_to_core_plugin`, `nodered_js_plugin`.
- **Outputs:** `uns_plugin` (`uns` output), `historian_plugin`, `snowflake_put_plugin`.

UNS messages, topics and payloads are described in the user docs: [tag processor](docs/processing/tag-processor.md), [topic parser](docs/libraries/umh-topic-parser.md), [UNS output](docs/output/uns-output.md) and umh-core's [payload formats](https://docs.umh.app/usage/unified-namespace/payload-formats). A time-series payload and a relational payload never share a topic. Give them different `data_contract` values.

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

Each plugin has a `test-<plugin>` target, and some have benchmarks. List them with `grep -E '^(test|bench)-' Makefile`.

Protocol integration tests need a device or simulator. Where a plugin has a CI workflow in `.github/workflows/test-*.yml`, that workflow shows how the simulator is started.

## Tests

- Ginkgo v2 with Gomega. Each package has a `*_suite_test.go` and `*_test.go` files.

## Adding a plugin

1. Create `<name>_plugin/` and register the component in `init()`.
2. Add the blank import to `cmd/benthos/bundle/package.go`.
3. Add docs under `docs/input/`, `docs/processing/` or `docs/output/`, and an entry in `docs/SUMMARY.md`.
4. Add a `test-<name>` Make target and a CI workflow in `.github/workflows/`.
5. Declare each config field with `.Default()`, `.Optional()` and `.Examples()` as the comments on `FieldSpec` in `cmd/schema-export/types.go` describe, because the Management Console renders the form from that schema.

## Engineering Handbook

Our shared standards live at https://engineering.umh.app. Look up the pages your task needs before you write code. Each page has a Markdown version: append `.md` to its URL. https://engineering.umh.app/llms.txt lists every page. Start with:

- Go: https://engineering.umh.app/engineering/development-process/how-to-build/coding-standards/go
- Error management: https://engineering.umh.app/engineering/development-process/how-to-build/coding-standards/error-management
- Product standards: https://engineering.umh.app/product/product-standards ([Opinionated simplicity](https://engineering.umh.app/product/product-standards/opinionated-simplicity), [Code and UI](https://engineering.umh.app/product/product-standards/code-and-ui), [Immediate trust](https://engineering.umh.app/product/product-standards/immediate-trust))
- How to build: https://engineering.umh.app/engineering/development-process/how-to-build
- Testing: https://engineering.umh.app/engineering/development-process/how-to-build/testing
- How to ship: https://engineering.umh.app/engineering/development-process/how-to-ship

