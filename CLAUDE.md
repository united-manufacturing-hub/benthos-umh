# CLAUDE.md

benthos-umh extends Benthos (Redpanda Connect) with industrial protocol inputs, processors and the UNS output. UNS means Unified Namespace: the topic tree under `umh.v1.` that all UMH data is written to. benthos-umh does not run on its own in production. umh-core generates each benthos config from a template and runs the process under S6 (see the root `CLAUDE.md` of the united-manufacturing-hub repo).

## Branches and pull requests

- PRs target `staging`, the default branch.
- Releases are cut from `staging`; `main` is no longer used. A release works like the ManagementConsole release:
  1. On a branch off `staging`, run `make release-changelog VERSION=vX.Y.Z`. Merge the result into `staging` through a PR.
  2. Publish a release on the `staging` tip, titled with the version: `gh release create vX.Y.Z --target staging --title vX.Y.Z --notes ""`. A prerelease starts nothing; for a test image, use the per-PR image CI pushes.
  3. `release.yml` checks that the tag is the `staging` tip, that the title equals the tag, that `.changelog/vX.Y.Z.md` has entries, and that `.changelog/unreleased/` is empty. It then runs every CI test job without the path filter, PLC tests included. Only then does it build the image and binaries, write the changelog into the release body, and open the umh-core version bump.
  4. If the check fails, the release turns back into a draft. Delete the draft and its tag, fix `staging`, and publish again. If the tests or the build fail, the release also turns back into a draft. Delete the draft, fix `staging`, and publish the next patch version. Do not reuse the tag: the Go module proxy may already have cached it.
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
- At release time, `make release-changelog VERSION=vX.Y.Z` batches the fragments into `.changelog/<version>.md` and regenerates `CHANGELOG.md`.

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

The [UMH Engineering Handbook](https://engineering.umh.app) describes how we build, test and ship. Reviewers check our work against its rules. https://engineering.umh.app/llms.txt lists every page. It also says how to ask the handbook a question.

Before you start any work, such as code, a pull request or a ticket, read every page whose rules apply to it. Ask the handbook about anything those pages do not answer. Before you hand the work to someone else, check it against those pages.
