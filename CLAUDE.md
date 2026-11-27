# CLAUDE.md

benthos-umh extends Benthos (Redpanda Connect) with industrial protocol inputs, processors and the UNS output. UNS means Unified Namespace: the topic tree under `umh.v1.` that all UMH data is written to. benthos-umh does not run on its own in production. umh-core generates each benthos config from a template and runs the process under S6 (see the root `CLAUDE.md` of the united-manufacturing-hub repo).

## Branches and pull requests

- PRs target `staging`, the default branch.
- Releases are cut from `staging`; `main` is no longer used. See [Releases](#releases).
- Every source file needs the Apache 2.0 license header (`make license-check`, `make license-fix`).

## Releases

A bot ([`prepare-release.yml`](.github/workflows/prepare-release.yml)) keeps one release PR open against `staging`, titled "Release vX.Y.Z" (branch `release/next`). It runs `make release-changelog` on every push to `staging`. changie picks the version from the fragments' kinds (`auto` in `.changie.yaml`): `fixes` bumps the patch number, `new` and `improvements` the minor number. Without fragments there is no release PR. Never create a `vX.Y.Z` tag or release by hand.

1. Review the changelog in the PR's diff; CI runs every test, PLC tests included. To fix an entry, fix its fragment on `staging`. To stop the release, request changes.
2. Approve and merge it with a merge commit. [`release.yml`](.github/workflows/release.yml) ships the commit the PR was generated from: it builds the image and binaries, then tags it, publishes the GitHub Release, moves the image's `X.Y` and `latest` tags, and opens the umh-core version bump PR and the ManagementConsole schema PR.
3. The umh-core bump PR sets `BENTHOS_UMH_VERSION` and re-records umh-core's benthosmetrics `/metrics` fixture from the released image ([`bump-umh-core.sh`](.github/scripts/bump-umh-core.sh)), which also updates `pkg/benthosmetrics/testdata/VERSION`. It carries a copy of each fragment in `.changelog/vX.Y.Z/`, without the Linear id. Before merging, reword or delete the fragments that umh-core users do not need. Keep at least one, or umh-core's `require-changelog-entry` check fails.

A release needs at least one fragment. To release changes that are not user-visible, merge a PR that adds only this fragment:

```bash
./.tools/changie new --kind improvements --body "Maintenance release: internal changes only, no user-visible changes. (ENG-xxxx)"
```

### When a release fails

The release PR gets a comment that mentions whoever merged it. Re-run the failed jobs; every step skips what exists. If the release commit is broken, merge a fix into `staging`: the next release PR gets the next version. The failed version keeps its `CHANGELOG.md` section but gets no tag or release, and its entries are not repeated in the next release's notes or umh-core PR. Never reuse a version: the Go module proxy may have cached its tag. A squash-merged release PR fails the release; revert it and merge the regenerated PR with a merge commit.

## Changelog (changie)

Do not edit `CHANGELOG.md` by hand. It is generated from per-PR fragments in `.changelog/unreleased/`.

```bash
make changelog   # installs changie into .tools/ on first use, prompts for kind and body, writes a fragment
./.tools/changie new --kind fixes --body "OPC UA browse no longer stalls on large address spaces (ENG-1234)"
```

- Kinds: `new`, `improvements`, `fixes`. They match umh-core's kinds, so the release workflow can copy the fragments into umh-core. File a breaking change under `improvements` and say in the entry what breaks.
- End the line with the uppercase Linear id in parentheses, e.g. `(ENG-1234)`. The release workflow removes it when it copies the entry into umh-core.
- Describe the user-visible change in product language. No "Added"/"Fixed" lead-ins.
- At release time, the release PR runs `make release-changelog VERSION=vX.Y.Z`, which batches the fragments into `.changelog/<version>.md`, moves them to `.changelog/<version>/` and regenerates `CHANGELOG.md`.

### Redpanda Connect updates

When a PR (usually Renovate's) moves `github.com/redpanda-data/connect/v4` to a new version, write the fragments from the upstream `CHANGELOG.md` of every version it skips:

1. One `improvements` fragment that names the old and new version and links the upstream release notes, e.g. `Updated the bundled Redpanda Connect components from v4.117.0 to v4.118.0. … See the [Redpanda Connect v4.118.0 release notes](https://github.com/redpanda-data/connect/releases/tag/v4.118.0) for the full list`.
2. One fragment for each upstream entry that a benthos-umh user can reach. Use the upstream kind: Added is `new`, Fixed is `fixes`, Changed is `improvements`.
3. Skip the entries users cannot reach:
   - Components that are not in our binary: `go list -deps ./cmd/benthos` does not list their `internal/impl/...` package. benthos-umh imports only `public/bundle/free/v4` (see `cmd/benthos/bundle/package.go`).
   - Components that call `license.CheckRunningEnterprise`, because they need a Redpanda license (e.g. `postgres_cdc`).
   - Changes to the Go API only, enterprise CLI features (e.g. the telemetry pipeline ID) and internal refactors.

   The `enterprise` mark in `internal/plugins/info.csv` is not enough: `aws_dynamodb_cdc` has it, but it is in our binary and runs without a license.
4. Check the libraries our own plugins use, which the same PR often bumps (e.g. franz-go, which the `uns` input and output use). Write a fragment for a fix that a user of our plugin would notice, under our plugin's name.

Write each entry like any other one. Start with the component's public name and its config name, e.g. `Kafka output (kafka) with idempotent_write enabled now starts`, not a `Redpanda Connect:` prefix. For a fix, say what the user saw go wrong. Read the upstream diff when the upstream entry does not say that. If the bump also clears a security advisory, the handbook's vulnerable-dependency rule decides that entry.

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
6. For an input plugin, add a `value_type_survival_test.go` table with the fixture values from `tag_processor_plugin/tagprocessortest`: a numeric string, a non-numeric string, the 17-digit numeric string, a number, a boolean, and arrays where the protocol has them. Each entry passes the message the plugin emits to `tagprocessortest.ExpectTagProcessorKeepsValue`, which fails when tag_processor writes a different JSON type or value than the plugin decoded. A plugin that emits a JSON object uses `ExpectTagProcessorKeepsPayloadField` instead. [`modbus_plugin/value_type_survival_test.go`](modbus_plugin/value_type_survival_test.go) is an example.

## Engineering Handbook

The [UMH Engineering Handbook](https://engineering.umh.app) describes how we build, test and ship. Reviewers check our work against its rules. https://engineering.umh.app/llms.txt lists every page. It also says how to ask the handbook a question.

Before you start any work, such as code, a pull request or a ticket, read every page whose rules apply to it. Ask the handbook about anything those pages do not answer. Before you hand the work to someone else, check it against those pages.
