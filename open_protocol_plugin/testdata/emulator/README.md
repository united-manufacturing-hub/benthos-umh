# Open Protocol emulator (integration-test fixture)

A headless Atlas Copco Open Protocol **controller** used by the plugin's
integration tests (`//go:build integration`, gated by `TEST_OPEN_PROTOCOL=true`).

## Licensing

The protocol emulator itself is the third-party project
[`art-x/open-protocol-emulator`](https://github.com/art-x/open-protocol-emulator),
which is licensed **GPL-3.0**. To keep this Apache-2.0 repository clean, that
source is **not vendored here** — the `Dockerfile` clones it at build time.

Only `headless_runner.py` (UMH-authored, Apache-2.0) is stored in this repo. It
imports the upstream module and runs the TCP server without the Tkinter GUI.

## Build & run

```bash
docker build -t umh-op-emulator open_protocol_plugin/testdata/emulator
docker run --rm -p 4545:4545 umh-op-emulator
```

The emulator acts as the controller (server) on `:4545`. Once a client logs in
(MID 0001) and subscribes to last-tightening results (MID 0060), it begins
pushing MID 0061 result telegrams on a timer.

The integration test builds and starts this container automatically via
testcontainers; this README is for manual/local runs.
