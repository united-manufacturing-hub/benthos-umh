# Open Protocol device simulator (second integration fixture)

A second, independent Open Protocol **controller** for cross-checking the
plugin's parser against a different implementation. This one is the Rust project
[`Jarrekstar/open-protocol-device-simulator`](https://github.com/Jarrekstar/open-protocol-device-simulator).

## Why a second simulator

The primary fixture (`../emulator`, the Python `art-x/open-protocol-emulator`)
emits a **non-spec 4-digit tightening ID**. This Rust simulator emits the
**spec-correct 10-digit tightening ID** (parameter 23, per Open Protocol R2.16
Table 98). Testing against both proves the plugin's MID 0061 parser handles the
real-hardware field width as well as the emulator's quirk.

## Licensing

Dual-licensed **MIT OR Apache-2.0**. As with the other fixture, the source is
**not vendored**; the `Dockerfile` clones it at build time. Only this Dockerfile
(UMH-authored, Apache-2.0) lives in the repo.

## Build & run

```bash
docker build -t umh-op-device-sim open_protocol_plugin/testdata/op-device-simulator
docker run --rm -p 8080:8080 -p 8081:8081 umh-op-device-sim
```

- TCP (Open Protocol): `:8080`
- HTTP (control API):  `:8081`

This simulator does **not** auto-push results. Start continuous auto-tightening:

```bash
curl -X POST http://localhost:8081/auto-tightening/start \
  -H 'Content-Type: application/json' \
  -d '{"interval_ms":2000,"duration_ms":500,"failure_rate":0.1}'
```

Subscribed clients then receive MID 0061 results continuously.
