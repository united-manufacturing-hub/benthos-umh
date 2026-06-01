# Copyright 2025 UMH Systems GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Headless runner for the Atlas Copco Open Protocol emulator.

This is a thin, UMH-authored wrapper used ONLY as an integration-test fixture.
It imports the third-party, GPL-3.0-licensed `open_protocol_emulator` module
(art-x/open-protocol-emulator), which is fetched at Docker build time and is
deliberately NOT vendored into this Apache-2.0 repository.

The upstream emulator starts a Tkinter GUI in the main thread; in a container
there is no display, so we instead start the TCP server thread and block the
main thread, giving us a deterministic, GUI-free controller on :4545.

Once a client logs in (MID 0001) and subscribes to last-tightening results
(MID 0060), the upstream emulator automatically begins pushing MID 0061 result
telegrams on a timer (default ~5s), which is exactly what the integration test
consumes.
"""

import argparse
import threading
import time

from open_protocol_emulator import OpenProtocolEmulator


def main() -> None:
    parser = argparse.ArgumentParser(description="Headless Open Protocol emulator")
    parser.add_argument("-p", "--port", type=int, default=4545,
                        help="TCP port to listen on (default: 4545)")
    parser.add_argument("-n", "--name", type=str, default="UMHTestSim",
                        help="Controller name reported in MID 0002")
    args = parser.parse_args()

    emulator = OpenProtocolEmulator(port=args.port, controller_name=args.name)
    server_thread = threading.Thread(target=emulator.start_server, daemon=True)
    server_thread.start()

    if not server_thread.is_alive():
        raise SystemExit("[Error] Server thread failed to start")

    print(f"[Headless] Open Protocol emulator listening on :{args.port} "
          f"(name={args.name!r}); no GUI.", flush=True)

    # Block the main thread forever; the server runs in the daemon thread.
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
