"""Use the installed worker-capable SQLite authority for §18 and §19 probes."""

from __future__ import annotations

import sys

from conformance import authority_adapter


def main() -> None:
    authority_adapter._WORKER_MODE = True
    payload = authority_adapter._parse(sys.stdin.read())
    print(authority_adapter._compact(authority_adapter.run(payload)))


if __name__ == "__main__":
    main()
