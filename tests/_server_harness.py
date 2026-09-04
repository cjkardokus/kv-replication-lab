"""Shared RunningServer/_free_port test harness for real, socket-bound
uvicorn servers.

tests/test_leader_follower.py and tests/test_leaderless.py both need to
run actual node apps against real loopback sockets -- not FastAPI's
in-process TestClient -- to genuinely exercise inter-process HTTP
(concurrent fan-out, a real clock-based ack/quorum timeout race, a real
"connection refused" from an unreachable peer). Each independently
defined the identical `_free_port()` and a `RunningServer` class to do
this; this module is that shared piece, extracted once both existed
(see docs/AUDIT_FINDINGS.md's §7).

The one real difference between the two callers -- leader-follower's
harness allocates its own port internally (a leader/follower doesn't
need to know its own address before build_app() runs), leaderless's
takes the port as a constructor argument (a leaderless node needs its
own port up front, to compute its peers list) -- is preserved via
RunningServer's `port` parameter (default: None, meaning auto-allocate
via _free_port()), covering both current call shapes with one class,
and via `address_cls` (each strategy's own Follower/Node address
dataclass -- structurally identical, `host: str, port: int`, but kept as
each strategy's own type rather than a shared one here, matching how
the rest of this project treats those two dataclasses as distinct).

Not a standalone entry point -- imported by the two test modules above,
same "leading underscore -- internal only" convention this project
already uses for experiments/_load_test_common.py and
experiments/_results_doc.py.
"""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Callable

import uvicorn
from fastapi import FastAPI


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class RunningServer[AddressT]:
    """A uvicorn server running one ASGI app in a background thread.

    Real socket, real event loop, separate from pytest's -- so calls
    against it exercise the same code paths a production deployment
    would.

    `address_cls` is the caller's own address dataclass (leader_follower.
    leader.Follower or leaderless.node.Node) -- both take `host`/`port`
    keyword args, so `.address` below can build either from the same
    code without this module depending on either strategy's module
    itself.
    """

    def __init__(
        self,
        app: FastAPI,
        address_cls: Callable[..., AddressT],
        *,
        host: str = "127.0.0.1",
        port: int | None = None,
    ) -> None:
        self.host = host
        self.port = port if port is not None else _free_port()
        self._address_cls = address_cls
        config = uvicorn.Config(app, host=self.host, port=self.port, log_level="warning")
        self.server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self) -> RunningServer[AddressT]:
        self._thread.start()
        deadline = time.time() + 5
        while not self.server.started:
            if time.time() > deadline:
                raise RuntimeError("uvicorn server did not start in time")
            time.sleep(0.01)
        return self

    def stop(self) -> None:
        self.server.should_exit = True
        self._thread.join(timeout=5)

    @property
    def address(self) -> AddressT:
        return self._address_cls(host=self.host, port=self.port)
