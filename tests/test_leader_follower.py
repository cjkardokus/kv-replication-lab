"""Tests for leader_follower/leader.py and leader_follower/follower.py.

Testing approach
----------------
common/server.py's own tests (test_server.py) use FastAPI's TestClient,
which calls the ASGI app in-process -- no sockets, no event loop of its
own. That's the right tool there because it's testing one app in
isolation.

The leader's write path is fundamentally about *inter-process* HTTP:
leader.py builds real httpx URLs (`http://{host}:{port}/...`) and races
real concurrent requests against a real clock-based timeout. A
TestClient-only approach can't exercise that -- there's no live socket
for httpx to connect to, and no way to observe real concurrency or a
real "connection refused" from an unstarted follower.

So these tests run actual uvicorn servers -- leader and followers alike
-- each in its own background thread, bound to real loopback ports, and
drive them with a real httpx client, exactly like leader.py does
against production followers. This is heavier than TestClient (thread
start/stop overhead, port allocation, small polling waits for
readiness/eventual replication) but it's the only way to genuinely
exercise:
  - concurrent fan-out (a slow follower must not block others),
  - the ack_required/timeout race as it actually races in real time,
  - "late" follower acks still landing after the client got a response,
  - a follower being unreachable (real connection-refused, not a mock).

Trade-off accepted: these tests are slower and slightly more prone to
timing flakiness than pure in-process tests, so timeouts/delays below
are chosen with generous margins (e.g. a "slow" follower sleeps for
several multiples of the leader's timeout) to keep them deterministic
under CI-level scheduling jitter, rather than shaving margins for
speed.
"""

from __future__ import annotations

import socket
import threading
import time
from unittest.mock import patch

import httpx
import pytest
import uvicorn
from fastapi import FastAPI
from starlette.responses import PlainTextResponse

from common.replication_client import (
    _CLIENT_MAX_CONNECTIONS,
    _CLIENT_MAX_KEEPALIVE_CONNECTIONS,
)
from common.server import ReplicateResponse, create_app, replace_route
from common.storage import KVStore
from leader_follower.follower import build_app as build_follower_app
from leader_follower.leader import (
    ClusterConfig,
    Follower,
    _parse_args,
    _resolve_config,
)
from leader_follower.leader import (
    build_app as build_leader_app,
)

# --- Real-server test harness --------------------------------------------


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class RunningServer:
    """A uvicorn server running one ASGI app in a background thread.

    Real socket, real event loop, separate from pytest's -- so calls
    against it exercise the same code paths a production deployment
    would.
    """

    def __init__(self, app: FastAPI) -> None:
        self.host = "127.0.0.1"
        self.port = _free_port()
        config = uvicorn.Config(app, host=self.host, port=self.port, log_level="warning")
        self.server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self) -> RunningServer:
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
    def address(self) -> Follower:
        return Follower(host=self.host, port=self.port)


@pytest.fixture
def cluster():
    """Yields a factory for starting real servers; stops them all after
    the test regardless of outcome.
    """
    servers: list[RunningServer] = []

    def _start(app: FastAPI) -> RunningServer:
        server = RunningServer(app).start()
        servers.append(server)
        return server

    yield _start

    for server in servers:
        server.stop()


def _slow_follower_app(node_id: str, delay_seconds: float) -> FastAPI:
    """A follower whose /internal/replicate sleeps before applying the
    write, to simulate a slow/unresponsive node for timeout tests.

    Built the same way leader.py overrides PUT: take the stock app from
    create_app() and swap out just the one route under test, so
    everything else (GET, health, ...) is the real, unmodified follower
    behavior.
    """
    storage = KVStore()
    app = create_follower_with_storage(node_id, storage)

    # See common.server.replace_route's docstring for why this is
    # necessary, not just tidy.
    replace_route(app, "/internal/replicate", {"POST"})

    @app.post("/internal/replicate", response_model=ReplicateResponse)
    def slow_replicate(body: dict) -> ReplicateResponse:  # type: ignore[type-arg]
        time.sleep(delay_seconds)
        applied = storage.put(
            body["key"], body["value"], timestamp=body["timestamp"], node_id=body["node_id"]
        )
        return ReplicateResponse(applied=applied)

    return app


def create_follower_with_storage(node_id: str, storage: KVStore) -> FastAPI:
    return create_app(storage, node_id)


def _malformed_follower_app(node_id: str) -> FastAPI:
    """A follower whose /internal/replicate responds 200 OK with a
    non-JSON body -- a real, if unusual, way a follower could misbehave
    (a crashed/misbehaving node, a proxy injecting a body, a
    version-skewed peer). Exercises Replicator._send's malformed-response
    handling (see docs/AUDIT_FINDINGS.md's §3): this must be treated as
    a failed ack, not crash the write path with an unhandled 500.
    """
    storage = KVStore()
    app = create_follower_with_storage(node_id, storage)

    # See common.server.replace_route's docstring for why this is
    # necessary, not just tidy.
    replace_route(app, "/internal/replicate", {"POST"})

    @app.post("/internal/replicate")
    def malformed_replicate(body: dict) -> PlainTextResponse:  # type: ignore[type-arg]
        return PlainTextResponse("not json", status_code=200)

    return app


# --- Tests ----------------------------------------------------------------


def test_write_replicates_identical_version_to_all_followers(cluster):
    f1 = cluster(build_follower_app("f1"))
    f2 = cluster(build_follower_app("f2"))

    config = ClusterConfig(
        followers=[f1.address, f2.address], ack_required=2, timeout_seconds=2.0
    )
    leader = cluster(build_leader_app("leader-1", config))

    resp = httpx.put(f"http://{leader.host}:{leader.port}/kv/k", json={"value": "v1"})
    assert resp.status_code == 200
    put_body = resp.json()
    assert put_body["applied"] is True

    leader_entry = httpx.get(f"http://{leader.host}:{leader.port}/kv/k").json()
    assert leader_entry["timestamp"] == put_body["timestamp"]
    for follower in (f1, f2):
        entry = httpx.get(f"http://{follower.host}:{follower.port}/kv/k").json()
        # Same timestamp and node_id as the leader's local write -- the
        # follower stored the identical versioned entry, not a fresh one
        # stamped with its own identity/clock.
        assert entry["value"] == "v1"
        assert entry["timestamp"] == leader_entry["timestamp"]
        assert entry["node_id"] == "leader-1"


def test_ack_required_met_by_fast_followers_ignores_slow_one(cluster):
    fast1 = cluster(build_follower_app("fast1"))
    fast2 = cluster(build_follower_app("fast2"))
    slow = cluster(_slow_follower_app("slow", delay_seconds=2.0))

    config = ClusterConfig(
        followers=[fast1.address, fast2.address, slow.address],
        ack_required=2,
        timeout_seconds=1.0,
    )
    leader = cluster(build_leader_app("leader-1", config))

    start = time.time()
    resp = httpx.put(f"http://{leader.host}:{leader.port}/kv/k", json={"value": "v1"})
    elapsed = time.time() - start

    assert resp.status_code == 200
    # Returned once the two fast followers acked -- well before the slow
    # follower's 2s delay or the 1s timeout.
    assert elapsed < 0.5


def test_unmet_ack_required_fails_loudly_within_timeout(cluster):
    slow1 = cluster(_slow_follower_app("slow1", delay_seconds=2.0))
    slow2 = cluster(_slow_follower_app("slow2", delay_seconds=2.0))

    config = ClusterConfig(
        followers=[slow1.address, slow2.address], ack_required=2, timeout_seconds=0.3
    )
    leader = cluster(build_leader_app("leader-1", config))

    start = time.time()
    resp = httpx.put(f"http://{leader.host}:{leader.port}/kv/k", json={"value": "v1"})
    elapsed = time.time() - start

    assert resp.status_code == 503
    assert "0/2" in resp.json()["detail"]
    # Failed at roughly the timeout, not after waiting for the slow
    # followers to actually finish.
    assert elapsed < 1.0


def test_late_follower_ack_still_applies_after_client_response(cluster):
    slow = cluster(_slow_follower_app("slow", delay_seconds=0.5))

    config = ClusterConfig(
        followers=[slow.address], ack_required=0, timeout_seconds=0.2
    )
    leader = cluster(build_leader_app("leader-1", config))

    resp = httpx.put(f"http://{leader.host}:{leader.port}/kv/k", json={"value": "v1"})
    assert resp.status_code == 200

    # The follower hasn't necessarily applied the write yet (its
    # /internal/replicate is still sleeping) -- poll until it does,
    # bounded well past its 0.5s delay, to confirm the in-flight
    # request wasn't cancelled when the client got its response.
    deadline = time.time() + 3
    entry = None
    while time.time() < deadline:
        r = httpx.get(f"http://{slow.host}:{slow.port}/kv/k")
        if r.status_code == 200:
            entry = r.json()
            break
        time.sleep(0.05)

    assert entry is not None, "late follower ack was never applied"
    assert entry["value"] == "v1"


def test_ack_required_zero_returns_immediately(cluster):
    slow = cluster(_slow_follower_app("slow", delay_seconds=1.0))

    config = ClusterConfig(followers=[slow.address], ack_required=0, timeout_seconds=5.0)
    leader = cluster(build_leader_app("leader-1", config))

    start = time.time()
    resp = httpx.put(f"http://{leader.host}:{leader.port}/kv/k", json={"value": "v1"})
    elapsed = time.time() - start

    assert resp.status_code == 200
    # Fire-and-forget: doesn't wait on the follower at all, even though
    # both its delay and the configured timeout are much longer.
    assert elapsed < 0.5


def test_unreachable_follower_does_not_block_success_when_others_suffice(cluster):
    good = cluster(build_follower_app("good"))
    dead_port = _free_port()  # nothing listening here

    config = ClusterConfig(
        followers=[good.address, Follower(host="127.0.0.1", port=dead_port)],
        ack_required=1,
        timeout_seconds=2.0,
    )
    leader = cluster(build_leader_app("leader-1", config))

    start = time.time()
    resp = httpx.put(f"http://{leader.host}:{leader.port}/kv/k", json={"value": "v1"})
    elapsed = time.time() - start

    assert resp.status_code == 200
    # A refused connection fails almost instantly; the single required
    # ack from the reachable follower should satisfy ack_required well
    # under the 2s timeout.
    assert elapsed < 1.0


# --- Malformed peer response handling ---------------------------------------


def test_malformed_follower_response_fails_ack_not_500(cluster):
    """docs/AUDIT_FINDINGS.md's §3: a follower that responds 200 with a
    malformed body used to crash the write path with an unhandled 500
    (json.JSONDecodeError propagating out of Replicator._send, through
    replicate(), to the client) instead of being counted as a plain
    failed ack. With one good follower and one malformed one at
    ack_required=2, only the good one can ever ack -- this should fail
    loudly with the project's usual 503/"N/M acks" shape, never a 500.
    """
    good = cluster(build_follower_app("good"))
    malformed = cluster(_malformed_follower_app("malformed"))

    config = ClusterConfig(
        followers=[good.address, malformed.address], ack_required=2, timeout_seconds=1.0
    )
    leader = cluster(build_leader_app("leader-1", config))

    resp = httpx.put(f"http://{leader.host}:{leader.port}/kv/k", json={"value": "v1"})

    assert resp.status_code == 503
    assert "1/2" in resp.json()["detail"]


# --- Connection-pool sizing (module-level, not per-config) -----------------


def test_build_app_pool_size_is_module_constant_regardless_of_config():
    """Guards docs/AUDIT_FINDINGS.md's §1 finding directly: pool sizing
    (_CLIENT_MAX_CONNECTIONS / _CLIENT_MAX_KEEPALIVE_CONNECTIONS) is a
    single module-level setting applied identically to the httpx client
    build_app() constructs, no matter what ack_required (or anything
    else in ClusterConfig) a given leader is launched with -- there is
    no per-config pool sizing threaded through ClusterConfig or CLI
    args. If a future change (e.g. while adding the message-queue
    replication path) reintroduces per-config pool sizing, this test
    should fail: every httpx.AsyncClient build_app() constructs, across
    every ack_required value, must be passed `limits=` equal to the two
    module constants exactly.

    httpx.AsyncClient itself is mocked (rather than inspecting the real
    client's private transport/pool internals) so this test asserts on
    what build_app() actually *requests*, via the public httpx.Limits
    object it constructs and passes in -- not on httpx/httpcore's
    internal representation of those limits, which isn't public API and
    could change independently of this project's own code. Patched at
    common.replication_client.httpx.AsyncClient, not
    leader_follower.leader's own reference to httpx, since that's where
    the actual client construction lives (build_app() calls
    common.replication_client.build_replication_client(), it doesn't
    build the client itself) -- see docs/AUDIT_FINDINGS.md's §5.
    """
    followers = [
        Follower(host="127.0.0.1", port=9001),
        Follower(host="127.0.0.1", port=9002),
        Follower(host="127.0.0.1", port=9003),
    ]

    with patch("common.replication_client.httpx.AsyncClient") as mock_async_client:
        for ack_required in range(len(followers) + 1):  # 0..3, every valid value
            config = ClusterConfig(
                followers=followers, ack_required=ack_required, timeout_seconds=1.0
            )
            build_leader_app(f"leader-{ack_required}", config)

    assert mock_async_client.call_count == len(followers) + 1
    for _, kwargs in mock_async_client.call_args_list:
        limits = kwargs["limits"]
        assert limits.max_connections == _CLIENT_MAX_CONNECTIONS
        assert limits.max_keepalive_connections == _CLIENT_MAX_KEEPALIVE_CONNECTIONS


# --- ClusterConfig parsing/validation -------------------------------------


def test_cluster_config_from_dict_parses_followers_and_policy():
    config = ClusterConfig.from_dict(
        {
            "followers": [
                {"host": "10.0.0.1", "port": 8001},
                {"host": "10.0.0.2", "port": 8002},
            ],
            "ack_required": 1,
            "timeout_seconds": 3.5,
        }
    )
    assert config.followers == [
        Follower(host="10.0.0.1", port=8001),
        Follower(host="10.0.0.2", port=8002),
    ]
    assert config.ack_required == 1
    assert config.timeout_seconds == 3.5


@pytest.mark.parametrize("ack_required", [-1, 3])
def test_cluster_config_rejects_ack_required_out_of_bounds(ack_required):
    with pytest.raises(ValueError):
        ClusterConfig.from_dict(
            {
                "followers": [{"host": "h", "port": 1}, {"host": "h", "port": 2}],
                "ack_required": ack_required,
                "timeout_seconds": 1.0,
            }
        )


def test_cluster_config_rejects_non_positive_timeout():
    with pytest.raises(ValueError):
        ClusterConfig.from_dict(
            {"followers": [], "ack_required": 0, "timeout_seconds": 0}
        )


def test_repo_cluster_config_file_parses():
    # Regression test tying the test suite to the real, checked-in
    # config file -- catches schema drift between the YAML and the
    # dataclass that reads it.
    config = ClusterConfig.from_yaml("config/leader_follower_cluster.yaml")
    assert len(config.followers) >= 1
    assert 0 <= config.ack_required <= len(config.followers)
    assert config.timeout_seconds > 0


# --- --ack-required CLI override ------------------------------------------


def _write_cluster_config(
    tmp_path, *, followers: int = 2, ack_required: int = 1, timeout_seconds: float = 2.0
):
    """Write a minimal cluster config YAML with `followers` followers
    and return its path, for exercising --config/--ack-required
    resolution without touching the checked-in config file.
    """
    lines = ["followers:"]
    for i in range(followers):
        lines.append(f"  - host: 127.0.0.1\n    port: {9000 + i}")
    lines.append(f"ack_required: {ack_required}")
    lines.append(f"timeout_seconds: {timeout_seconds}")
    path = tmp_path / "cluster.yaml"
    path.write_text("\n".join(lines))
    return path


def test_ack_required_cli_flag_overrides_yaml_value(tmp_path):
    config_path = _write_cluster_config(tmp_path, followers=2, ack_required=1)

    args = _parse_args(
        ["--node-id", "leader-1", "--config", str(config_path), "--ack-required", "2"]
    )
    config = _resolve_config(args)

    assert config.ack_required == 2


def test_ack_required_omitted_uses_yaml_default(tmp_path):
    config_path = _write_cluster_config(tmp_path, followers=2, ack_required=1)

    args = _parse_args(["--node-id", "leader-1", "--config", str(config_path)])
    config = _resolve_config(args)

    # No --ack-required given -- behavior unchanged, YAML value used as-is.
    assert config.ack_required == 1


@pytest.mark.parametrize("ack_required", [-1, 3])
def test_ack_required_cli_flag_out_of_range_is_rejected(tmp_path, ack_required):
    # YAML's own value (1) is in-range; it's the CLI override that's
    # out of bounds for 2 followers here.
    config_path = _write_cluster_config(tmp_path, followers=2, ack_required=1)

    args = _parse_args(
        [
            "--node-id",
            "leader-1",
            "--config",
            str(config_path),
            "--ack-required",
            str(ack_required),
        ]
    )
    with pytest.raises(ValueError):
        _resolve_config(args)


@pytest.mark.parametrize("ack_required", [-1, 3])
def test_ack_required_yaml_out_of_range_still_rejected_without_cli_flag(
    tmp_path, ack_required
):
    # No --ack-required override at all -- the out-of-range value comes
    # straight from the YAML, same validation path as before this flag
    # existed.
    config_path = _write_cluster_config(tmp_path, followers=2, ack_required=ack_required)

    args = _parse_args(["--node-id", "leader-1", "--config", str(config_path)])
    with pytest.raises(ValueError):
        _resolve_config(args)
