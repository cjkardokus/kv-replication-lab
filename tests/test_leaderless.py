"""Tests for leaderless/node.py.

Testing approach
----------------
Same rationale as tests/test_leader_follower.py: node.py's coordinator
logic is fundamentally about *inter-process* HTTP -- real httpx calls to
real peer addresses, racing a real clock-based timeout. So these tests
run actual uvicorn servers, each in its own background thread bound to a
real loopback port, and drive them with a real httpx client, exactly
like a node does against its real peers in production. See that file's
docstring for the fuller trade-off discussion (heavier than TestClient,
but the only way to genuinely exercise concurrent fan-out, the
w/r-vs-timeout race, and unreachable/slow peers).

One extra wrinkle here versus the leader-follower tests: every
leaderless node is a coordinator, including the peers in a test cluster.
Some tests need a peer that behaves like a real node for
/internal/replicate and /internal/kv/{key} (what a coordinator actually
calls on its peers) without running full coordinator logic itself, or
that responds slowly on purpose -- see _peer_app/_slow_peer_app below,
built the same way node.py's own build_app() swaps routes onto the
stock common.server app.
"""

from __future__ import annotations

import socket
import threading
import time

import httpx
import pytest
import uvicorn
from fastapi import FastAPI, HTTPException

from common.server import KVResponse, ReplicateResponse, create_app
from common.storage import KVStore
from leaderless.node import (
    ClusterConfig,
    Node,
    _parse_args,
    _resolve_config,
    build_app,
)


# --- Real-server test harness --------------------------------------------


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RunningServer:
    """A uvicorn server running one ASGI app in a background thread, on
    a caller-chosen port. The port is chosen by the caller (not picked
    internally, unlike test_leader_follower.py's harness) because
    leaderless nodes need to know their own port *before* the app is
    built, to compute their peers list.
    """

    def __init__(self, app: FastAPI, host: str, port: int) -> None:
        self.host = host
        self.port = port
        config = uvicorn.Config(app, host=self.host, port=self.port, log_level="warning")
        self.server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self) -> "RunningServer":
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
    def address(self) -> Node:
        return Node(host=self.host, port=self.port)


@pytest.fixture
def cluster():
    """Yields a factory for starting real servers on chosen ports; stops
    them all after the test regardless of outcome.
    """
    servers: list[RunningServer] = []

    def _start(app: FastAPI, port: int) -> RunningServer:
        server = RunningServer(app, "127.0.0.1", port).start()
        servers.append(server)
        return server

    yield _start

    for server in servers:
        server.stop()


def _build_cluster(
    cluster, node_ids: list[str], *, default_w: int, default_r: int,
    timeout_seconds: float = 2.0,
) -> tuple[list[RunningServer], ClusterConfig]:
    """Start len(node_ids) real leaderless nodes (via node.build_app),
    each configured with the full node list so every one's peers are
    every other one.
    """
    ports = [_free_port() for _ in node_ids]
    nodes = [Node(host="127.0.0.1", port=p) for p in ports]
    config = ClusterConfig(
        nodes=nodes, default_w=default_w, default_r=default_r,
        timeout_seconds=timeout_seconds,
    )
    servers = [
        cluster(build_app(node_id, port, config), port)
        for node_id, port in zip(node_ids, ports)
    ]
    return servers, config


def _seed(server: RunningServer, key: str, *, value, timestamp: float, node_id: str) -> None:
    """Write a specific (value, timestamp, node_id) directly to one
    node's storage via /internal/replicate, bypassing coordinator logic
    entirely -- so tests can set up specific staleness/ownership
    scenarios across nodes without racing real concurrent quorum writes.
    """
    resp = httpx.post(
        f"http://{server.host}:{server.port}/internal/replicate",
        json={"key": key, "value": value, "timestamp": timestamp, "node_id": node_id},
    )
    assert resp.status_code == 200


def _peer_app(node_id: str, storage: KVStore) -> FastAPI:
    """A minimal stand-in for a leaderless peer: the stock
    common.server app (GET/PUT/DELETE, /internal/replicate, /health)
    plus GET /internal/kv/{key}, the one extra route real leaderless
    nodes add for peers to serve raw local reads (see
    leaderless/node.py's build_app). No coordinator logic of its own --
    just enough surface for another node to treat it as a peer.
    """
    app = create_app(storage, node_id)

    @app.get("/internal/kv/{key}", response_model=KVResponse)
    def internal_get_key(key: str) -> KVResponse:
        entry = storage.get(key)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"key {key!r} not found")
        return KVResponse(
            key=key, value=entry.value, timestamp=entry.timestamp, node_id=entry.node_id,
        )

    return app


def _counting_peer_app(node_id: str, storage: KVStore, received: list[str]) -> FastAPI:
    """A _peer_app whose /internal/replicate and /internal/kv/{key}
    routes append `node_id` to the shared `received` list before doing
    their normal job -- lets a test assert exactly how many/which peers
    were actually contacted for a given quorum-bounded write or read,
    independent of how many of them ended up counting toward quorum.
    """
    app = _peer_app(node_id, storage)

    app.router.routes = [
        route
        for route in app.router.routes
        if getattr(route, "path", None) not in ("/internal/replicate", "/internal/kv/{key}")
    ]

    @app.post("/internal/replicate", response_model=ReplicateResponse)
    def replicate(body: dict) -> ReplicateResponse:  # type: ignore[type-arg]
        received.append(node_id)
        applied = storage.put(
            body["key"], body["value"], timestamp=body["timestamp"], node_id=body["node_id"]
        )
        return ReplicateResponse(applied=applied)

    @app.get("/internal/kv/{key}", response_model=KVResponse)
    def internal_get_key(key: str) -> KVResponse:
        received.append(node_id)
        entry = storage.get(key)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"key {key!r} not found")
        return KVResponse(
            key=key, value=entry.value, timestamp=entry.timestamp, node_id=entry.node_id,
        )

    return app


def _slow_peer_app(node_id: str, storage: KVStore, *, replicate_delay: float) -> FastAPI:
    """A peer whose /internal/replicate sleeps before applying the
    write, to simulate a slow/unresponsive node for write-timeout tests.
    Built the same way node.py's build_app overrides routes: take the
    stock peer app and swap out just the route under test.
    """
    app = _peer_app(node_id, storage)

    app.router.routes = [
        route
        for route in app.router.routes
        if not (
            getattr(route, "path", None) == "/internal/replicate"
            and "POST" in getattr(route, "methods", ())
        )
    ]

    @app.post("/internal/replicate", response_model=ReplicateResponse)
    def slow_replicate(body: dict) -> ReplicateResponse:  # type: ignore[type-arg]
        time.sleep(replicate_delay)
        applied = storage.put(
            body["key"], body["value"], timestamp=body["timestamp"], node_id=body["node_id"]
        )
        return ReplicateResponse(applied=applied)

    return app


# --- Write quorum tests -----------------------------------------------------


def test_write_reaches_w_nodes_successfully(cluster):
    servers, _ = _build_cluster(cluster, ["n1", "n2", "n3"], default_w=2, default_r=2)
    coordinator, peers = servers[0], servers[1:]

    resp = httpx.put(f"http://{coordinator.host}:{coordinator.port}/kv/k", json={"value": "v1"})
    assert resp.status_code == 200
    assert resp.json()["applied"] is True

    # w=2 is satisfied by the coordinator's own local write plus one
    # peer ack -- poll until at least one peer has the value (both may
    # still be racing to land it in the background).
    deadline = time.time() + 2
    landed = 0
    while time.time() < deadline:
        landed = sum(
            1 for p in peers
            if httpx.get(f"http://{p.host}:{p.port}/kv/k").status_code == 200
        )
        if landed >= 1:
            break
        time.sleep(0.02)
    assert landed >= 1


def test_write_times_out_when_w_not_reached(cluster):
    # w=3 needs both peers to ack, but both are far slower than the
    # timeout -- only the coordinator's own local write (1) lands in
    # time.
    ports = [_free_port() for _ in range(3)]
    nodes = [Node(host="127.0.0.1", port=p) for p in ports]
    config = ClusterConfig(nodes=nodes, default_w=3, default_r=2, timeout_seconds=0.3)

    cluster(_slow_peer_app("slow1", KVStore(), replicate_delay=2.0), ports[1])
    cluster(_slow_peer_app("slow2", KVStore(), replicate_delay=2.0), ports[2])
    coordinator = cluster(build_app("coord", ports[0], config), ports[0])

    start = time.time()
    resp = httpx.put(f"http://{coordinator.host}:{coordinator.port}/kv/k", json={"value": "v1"})
    elapsed = time.time() - start

    assert resp.status_code == 503
    assert "1/3" in resp.json()["detail"]
    # Failed at roughly the timeout, not after waiting for the slow
    # peers to actually finish.
    assert elapsed < 1.0


# --- Fan-out: writes flood, reads are exact (no resilience margin) ----------


def test_write_floods_every_peer_regardless_of_w(cluster):
    """A W-write's ack-*wait* only needs W-1 peer acks, but every peer
    should still be *contacted* -- durability fan-out is unconditional,
    independent of W (see QuorumCoordinator.replicate_write). With
    N=5, w=2 (needed=1), all 4 peers should eventually receive the
    replicate call, not just the 1 the coordinator waits on.
    """
    received: list[str] = []
    ports = [_free_port() for _ in range(5)]
    nodes = [Node(host="127.0.0.1", port=p) for p in ports]
    config = ClusterConfig(nodes=nodes, default_w=2, default_r=2, timeout_seconds=2.0)

    for i, port in enumerate(ports[1:], start=1):
        cluster(_counting_peer_app(f"peer{i}", KVStore(), received), port)
    coordinator = cluster(build_app("coord", ports[0], config), ports[0])

    resp = httpx.put(f"http://{coordinator.host}:{coordinator.port}/kv/k", json={"value": "v1"})
    assert resp.status_code == 200

    deadline = time.time() + 2
    while len(set(received)) < 4 and time.time() < deadline:
        time.sleep(0.02)
    assert set(received) == {"peer1", "peer2", "peer3", "peer4"}


def test_read_contacts_exactly_r_minus_one_peers(cluster):
    """An R-read should touch exactly R nodes total (coordinator plus
    R-1 peers) -- no extra peers contacted for resilience margin, unlike
    the write path. N=5, r=2 -> needed=1 peer, no matter how many peers
    exist.
    """
    received: list[str] = []
    ports = [_free_port() for _ in range(5)]
    nodes = [Node(host="127.0.0.1", port=p) for p in ports]
    config = ClusterConfig(nodes=nodes, default_w=2, default_r=2, timeout_seconds=2.0)

    for i, port in enumerate(ports[1:], start=1):
        cluster(_counting_peer_app(f"peer{i}", KVStore(), received), port)
    coordinator = cluster(build_app("coord", ports[0], config), ports[0])

    resp = httpx.get(f"http://{coordinator.host}:{coordinator.port}/kv/k", params={"r": 2})
    # Nobody has this key -- 404 is expected; peer contact count is what
    # this test actually checks.
    assert resp.status_code == 404
    assert len(received) == 1


# --- Read quorum tests -------------------------------------------------------


def test_read_picks_newest_value_among_r_with_mixed_staleness(cluster):
    servers, _ = _build_cluster(cluster, ["n1", "n2", "n3"], default_w=1, default_r=3)
    n1, n2, n3 = servers

    _seed(n1, "k", value="old", timestamp=100.0, node_id="n1")
    _seed(n2, "k", value="middle", timestamp=200.0, node_id="n2")
    _seed(n3, "k", value="newest", timestamp=300.0, node_id="n3")

    resp = httpx.get(f"http://{n1.host}:{n1.port}/kv/k", params={"r": 3})
    assert resp.status_code == 200
    body = resp.json()
    assert body["value"] == "newest"
    assert body["timestamp"] == 300.0
    assert body["node_id"] == "n3"


def test_read_missing_entry_on_one_node_not_error_when_others_have_it(cluster):
    servers, _ = _build_cluster(cluster, ["n1", "n2", "n3"], default_w=1, default_r=3)
    n1, n2, n3 = servers
    # n1 (the coordinator for this read) has never seen this key --
    # should count as "responded, no entry", not an error, and not
    # prevent the other two nodes' entries from being compared.
    _seed(n2, "k", value="v-mid", timestamp=200.0, node_id="n2")
    _seed(n3, "k", value="v-new", timestamp=300.0, node_id="n3")

    resp = httpx.get(f"http://{n1.host}:{n1.port}/kv/k", params={"r": 3})
    assert resp.status_code == 200
    assert resp.json()["value"] == "v-new"


def test_read_returns_404_when_none_of_r_responses_have_key(cluster):
    servers, _ = _build_cluster(cluster, ["n1", "n2", "n3"], default_w=1, default_r=3)
    n1 = servers[0]
    # Nobody has ever written this key -- all r responses come back
    # with "no entry", so this is a genuine 404, not a coordinator error.
    resp = httpx.get(f"http://{n1.host}:{n1.port}/kv/missing", params={"r": 3})
    assert resp.status_code == 404


# --- W/R validation -----------------------------------------------------


@pytest.mark.parametrize("field", ["default_w", "default_r"])
@pytest.mark.parametrize("value", [0, 4])
def test_cluster_config_rejects_quorum_out_of_bounds(field, value):
    raw = {
        "nodes": [{"host": "h", "port": 1}, {"host": "h", "port": 2}, {"host": "h", "port": 3}],
        "default_w": 2,
        "default_r": 2,
        "timeout_seconds": 1.0,
    }
    raw[field] = value
    with pytest.raises(ValueError):
        ClusterConfig.from_dict(raw)


def test_w_query_param_out_of_range_rejected(cluster):
    servers, _ = _build_cluster(cluster, ["n1", "n2", "n3"], default_w=2, default_r=2)
    n1 = servers[0]
    # N=3, so w must be between 1 and 3.
    resp = httpx.put(f"http://{n1.host}:{n1.port}/kv/k", json={"value": "v"}, params={"w": 4})
    assert resp.status_code == 422


def test_r_query_param_out_of_range_rejected(cluster):
    servers, _ = _build_cluster(cluster, ["n1", "n2", "n3"], default_w=2, default_r=2)
    n1 = servers[0]
    resp = httpx.get(f"http://{n1.host}:{n1.port}/kv/k", params={"r": 0})
    assert resp.status_code == 422


# --- Fault injection (--fault-inject-delay-ms, off by default) -------------


def test_fault_inject_delay_disabled_by_default_replicate_is_unmodified(cluster):
    """build_app()'s default (fault_inject_delay_seconds=0.0) must leave
    /internal/replicate exactly as create_app() provides it -- no
    artificial delay, unless a caller explicitly opts in.
    """
    port = _free_port()
    node = Node(host="127.0.0.1", port=port)
    config = ClusterConfig(nodes=[node], default_w=1, default_r=1, timeout_seconds=2.0)
    server = cluster(build_app("n1", port, config), port)

    start = time.time()
    resp = httpx.post(
        f"http://{server.host}:{server.port}/internal/replicate",
        json={"key": "k", "value": "v1", "timestamp": 1.0, "node_id": "n1"},
    )
    elapsed = time.time() - start

    assert resp.status_code == 200
    assert elapsed < 0.2


def test_fault_inject_delay_delays_replicate_ack_and_apply(cluster):
    """A positive fault_inject_delay_seconds makes /internal/replicate
    sleep before acknowledging *and* before applying the write -- both
    the HTTP round trip and visibility of the new value are delayed by
    (at least) that long, which is the whole mechanism the boundary-case
    demo experiment relies on to widen the write-vs-read race window.
    """
    port = _free_port()
    node = Node(host="127.0.0.1", port=port)
    config = ClusterConfig(nodes=[node], default_w=1, default_r=1, timeout_seconds=2.0)
    server = cluster(
        build_app("n1", port, config, fault_inject_delay_seconds=0.3), port
    )

    start = time.time()
    resp = httpx.post(
        f"http://{server.host}:{server.port}/internal/replicate",
        json={"key": "k", "value": "v1", "timestamp": 1.0, "node_id": "n1"},
        timeout=2.0,
    )
    elapsed = time.time() - start

    assert resp.status_code == 200
    assert elapsed >= 0.3
    entry = httpx.get(f"http://{server.host}:{server.port}/internal/kv/k").json()
    assert entry["value"] == "v1"


# --- ClusterConfig parsing --------------------------------------------------


def test_cluster_config_from_dict_parses_nodes_and_quorum_defaults():
    config = ClusterConfig.from_dict(
        {
            "nodes": [
                {"host": "10.0.0.1", "port": 8001},
                {"host": "10.0.0.2", "port": 8002},
                {"host": "10.0.0.3", "port": 8003},
            ],
            "default_w": 2,
            "default_r": 1,
            "timeout_seconds": 3.5,
        }
    )
    assert config.nodes == [
        Node(host="10.0.0.1", port=8001),
        Node(host="10.0.0.2", port=8002),
        Node(host="10.0.0.3", port=8003),
    ]
    assert config.default_w == 2
    assert config.default_r == 1
    assert config.timeout_seconds == 3.5


def test_peers_excluding_removes_self_by_port():
    config = ClusterConfig(
        nodes=[Node(host="127.0.0.1", port=p) for p in (8001, 8002, 8003)],
        default_w=1, default_r=1, timeout_seconds=1.0,
    )
    assert config.peers_excluding(8002) == [
        Node(host="127.0.0.1", port=8001),
        Node(host="127.0.0.1", port=8003),
    ]


def test_repo_cluster_config_file_parses():
    # Regression test tying the test suite to the real, checked-in
    # config file -- catches schema drift between the YAML and the
    # dataclass that reads it.
    config = ClusterConfig.from_yaml("config/leaderless_cluster.yaml")
    assert len(config.nodes) >= 1
    assert 1 <= config.default_w <= len(config.nodes)
    assert 1 <= config.default_r <= len(config.nodes)
    assert config.timeout_seconds > 0


# --- --default-w/--default-r CLI override -----------------------------------


def _write_cluster_config(
    tmp_path, *, nodes: int = 3, default_w: int = 1, default_r: int = 1,
    timeout_seconds: float = 2.0,
):
    lines = ["nodes:"]
    for i in range(nodes):
        lines.append(f"  - host: 127.0.0.1\n    port: {9000 + i}")
    lines.append(f"default_w: {default_w}")
    lines.append(f"default_r: {default_r}")
    lines.append(f"timeout_seconds: {timeout_seconds}")
    path = tmp_path / "cluster.yaml"
    path.write_text("\n".join(lines))
    return path


def test_default_w_cli_flag_overrides_yaml_value(tmp_path):
    config_path = _write_cluster_config(tmp_path, default_w=1, default_r=1)
    args = _parse_args(
        ["--node-id", "n1", "--config", str(config_path), "--default-w", "2"]
    )
    config = _resolve_config(args)
    assert config.default_w == 2


def test_default_r_cli_flag_overrides_yaml_value(tmp_path):
    config_path = _write_cluster_config(tmp_path, default_w=1, default_r=1)
    args = _parse_args(
        ["--node-id", "n1", "--config", str(config_path), "--default-r", "3"]
    )
    config = _resolve_config(args)
    assert config.default_r == 3


def test_default_w_r_omitted_uses_yaml_defaults(tmp_path):
    config_path = _write_cluster_config(tmp_path, default_w=2, default_r=3)
    args = _parse_args(["--node-id", "n1", "--config", str(config_path)])
    config = _resolve_config(args)
    assert config.default_w == 2
    assert config.default_r == 3


@pytest.mark.parametrize("default_w", [0, 4])
def test_default_w_cli_flag_out_of_range_is_rejected(tmp_path, default_w):
    config_path = _write_cluster_config(tmp_path, nodes=3, default_w=1, default_r=1)
    args = _parse_args(
        [
            "--node-id", "n1", "--config", str(config_path),
            "--default-w", str(default_w),
        ]
    )
    with pytest.raises(ValueError):
        _resolve_config(args)
