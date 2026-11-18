"""Kiểm thử API, WAL và đồng bộ cho Backup Replica Node."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from fastapi.testclient import TestClient

from main import create_app
from node import BackupNode


class FakePrimary:
    """Primary giả có WAL commit và có thể ngắt kết nối hoặc làm chậm."""

    def __init__(self, entries=None, delay: float = 0.0):
        self.entries = list(entries or [])
        self.delay = delay
        self.unreachable = False
        self.catchup_calls = 0
        self.health_calls = 0
        self.transport = httpx.MockTransport(self.handle_async_request)

    @property
    def version(self):
        return max((entry["version"] for entry in self.entries), default=0)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = urlparse(str(request.url)).path
        if self.unreachable:
            return httpx.Response(503, json={"detail": "Primary không khả dụng"})
        if path.endswith("/api/v1/catch-up"):
            self.catchup_calls += 1
            if self.delay:
                await asyncio.sleep(self.delay)
            query = parse_qs(urlparse(str(request.url)).query)
            from_version = int(query["from_version"][0])
            return httpx.Response(
                200,
                json={
                    "current_primary_version": self.version,
                    "missed_count": sum(
                        entry["version"] > from_version for entry in self.entries
                    ),
                    "entries": [
                        dict(entry)
                        for entry in self.entries
                        if entry["version"] > from_version
                    ],
                },
            )
        if path.endswith("/api/v1/health"):
            self.health_calls += 1
            return httpx.Response(200, json={"current_version": self.version})
        return httpx.Response(404, json={"detail": "Not found"})


def make_entry(version: int, key="k", value=None, operation="PUT", **extra):
    return {
        "version": version,
        "commit_version": version - 1,
        "operation": operation,
        "key": key,
        "value": value if operation == "PUT" else None,
        "timestamp": float(version),
        **extra,
    }


def make_node(tmp_path: Path, primary: FakePrimary, **options):
    values = {
        "node_id": "backup-test",
        "primary_url": "http://fake-primary",
        "data_dir": tmp_path,
        "transport": primary.transport,
        "catchup_http_timeout": 0.2,
        "connect_timeout": 0.05,
        "replicate_catchup_deadline": 0.15,
        "primary_failures_before_degraded": 5,
        "sync_interval": 10.0,
        "catchup_delay_ms": 0,
        "pending_ttl": 30.0,
    }
    values.update(options)
    return BackupNode(**values)


def wait_until(check, timeout=1.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if check():
            return True
        time.sleep(0.005)
    return bool(check())


def open_client(node):
    return TestClient(create_app(node))


def body(version, commit_version, value="value", key="k", **extra):
    return {
        "version": version,
        "commit_version": commit_version,
        "operation": "PUT",
        "key": key,
        "value": value,
        "timestamp": float(version),
        **extra,
    }


def committed_wal(path):
    wal = path / "backup_wal.jsonl"
    if not wal.exists():
        return []
    return [json.loads(line) for line in wal.read_text(encoding="utf-8").splitlines()]


def start_alive(client, primary):
    assert wait_until(lambda: client.get("/api/v1/health").json()["status"] == "ALIVE")
    # Bỏ lần gọi catch-up khởi động khỏi số đo của từng ca kiểm thử.
    primary.catchup_calls = 0


def test_prepare_commit_read_and_pending_does_not_advance_version(tmp_path):
    primary = FakePrimary()
    node = make_node(tmp_path, primary)
    with open_client(node) as client:
        start_alive(client, primary)
        prepared = client.post("/api/v1/replicate", json=body(1, 0, "xin chào"))
        assert prepared.status_code == 200
        assert prepared.json() == {
            "status": "ACK",
            "node_id": "backup-test",
            "applied_version": 1,
        }
        assert client.get("/api/v1/health").json()["current_version"] == 0
        assert client.get("/api/v1/read/k").json()["found"] is False

        committed = client.post("/api/v1/commit", json={"commit_version": 1})
        assert committed.status_code == 200
        read = client.get("/api/v1/read/k").json()
        assert read["value"] == "xin chào"
        assert read["version"] == read["node_version"] == 1
        assert read["found"] is True


def test_delete_keeps_tombstone_and_key_version(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        client.post("/api/v1/replicate", json=body(1, 0, "cũ"))
        client.post("/api/v1/commit", json={"commit_version": 1})
        deleted = client.post(
            "/api/v1/replicate",
            json={
                "version": 2,
                "commit_version": 1,
                "operation": "DELETE",
                "key": "k",
                "timestamp": 2.0,
            },
        )
        assert deleted.status_code == 200
        client.post("/api/v1/commit", json={"commit_version": 2})
        assert client.get("/api/v1/read/k").json() == {
            "node_id": "backup-test",
            "key": "k",
            "value": None,
            "version": 2,
            "node_version": 2,
            "found": False,
            "deleted": True,
        }


def test_unknown_key_read_shape(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        assert client.get("/api/v1/read/missing").json() == {
            "node_id": "backup-test",
            "key": "missing",
            "value": None,
            "version": 0,
            "node_version": 0,
            "found": False,
            "deleted": False,
        }


def test_in_order_async_entry_commits_without_primary_call(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        response = client.post("/api/v1/replicate", json=body(1, 1, "async"))
        assert response.status_code == 200
        assert client.get("/api/v1/read/k").json()["value"] == "async"
        assert client.get("/api/v1/health").json()["current_version"] == 1
        assert primary.catchup_calls == 0


def test_next_replicate_applies_prior_pending_version_first(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        client.post("/api/v1/replicate", json=body(1, 0, "one"))
        result = client.post("/api/v1/replicate", json=body(2, 1, "two"))
        assert result.status_code == 200
        assert client.get("/api/v1/read/k").json()["value"] == "one"
        state = client.get("/api/v1/debug/state").json()
        assert state["current_version"] == 1
        assert state["pending"] == [2]


def test_replicate_commit_entry_id_checks_only_commit_target(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        first = client.post(
            "/api/v1/replicate",
            json=body(1, 0, "one", entry_id="a"),
        )
        second = client.post(
            "/api/v1/replicate",
            json=body(2, 2, "two", entry_id="b", commit_entry_id="b"),
        )
        assert first.status_code == second.status_code == 200
        assert client.get("/api/v1/health").json()["current_version"] == 2
        assert [entry["value"] for entry in committed_wal(tmp_path)] == ["one", "two"]
        assert primary.catchup_calls == 0


def test_duplicate_and_out_of_order_commit_do_not_duplicate_wal(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        client.post("/api/v1/replicate", json=body(1, 0, "one"))
        primary.entries = [make_entry(1, value="one")]
        client.post("/api/v1/commit", json={"commit_version": 2})
        assert wait_until(lambda: client.get("/api/v1/health").json()["current_version"] == 1)
        # Gửi lặp bản ghi và commit cũ sau khi Primary đã bù version 1.
        client.post("/api/v1/replicate", json=body(1, 0, "one"))
        client.post("/api/v1/commit", json={"commit_version": 1})
        assert [entry["version"] for entry in committed_wal(tmp_path)] == [1]


def test_resending_aborted_version_overwrites_stale_pending(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        client.post("/api/v1/replicate", json=body(1, 0, "aborted"))
        client.post("/api/v1/replicate", json=body(1, 0, "committed"))
        client.post("/api/v1/commit", json={"commit_version": 1})
        assert client.get("/api/v1/read/k").json()["value"] == "committed"
        assert committed_wal(tmp_path)[0]["value"] == "committed"


def test_gap_catches_up_then_acks_incoming_entry(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        primary.entries = [make_entry(1, value="one"), make_entry(2, value="two")]
        response = client.post("/api/v1/replicate", json=body(3, 2, "three"))
        assert response.status_code == 200
        assert response.json()["status"] == "ACK"
        assert primary.catchup_calls == 1
        assert client.get("/api/v1/read/k").json()["value"] == "two"
        assert client.get("/api/v1/debug/state").json()["pending"] == [3]


def test_unreachable_primary_returns_top_level_nack_without_wal_append(tmp_path):
    primary = FakePrimary()
    node = make_node(
        tmp_path,
        primary,
        replicate_catchup_deadline=0.05,
        catchup_http_timeout=0.05,
    )
    with open_client(node) as client:
        start_alive(client, primary)
        primary.unreachable = True
        response = client.post("/api/v1/replicate", json=body(3, 2, "three"))
        assert response.status_code == 503
        assert response.json() == {
            "status": "NACK",
            "node_id": "backup-test",
            "reason": "GAP_NOT_FILLED",
            "current_version": 0,
        }
        assert committed_wal(tmp_path) == []


def test_gap_deadline_returns_catching_up_nack_while_shared_task_runs(tmp_path):
    primary = FakePrimary(delay=0.12)
    node = make_node(
        tmp_path,
        primary,
        replicate_catchup_deadline=0.02,
        catchup_http_timeout=0.3,
    )
    with open_client(node) as client:
        start_alive(client, primary)
        primary.entries = [make_entry(1, value="one"), make_entry(2, value="two")]
        response = client.post("/api/v1/replicate", json=body(3, 2, "three"))
        assert response.status_code == 503
        assert response.json()["reason"] == "CATCHING_UP"
        assert wait_until(
            lambda: client.get("/api/v1/health").json()["current_version"] == 2
        )


def test_commit_missing_pending_starts_background_catchup(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        primary.entries = [make_entry(1, value="from-primary")]
        response = client.post("/api/v1/commit", json={"commit_version": 1})
        assert response.status_code == 200
        assert response.json()["status"] == "COMMITTED"
        assert wait_until(lambda: client.get("/api/v1/health").json()["current_version"] == 1)
        assert client.get("/api/v1/read/k").json()["value"] == "from-primary"


def test_restart_recovers_committed_wal(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        client.post("/api/v1/replicate", json=body(1, 0, "persisted"))
        client.post("/api/v1/commit", json={"commit_version": 1})

    primary.entries = [make_entry(1, value="persisted")]
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        assert client.get("/api/v1/read/k").json()["value"] == "persisted"
        assert client.get("/api/v1/health").json()["current_version"] == 1


def test_torn_last_wal_line_is_truncated(tmp_path):
    wal = tmp_path / "backup_wal.jsonl"
    wal.write_text(json.dumps(make_entry(1, value="ok")) + "\n{" + '"version":2', encoding="utf-8")
    node = make_node(tmp_path, FakePrimary())
    node.load_wal()
    assert node.current_version == 1
    assert node.wal_file.read_text(encoding="utf-8").splitlines() == [
        json.dumps(make_entry(1, value="ok"))
    ]


def test_valid_json_missing_fields_on_last_wal_line_is_truncated(tmp_path):
    wal = tmp_path / "backup_wal.jsonl"
    wal.write_text(
        json.dumps(make_entry(1, value="ok")) + "\n" + json.dumps({"version": 2}) + "\n",
        encoding="utf-8",
    )
    node = make_node(tmp_path, FakePrimary())
    node.load_wal()
    assert node.current_version == 1
    assert len(wal.read_text(encoding="utf-8").splitlines()) == 1


def test_corrupt_middle_wal_line_refuses_start(tmp_path):
    wal = tmp_path / "backup_wal.jsonl"
    wal.write_text(
        json.dumps(make_entry(1, value="one"))
        + "\nnot-json\n"
        + json.dumps(make_entry(2, value="two"))
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="giữa tệp"):
        make_node(tmp_path, FakePrimary()).load_wal()


@pytest.mark.parametrize("versions", [[2], [1, 3]])
def test_non_contiguous_wal_refuses_start(tmp_path, versions):
    wal = tmp_path / "backup_wal.jsonl"
    wal.write_text(
        "".join(json.dumps(make_entry(version, value=str(version))) + "\n" for version in versions),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="không liên tục"):
        make_node(tmp_path, FakePrimary()).load_wal()


def test_valid_last_wal_line_without_newline_gets_separator(tmp_path):
    wal = tmp_path / "backup_wal.jsonl"
    wal.write_text(json.dumps(make_entry(1, value="ok")), encoding="utf-8")
    node = make_node(tmp_path, FakePrimary())
    node.load_wal()
    assert wal.read_bytes().endswith(b"\n")
    node._append_one(make_entry(2, value="next"))
    assert len(wal.read_text(encoding="utf-8").splitlines()) == 2


def test_startup_catchup_recovers_downtime_versions(tmp_path):
    primary = FakePrimary([make_entry(i, value=f"v{i}") for i in range(1, 4)])
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        assert client.get("/api/v1/health").json()["current_version"] == 3
        assert [entry["version"] for entry in committed_wal(tmp_path)] == [1, 2, 3]


def test_reads_return_503_during_catchup(tmp_path):
    primary = FakePrimary([make_entry(1, value="one")], delay=0.12)
    node = make_node(tmp_path, primary, catchup_delay_ms=20)
    with open_client(node) as client:
        response = client.get("/api/v1/read/k")
        assert response.status_code == 503
        assert response.json() == {"detail": "NODE_CATCHING_UP"}
        assert wait_until(lambda: client.get("/api/v1/health").json()["status"] == "ALIVE")


def test_primary_down_boot_degrades_serves_local_then_recovers(tmp_path):
    wal = tmp_path / "backup_wal.jsonl"
    wal.parent.mkdir(parents=True, exist_ok=True)
    wal.write_text(json.dumps(make_entry(1, value="local")) + "\n", encoding="utf-8")
    primary = FakePrimary([make_entry(1, value="local"), make_entry(2, value="new")])
    primary.unreachable = True
    node = make_node(
        tmp_path,
        primary,
        primary_failures_before_degraded=2,
        sync_interval=0.01,
        catchup_http_timeout=0.05,
    )
    with open_client(node) as client:
        assert wait_until(lambda: client.get("/api/v1/health").json()["status"] == "DEGRADED")
        assert client.get("/api/v1/read/k").json()["value"] == "local"
        primary.unreachable = False
        assert wait_until(lambda: client.get("/api/v1/health").json()["status"] == "ALIVE")
        assert client.get("/api/v1/read/k").json()["value"] == "new"


def test_anti_entropy_repairs_a_lost_commit_without_new_write(tmp_path):
    primary = FakePrimary()
    node = make_node(tmp_path, primary, sync_interval=0.02)
    with open_client(node) as client:
        start_alive(client, primary)
        # Primary đã commit ASYNC nhưng Backup chỉ còn bản ghi pending.
        client.post("/api/v1/replicate", json=body(1, 0, "async-value"))
        assert client.get("/api/v1/health").json()["current_version"] == 0
        primary.entries = [make_entry(1, value="async-value")]
        assert wait_until(lambda: client.get("/api/v1/health").json()["current_version"] == 1)
        assert client.get("/api/v1/read/k").json()["value"] == "async-value"


def test_expired_pending_is_replaced_by_primary_during_replicate(tmp_path):
    primary = FakePrimary()
    node = make_node(tmp_path, primary, pending_ttl=0.05)
    with open_client(node) as client:
        start_alive(client, primary)
        client.post("/api/v1/replicate", json=body(1, 0, "stale"))
        node._pending_received_at[1] = time.monotonic() - 1
        primary.entries = [make_entry(1, value="authoritative")]

        response = client.post("/api/v1/replicate", json=body(2, 1, "next"))

        assert response.status_code == 200
        assert client.get("/api/v1/read/k").json()["value"] == "authoritative"
        assert client.get("/api/v1/debug/state").json()["pending"] == [2]
        assert primary.catchup_calls == 1


def test_pending_within_ttl_commits_without_catchup(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        client.post("/api/v1/replicate", json=body(1, 0, "one"))

        response = client.post("/api/v1/replicate", json=body(2, 1, "two"))

        assert response.status_code == 200
        assert client.get("/api/v1/read/k").json()["value"] == "one"
        assert client.get("/api/v1/debug/state").json()["pending"] == [2]
        assert primary.catchup_calls == 0


def test_failed_background_catchup_restores_reads_for_alive_node(tmp_path):
    primary = FakePrimary()
    node = make_node(tmp_path, primary)
    with open_client(node) as client:
        start_alive(client, primary)
        primary.unreachable = True
        response = client.post("/api/v1/commit", json={"commit_version": 1})
        assert response.status_code == 200
        assert wait_until(
            lambda: node._catchup_task is not None and node._catchup_task.done()
        )

        assert client.get("/api/v1/read/k").status_code == 200
        assert client.get("/api/v1/health").json()["status"] != "CATCHING_UP"


def test_degraded_node_still_acks_in_order_replicate(tmp_path):
    primary = FakePrimary()
    primary.unreachable = True
    node = make_node(
        tmp_path,
        primary,
        primary_failures_before_degraded=1,
    )
    with open_client(node) as client:
        assert wait_until(
            lambda: client.get("/api/v1/health").json()["status"] == "DEGRADED"
        )
        prepared = client.post("/api/v1/replicate", json=body(1, 0, "local"))
        committed = client.post("/api/v1/commit", json={"commit_version": 1})

        assert prepared.status_code == 200
        assert prepared.json()["status"] == "ACK"
        assert committed.status_code == 200
        assert client.get("/api/v1/health").json()["current_version"] == 1
        assert client.get("/api/v1/read/k").json()["value"] == "local"


def test_commit_is_idempotent(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        client.post("/api/v1/replicate", json=body(1, 0, "one"))

        responses = [
            client.post("/api/v1/commit", json={"commit_version": 1})
            for _ in range(3)
        ]

        assert all(response.status_code == 200 for response in responses)
        assert client.get("/api/v1/health").json()["current_version"] == 1
        assert [entry["version"] for entry in committed_wal(tmp_path)] == [1]


def test_stale_commit_older_than_current_is_ignored(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        client.post("/api/v1/replicate", json=body(1, 1, "async"))
        primary.catchup_calls = 0

        response = client.post("/api/v1/commit", json={"commit_version": 0})

        assert response.status_code == 200
        assert client.get("/api/v1/health").json()["current_version"] == 1
        assert [entry["version"] for entry in committed_wal(tmp_path)] == [1]
        assert primary.catchup_calls == 0


def test_entry_id_mismatch_discards_stale_pending_and_catches_up(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        primary.entries = [make_entry(1, value="authoritative", entry_id="right")]
        client.post("/api/v1/replicate", json=body(1, 0, "stale", entry_id="wrong"))
        result = client.post(
            "/api/v1/replicate",
            json=body(2, 1, "next", commit_entry_id="right"),
        )
        assert result.status_code == 200
        assert client.get("/api/v1/read/k").json()["value"] == "authoritative"
        assert client.get("/api/v1/debug/state").json()["pending"] == [2]


def test_identity_check_is_inert_when_ids_are_absent(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        client.post("/api/v1/replicate", json=body(1, 0, "plain-v4"))
        result = client.post("/api/v1/replicate", json=body(2, 1, "next"))
        assert result.status_code == 200
        assert client.get("/api/v1/read/k").json()["value"] == "plain-v4"


def test_primary_behind_backup_degrades_without_changing_local_data(tmp_path):
    wal = tmp_path / "backup_wal.jsonl"
    wal.write_text(
        "".join(
            json.dumps(make_entry(version, value=f"keep-{version}")) + "\n"
            for version in (1, 2)
        ),
        encoding="utf-8",
    )
    primary = FakePrimary()
    node = make_node(tmp_path, primary)
    with open_client(node) as client:
        assert wait_until(lambda: client.get("/api/v1/health").json()["status"] == "DEGRADED")
        before_wal = node.wal_file.read_bytes()
        primary.catchup_calls = 0

        for incoming_version, commit_version in ((3, 2), (2, 1)):
            response = client.post(
                "/api/v1/replicate",
                json=body(incoming_version, commit_version, f"incoming-{incoming_version}"),
            )
            assert response.status_code == 503
            assert response.json() == {
                "status": "NACK",
                "node_id": "backup-test",
                "reason": "DIVERGED",
                "current_version": 2,
            }
            assert node.wal_file.read_bytes() == before_wal

        commit = client.post("/api/v1/commit", json={"commit_version": 2})
        assert commit.status_code == 200
        assert commit.json()["status"] == "COMMITTED"
        assert node.wal_file.read_bytes() == before_wal
        assert primary.catchup_calls == 0

        local_read = client.get("/api/v1/read/k")
        assert local_read.status_code == 200
        assert local_read.json()["value"] == "keep-2"
        assert local_read.json()["node_version"] == 2
        assert client.get("/api/v1/debug/state").json()["diverged"] is True

        primary.entries = [
            make_entry(1, value="keep-1"),
            make_entry(2, value="keep-2"),
        ]
        assert primary.version >= node.current_version
        assert client.get("/api/v1/health").json()["status"] == "DEGRADED"
        assert client.get("/api/v1/debug/state").json()["diverged"] is True
        assert client.get("/api/v1/read/k").json()["value"] == "keep-2"
        assert node.wal_file.read_bytes() == before_wal


def test_primary_success_probe_does_not_clear_diverged_flag(tmp_path):
    wal = tmp_path / "backup_wal.jsonl"
    wal.write_text(json.dumps(make_entry(1, value="local")) + "\n", encoding="utf-8")
    primary = FakePrimary()
    node = make_node(tmp_path, primary)
    assert node.diverged is False
    node.load_wal()

    async def record_primary_versions():
        lower_version_result = await node._record_primary_success(primary.version)
        primary.entries = [
            make_entry(1, value="primary"),
            make_entry(2, value="new-primary-entry"),
        ]
        catchup_result = await node._perform_catchup()
        current_version_result = await node._record_primary_success(primary.version)
        return lower_version_result, catchup_result, current_version_result

    before_wal = node.wal_file.read_bytes()
    assert asyncio.run(record_primary_versions()) == (False, False, False)
    assert primary.version >= node.current_version
    assert node.diverged is True
    assert node.status == "DEGRADED"
    assert node.current_version == 1
    assert node.wal_file.read_bytes() == before_wal


def test_invalid_replicate_payload_returns_422(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        response = client.post(
            "/api/v1/replicate",
            json={"version": 0, "commit_version": -1, "operation": "UPDATE", "key": 2},
        )
        assert response.status_code == 422


def test_put_without_value_returns_422(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        response = client.post(
            "/api/v1/replicate",
            json={
                "version": 1,
                "commit_version": 0,
                "operation": "PUT",
                "key": "k",
            },
        )
        assert response.status_code == 422


JSON_VALUES = [42, 3.5, True, False, 0, "", {"a": 1, "b": [1, 2]}, [1, "hai", None]]


@pytest.mark.parametrize("value", JSON_VALUES, ids=repr)
def test_non_string_json_value_is_replicated_persisted_and_recovered(tmp_path, value):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        response = client.post("/api/v1/replicate", json=body(1, 0, value))
        assert response.status_code == 200, response.text
        client.post("/api/v1/commit", json={"commit_version": 1})
        assert client.get("/api/v1/read/k").json()["value"] == value
    assert committed_wal(tmp_path)[0]["value"] == value

    # Khởi động lại phải đọc được WAL chứa value không phải chuỗi.
    primary.entries = [make_entry(1, value=value)]
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        item = client.get("/api/v1/read/k").json()
        assert item["value"] == value
        assert item["found"] is True


def test_catchup_accepts_non_string_json_values(tmp_path):
    values = [7, {"nested": {"x": [1, 2]}}, False]
    primary = FakePrimary(
        [make_entry(i, key=f"k{i}", value=v) for i, v in enumerate(values, start=1)]
    )
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        assert client.get("/api/v1/health").json()["current_version"] == 3
        for i, value in enumerate(values, start=1):
            assert client.get(f"/api/v1/read/k{i}").json()["value"] == value


def test_catchup_rejects_put_with_null_value(tmp_path):
    primary = FakePrimary([make_entry(1, value=None)])
    with open_client(make_node(tmp_path, primary)) as client:
        assert wait_until(lambda: node_error(client) is not None)
        assert client.get("/api/v1/health").json()["current_version"] == 0
    assert committed_wal(tmp_path) == []


def node_error(client):
    return client.app.state.backup_node.last_catchup.get("last_error")


def test_hierarchical_key_is_readable(tmp_path):
    primary = FakePrimary()
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        key = "users/101/profile"
        client.post("/api/v1/replicate", json=body(1, 0, "hồ sơ", key=key))
        client.post("/api/v1/commit", json={"commit_version": 1})
        item = client.get(f"/api/v1/read/{key}").json()
        assert item["key"] == key
        assert item["value"] == "hồ sơ"
        assert client.get("/api/v1/read/users/101").json()["found"] is False


def test_concurrent_gap_detections_share_one_catchup_call(tmp_path):
    primary = FakePrimary(delay=0.05)
    with open_client(make_node(tmp_path, primary)) as client:
        start_alive(client, primary)
        primary.entries = [make_entry(1, value="one"), make_entry(2, value="two")]
        with ThreadPoolExecutor(max_workers=2) as executor:
            requests = [
                executor.submit(
                    client.post, "/api/v1/replicate", json=body(3, 2, "three")
                )
                for _ in range(2)
            ]
            results = [request.result() for request in requests]
        assert [response.status_code for response in results] == [200, 200]
        assert primary.catchup_calls == 1

