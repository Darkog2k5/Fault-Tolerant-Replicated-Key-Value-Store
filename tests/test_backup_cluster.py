"""Kiểm thử tích hợp Backup với Primary thật qua tiến trình con."""

from __future__ import annotations

import json
import time

import pytest

from cluster_harness import Cluster, ClusterUnavailable


pytestmark = pytest.mark.integration


@pytest.fixture
def cluster(tmp_path):
    """Tạo cụm độc lập và luôn dừng mọi tiến trình khi kết thúc ca kiểm thử."""
    current = None
    try:
        current = Cluster(tmp_path)
        current.check_runtime()
    except ClusterUnavailable as error:
        if current is not None:
            current.close()
        pytest.skip(f"Môi trường không hỗ trợ socket/tiến trình con: {error}")
    try:
        try:
            current.start_primary()
            current.wait_health("primary", status="ALIVE")
            current.start_backup(1)
            current.start_backup(2)
            current.wait_health("backup-1", status="ALIVE")
            current.wait_health("backup-2", status="ALIVE")
        except ClusterUnavailable as error:
            pytest.skip(f"Môi trường không cho tạo tiến trình node: {error}")
        except AssertionError as error:
            logs = "\n".join(current.log(node) for node in ("primary", "backup-1", "backup-2"))
            socket_errors = (
                "PermissionError",
                "WinError 10013",
                "WinError 10048",
                "Operation not permitted",
                "Access is denied",
            )
            if any(marker in logs for marker in socket_errors):
                pytest.skip(f"Môi trường không cho node bind socket localhost: {error}")
            raise
        yield current
    finally:
        current.close()


def wait_until(condition, timeout=10.0, description="điều kiện"):
    """Poll một điều kiện ngắn cho tới khi đạt hoặc hết thời gian."""
    deadline = time.monotonic() + timeout
    last_value = None
    while time.monotonic() < deadline:
        last_value = condition()
        if last_value:
            return last_value
        time.sleep(0.04)
    raise AssertionError(f"Hết thời gian chờ {description}; giá trị cuối={last_value}")


def wait_node_version(cluster, nodes, version, timeout=12.0):
    """Chờ tất cả node đạt cùng version đã commit."""
    return wait_until(
        lambda: all(
            cluster.health(node).get("current_version") == version for node in nodes
        ),
        timeout=timeout,
        description=f"các node đạt version {version}",
    )


def read_data(cluster, node, key):
    response = cluster.read(node, key)
    assert response.status_code == 200, response.text
    return response.json()


def assert_cluster_consistent(cluster, keys):
    """So version cụm và trạng thái từng khóa trên cả ba node."""
    primary_version = cluster.health("primary")["current_version"]
    for node in ("backup-1", "backup-2"):
        cluster.wait_health(node, status="ALIVE", version=primary_version)
    for key in keys:
        values = [read_data(cluster, node, key) for node in ("primary", "backup-1", "backup-2")]
        fields = ("value", "version", "node_version", "found", "deleted")
        expected = {field: values[0][field] for field in fields}
        for item in values[1:]:
            assert {field: item[field] for field in fields} == expected


def test_t1_sync_write_is_readable_from_both_backups(cluster):
    result = cluster.write("t1-key", "t1-value", consistency="SYNC")
    assert result.status_code == 200, result.text
    version = result.json()["version"]
    wait_node_version(cluster, ("primary", "backup-1", "backup-2"), version)
    for node in ("backup-1", "backup-2"):
        item = read_data(cluster, node, "t1-key")
        assert item["value"] == "t1-value"
        assert item["node_version"] == version
    assert_cluster_consistent(cluster, ["t1-key"])


def test_t2_sync_delete_keeps_tombstone_on_backups(cluster):
    created = cluster.write("t2-key", "before", consistency="SYNC")
    assert created.status_code == 200, created.text
    removed = cluster.write("t2-key", None, consistency="SYNC", operation="DELETE")
    assert removed.status_code == 200, removed.text
    version = removed.json()["version"]
    wait_node_version(cluster, ("primary", "backup-1", "backup-2"), version)
    for node in ("backup-1", "backup-2"):
        item = read_data(cluster, node, "t2-key")
        assert item["found"] is False
        assert item["deleted"] is True
        assert item["value"] is None
        assert item["version"] == version
    assert_cluster_consistent(cluster, ["t2-key"])


def test_t3_backup_restart_recovers_wal(cluster):
    result = cluster.write("t3-key", "persisted", consistency="SYNC")
    assert result.status_code == 200, result.text
    version = result.json()["version"]
    wait_node_version(cluster, ("primary", "backup-1", "backup-2"), version)

    cluster.stop_backup(1)
    cluster.start_backup(1)
    cluster.wait_health("backup-1", status="ALIVE", version=version)

    assert read_data(cluster, "backup-1", "t3-key")["value"] == "persisted"
    assert "[RECOVERY]" in cluster.log("backup-1")
    assert_cluster_consistent(cluster, ["t3-key"])


def test_t4_backup_catches_up_twenty_missed_writes(cluster):
    cluster.stop_backup(1)
    keys = []
    for index in range(20):
        key = f"t4-key-{index}"
        keys.append(key)
        # Mỗi lệnh QUORUM phải chờ hết timeout kết nối tới Backup đã tắt (~2s trên
        # Windows), nên chỉ dùng 4 lệnh QUORUM, còn lại ASYNC để ca chạy nhanh.
        mode = "QUORUM" if index % 5 == 0 else "ASYNC"
        response = cluster.write(key, f"value-{index}", consistency=mode)
        assert response.status_code == 200, response.text

    target_version = cluster.health("primary")["current_version"]
    cluster.start_backup(1)
    cluster.wait_health("backup-1", status="ALIVE", version=target_version)
    assert "[CATCH-UP]" in cluster.log("backup-1")
    assert "missed=20" in cluster.log("backup-1")
    assert_cluster_consistent(cluster, keys)


def test_t5_restart_after_async_gap_and_rejects_unfillable_version(cluster):
    cluster.stop_backup(2)
    keys = []
    for index in range(4):
        key = f"t5-key-{index}"
        keys.append(key)
        response = cluster.write(key, f"async-{index}", consistency="ASYNC")
        assert response.status_code == 200, response.text
    missed_version = cluster.health("primary")["current_version"]

    cluster.start_backup(2)
    cluster.wait_health("backup-2", status="ALIVE", version=missed_version)
    next_write = cluster.write("t5-next", "after-restart", consistency="ASYNC")
    assert next_write.status_code == 200, next_write.text
    target_version = next_write.json()["version"]
    wait_node_version(cluster, ("primary", "backup-1", "backup-2"), target_version)

    before_wal = cluster.wal_path("backup-2").read_bytes()
    current = cluster.health("backup-2")["current_version"]
    invalid = cluster.request(
        "backup-2",
        "POST",
        "/api/v1/replicate",
        json={
            "version": current + 3,
            "commit_version": current,
            "operation": "PUT",
            "key": "never-committed",
            "value": "gap",
            "timestamp": time.time(),
        },
    )
    assert invalid.status_code == 503, invalid.text
    assert invalid.json().get("reason") == "GAP_NOT_FILLED"
    assert "[GAP]" in cluster.log("backup-2")
    assert cluster.wal_path("backup-2").read_bytes() == before_wal
    assert cluster.request("backup-2", "GET", "/api/v1/health").status_code == 200
    assert_cluster_consistent(cluster, keys + ["t5-next"])


def test_t6_failed_sync_value_is_not_committed_and_quorum_replacement_wins(cluster):
    cluster.stop_backup(2)
    failed = cluster.write("t6-key", "A", consistency="SYNC")
    assert failed.status_code == 503
    assert failed.json()["detail"]["status"] == "WRITE_FAILED"

    accepted = cluster.write("t6-key", "B", consistency="QUORUM")
    assert accepted.status_code == 200, accepted.text
    version = accepted.json()["version"]
    wait_node_version(cluster, ("primary", "backup-1"), version)
    assert read_data(cluster, "backup-1", "t6-key")["value"] == "B"
    assert read_data(cluster, "primary", "t6-key")["value"] == "B"
    committed = [
        json.loads(line)
        for line in cluster.wal_path("backup-1").read_text(encoding="utf-8").splitlines()
    ]
    assert not any(entry.get("key") == "t6-key" and entry.get("value") == "A" for entry in committed)

    cluster.start_backup(2)
    cluster.wait_health("backup-2", status="ALIVE", version=version)
    assert read_data(cluster, "backup-2", "t6-key")["value"] == "B"
    assert_cluster_consistent(cluster, ["t6-key"])


def test_t7_stale_sync_pending_does_not_overwrite_quorum_value(cluster):
    """Xác minh hội tụ, không lưu A và WAL liên tục; không phân biệt TTL với gói B được xử lý sau resume."""
    pytest.importorskip("psutil")

    base = cluster.write("t7-base", "base", consistency="SYNC")
    assert base.status_code == 200, base.text
    base_version = base.json()["version"]
    for node in ("primary", "backup-1", "backup-2"):
        cluster.wait_health(node, status="ALIVE", version=base_version, timeout=20.0)

    cluster.stop_backup(2)
    failed = cluster.write("t7-x", "A", consistency="SYNC")
    assert failed.status_code == 503
    assert cluster.request("backup-1", "GET", "/api/v1/debug/state").json()["pending"] == [
        base_version + 1
    ]

    cluster.start_backup(2)
    cluster.wait_health("backup-2", status="ALIVE", version=base_version, timeout=20.0)
    cluster.suspend_backup(1)
    suspended = True
    try:
        accepted = cluster.write(
            "t7-x", "B", consistency="QUORUM", quorum_w=2
        )
        assert accepted.status_code == 200, accepted.text
        assert accepted.json()["acks"] == 2
        assert "backup-2" in accepted.json()["replicated_nodes"]
        target_version = accepted.json()["version"]
        cluster.wait_health(
            "backup-2", status="ALIVE", version=target_version, timeout=20.0
        )
    finally:
        if suspended:
            cluster.resume_backup(1)

    later = cluster.write("t7-y", "C", consistency="SYNC")
    assert later.status_code == 200, later.text
    target_version = later.json()["version"]
    for node in ("primary", "backup-1", "backup-2"):
        cluster.wait_health(node, status="ALIVE", version=target_version, timeout=30.0)

    for node in ("primary", "backup-1", "backup-2"):
        assert read_data(cluster, node, "t7-x")["value"] == "B"
        assert read_data(cluster, node, "t7-y")["value"] == "C"

    wal_entries = [
        json.loads(line)
        for line in cluster.wal_path("backup-1").read_text(encoding="utf-8").splitlines()
    ]
    versions = [entry["version"] for entry in wal_entries]
    assert versions == list(range(1, target_version + 1))


def test_t8_torn_final_wal_line_is_truncated_on_restart(cluster):
    result = cluster.write("t8-key", "kept", consistency="SYNC")
    assert result.status_code == 200, result.text
    version = result.json()["version"]
    wait_node_version(cluster, ("primary", "backup-1", "backup-2"), version)

    cluster.stop_backup(1)
    with cluster.wal_path("backup-1").open("ab") as wal:
        wal.write(b'{"version": 9')
    cluster.start_backup(1)
    cluster.wait_health("backup-1", status="ALIVE", version=version)

    assert cluster.wal_path("backup-1").read_bytes().endswith(b"\n")
    assert read_data(cluster, "backup-1", "t8-key")["value"] == "kept"
    assert_cluster_consistent(cluster, ["t8-key"])


def test_t9_backup_serves_local_reads_while_primary_is_down(cluster):
    result = cluster.write("t9-key", "local-data", consistency="SYNC")
    assert result.status_code == 200, result.text
    version = result.json()["version"]
    wait_node_version(cluster, ("primary", "backup-1", "backup-2"), version)

    cluster.stop_primary()
    response = cluster.read("backup-1", "t9-key")
    assert response.status_code == 200, response.text
    assert response.json()["value"] == "local-data"

    cluster.stop_backup(1)
    cluster.start_backup(
        1,
        PRIMARY_FAILURES_BEFORE_DEGRADED=2,
        SYNC_INTERVAL=0.05,
        CATCHUP_HTTP_TIMEOUT=0.08,
    )
    cluster.wait_health("backup-1", status="DEGRADED", version=version, timeout=20.0)
    local_read = cluster.read("backup-1", "t9-key")
    assert local_read.status_code == 200, local_read.text
    assert local_read.json()["value"] == "local-data"

    cluster.start_primary()
    cluster.wait_health("primary", status="ALIVE", version=version)
    cluster.wait_health("backup-1", status="ALIVE", version=version, timeout=20.0)
    assert_cluster_consistent(cluster, ["t9-key"])


def test_t10_duplicate_replicate_and_commit_do_not_duplicate_wal(cluster):
    written = cluster.write("t10-key", "once", consistency="SYNC")
    assert written.status_code == 200, written.text
    body = written.json()
    payload = {
        "version": body["version"],
        "commit_version": body["version"] - 1,
        "operation": body["operation"],
        "key": body["key"],
        "value": body["value"],
        "timestamp": body["timestamp"],
    }
    for _ in range(2):
        response = cluster.request("backup-1", "POST", "/api/v1/replicate", json=payload)
        assert response.status_code == 200, response.text
    for _ in range(2):
        response = cluster.request(
            "backup-1", "POST", "/api/v1/commit", json={"commit_version": body["version"]}
        )
        assert response.status_code == 200, response.text

    lines = cluster.wal_path("backup-1").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["version"] == body["version"]
    assert_cluster_consistent(cluster, ["t10-key"])


def test_t11_reads_are_rejected_during_startup_catchup(cluster):
    result = cluster.write("t11-key", "catching-up", consistency="SYNC")
    assert result.status_code == 200, result.text
    version = result.json()["version"]
    wait_node_version(cluster, ("primary", "backup-1", "backup-2"), version)

    cluster.stop_backup(1)
    cluster.start_backup(1, CATCHUP_DELAY_MS=1500)
    cluster.wait_health("backup-1", status="CATCHING_UP", timeout=5.0)
    response = cluster.read("backup-1", "t11-key")
    assert response.status_code == 503
    assert response.json() == {"detail": "NODE_CATCHING_UP"}

    cluster.wait_health("backup-1", status="ALIVE", version=version, timeout=20.0)
    assert read_data(cluster, "backup-1", "t11-key")["value"] == "catching-up"
    assert_cluster_consistent(cluster, ["t11-key"])


def test_t12_mixed_consistency_writes_converge_on_all_nodes(cluster):
    keys = []
    modes = ("ASYNC", "SYNC", "QUORUM")
    for index in range(30):
        key = f"t12-key-{index}"
        keys.append(key)
        mode = modes[index % len(modes)]
        response = cluster.write(key, f"value-{index}", consistency=mode)
        assert response.status_code == 200, response.text

    target_version = cluster.health("primary")["current_version"]
    wait_node_version(cluster, ("primary", "backup-1", "backup-2"), target_version)
    assert_cluster_consistent(cluster, keys)

def test_t13_non_string_values_and_hierarchical_keys_replicate(cluster):
    """ASYNC ghi số rồi SYNC/QUORUM ghi object, mảng, khóa có '/' vẫn hội tụ."""
    writes = [
        ("t13-async-int", 7, "ASYNC"),
        ("t13-sync-dict", {"ten": "An", "tuoi": 20}, "SYNC"),
        ("t13-quorum-list", [1, "hai", None], "QUORUM"),
        ("t13/users/101/profile", True, "SYNC"),
        ("t13-after", "chuỗi", "SYNC"),
    ]
    for key, value, mode in writes:
        response = cluster.write(key, value, consistency=mode)
        assert response.status_code == 200, response.text

    target_version = cluster.health("primary")["current_version"]
    wait_node_version(cluster, ("primary", "backup-1", "backup-2"), target_version)
    for node in ("backup-1", "backup-2"):
        for key, value, _ in writes:
            assert read_data(cluster, node, key)["value"] == value
    assert_cluster_consistent(cluster, [key for key, _, _ in writes])
