import os
import sys
import json
import time
import socket
import shutil
import asyncio
import tempfile
import threading
import unittest
import httpx
import uvicorn
from typing import Tuple, List

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TESTS_DIR)
PRIMARY_DIR = os.path.join(REPO_ROOT, "primary_node")

if TESTS_DIR not in sys.path:
    sys.path.insert(0, TESTS_DIR)
if PRIMARY_DIR not in sys.path:
    sys.path.insert(0, PRIMARY_DIR)

from mock_backup import create_mock_backup_app


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _ThreadUvicornServer(uvicorn.Server):
    """Uvicorn server chạy trong background thread không cài signal handler."""

    def install_signal_handlers(self):
        pass


class ServerHandle:
    """Quản lý vòng đời 1 server FastAPI trên background thread."""

    def __init__(self, app, host: str, port: int):
        self.app = app
        self.host = host
        self.port = port
        self.url = f"http://{host}:{port}"
        self._server: _ThreadUvicornServer = None  # type: ignore
        self._thread: threading.Thread = None  # type: ignore

    def start(self, timeout: float = 5.0):
        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.port,
            log_level="warning",
            access_log=False,
        )
        self._server = _ThreadUvicornServer(config)

        def _run():
            if sys.platform == "win32":
                loop = asyncio.SelectorEventLoop()
            else:
                loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._server.serve())
            finally:
                loop.close()

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()

        deadline = time.time() + timeout
        while time.time() < deadline:
            if getattr(self._server, "started", False):
                try:
                    r = httpx.get(f"{self.url}/api/v1/health", timeout=0.5)
                    if r.status_code == 200:
                        return
                except Exception:
                    pass
            time.sleep(0.05)
        raise RuntimeError(f"Server tại {self.url} không khởi động kịp sau {timeout}s")

    def stop(self, timeout: float = 5.0):
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=timeout)


class TestPrimaryNodeComprehensive(unittest.TestCase):
    temp_dir: str = ""
    b1_port: int = 0
    b2_port: int = 0
    primary_port: int = 0
    b1_app = None
    b2_app = None
    b1_handle: ServerHandle = None  # type: ignore
    b2_handle: ServerHandle = None  # type: ignore
    primary_handle: ServerHandle = None  # type: ignore
    primary_url: str = ""

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp(prefix="kv_primary_test_")
        cls.b1_port = _find_free_port()
        cls.b2_port = _find_free_port()
        cls.primary_port = _find_free_port()
        cls.primary_url = f"http://127.0.0.1:{cls.primary_port}"

        os.environ["NODE_ID"] = "primary"
        os.environ["PORT"] = str(cls.primary_port)
        os.environ["REPLICAS"] = (
            f"http://127.0.0.1:{cls.b1_port},http://127.0.0.1:{cls.b2_port}"
        )
        os.environ["DEFAULT_QUORUM_W"] = "2"
        os.environ["DATA_DIR"] = cls.temp_dir

        # Import sau khi set env để config đọc đúng thư mục tạm và port test
        global cfg, repl_mod, main_mod
        import config as cfg
        import replication as repl_mod
        import main as main_mod

        cls.b1_app = create_mock_backup_app("backup-1", primary_url=cls.primary_url)
        cls.b2_app = create_mock_backup_app("backup-2", primary_url=cls.primary_url)

        cls.b1_handle = ServerHandle(cls.b1_app, "127.0.0.1", cls.b1_port)
        cls.b2_handle = ServerHandle(cls.b2_app, "127.0.0.1", cls.b2_port)
        cls.b1_handle.start()
        cls.b2_handle.start()

        cls.primary_handle = ServerHandle(main_mod.app, "127.0.0.1", cls.primary_port)
        cls.primary_handle.start()

    @classmethod
    def tearDownClass(cls):
        if cls.primary_handle is not None:
            cls.primary_handle.stop()
        if cls.b1_handle is not None:
            cls.b1_handle.stop()
        if cls.b2_handle is not None:
            cls.b2_handle.stop()
        if cls.temp_dir and os.path.exists(cls.temp_dir):
            shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def _reset_backup_faults(self):
        for app in (self.b1_app, self.b2_app):
            st = app.state.mock_state
            st.delay_sec = 0.0
            st.fail_replicate = False
            st.drop_commit = False

    def _wait_backups_reach(self, target_version: int, timeout: float = 3.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            v1 = self.b1_app.state.mock_state.current_version
            v2 = self.b2_app.state.mock_state.current_version
            if v1 >= target_version and v2 >= target_version:
                return
            time.sleep(0.03)
        self.fail(
            f"Backups chưa đạt version={target_version} trong {timeout}s "
            f"(b1={self.b1_app.state.mock_state.current_version}, "
            f"b2={self.b2_app.state.mock_state.current_version})"
        )

    def setUp(self):
        self._reset_backup_faults()

    def test_01_initial_health_clean(self):
        r = httpx.get(f"{self.primary_url}/api/v1/health", timeout=2.0)
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(data["status"], "ALIVE")
        self.assertEqual(data["node_id"], "primary")
        self.assertEqual(data["role"], "PRIMARY")
        self.assertEqual(data["current_version"], 0)
        self.assertEqual(len(data["backups"]), 2)

    def test_02_write_modes_sync_quorum_async_and_read(self):
        # SYNC (3/3)
        r_sync = httpx.post(
            f"{self.primary_url}/api/v1/write",
            json={"operation": "PUT", "key": "k_sync", "value": "v1", "consistency": "SYNC"},
            timeout=3.0,
        )
        self.assertEqual(r_sync.status_code, 200)
        d_sync = r_sync.json()
        self.assertEqual(d_sync["version"], 1)
        self.assertEqual(d_sync["acks"], 3)
        self.assertEqual(set(d_sync["replicated_nodes"]), {"primary", "backup-1", "backup-2"})

        # QUORUM (W=2)
        r_quorum = httpx.post(
            f"{self.primary_url}/api/v1/write",
            json={
                "operation": "PUT",
                "key": "k_quorum",
                "value": "v2",
                "consistency": "QUORUM",
                "quorum_w": 2,
            },
            timeout=3.0,
        )
        self.assertEqual(r_quorum.status_code, 200)
        d_quorum = r_quorum.json()
        self.assertEqual(d_quorum["version"], 2)
        self.assertGreaterEqual(d_quorum["acks"], 2)

        # ASYNC (acks=1 ngay lập tức)
        r_async = httpx.post(
            f"{self.primary_url}/api/v1/write",
            json={"operation": "PUT", "key": "k_async", "value": "v3", "consistency": "ASYNC"},
            timeout=3.0,
        )
        self.assertEqual(r_async.status_code, 200)
        d_async = r_async.json()
        self.assertEqual(d_async["version"], 3)
        self.assertEqual(d_async["acks"], 1)
        self.assertEqual(d_async["replicated_nodes"], ["primary"])

        # Đợi background workers gửi hết tới Backup
        self._wait_backups_reach(3)

        # Đọc key tồn tại & key không tồn tại
        r_read = httpx.get(f"{self.primary_url}/api/v1/read/k_sync", timeout=2.0).json()
        self.assertEqual(r_read["value"], "v1")
        self.assertEqual(r_read["version"], 1)
        self.assertEqual(r_read["node_version"], 3)
        self.assertTrue(r_read["found"])
        self.assertFalse(r_read["deleted"])

        r_miss = httpx.get(f"{self.primary_url}/api/v1/read/never_existed", timeout=2.0).json()
        self.assertIsNone(r_miss["value"])
        self.assertEqual(r_miss["version"], 0)
        self.assertEqual(r_miss["node_version"], 3)
        self.assertFalse(r_miss["found"])
        self.assertFalse(r_miss["deleted"])

    def test_03_delete_tombstone_and_hierarchical_json_values(self):
        # Xoá Tombstone
        r_del = httpx.post(
            f"{self.primary_url}/api/v1/write",
            json={"operation": "DELETE", "key": "k_sync", "consistency": "SYNC"},
            timeout=3.0,
        )
        self.assertEqual(r_del.status_code, 200)
        v_del = r_del.json()["version"]

        r_after = httpx.get(f"{self.primary_url}/api/v1/read/k_sync", timeout=2.0).json()
        self.assertIsNone(r_after["value"])
        self.assertEqual(r_after["version"], v_del)
        self.assertFalse(r_after["found"])
        self.assertTrue(r_after["deleted"])

        # Khoá chứa dấu '/' và giá trị đa kiểu (dict, số, tiếng Việt UTF-8)
        complex_val = {"name": "Nguyễn Văn Á", "age": 21, "active": True}
        r_hier = httpx.post(
            f"{self.primary_url}/api/v1/write",
            json={
                "operation": "PUT",
                "key": "users/101/profile",
                "value": complex_val,
                "consistency": "SYNC",
            },
            timeout=3.0,
        )
        self.assertEqual(r_hier.status_code, 200)
        r_read_hier = httpx.get(
            f"{self.primary_url}/api/v1/read/users/101/profile", timeout=2.0
        ).json()
        self.assertEqual(r_read_hier["key"], "users/101/profile")
        self.assertEqual(r_read_hier["value"], complex_val)
        self.assertTrue(r_read_hier["found"])

        r_num = httpx.post(
            f"{self.primary_url}/api/v1/write",
            json={"operation": "PUT", "key": "counter", "value": 42, "consistency": "SYNC"},
            timeout=3.0,
        )
        self.assertEqual(r_num.status_code, 200)
        self.assertEqual(
            httpx.get(f"{self.primary_url}/api/v1/read/counter", timeout=2.0).json()["value"],
            42,
        )
        self._wait_backups_reach(r_num.json()["version"])

    def test_04_input_validation_400(self):
        v_before = httpx.get(f"{self.primary_url}/api/v1/health", timeout=2.0).json()[
            "current_version"
        ]
        bad_payloads = [
            {"operation": "UPSERT", "key": "a", "value": "1"},
            {"operation": "PUT", "key": "a"},
            {"operation": "PUT", "key": "", "value": "1"},
            {"operation": "PUT", "key": "a", "value": "1", "consistency": "LINEAR"},
            {"operation": "PUT", "key": "a", "value": "1", "consistency": "QUORUM", "quorum_w": 0},
            {"operation": "PUT", "key": "a", "value": "1", "consistency": "QUORUM", "quorum_w": 99},
        ]
        for payload in bad_payloads:
            resp = httpx.post(f"{self.primary_url}/api/v1/write", json=payload, timeout=2.0)
            self.assertEqual(resp.status_code, 400, f"Payload phải trả 400: {payload}")

        r_empty_read = httpx.get(f"{self.primary_url}/api/v1/read/", timeout=2.0)
        self.assertEqual(r_empty_read.status_code, 400)

        v_after = httpx.get(f"{self.primary_url}/api/v1/health", timeout=2.0).json()[
            "current_version"
        ]
        self.assertEqual(v_before, v_after)

    def test_05_sync_fails_503_no_dirty_commit(self):
        v_before = httpx.get(f"{self.primary_url}/api/v1/health", timeout=2.0).json()[
            "current_version"
        ]
        self.b2_app.state.mock_state.fail_replicate = True

        resp = httpx.post(
            f"{self.primary_url}/api/v1/write",
            json={"operation": "PUT", "key": "dirty_sync", "value": "should_abort", "consistency": "SYNC"},
            timeout=4.0,
        )
        self.assertEqual(resp.status_code, 503)
        detail = resp.json()["detail"]
        self.assertEqual(detail["status"], "WRITE_FAILED")
        self.assertEqual(detail["mode"], "SYNC")
        self.assertEqual(detail["required_acks"], 3)
        self.assertEqual(detail["actual_acks"], 2)

        v_after = httpx.get(f"{self.primary_url}/api/v1/health", timeout=2.0).json()[
            "current_version"
        ]
        self.assertEqual(v_after, v_before)
        r_read = httpx.get(f"{self.primary_url}/api/v1/read/dirty_sync", timeout=2.0).json()
        self.assertFalse(r_read["found"])

    def test_06_quorum_early_return_and_late_commit(self):
        # Backup 2 bị trễ 1.5s; QUORUM (W=2) phải trả về ngay (< 0.4s) qua Primary + Backup 1
        self.b2_app.state.mock_state.delay_sec = 1.5
        t0 = time.perf_counter()
        resp = httpx.post(
            f"{self.primary_url}/api/v1/write",
            json={
                "operation": "PUT",
                "key": "fast_quorum",
                "value": "early_ok",
                "consistency": "QUORUM",
                "quorum_w": 2,
            },
            timeout=3.0,
        )
        elapsed = time.perf_counter() - t0
        self.assertEqual(resp.status_code, 200)
        self.assertLess(
            elapsed,
            0.4,
            f"QUORUM bị chặn chờ Backup chậm quá lâu ({elapsed:.3f}s thay vì < 0.4s)",
        )
        v_ok = resp.json()["version"]

        # Đợi Backup 2 hoàn tất phản hồi muộn -> phải tự nhận được /commit bù để lên v_ok
        self._wait_backups_reach(v_ok, timeout=3.0)
        b2_read = httpx.get(
            f"http://127.0.0.1:{self.b2_port}/api/v1/read/fast_quorum", timeout=2.0
        ).json()
        self.assertEqual(b2_read["value"], "early_ok")
        self.assertEqual(b2_read["node_version"], v_ok)

    def test_07_quorum_fails_503_when_both_backups_down(self):
        v_before = httpx.get(f"{self.primary_url}/api/v1/health", timeout=2.0).json()[
            "current_version"
        ]
        self.b1_app.state.mock_state.fail_replicate = True
        self.b2_app.state.mock_state.fail_replicate = True

        resp = httpx.post(
            f"{self.primary_url}/api/v1/write",
            json={
                "operation": "PUT",
                "key": "both_down",
                "value": "abort_me",
                "consistency": "QUORUM",
                "quorum_w": 2,
            },
            timeout=4.0,
        )
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(resp.json()["detail"]["actual_acks"], 1)
        self.assertEqual(
            httpx.get(f"{self.primary_url}/api/v1/health", timeout=2.0).json()[
                "current_version"
            ],
            v_before,
        )

    def test_08_split_brain_503_followed_by_async(self):
        """
        Kiểm tra lỗ hổng #1 (CRITICAL):
        1. Lệnh SYNC ở version v lỗi 503 vì Backup 2 chết, nhưng Backup 1 đã lưu pending[v] = BAD_ABORTED.
        2. Backup 2 sống lại, Client gửi tiếp lệnh ASYNC ở cùng version v với giá trị GOOD_ASYNC.
        3. Cả Backup 1 và Backup 2 phải commit GOOD_ASYNC (TUYỆT ĐỐI không commit nhầm BAD_ABORTED).
        """
        v_before = httpx.get(f"{self.primary_url}/api/v1/health", timeout=2.0).json()[
            "current_version"
        ]
        target_v = v_before + 1

        self.b2_app.state.mock_state.fail_replicate = True
        r_abort = httpx.post(
            f"{self.primary_url}/api/v1/write",
            json={
                "operation": "PUT",
                "key": "split_key",
                "value": "BAD_ABORTED",
                "consistency": "SYNC",
            },
            timeout=4.0,
        )
        self.assertEqual(r_abort.status_code, 503)
        # Xác nhận Backup 1 thực sự đang giữ bản ghi rác trong pending[target_v]
        self.assertIn(target_v, self.b1_app.state.mock_state.pending)
        self.assertEqual(
            self.b1_app.state.mock_state.pending[target_v]["value"], "BAD_ABORTED"
        )
        self.assertEqual(self.b1_app.state.mock_state.current_version, v_before)

        # Khôi phục Backup 2 và ghi ASYNC đè lên đúng target_v
        self.b2_app.state.mock_state.fail_replicate = False
        r_async = httpx.post(
            f"{self.primary_url}/api/v1/write",
            json={
                "operation": "PUT",
                "key": "split_key",
                "value": "GOOD_ASYNC",
                "consistency": "ASYNC",
            },
            timeout=3.0,
        )
        self.assertEqual(r_async.status_code, 200)
        self.assertEqual(r_async.json()["version"], target_v)

        self._wait_backups_reach(target_v, timeout=3.0)

        p_val = httpx.get(f"{self.primary_url}/api/v1/read/split_key", timeout=2.0).json()
        b1_val = httpx.get(
            f"http://127.0.0.1:{self.b1_port}/api/v1/read/split_key", timeout=2.0
        ).json()
        b2_val = httpx.get(
            f"http://127.0.0.1:{self.b2_port}/api/v1/read/split_key", timeout=2.0
        ).json()

        self.assertEqual(p_val["value"], "GOOD_ASYNC")
        self.assertEqual(b1_val["value"], "GOOD_ASYNC")
        self.assertEqual(b2_val["value"], "GOOD_ASYNC")
        self.assertNotIn(target_v, self.b1_app.state.mock_state.pending)

    def test_09_fifo_ordering_async_mixed_with_sync(self):
        # Gây trễ nhẹ 30ms trên backup để hàng đợi ASYNC thực sự có backlog trước khi SYNC chạy
        self.b1_app.state.mock_state.delay_sec = 0.03
        self.b2_app.state.mock_state.delay_sec = 0.03

        self.b1_app.state.mock_state.replicate_order.clear()
        self.b2_app.state.mock_state.replicate_order.clear()

        versions: List[int] = []
        for i in range(4):
            r = httpx.post(
                f"{self.primary_url}/api/v1/write",
                json={"operation": "PUT", "key": "seq_k", "value": f"a_{i}", "consistency": "ASYNC"},
                timeout=3.0,
            )
            self.assertEqual(r.status_code, 200)
            versions.append(r.json()["version"])

        r_sync = httpx.post(
            f"{self.primary_url}/api/v1/write",
            json={"operation": "PUT", "key": "seq_k", "value": "sync_final", "consistency": "SYNC"},
            timeout=5.0,
        )
        self.assertEqual(r_sync.status_code, 200)
        versions.append(r_sync.json()["version"])

        self._wait_backups_reach(versions[-1], timeout=4.0)

        for name, order in (
            ("backup-1", self.b1_app.state.mock_state.replicate_order),
            ("backup-2", self.b2_app.state.mock_state.replicate_order),
        ):
            self.assertEqual(
                order,
                versions,
                f"Thứ tự nhận version trên {name} bị đảo lộn: {order} != {versions}",
            )

    def test_10_catch_up_endpoint_only_committed(self):
        resp = httpx.get(
            f"{self.primary_url}/api/v1/catch-up", params={"from_version": 1}, timeout=2.0
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        entries = data["entries"]
        self.assertEqual(data["missed_count"], len(entries))
        expected_versions = list(range(2, data["current_primary_version"] + 1))
        self.assertEqual([e["version"] for e in entries], expected_versions)
        # Tuyệt đối không chứa bản ghi BAD_ABORTED đã bị 503
        for e in entries:
            self.assertNotEqual(e.get("value"), "BAD_ABORTED")

    def test_11_wal_recovery_and_utf8_truncation(self):
        health_before = httpx.get(f"{self.primary_url}/api/v1/health", timeout=2.0).json()
        expected_v = health_before["current_version"]

        # Dừng Primary, mô phỏng docker kill cắt ngang giữa ký tự UTF-8 tiếng Việt ở dòng cuối
        self.primary_handle.stop()

        wal_path = cfg.WAL_FILE
        with open(wal_path, "rb") as f:
            original_size = len(f.read())

        torn_line = json.dumps(
            {
                "version": expected_v + 1,
                "commit_version": expected_v,
                "operation": "PUT",
                "key": "torn",
                "value": "Nguyễn Văn Á",
            },
            ensure_ascii=False,
        ).encode("utf-8")
        # Cắt ngang đúng vào giữa 2 byte UTF-8 của chữ 'Á' (b"\xc3\x81")
        cut_idx = torn_line.rfind("Á".encode("utf-8")) + 1
        torn_prefix = torn_line[:cut_idx]
        with open(wal_path, "ab") as f:
            f.write(torn_prefix)

        # Khởi động lại Primary -> phải tự cắt dòng hỏng và khôi phục nguyên vẹn expected_v
        self.__class__.primary_handle = ServerHandle(
            main_mod.app, "127.0.0.1", self.primary_port
        )
        self.__class__.primary_handle.start()

        health_after = httpx.get(f"{self.primary_url}/api/v1/health", timeout=2.0).json()
        self.assertEqual(health_after["current_version"], expected_v)

        with open(wal_path, "rb") as f:
            repaired_size = len(f.read())
        self.assertEqual(repaired_size, original_size)

        # Xác nhận dữ liệu tiếng Việt & Tombstone trước đó vẫn nguyên vẹn
        r_hier = httpx.get(
            f"{self.primary_url}/api/v1/read/users/101/profile", timeout=2.0
        ).json()
        self.assertEqual(r_hier["value"]["name"], "Nguyễn Văn Á")
        r_del = httpx.get(f"{self.primary_url}/api/v1/read/k_sync", timeout=2.0).json()
        self.assertTrue(r_del["deleted"])
        self.assertFalse(r_del["found"])

    def test_12_wal_midfile_corruption_raises_runtime_error(self):
        # Đảm bảo hỏng dữ liệu ở dòng GIỮA file phải báo RuntimeError chứ không bỏ qua im lặng
        bad_wal_dir = tempfile.mkdtemp(prefix="kv_bad_wal_")
        orig_data_dir = main_mod.DATA_DIR
        orig_wal_file = main_mod.WAL_FILE
        try:
            bad_wal = os.path.join(bad_wal_dir, "primary_wal.jsonl")
            with open(bad_wal, "w", encoding="utf-8") as f:
                f.write('{"version": 1, "operation": "PUT", "key": "a", "value": "1"}\n')
                f.write('{"version": 2, BROKEN_MID_FILE\n')
                f.write('{"version": 3, "operation": "PUT", "key": "b", "value": "2"}\n')

            main_mod.DATA_DIR = bad_wal_dir
            main_mod.WAL_FILE = bad_wal
            with self.assertRaises(RuntimeError):
                main_mod.load_wal_from_disk()
        finally:
            main_mod.DATA_DIR = orig_data_dir
            main_mod.WAL_FILE = orig_wal_file
            main_mod.load_wal_from_disk()
            shutil.rmtree(bad_wal_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
