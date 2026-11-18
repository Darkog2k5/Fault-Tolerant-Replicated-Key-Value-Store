import os
import sys
import time
import socket
import shutil
import asyncio
import tempfile
import threading
import unittest
import httpx
import uvicorn

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TESTS_DIR)
PRIMARY_DIR = os.path.join(REPO_ROOT, "primary_node")
PROXY_DIR = os.path.join(REPO_ROOT, "client_proxy")

for p in (TESTS_DIR, PRIMARY_DIR, PROXY_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)

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


class TestClientProxyComprehensive(unittest.TestCase):
    """Bộ kiểm thử tự động toàn diện cho Smart Client Proxy (Sinh viên 3)."""

    temp_dir: str = ""
    b1_port: int = 0
    b2_port: int = 0
    primary_port: int = 0
    proxy_port: int = 0
    b1_url: str = ""
    b2_url: str = ""
    primary_url: str = ""
    proxy_url: str = ""

    b1_app = None
    b2_app = None
    proxy_app = None
    b1_handle: ServerHandle = None  # type: ignore
    b2_handle: ServerHandle = None  # type: ignore
    primary_handle: ServerHandle = None  # type: ignore
    proxy_handle: ServerHandle = None  # type: ignore

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp(prefix="kv_proxy_test_")
        cls.b1_port = _find_free_port()
        cls.b2_port = _find_free_port()
        cls.primary_port = _find_free_port()
        cls.proxy_port = _find_free_port()

        cls.b1_url = f"http://127.0.0.1:{cls.b1_port}"
        cls.b2_url = f"http://127.0.0.1:{cls.b2_port}"
        cls.primary_url = f"http://127.0.0.1:{cls.primary_port}"
        cls.proxy_url = f"http://127.0.0.1:{cls.proxy_port}"

        os.environ["NODE_ID"] = "primary"
        os.environ["PORT"] = str(cls.primary_port)
        os.environ["REPLICAS"] = f"{cls.b1_url},{cls.b2_url}"
        os.environ["DEFAULT_QUORUM_W"] = "2"
        os.environ["DATA_DIR"] = cls.temp_dir

        global main_mod, proxy_mod
        import main as main_mod
        import proxy as proxy_mod

        cls.b1_app = create_mock_backup_app("backup-1", primary_url=cls.primary_url)
        cls.b2_app = create_mock_backup_app("backup-2", primary_url=cls.primary_url)

        cls.b1_handle = ServerHandle(cls.b1_app, "127.0.0.1", cls.b1_port)
        cls.b2_handle = ServerHandle(cls.b2_app, "127.0.0.1", cls.b2_port)
        cls.b1_handle.start()
        cls.b2_handle.start()

        cls.primary_handle = ServerHandle(main_mod.app, "127.0.0.1", cls.primary_port)
        cls.primary_handle.start()

        cls.proxy_app = proxy_mod.create_proxy_app(
            node_id="proxy",
            primary_url=cls.primary_url,
            backup_urls=[cls.b1_url, cls.b2_url],
            read_timeout=0.5,
            write_timeout=4.0,
            use_global_default_session=True,
        )
        cls.proxy_handle = ServerHandle(cls.proxy_app, "127.0.0.1", cls.proxy_port)
        cls.proxy_handle.start()

    @classmethod
    def tearDownClass(cls):
        for h in (cls.proxy_handle, cls.primary_handle, cls.b1_handle, cls.b2_handle):
            if h is not None:
                h.stop()
        if cls.temp_dir and os.path.exists(cls.temp_dir):
            shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def _reset_backup_faults(self):
        for app in (self.b1_app, self.b2_app):
            st = app.state.mock_state
            st.delay_sec = 0.0
            st.fail_replicate = False
            st.drop_commit = False
            st.is_catching_up = False
            st.fail_read = False
            st.read_delay_sec = 0.0

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
        # Đảm bảo primary_url của Proxy luôn trỏ đúng
        self.proxy_app.state.proxy_state.primary_url = self.primary_url

    def test_01_proxy_health_and_config_normalization(self):
        r = httpx.get(f"{self.proxy_url}/api/v1/health", timeout=2.0)
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(data["status"], "ALIVE")
        self.assertEqual(data["node_id"], "proxy")
        self.assertEqual(data["role"], "PROXY")
        self.assertEqual(data["primary_url"], self.primary_url)
        self.assertEqual(data["backups"], [self.b1_url, self.b2_url])

        # Kiểm tra chuẩn hoá danh sách URL trong config
        parsed = proxy_mod.cfg.parse_backup_urls(
            " http://backup1:8001/ , http://backup2:8002 , http://backup1:8001 , http://localhost:8080/ ",
            node_id="proxy",
            port=8080,
        )
        self.assertEqual(parsed, ["http://backup1:8001", "http://backup2:8002"])

    def test_02_write_forwarding_session_tracking_and_local_cache(self):
        httpx.delete(f"{self.proxy_url}/api/v1/session?all_sessions=true", timeout=2.0)

        for idx, mode in enumerate(["SYNC", "QUORUM", "ASYNC"], start=1):
            r = httpx.post(
                f"{self.proxy_url}/api/v1/write",
                json={
                    "operation": "PUT",
                    "key": f"w_key_{idx}",
                    "value": f"val_{mode}",
                    "consistency": mode,
                },
                timeout=3.0,
            )
            self.assertEqual(r.status_code, 200)
            body = r.json()
            self.assertEqual(body["status"], "SUCCESS")
            self.assertEqual(body["version"], idx)

            sess = httpx.get(f"{self.proxy_url}/api/v1/session", timeout=2.0).json()
            self.assertEqual(sess["last_written_version"], idx)
            self.assertEqual(sess["local_cache"][f"w_key_{idx}"]["value"], f"val_{mode}")
            self.assertEqual(sess["local_cache"][f"w_key_{idx}"]["version"], idx)
            self.assertEqual(proxy_mod.last_written_version, idx)
            self.assertEqual(proxy_mod.local_cache[f"w_key_{idx}"]["value"], f"val_{mode}")

        self._wait_backups_reach(3)

    def test_03_load_balancing_round_robin_across_backups(self):
        self._wait_backups_reach(3)
        served_nodes = []
        for _ in range(4):
            r = httpx.get(f"{self.proxy_url}/api/v1/read/w_key_1", timeout=2.0)
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.headers.get("X-Proxy-Source"), "backup")
            data = r.json()
            self.assertEqual(data["value"], "val_SYNC")
            self.assertTrue(data["found"])
            served_nodes.append(data["node_id"])

        # Round-Robin phải phân tải đều cho cả backup-1 và backup-2
        self.assertEqual(set(served_nodes), {"backup-1", "backup-2"})
        self.assertEqual(served_nodes.count("backup-1"), 2)
        self.assertEqual(served_nodes.count("backup-2"), 2)

        sess = httpx.get(f"{self.proxy_url}/api/v1/session", timeout=2.0).json()
        self.assertGreaterEqual(sess["last_read_version"], 3)

    def test_04_read_your_writes_on_async_replication_lag(self):
        """
        Kiểm tra Read-Your-Writes (RYW):
        - Chặn tạm thời replicate xuống cả 2 Backup (hoặc làm trễ).
        - Ghi ASYNC qua Proxy -> Primary tăng version lên v_new, nhưng cả 2 Backup vẫn ở version cũ.
        - Đọc ngay qua Proxy -> Proxy phải phát hiện cả 2 Backup có node_version < required_version,
          từ chối dữ liệu cũ (Stale Read) và fallback về Primary để trả đúng giá trị mới.
        """
        self.b1_app.state.mock_state.fail_replicate = True
        self.b2_app.state.mock_state.fail_replicate = True

        r_write = httpx.post(
            f"{self.proxy_url}/api/v1/write",
            json={
                "operation": "PUT",
                "key": "ryw_key",
                "value": "fresh_value_v4",
                "consistency": "ASYNC",
            },
            timeout=3.0,
        )
        self.assertEqual(r_write.status_code, 200)
        v_new = r_write.json()["version"]

        # Xác nhận 2 Backup vẫn đang ở version cũ (< v_new)
        self.assertLess(self.b1_app.state.mock_state.current_version, v_new)
        self.assertLess(self.b2_app.state.mock_state.current_version, v_new)

        # Đọc ngay lập tức qua Proxy
        r_read = httpx.get(f"{self.proxy_url}/api/v1/read/ryw_key", timeout=2.0)
        self.assertEqual(r_read.status_code, 200)
        self.assertEqual(r_read.headers.get("X-Proxy-Source"), "primary")
        self.assertEqual(r_read.headers.get("X-Stale-Rejected"), "true")

        read_data = r_read.json()
        self.assertEqual(read_data["node_id"], "primary")
        self.assertEqual(read_data["value"], "fresh_value_v4")
        self.assertEqual(read_data["version"], v_new)
        self.assertGreaterEqual(read_data["node_version"], v_new)

        # Khôi phục Backup và đồng bộ bù bằng 1 lệnh SYNC
        self.b1_app.state.mock_state.fail_replicate = False
        self.b2_app.state.mock_state.fail_replicate = False
        r_sync = httpx.post(
            f"{self.proxy_url}/api/v1/write",
            json={"operation": "PUT", "key": "sync_heal", "value": "ok", "consistency": "SYNC"},
            timeout=3.0,
        )
        self.assertEqual(r_sync.status_code, 200)
        self._wait_backups_reach(r_sync.json()["version"])

    def test_05_read_your_writes_on_quorum_partial_backup_lag(self):
        """
        Ghi QUORUM (W=2) khi Backup 2 bị lỗi replicate -> Primary và Backup 1 lên v_new, Backup 2 tụt hậu.
        Khi đọc qua Proxy, nếu Round-Robin trúng Backup 2 (cũ), Proxy phải từ chối Backup 2 và lấy từ Backup 1.
        """
        self.b2_app.state.mock_state.fail_replicate = True

        r_q = httpx.post(
            f"{self.proxy_url}/api/v1/write",
            json={
                "operation": "PUT",
                "key": "quorum_ryw",
                "value": "quorum_fresh",
                "consistency": "QUORUM",
                "quorum_w": 2,
            },
            timeout=3.0,
        )
        self.assertEqual(r_q.status_code, 200)
        v_q = r_q.json()["version"]

        # Đợi Primary gửi ngầm /commit tới Backup 1
        deadline = time.time() + 2.0
        while time.time() < deadline and self.b1_app.state.mock_state.current_version < v_q:
            time.sleep(0.03)
        self.assertEqual(self.b1_app.state.mock_state.current_version, v_q)
        self.assertLess(self.b2_app.state.mock_state.current_version, v_q)

        # Đọc 2 lần liên tiếp qua Proxy (chắc chắn có 1 lần chạm vào Backup 2 trước)
        for _ in range(2):
            r = httpx.get(f"{self.proxy_url}/api/v1/read/quorum_ryw", timeout=2.0)
            self.assertEqual(r.status_code, 200)
            d = r.json()
            self.assertEqual(d["value"], "quorum_fresh")
            self.assertEqual(d["node_id"], "backup-1")
            self.assertGreaterEqual(d["node_version"], v_q)

        # Khôi phục Backup 2
        self.b2_app.state.mock_state.fail_replicate = False
        r_heal = httpx.post(
            f"{self.proxy_url}/api/v1/write",
            json={"operation": "PUT", "key": "heal_b2", "value": "1", "consistency": "SYNC"},
            timeout=3.0,
        )
        self.assertEqual(r_heal.status_code, 200)
        self._wait_backups_reach(r_heal.json()["version"])

    def test_06_monotonic_reads_consistency_for_reader_session(self):
        """
        Kiểm tra Monotonic Reads (MR):
        - Tạo một phiên đọc mới ('reader-only') chưa từng ghi gì (last_written_version = 0).
        - Lần đọc 1: Đọc từ cụm ở node_version = V -> last_read_version của phiên lên V.
        - Giả lập Backup 2 bị tụt version về 1 (< V).
        - Lần đọc 2 & 3: Dù không ghi gì, phiên 'reader-only' tuyệt đối không chấp nhận dữ liệu
          từ Backup 2 có node_version = 1 < last_read_version (V).
        """
        headers = {"X-Session-ID": "reader-only"}
        httpx.delete(f"{self.proxy_url}/api/v1/session", headers=headers, timeout=2.0)

        # Lần đọc 1: Cả 2 backup đều đang ở version mới nhất
        r1 = httpx.get(f"{self.proxy_url}/api/v1/read/w_key_1", headers=headers, timeout=2.0)
        self.assertEqual(r1.status_code, 200)
        observed_v = r1.json()["node_version"]
        self.assertGreater(observed_v, 1)

        sess = httpx.get(f"{self.proxy_url}/api/v1/session", headers=headers, timeout=2.0).json()
        self.assertEqual(sess["last_written_version"], 0)
        self.assertEqual(sess["last_read_version"], observed_v)

        # Giả lập Backup 2 bị tụt lùi version về 1
        orig_b2_v = self.b2_app.state.mock_state.current_version
        self.b2_app.state.mock_state.current_version = 1
        try:
            for _ in range(2):
                r2 = httpx.get(
                    f"{self.proxy_url}/api/v1/read/w_key_1", headers=headers, timeout=2.0
                )
                self.assertEqual(r2.status_code, 200)
                d2 = r2.json()
                self.assertGreaterEqual(d2["node_version"], observed_v)
                self.assertNotEqual(d2["node_id"], "backup-2")
        finally:
            self.b2_app.state.mock_state.current_version = orig_b2_v

    def test_07_failover_on_backup_503_catching_up_and_both_backups_down(self):
        """
        Kiểm tra Failover:
        1. Backup 1 trả 503 NODE_CATCHING_UP -> Proxy tự chuyển sang Backup 2.
        2. Cả Backup 1 (503 CATCHING_UP) và Backup 2 (503 lỗi đọc) -> Proxy tự chuyển về Primary.
        """
        self.b1_app.state.mock_state.is_catching_up = True

        for _ in range(2):
            r = httpx.get(f"{self.proxy_url}/api/v1/read/w_key_1", timeout=2.0)
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json()["node_id"], "backup-2")

        # Cả 2 Backup đều trả 503 -> Phải tự động gọi vào Primary
        self.b2_app.state.mock_state.fail_read = True
        r_prim = httpx.get(f"{self.proxy_url}/api/v1/read/w_key_1", timeout=2.0)
        self.assertEqual(r_prim.status_code, 200)
        self.assertEqual(r_prim.headers.get("X-Proxy-Source"), "primary")
        self.assertEqual(r_prim.json()["node_id"], "primary")
        self.assertEqual(r_prim.json()["value"], "val_SYNC")

    def test_08_failover_on_backup_read_timeout(self):
        """Backup 1 bị treo quá read_timeout -> Proxy tự ngắt và chuyển sang Backup 2."""
        self.b1_app.state.mock_state.read_delay_sec = 1.2  # > read_timeout (0.5s)

        for _ in range(2):
            r = httpx.get(f"{self.proxy_url}/api/v1/read/w_key_2", timeout=3.0)
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json()["node_id"], "backup-2")
            self.assertEqual(r.json()["value"], "val_QUORUM")

    def test_09_local_cache_fallback_and_tombstone_in_cache(self):
        """
        Khi cả 2 Backup và Primary đều không thể phản hồi lúc Đọc:
        - Key đã ghi PUT hoặc đã đọc trong phiên -> Phục hồi từ local_cache (node_id='local_cache').
        - Key đã xóa DELETE trong phiên -> Phục hồi Tombstone từ local_cache (found=False, deleted=True).
        - Key chưa từng biết -> Trả 503 Service Unavailable.
        """
        # Ghi 1 key mới và xoá 1 key qua Proxy
        r_put = httpx.post(
            f"{self.proxy_url}/api/v1/write",
            json={"operation": "PUT", "key": "cached_alive", "value": "from_cache_val", "consistency": "SYNC"},
            timeout=3.0,
        )
        self.assertEqual(r_put.status_code, 200)

        r_del = httpx.post(
            f"{self.proxy_url}/api/v1/write",
            json={"operation": "DELETE", "key": "w_key_3", "consistency": "SYNC"},
            timeout=3.0,
        )
        self.assertEqual(r_del.status_code, 200)
        v_del = r_del.json()["version"]

        # Giả lập cả 2 Backup lỗi đọc và Primary không kết nối được
        self.b1_app.state.mock_state.fail_read = True
        self.b2_app.state.mock_state.fail_read = True
        self.proxy_app.state.proxy_state.primary_url = "http://127.0.0.1:9"

        # 1. Đọc key còn sống từ local_cache
        r_c1 = httpx.get(f"{self.proxy_url}/api/v1/read/cached_alive", timeout=2.0)
        self.assertEqual(r_c1.status_code, 200)
        self.assertEqual(r_c1.headers.get("X-Proxy-Source"), "local_cache")
        d_c1 = r_c1.json()
        self.assertEqual(d_c1["node_id"], "local_cache")
        self.assertEqual(d_c1["value"], "from_cache_val")
        self.assertTrue(d_c1["found"])
        self.assertFalse(d_c1["deleted"])

        # 2. Đọc key đã DELETE (Tombstone) từ local_cache
        r_c2 = httpx.get(f"{self.proxy_url}/api/v1/read/w_key_3", timeout=2.0)
        self.assertEqual(r_c2.status_code, 200)
        d_c2 = r_c2.json()
        self.assertEqual(d_c2["node_id"], "local_cache")
        self.assertIsNone(d_c2["value"])
        self.assertEqual(d_c2["version"], v_del)
        self.assertFalse(d_c2["found"])
        self.assertTrue(d_c2["deleted"])

        # 3. Đọc key chưa từng tồn tại trong local_cache -> Phải trả 503
        r_miss = httpx.get(f"{self.proxy_url}/api/v1/read/unknown_key_xyz", timeout=2.0)
        self.assertEqual(r_miss.status_code, 503)

    def test_10_hierarchical_keys_and_complex_json_via_proxy(self):
        payload_val = {"full_name": "Trần Thị Bích", "roles": ["admin", "user"], "score": 99.5}
        r_w = httpx.post(
            f"{self.proxy_url}/api/v1/write",
            json={
                "operation": "PUT",
                "key": "orgs/uet/users/101",
                "value": payload_val,
                "consistency": "SYNC",
            },
            timeout=3.0,
        )
        self.assertEqual(r_w.status_code, 200)
        v_hier = r_w.json()["version"]
        self._wait_backups_reach(v_hier)

        r_r = httpx.get(f"{self.proxy_url}/api/v1/read/orgs/uet/users/101", timeout=2.0)
        self.assertEqual(r_r.status_code, 200)
        d_r = r_r.json()
        self.assertEqual(d_r["key"], "orgs/uet/users/101")
        self.assertEqual(d_r["value"], payload_val)
        self.assertTrue(d_r["found"])

    def test_11_failed_writes_400_and_503_do_not_update_session(self):
        sess_before = httpx.get(f"{self.proxy_url}/api/v1/session", timeout=2.0).json()
        w_before = sess_before["last_written_version"]

        # 1. Lỗi 400 Bad Request
        bad_reqs = [
            {"operation": "PATCH", "key": "a", "value": "1"},
            {"operation": "PUT", "key": ""},
            {"operation": "PUT", "key": "a"},
            {"operation": "PUT", "key": "a", "value": "1", "consistency": "INVALID"},
        ]
        for b in bad_reqs:
            resp = httpx.post(f"{self.proxy_url}/api/v1/write", json=b, timeout=2.0)
            self.assertEqual(resp.status_code, 400)

        r_empty_read = httpx.get(f"{self.proxy_url}/api/v1/read/", timeout=2.0)
        self.assertEqual(r_empty_read.status_code, 400)

        # 2. Lỗi 503 khi ghi SYNC mà 1 Backup sập
        self.b2_app.state.mock_state.fail_replicate = True
        r_503 = httpx.post(
            f"{self.proxy_url}/api/v1/write",
            json={"operation": "PUT", "key": "aborted_k", "value": "never_saved", "consistency": "SYNC"},
            timeout=3.0,
        )
        self.assertEqual(r_503.status_code, 503)

        sess_after = httpx.get(f"{self.proxy_url}/api/v1/session", timeout=2.0).json()
        self.assertEqual(sess_after["last_written_version"], w_before)
        self.assertNotIn("aborted_k", sess_after["local_cache"])

    def test_12_multi_session_isolation_and_stats(self):
        h_a = {"X-Session-ID": "session-A"}
        h_b = {"X-Session-ID": "session-B"}
        httpx.delete(f"{self.proxy_url}/api/v1/session", headers=h_a, timeout=2.0)
        httpx.delete(f"{self.proxy_url}/api/v1/session", headers=h_b, timeout=2.0)

        r_wa = httpx.post(
            f"{self.proxy_url}/api/v1/write",
            headers=h_a,
            json={"operation": "PUT", "key": "isolated_k", "value": "val_A", "consistency": "SYNC"},
            timeout=3.0,
        )
        self.assertEqual(r_wa.status_code, 200)
        v_a = r_wa.json()["version"]

        sa = httpx.get(f"{self.proxy_url}/api/v1/session", headers=h_a, timeout=2.0).json()
        sb = httpx.get(f"{self.proxy_url}/api/v1/session", headers=h_b, timeout=2.0).json()

        self.assertEqual(sa["last_written_version"], v_a)
        self.assertIn("isolated_k", sa["local_cache"])
        self.assertEqual(sb["last_written_version"], 0)
        self.assertNotIn("isolated_k", sb["local_cache"])

        stats_resp = httpx.get(f"{self.proxy_url}/api/v1/stats", timeout=2.0)
        self.assertEqual(stats_resp.status_code, 200)
        stats_data = stats_resp.json()["stats"]
        self.assertGreater(stats_data["total_writes"], 0)
        self.assertGreater(stats_data["total_reads"], 0)
        self.assertGreater(stats_data["stale_rejections"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)

