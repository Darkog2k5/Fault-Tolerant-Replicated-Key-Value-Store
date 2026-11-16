"""Khởi chạy một cụm Primary và hai Backup bằng các tiến trình Python riêng."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx


REPO_ROOT = Path(__file__).resolve().parents[1]


class ClusterUnavailable(RuntimeError):
    """Môi trường hiện tại không cho phép mở socket hoặc tạo tiến trình con."""


class Cluster:
    """Quản lý tiến trình, cổng, HTTP client và log của cụm kiểm thử."""

    def __init__(self, temp_dir: str | Path):
        self.temp_dir = Path(temp_dir).resolve()
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir = self.temp_dir
        self.log_dir = self.temp_dir / "logs"
        self.log_dir.mkdir(parents=True, exist_ok=True)

        try:
            allocated = []
            while len(allocated) < 3:
                candidate = self._free_port()
                if candidate not in allocated:
                    allocated.append(candidate)
            self.ports = dict(zip(("primary", "backup-1", "backup-2"), allocated))
        except OSError as error:
            raise ClusterUnavailable(f"Không cấp phát được cổng localhost: {error}") from error

        self._processes: dict[str, subprocess.Popen[Any]] = {}
        self._log_handles: dict[str, Any] = {}
        self._started_at: dict[str, float] = {}
        self._suspended: set[str] = set()
        self._client = httpx.Client(timeout=httpx.Timeout(4.0), trust_env=False)

    @staticmethod
    def _free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    def check_runtime(self) -> None:
        """Kiểm tra sớm xem interpreter có chạy được tiến trình con không."""
        try:
            result = subprocess.run(
                [sys.executable, "--version"],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                timeout=8,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise ClusterUnavailable(f"Không chạy được Python con: {error}") from error
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).strip()
            raise ClusterUnavailable(f"Python con không khởi động được: {detail}")

    def _url(self, node: str) -> str:
        if node.startswith("http://") or node.startswith("https://"):
            return node.rstrip("/")
        if node not in self.ports:
            raise ValueError(f"Node không hợp lệ: {node}")
        return f"http://127.0.0.1:{self.ports[node]}"

    def url(self, node: str) -> str:
        """Trả URL HTTP của node hoặc chuẩn hóa URL đã truyền vào."""
        return self._url(node)

    def request(self, node: str, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Gửi HTTP request trực tiếp tới một node trong cụm."""
        return self._client.request(method, f"{self._url(node)}{path}", **kwargs)

    def _spawn(self, node: str, directory: str, extra_env: dict[str, str]) -> None:
        self._stop(node)
        port = self.ports[node]
        node_data = self.data_dir / node
        node_data.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()
        # Các HTTP client bên trong node cũng phải đi thẳng tới localhost.
        env["NO_PROXY"] = "127.0.0.1,localhost"
        env["no_proxy"] = "127.0.0.1,localhost"
        if node == "primary":
            env["DEFAULT_QUORUM_W"] = "2"
        else:
            env.update(
                {
                    "CATCHUP_HTTP_TIMEOUT": "5.0",
                    "REPLICATE_CATCHUP_DEADLINE": "1.5",
                    "PRIMARY_FAILURES_BEFORE_DEGRADED": "5",
                    "SYNC_INTERVAL": "5.0",
                    "CATCHUP_DELAY_MS": "0",
                    "PENDING_TTL": "3.0",
                }
            )
        env.update(extra_env)
        env.update(
            {
                "NODE_ID": node,
                "PORT": str(port),
                "DATA_DIR": str(node_data),
                "PYTHONIOENCODING": "utf-8",
                "PYTHONUNBUFFERED": "1",
            }
        )
        if node == "primary":
            replicas = ",".join(
                self._url(backup) for backup in ("backup-1", "backup-2")
            )
            env["REPLICAS"] = replicas
        else:
            env["PRIMARY_URL"] = self._url("primary")

        log_path = self.log_dir / f"{node}.log"
        handle = log_path.open("a", encoding="utf-8", errors="replace", buffering=1)
        try:
            started_at = time.perf_counter()
            process = subprocess.Popen(
                [sys.executable, "main.py"],
                cwd=REPO_ROOT / directory,
                env=env,
                stdout=handle,
                stderr=subprocess.STDOUT,
            )
        except OSError as error:
            handle.close()
            raise ClusterUnavailable(f"Không tạo được tiến trình {node}: {error}") from error
        self._processes[node] = process
        self._log_handles[node] = handle
        self._started_at[node] = started_at

    def start_primary(self) -> None:
        """Khởi động lại Primary trên cùng cổng và thư mục dữ liệu."""
        self._spawn("primary", "primary_node", {})

    def stop_primary(self) -> None:
        """Dừng cứng Primary và chờ tiến trình kết thúc."""
        self._stop("primary")

    def start_backup(self, i: int | str, **env: Any) -> float:
        """Khởi động Backup; có thể truyền thêm biến môi trường cấu hình."""
        node = self._backup_name(i)
        self._spawn(node, "backup_node", {key: str(value) for key, value in env.items()})
        return self._started_at[node]

    def stop_backup(self, i: int | str) -> None:
        """Dừng cứng Backup và chờ tiến trình kết thúc."""
        self._stop(self._backup_name(i))

    def suspend_backup(self, i: int | str) -> None:
        """Tạm dừng Backup nhưng giữ nguyên tiến trình và trạng thái trong RAM."""
        try:
            import psutil
        except ImportError as error:
            raise ClusterUnavailable("Cần cài psutil để tạm dừng tiến trình Backup") from error
        node = self._backup_name(i)
        process = self._processes.get(node)
        if process is None or process.poll() is not None:
            raise ClusterUnavailable(f"Tiến trình {node} chưa chạy")
        for target in self._process_tree(process.pid):
            target.suspend()
        self._suspended.add(node)

    def resume_backup(self, i: int | str) -> None:
        """Tiếp tục Backup đã tạm dừng."""
        try:
            import psutil
        except ImportError as error:
            raise ClusterUnavailable("Cần cài psutil để tiếp tục tiến trình Backup") from error
        node = self._backup_name(i)
        if node not in self._suspended:
            return
        process = self._processes.get(node)
        if process is not None and process.poll() is None:
            for target in self._process_tree(process.pid):
                target.resume()
        self._suspended.discard(node)

    @staticmethod
    def _process_tree(pid: int) -> list[Any]:
        """Trả về tiến trình và mọi tiến trình con.

        Trên Windows, python.exe của venv chỉ là launcher sinh ra interpreter thật,
        nên tạm dừng riêng PID của Popen không dừng được node.
        """
        import psutil

        root = psutil.Process(pid)
        return [root, *root.children(recursive=True)]

    @staticmethod
    def _backup_name(i: int | str) -> str:
        if isinstance(i, int):
            node = f"backup-{i}"
        else:
            node = i if i.startswith("backup-") else f"backup-{i}"
        if node not in ("backup-1", "backup-2"):
            raise ValueError(f"Backup không hợp lệ: {i}")
        return node

    def _stop(self, node: str) -> None:
        process = self._processes.get(node)
        if node in self._suspended and process is not None:
            try:
                for target in self._process_tree(process.pid):
                    target.resume()
            except Exception:
                pass
            self._suspended.discard(node)
        process = self._processes.pop(node, None)
        self._started_at.pop(node, None)
        if process is not None and process.poll() is None:
            try:
                process.kill()
            except OSError:
                pass
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        handle = self._log_handles.pop(node, None)
        if handle is not None:
            handle.close()

    def wait_health(
        self,
        url: str,
        status: str | None = None,
        version: int | None = None,
        timeout: float = 10.0,
    ) -> dict[str, Any]:
        """Poll health đến khi đạt trạng thái/phiên bản yêu cầu hoặc hết hạn."""
        target = self._url(url)
        deadline = time.monotonic() + timeout
        last_error = "chưa nhận được phản hồi"
        while time.monotonic() < deadline:
            try:
                response = self._client.get(f"{target}/api/v1/health")
                if response.status_code == 200:
                    data = response.json()
                    if (status is None or data.get("status") == status) and (
                        version is None or data.get("current_version") == version
                    ):
                        return data
                    last_error = f"health={data}"
                else:
                    last_error = f"HTTP {response.status_code}: {response.text}"
            except (httpx.HTTPError, ValueError) as error:
                last_error = str(error)
            time.sleep(0.04)
        raise AssertionError(f"Hết thời gian chờ health tại {target}: {last_error}")

    def write(
        self,
        key: str,
        value: str | None,
        consistency: str = "SYNC",
        operation: str = "PUT",
        quorum_w: int | None = None,
    ) -> httpx.Response:
        """Gửi một lệnh ghi tới Primary và trả nguyên phản hồi HTTP."""
        body: dict[str, Any] = {
            "operation": operation,
            "key": key,
            "value": value if operation.upper() == "PUT" else None,
            "consistency": consistency,
        }
        if quorum_w is not None:
            body["quorum_w"] = quorum_w
        return self._client.post(f"{self._url('primary')}/api/v1/write", json=body)

    def read(self, node: str, key: str) -> httpx.Response:
        """Đọc một khóa từ Primary hoặc Backup đã chỉ định."""
        return self._client.get(f"{self._url(node)}/api/v1/read/{key}")

    def health(self, node: str) -> dict[str, Any]:
        """Đọc trạng thái sức khỏe của một node."""
        response = self._client.get(f"{self._url(node)}/api/v1/health")
        response.raise_for_status()
        return response.json()

    def log(self, node: str) -> str:
        """Đọc log của một node, kể cả log từ các lần khởi động trước."""
        path = self.log_dir / f"{node}.log"
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8", errors="replace")

    def wal_path(self, node: str) -> Path:
        """Trả đường dẫn WAL riêng của node."""
        return self.data_dir / node / (
            "primary_wal.jsonl" if node == "primary" else "backup_wal.jsonl"
        )

    def close(self) -> None:
        """Dừng toàn bộ tiến trình còn chạy và đóng tài nguyên HTTP/log."""
        for node in ("backup-2", "backup-1", "primary"):
            self._stop(node)
        self._client.close()
