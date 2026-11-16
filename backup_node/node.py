"""Trạng thái, WAL, Prepare/Commit và phục hồi cho Backup Replica Node."""

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import httpx


class BackupNode:
    """Một bản sao dự phòng với WAL liên tục và đồng bộ bù từ Primary."""

    def __init__(
        self,
        node_id: str = "backup-1",
        primary_url: str = "http://localhost:8000",
        data_dir: str | Path = "./data",
        primary_client: httpx.AsyncClient | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        catchup_http_timeout: float = 5.0,
        connect_timeout: float = 2.0,
        replicate_catchup_deadline: float = 1.5,
        primary_failures_before_degraded: int = 5,
        sync_interval: float = 5.0,
        catchup_delay_ms: int = 0,
        pending_ttl: float = 3.0,
    ) -> None:
        self.node_id = node_id
        self.primary_url = primary_url.rstrip("/")
        self.data_dir = Path(data_dir)
        self.wal_file = self.data_dir / "backup_wal.jsonl"
        self.catchup_http_timeout = catchup_http_timeout
        self.connect_timeout = connect_timeout
        self.replicate_catchup_deadline = replicate_catchup_deadline
        self.primary_failures_before_degraded = primary_failures_before_degraded
        self.sync_interval = sync_interval
        self.catchup_delay_ms = catchup_delay_ms
        self.pending_ttl = pending_ttl

        self.current_version = 0
        self.storage: dict[str, dict[str, Any]] = {}
        self.pending: dict[int, dict[str, Any]] = {}
        self._pending_received_at: dict[int, float] = {}
        self.is_catching_up = True
        self.degraded = False
        self.diverged = False
        self.lock = asyncio.Lock()

        self.last_catchup: dict[str, Any] = {
            "time": None,
            "entries_applied": 0,
            "primary_version": None,
            "last_error": None,
        }
        self._primary_failures = 0
        self._primary_client = primary_client
        self._owns_primary_client = False
        self._transport = transport
        self._catchup_task: asyncio.Task[bool] | None = None
        self._background_task: asyncio.Task[None] | None = None

    @property
    def status(self) -> str:
        if self.is_catching_up:
            return "CATCHING_UP"
        if self.degraded:
            return "DEGRADED"
        return "ALIVE"

    def log(self, tag: str, message: str) -> None:
        """Ghi log an toàn cả khi terminal không hỗ trợ tiếng Việt."""
        line = f"[{self.node_id}] [{tag}] {message}"
        try:
            print(line, flush=True)
        except Exception:
            try:
                encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
                escaped = line.encode(encoding, errors="backslashreplace").decode(
                    encoding, errors="replace"
                )
                print(escaped, flush=True)
            except Exception:
                # Log không được làm hỏng quá trình xử lý của node.
                pass

    def load_wal(self) -> None:
        """Đọc WAL, bỏ dòng cuối bị ghi dở và từ chối mọi WAL sai ở giữa."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.current_version = 0
        self.storage.clear()
        self.pending.clear()
        self._pending_received_at.clear()
        if not self.wal_file.exists():
            return

        with self.wal_file.open("rb") as file:
            raw_lines = file.readlines()

        good_lines: list[bytes] = []
        entries: list[dict[str, Any]] = []
        expected_version = 1
        truncated = False
        for index, raw_line in enumerate(raw_lines):
            is_last = index == len(raw_lines) - 1
            try:
                entry = json.loads(raw_line.decode("utf-8").strip())
                self._validate_wal_entry(entry)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as error:
                if is_last:
                    self._truncate_wal(b"".join(good_lines))
                    truncated = True
                    self.log(
                        "RECOVERY",
                        f"Đã cắt dòng WAL cuối bị ghi dở tại dòng {index + 1}: {error}",
                    )
                    break
                raise RuntimeError(
                    f"Dòng WAL {index + 1} bị hỏng ở giữa tệp: {error}"
                ) from error

            version = entry["version"]
            if version != expected_version:
                raise RuntimeError(
                    f"WAL không liên tục tại dòng {index + 1}: "
                    f"cần version {expected_version}, nhận {version}"
                )
            expected_version += 1
            entries.append(entry)
            good_lines.append(raw_line if raw_line.endswith(b"\n") else raw_line + b"\n")

        # Dòng cuối hợp lệ nhưng thiếu newline cần được ngăn cách với lần append sau.
        if not truncated and raw_lines and good_lines and not raw_lines[-1].endswith(b"\n"):
            with self.wal_file.open("ab") as file:
                file.write(b"\n")
                file.flush()
                os.fsync(file.fileno())

        for entry in entries:
            self._apply_to_storage(entry)
            self.current_version = entry["version"]

        if entries:
            self.log(
                "RECOVERY",
                f"Đã khôi phục current_version={self.current_version} từ WAL",
            )

    def _truncate_wal(self, contents: bytes) -> None:
        with self.wal_file.open("wb") as file:
            file.write(contents)
            file.flush()
            os.fsync(file.fileno())

    @staticmethod
    def _validate_wal_entry(entry: Any) -> None:
        required = {"version", "operation", "key", "value", "timestamp"}
        if not isinstance(entry, dict) or not required.issubset(entry):
            raise ValueError("bản ghi thiếu trường bắt buộc")
        if type(entry["version"]) is not int or entry["version"] < 1:
            raise ValueError("version trong WAL phải là số nguyên dương")
        if entry["operation"] not in ("PUT", "DELETE"):
            raise ValueError("operation trong WAL không hợp lệ")
        if not isinstance(entry["key"], str):
            raise ValueError("key trong WAL phải là chuỗi")
        if entry["value"] is not None and not isinstance(entry["value"], str):
            raise ValueError("value trong WAL phải là chuỗi hoặc null")
        if type(entry["timestamp"]) not in (int, float):
            raise ValueError("timestamp trong WAL phải là số")
        if "entry_id" in entry and not isinstance(entry["entry_id"], str):
            raise ValueError("entry_id trong WAL phải là chuỗi")

    @staticmethod
    def _wal_entry(entry: dict[str, Any]) -> dict[str, Any]:
        result = {
            "version": entry["version"],
            "operation": entry["operation"],
            "key": entry["key"],
            "value": entry.get("value"),
            "timestamp": entry.get("timestamp", time.time()),
        }
        if entry.get("entry_id") is not None:
            result["entry_id"] = entry["entry_id"]
        return result

    def _append_one(self, entry: dict[str, Any]) -> None:
        if entry["version"] != self.current_version + 1:
            raise RuntimeError(
                f"Chỉ được ghi version {self.current_version + 1}, "
                f"không thể ghi {entry['version']}"
            )
        self.data_dir.mkdir(parents=True, exist_ok=True)
        line = json.dumps(self._wal_entry(entry), ensure_ascii=False) + "\n"
        with self.wal_file.open("a", encoding="utf-8", newline="") as file:
            file.write(line)
            file.flush()
            os.fsync(file.fileno())

    def _append_batch(self, entries: list[dict[str, Any]]) -> None:
        if not entries:
            return
        expected = self.current_version + 1
        for entry in entries:
            if entry["version"] != expected:
                raise RuntimeError(
                    f"Lô catch-up không liên tục: cần version {expected}, "
                    f"nhận {entry['version']}"
                )
            expected += 1
        self.data_dir.mkdir(parents=True, exist_ok=True)
        lines = "".join(
            json.dumps(self._wal_entry(entry), ensure_ascii=False) + "\n"
            for entry in entries
        )
        with self.wal_file.open("a", encoding="utf-8", newline="") as file:
            file.write(lines)
            file.flush()
            os.fsync(file.fileno())

    def _apply_to_storage(self, entry: dict[str, Any]) -> None:
        deleted = entry["operation"] == "DELETE"
        self.storage[entry["key"]] = {
            "value": None if deleted else entry.get("value"),
            "version": entry["version"],
            "deleted": deleted,
        }

    def _commit_one(self, entry: dict[str, Any]) -> None:
        if entry["version"] != self.current_version + 1:
            raise RuntimeError(
                f"Không thể commit version {entry['version']} sau {self.current_version}"
            )
        self._append_one(entry)
        self._apply_to_storage(entry)
        self.current_version = entry["version"]

    def _apply_pending_up_to(self, target: int, commit_entry_id: str | None) -> str:
        """Commit các version liên tục đến target; caller phải giữ lock."""
        for version in [version for version in self.pending if version <= self.current_version]:
            self.pending.pop(version, None)
            self._pending_received_at.pop(version, None)

        while self.current_version < target:
            next_version = self.current_version + 1
            entry = self.pending.get(next_version)
            if entry is None:
                return "GAP"
            received_at = self._pending_received_at.get(next_version)
            if (
                received_at is not None
                and time.monotonic() - received_at > self.pending_ttl
            ):
                self.pending.pop(next_version, None)
                self._pending_received_at.pop(next_version, None)
                self.log(
                    "GAP",
                    f"Đã bỏ pending version={next_version} vì chờ quá TTL={self.pending_ttl}s",
                )
                return "STALE"
            if (
                next_version == target
                and commit_entry_id is not None
                and entry.get("entry_id") is not None
                and entry["entry_id"] != commit_entry_id
            ):
                self.pending.pop(next_version, None)
                self._pending_received_at.pop(next_version, None)
                return "STALE"
            self._commit_one(entry)
            self.pending.pop(next_version, None)
            self._pending_received_at.pop(next_version, None)
        return "OK"

    async def replicate(self, payload: dict[str, Any]) -> tuple[bool, str | None, int]:
        """Chuẩn bị một bản ghi; trả về ACK hoặc lý do NACK cùng version hiện tại."""
        version = payload["version"]
        commit_version = payload["commit_version"]
        entry = {
            "version": version,
            "commit_version": commit_version,
            "operation": payload["operation"],
            "key": payload["key"],
            "value": payload.get("value"),
            "timestamp": (
                payload["timestamp"]
                if payload.get("timestamp") is not None
                else time.time()
            ),
        }
        if payload.get("entry_id") is not None:
            entry["entry_id"] = payload["entry_id"]

        catching_up_at_deadline = False
        for attempt in (0, 1):
            async with self.lock:
                if self.diverged:
                    return False, "DIVERGED", self.current_version
                target = min(commit_version, version - 1)
                result = self._apply_pending_up_to(
                    target,
                    payload.get("commit_entry_id") if target == commit_version else None,
                )
                if result == "OK":
                    if version <= self.current_version:
                        self.log("PREPARE", f"Đã nhận bản ghi trùng version={version}")
                        return True, None, self.current_version
                    if version == self.current_version + 1:
                        self.pending[version] = entry
                        self._pending_received_at[version] = time.monotonic()
                        if commit_version >= version:
                            self._commit_one(self.pending[version])
                            self.pending.pop(version, None)
                            self._pending_received_at.pop(version, None)
                        self.log("PREPARE", f"Đã nhận version={version}")
                        return True, None, self.current_version

            if attempt == 0:
                self.log("GAP", f"Thiếu version trước bản ghi {version}, bắt đầu catch-up")
                task = self._get_or_start_catchup()
                try:
                    await asyncio.wait_for(
                        asyncio.shield(task), timeout=self.replicate_catchup_deadline
                    )
                except asyncio.TimeoutError:
                    catching_up_at_deadline = not task.done()
                except Exception:
                    # Lỗi đã được ghi nhận trong tác vụ catch-up; thử lại trạng thái một lần.
                    pass

        async with self.lock:
            reason = "CATCHING_UP" if catching_up_at_deadline else "GAP_NOT_FILLED"
            return False, reason, self.current_version

    async def commit(self, commit_version: int, commit_entry_id: str | None) -> None:
        async with self.lock:
            if self.diverged:
                self.log("COMMIT", f"Bỏ qua commit_version={commit_version} vì Backup đã phân kỳ")
                return
            result = self._apply_pending_up_to(commit_version, commit_entry_id)
            self.log("COMMIT", f"Đã xử lý commit_version={commit_version}, kết quả={result}")
        if result != "OK":
            self._get_or_start_catchup()

    def _get_or_start_catchup(self) -> asyncio.Task[bool]:
        task = self._catchup_task
        if task is None or task.done():
            task = asyncio.create_task(self._perform_catchup())
            self._catchup_task = task
        return task

    async def catch_up(self, deadline: float | None = None) -> bool:
        task = self._get_or_start_catchup()
        if deadline is None:
            return await asyncio.shield(task)
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout=deadline)
        except asyncio.TimeoutError:
            return False

    async def _get_primary_client(self) -> httpx.AsyncClient:
        if self._primary_client is None:
            self._primary_client = httpx.AsyncClient(
                transport=self._transport,
                timeout=httpx.Timeout(
                    self.catchup_http_timeout, connect=self.connect_timeout
                ),
            )
            self._owns_primary_client = True
        return self._primary_client

    async def _perform_catchup(self) -> bool:
        async with self.lock:
            if self.diverged:
                self.degraded = True
                self.is_catching_up = False
                return False
            was_catching_up = self.is_catching_up
            if not self.degraded:
                self.is_catching_up = True

        applied = 0
        primary_version: int | None = None
        contact_succeeded = False
        try:
            if self.catchup_delay_ms > 0:
                await asyncio.sleep(self.catchup_delay_ms / 1000)

            async with self.lock:
                from_version = self.current_version
            client = await self._get_primary_client()
            response = await client.get(
                f"{self.primary_url}/api/v1/catch-up",
                params={"from_version": from_version},
                timeout=httpx.Timeout(
                    self.catchup_http_timeout, connect=self.connect_timeout
                ),
            )
            response.raise_for_status()
            data = response.json()
            primary_version, entries = self._validate_catchup_response(data)
            contact_succeeded = True

            async with self.lock:
                self._primary_failures = 0
                self.last_catchup["time"] = time.time()
                self.last_catchup["primary_version"] = primary_version
                self.last_catchup["last_error"] = None
                if self.diverged:
                    self.degraded = True
                    self.is_catching_up = False
                    self.last_catchup["entries_applied"] = 0
                    return False
                if primary_version < self.current_version:
                    self._mark_diverged_locked(
                        f"Primary báo version={primary_version}, thấp hơn Backup "
                        f"version={self.current_version}; giữ nguyên dữ liệu cục bộ"
                    )
                    self.last_catchup["entries_applied"] = 0
                    return False

                entries.sort(key=lambda item: item["version"])
                batch: list[dict[str, Any]] = []
                expected = self.current_version + 1
                for entry in entries:
                    if entry["version"] <= self.current_version:
                        continue
                    if entry["version"] != expected:
                        self.log(
                            "CATCH-UP",
                            f"Dừng tại lỗ hổng version={expected}; nhận version={entry['version']}",
                        )
                        break
                    batch.append(entry)
                    expected += 1

                self._append_batch(batch)
                for entry in batch:
                    self._apply_to_storage(entry)
                    self.current_version = entry["version"]
                applied = len(batch)
                for version in [v for v in self.pending if v <= self.current_version]:
                    self.pending.pop(version, None)
                    self._pending_received_at.pop(version, None)

                caught_up = self.current_version >= primary_version
                if caught_up:
                    self.degraded = False
                    self.is_catching_up = False
                elif self.degraded:
                    self.is_catching_up = False
                else:
                    self.is_catching_up = True
                self.last_catchup["entries_applied"] = applied

            if applied:
                self.log(
                    "CATCH-UP",
                    f"missed={applied}, current_version={self.current_version}, "
                    f"primary_version={primary_version}",
                )
            return self.current_version >= primary_version
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if not contact_succeeded:
                await self._record_primary_failure(f"Catch-up thất bại: {error}")
            else:
                self.log("CATCH-UP", f"Không ghi được dữ liệu catch-up xuống WAL: {error}")
            async with self.lock:
                self.last_catchup["time"] = time.time()
                self.last_catchup["entries_applied"] = applied
                self.last_catchup["primary_version"] = primary_version
                self.last_catchup["last_error"] = str(error)
                if not was_catching_up and not self.degraded:
                    self.is_catching_up = False
            return False

    @staticmethod
    def _validate_catchup_response(data: Any) -> tuple[int, list[dict[str, Any]]]:
        if not isinstance(data, dict):
            raise ValueError("phản hồi catch-up không phải JSON object")
        primary_version = data.get("current_primary_version")
        entries = data.get("entries")
        if type(primary_version) is not int or primary_version < 0:
            raise ValueError("current_primary_version không hợp lệ")
        if not isinstance(entries, list):
            raise ValueError("entries không phải danh sách")
        parsed: list[dict[str, Any]] = []
        for raw_entry in entries:
            BackupNode._validate_wal_entry(raw_entry)
            if raw_entry["version"] > primary_version:
                raise ValueError("entry có version cao hơn Primary")
            parsed.append(dict(raw_entry))
        return primary_version, parsed

    async def _record_primary_success(self, primary_version: int) -> bool:
        async with self.lock:
            self._primary_failures = 0
            if primary_version < self.current_version:
                self._mark_diverged_locked(
                    f"Primary báo version={primary_version}, thấp hơn Backup "
                    f"version={self.current_version}; giữ nguyên dữ liệu cục bộ"
                )
                return False
            if self.diverged:
                self.degraded = True
                self.is_catching_up = False
                return False
            return True

    async def _record_primary_failure(self, message: str) -> None:
        async with self.lock:
            self._primary_failures += 1
            self.last_catchup["last_error"] = message
            self.log(
                "CATCH-UP",
                f"{message} (lần lỗi liên tiếp {self._primary_failures})",
            )
            if self._primary_failures >= self.primary_failures_before_degraded:
                self._mark_degraded_locked(
                    f"Primary không phản hồi sau {self._primary_failures} lần"
                )

    def _mark_degraded_locked(self, message: str) -> None:
        was_degraded = self.degraded
        self.degraded = True
        self.is_catching_up = False
        if not was_degraded:
            self.log("DEGRADED", message)

    def _mark_diverged_locked(self, message: str) -> None:
        if not self.diverged:
            self.log("DEGRADED", message)
        self.diverged = True
        self.degraded = True
        self.is_catching_up = False

    async def probe_primary(self) -> int | None:
        try:
            client = await self._get_primary_client()
            response = await client.get(
                f"{self.primary_url}/api/v1/health",
                timeout=httpx.Timeout(
                    self.catchup_http_timeout, connect=self.connect_timeout
                ),
            )
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict) or type(data.get("current_version")) is not int:
                raise ValueError("phản hồi health không hợp lệ")
            version = data["current_version"]
            if version < 0:
                raise ValueError("current_version âm")
            await self._record_primary_success(version)
            return version
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await self._record_primary_failure(f"Health probe thất bại: {error}")
            return None

    async def start(self) -> None:
        """Nạp WAL và khởi động vòng lặp đồng bộ nền."""
        self.load_wal()
        async with self.lock:
            self.is_catching_up = True
        if self._background_task is None or self._background_task.done():
            self._background_task = asyncio.create_task(self._background_loop())

    async def _background_loop(self) -> None:
        backoff = (0.5, 1.0, 2.0, 4.0, 5.0)
        backoff_index = 0
        while True:
            delay = self.sync_interval
            try:
                async with self.lock:
                    should_catch_up = self.is_catching_up or self.degraded
                    is_degraded = self.degraded

                if should_catch_up:
                    success = await self.catch_up()
                    async with self.lock:
                        is_degraded = self.degraded
                        still_catching_up = self.is_catching_up
                    if success:
                        backoff_index = 0
                        delay = self.sync_interval
                    elif still_catching_up and not is_degraded:
                        delay = backoff[min(backoff_index, len(backoff) - 1)]
                        backoff_index = min(backoff_index + 1, len(backoff) - 1)
                else:
                    primary_version = await self.probe_primary()
                    if primary_version is not None:
                        async with self.lock:
                            local_version = self.current_version
                            now_degraded = self.degraded
                        if primary_version > local_version or now_degraded:
                            await self.catch_up()
                await asyncio.sleep(max(delay, 0.01))
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.log("CATCH-UP", f"Lỗi vòng lặp nền, sẽ tiếp tục: {error}")
                await asyncio.sleep(max(self.sync_interval, 0.01))

    async def close(self) -> None:
        if self._background_task is not None:
            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass
            self._background_task = None
        if self._catchup_task is not None and not self._catchup_task.done():
            self._catchup_task.cancel()
            try:
                await self._catchup_task
            except asyncio.CancelledError:
                pass
        if self._owns_primary_client and self._primary_client is not None:
            await self._primary_client.aclose()
            self._primary_client = None
            self._owns_primary_client = False

    async def health_state(self) -> dict[str, Any]:
        async with self.lock:
            return {
                "status": self.status,
                "node_id": self.node_id,
                "role": "BACKUP",
                "current_version": self.current_version,
            }

    async def read(self, key: str) -> dict[str, Any]:
        async with self.lock:
            if self.is_catching_up:
                raise RuntimeError("NODE_CATCHING_UP")
            item = self.storage.get(key)
            if item is None:
                return {
                    "node_id": self.node_id,
                    "key": key,
                    "value": None,
                    "version": 0,
                    "node_version": self.current_version,
                    "found": False,
                    "deleted": False,
                }
            deleted = item["deleted"]
            return {
                "node_id": self.node_id,
                "key": key,
                "value": None if deleted else item["value"],
                "version": item["version"],
                "node_version": self.current_version,
                "found": not deleted,
                "deleted": deleted,
            }

    async def debug_state(self) -> dict[str, Any]:
        async with self.lock:
            return {
                "status": self.status,
                "current_version": self.current_version,
                "pending": sorted(self.pending),
                "key_count": len(self.storage),
                "wal_path": str(self.wal_file),
                "diverged": self.diverged,
                "last_catchup": dict(self.last_catchup),
            }
