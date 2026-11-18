import os
import json
import time
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

from config import NODE_ID, PORT, BACKUP_URLS, DEFAULT_QUORUM_W, DATA_DIR, WAL_FILE
from replication import (
    replicate_entry,
    schedule_broadcast_commit,
    finalize_late_replications,
    cancel_pending_replications,
    start_async_workers,
    stop_async_workers,
)

# Trạng thái trong bộ nhớ (In-Memory State)
current_version: int = 0
write_lock = asyncio.Lock()
# Theo dõi version bị huỷ (503) nhưng có ít nhất 1 Backup đã kịp lưu vào pending
aborted_pending_version: Optional[int] = None
# storage lưu: { key: {"value": val, "version": ver, "deleted": bool} }
storage: Dict[str, Dict[str, Any]] = {}
# write_log chỉ lưu danh sách các log_entry ĐÃ COMMIT theo thứ tự version tăng dần
write_log: List[Dict[str, Any]] = []


def apply_entry_to_memory(entry: Dict[str, Any]):
    """Áp dụng một bản ghi log vào bộ nhớ RAM (Hỗ trợ Tombstone khi DELETE)."""
    key = entry["key"]
    op = entry["operation"]
    ver = entry["version"]
    val = entry["value"]

    if op == "PUT":
        storage[key] = {"value": val, "version": ver, "deleted": False}
    elif op == "DELETE":
        storage[key] = {"value": None, "version": ver, "deleted": True}


def _parse_and_validate_wal_line(raw_line: bytes, expected_min_version: int) -> Dict[str, Any]:
    """Giải mã UTF-8, parse JSON và kiểm tra schema + thứ tự version của 1 dòng WAL."""
    text = raw_line.decode("utf-8").strip()
    entry = json.loads(text)
    if not isinstance(entry, dict):
        raise ValueError("Dòng WAL không phải JSON object")

    ver = entry.get("version")
    op = entry.get("operation")
    key = entry.get("key")

    if not isinstance(ver, int) or isinstance(ver, bool) or ver <= expected_min_version:
        raise ValueError(f"Version WAL không hợp lệ hoặc không tăng đơn điệu: {ver}")
    if op not in ("PUT", "DELETE"):
        raise ValueError(f"Operation WAL không hợp lệ: {op}")
    if not isinstance(key, str) or key == "":
        raise ValueError("Key WAL không hợp lệ")
    if op == "PUT" and ("value" not in entry or entry["value"] is None):
        raise ValueError("Bản ghi PUT trong WAL thiếu trường value")
    if op == "DELETE" and "value" not in entry:
        entry["value"] = None

    return entry


def load_wal_from_disk():
    """
    Khôi phục trạng thái từ file WAL tự tạo khi Primary khởi động lại.
    Đọc nhị phân để xử lý an toàn trường hợp docker kill cắt ngang ký tự UTF-8 đa byte
    hoặc JSON dở dang ở dòng cuối file, tự động cắt (truncate) về cuối dòng hợp lệ.
    """
    global current_version, aborted_pending_version

    current_version = 0
    aborted_pending_version = None
    storage.clear()
    write_log.clear()

    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(WAL_FILE):
        return

    with open(WAL_FILE, "rb+") as f:
        raw_bytes = f.read()
        if not raw_bytes:
            return

        raw_lines = raw_bytes.split(b"\n")
        # Tìm chỉ số dòng không rỗng cuối cùng (tránh coi trailing newline là dòng hỏng)
        last_non_empty_idx = -1
        for idx in range(len(raw_lines) - 1, -1, -1):
            if raw_lines[idx].strip():
                last_non_empty_idx = idx
                break

        if last_non_empty_idx == -1:
            f.seek(0)
            f.truncate()
            f.flush()
            os.fsync(f.fileno())
            return

        valid_lines: List[bytes] = []
        need_truncate = False

        for idx in range(last_non_empty_idx + 1):
            raw_line = raw_lines[idx]
            if not raw_line.strip():
                continue
            try:
                entry = _parse_and_validate_wal_line(raw_line, current_version)
                write_log.append(entry)
                apply_entry_to_memory(entry)
                current_version = entry["version"]
                valid_lines.append(raw_line.rstrip(b"\r") + b"\n")
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as e:
                if idx == last_non_empty_idx:
                    print(
                        f"[!] Phát hiện dòng cuối WAL bị ghi dở (do docker kill), tự động cắt bỏ: {e}"
                    )
                    need_truncate = True
                else:
                    raise RuntimeError(
                        f"Lỗi hỏng dữ liệu WAL ở dòng {idx + 1} (giữa file): {e}"
                    )

        if need_truncate:
            f.seek(0)
            f.writelines(valid_lines)
            f.truncate()
            f.flush()
            os.fsync(f.fileno())

    print(
        f"[*] Primary đã khôi phục từ đĩa: current_version={current_version}, tổng số log={len(write_log)}"
    )


def append_wal_to_disk(entry: Dict[str, Any]):
    """Ghi bền vững 1 bản ghi xuống đĩa (Append-only JSONL + fsync)."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(WAL_FILE, "a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_wal_from_disk()
    start_async_workers()
    yield
    await stop_async_workers()


app = FastAPI(title="Primary Coordinator Node", version="4.1.0", lifespan=lifespan)


class WriteRequest(BaseModel):
    operation: str                          # "PUT" hoặc "DELETE"
    key: str
    value: Optional[Any] = None             # Hỗ trợ str, số, bool, dict, list
    consistency: Optional[str] = "SYNC"     # "SYNC", "ASYNC", "QUORUM"
    quorum_w: Optional[int] = None          # Tuỳ chọn ghi đè W cho request này


@app.get("/api/v1/health")
async def health():
    return {
        "status": "ALIVE",
        "node_id": NODE_ID,
        "role": "PRIMARY",
        "current_version": current_version,
        "backups": BACKUP_URLS,
    }


@app.post("/api/v1/write")
async def handle_write(req: WriteRequest):
    global current_version, aborted_pending_version

    op = req.operation.upper()
    if op not in ("PUT", "DELETE"):
        raise HTTPException(status_code=400, detail="Operation phải là PUT hoặc DELETE")
    if req.key == "":
        raise HTTPException(status_code=400, detail="Key không được để trống")
    if op == "PUT" and req.value is None:
        raise HTTPException(status_code=400, detail="Thao tác PUT bắt buộc phải có value")

    mode = (req.consistency or "SYNC").upper()
    if mode not in ("SYNC", "ASYNC", "QUORUM"):
        raise HTTPException(
            status_code=400, detail="Consistency phải là SYNC, ASYNC hoặc QUORUM"
        )

    total_nodes = 1 + len(BACKUP_URLS)
    w_required = req.quorum_w if req.quorum_w is not None else DEFAULT_QUORUM_W
    if mode == "QUORUM" and (w_required < 1 or w_required > total_nodes):
        raise HTTPException(
            status_code=400,
            detail=f"quorum_w={w_required} không hợp lệ, phải nằm trong khoảng [1, {total_nodes}]",
        )

    # Khoá tuần tự toàn bộ quá trình cấp phát version + lan truyền để chống đảo thứ tự gói tin
    async with write_lock:
        assigned_version = current_version + 1
        now = time.time()
        is_poisoned = aborted_pending_version == assigned_version

        # Ở chế độ ASYNC thông thường: Primary commit ngay nên commit_version = assigned_version.
        # Nếu version này trước đó có bản ghi pending rác do 503 để lại trên Backup,
        # hạ commit_version về current_version để Backup ghi đè pending[v] rồi mới commit(v).
        if mode == "ASYNC":
            commit_ver_in_payload = (
                current_version if is_poisoned else assigned_version
            )
        else:
            commit_ver_in_payload = current_version

        log_entry = {
            "version": assigned_version,
            "commit_version": commit_ver_in_payload,
            "operation": op,
            "key": req.key,
            "value": req.value if op == "PUT" else None,
            "timestamp": now,
        }

        loop = asyncio.get_running_loop()

        if mode == "ASYNC":
            # Ghi WAL bền vững trên đĩa TRƯỚC khi cập nhật trạng thái trong RAM
            await loop.run_in_executor(None, append_wal_to_disk, log_entry)

            # Cập nhật nguyên tử trong bộ nhớ (không có await xen giữa)
            write_log.append(log_entry)
            apply_entry_to_memory(log_entry)
            current_version = assigned_version
            aborted_pending_version = None

            repl_result = await replicate_entry(
                backup_urls=BACKUP_URLS,
                payload=log_entry,
                mode=mode,
                quorum_w=w_required,
                poisoned_pending=is_poisoned,
            )
        else:
            # Chế độ SYNC và QUORUM: Gửi sang Backup (lưu vào pending) và đợi đủ ACK
            repl_result = await replicate_entry(
                backup_urls=BACKUP_URLS,
                payload=log_entry,
                mode=mode,
                quorum_w=w_required,
            )

            # Nếu không đủ ACK -> Không ghi vào write_log/WAL, trả lỗi 503 ngay
            if not repl_result["success"]:
                if repl_result["acks"] > 1:
                    aborted_pending_version = assigned_version
                raise HTTPException(
                    status_code=503,
                    detail={
                        "status": "WRITE_FAILED",
                        "mode": mode,
                        "required_acks": repl_result["required_acks"],
                        "actual_acks": repl_result["acks"],
                        "replicated_nodes": repl_result["replicated_nodes"],
                        "message": "Không đủ số node phản hồi ACK, thao tác đã được huỷ trên Primary.",
                    },
                )

            # Đã đủ ACK -> Ghi WAL xuống đĩa TRƯỚC khi commit trong RAM
            try:
                await loop.run_in_executor(None, append_wal_to_disk, log_entry)
            except Exception:
                await cancel_pending_replications(repl_result)
                raise

            # Cập nhật nguyên tử trong bộ nhớ (không có await xen giữa)
            write_log.append(log_entry)
            apply_entry_to_memory(log_entry)
            current_version = assigned_version
            aborted_pending_version = None

            # Gửi ngầm /commit cho các Backup đã ACK và xử lý tiếp các Backup về muộn (QUORUM)
            schedule_broadcast_commit(
                repl_result.get("replicated_urls", []), current_version
            )
            finalize_late_replications(repl_result, current_version)

    return {
        "status": "SUCCESS",
        "version": assigned_version,
        "key": req.key,
        "operation": op,
        "value": log_entry["value"],
        "consistency": mode,
        "timestamp": now,
        "acks": repl_result["acks"],
        "replicated_nodes": repl_result["replicated_nodes"],
    }


@app.get("/api/v1/read/{key:path}")
async def handle_read(key: str):
    if key == "":
        raise HTTPException(status_code=400, detail="Key không được để trống")

    if key in storage:
        item = storage[key]
        is_deleted = item.get("deleted", False)
        return {
            "node_id": NODE_ID,
            "key": key,
            "value": None if is_deleted else item["value"],
            "version": item["version"],
            "node_version": current_version,
            "found": not is_deleted,
            "deleted": is_deleted,
        }
    return {
        "node_id": NODE_ID,
        "key": key,
        "value": None,
        "version": 0,
        "node_version": current_version,
        "found": False,
        "deleted": False,
    }


@app.get("/api/v1/catch-up")
async def handle_catch_up(from_version: int = Query(..., ge=0)):
    """Cung cấp danh sách các bản ghi ĐÃ COMMIT (version > from_version) cho Backup."""
    missed_entries = [entry for entry in write_log if entry["version"] > from_version]
    return {
        "current_primary_version": current_version,
        "missed_count": len(missed_entries),
        "entries": missed_entries,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
