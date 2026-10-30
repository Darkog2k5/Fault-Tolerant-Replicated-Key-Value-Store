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
    broadcast_commit_async,
    start_async_workers,
    stop_async_workers,
)

# Trạng thái trong bộ nhớ (In-Memory State)
current_version: int = 0
write_lock = asyncio.Lock()
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


def load_wal_from_disk():
    """
    Khôi phục trạng thái từ file WAL tự tạo khi Primary khởi động lại.
    Nếu dòng cuối bị ghi dở do docker kill, tự động bỏ dòng đó và cắt file về cuối dòng hợp lệ.
    """
    global current_version
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(WAL_FILE):
        return

    with open(WAL_FILE, "r+", encoding="utf-8") as f:
        lines = f.readlines()
        valid_lines: List[str] = []
        need_truncate = False

        for idx, raw_line in enumerate(lines):
            stripped = raw_line.strip()
            if not stripped:
                continue
            try:
                entry = json.loads(stripped)
                write_log.append(entry)
                apply_entry_to_memory(entry)
                if entry["version"] > current_version:
                    current_version = entry["version"]
                valid_lines.append(raw_line if raw_line.endswith("\n") else raw_line + "\n")
            except json.JSONDecodeError as e:
                if idx == len(lines) - 1:
                    print(f"[!] Phát hiện dòng cuối WAL bị ghi dở (do docker kill), tự động cắt bỏ: {stripped}")
                    need_truncate = True
                else:
                    raise RuntimeError(f"Lỗi hỏng dữ liệu WAL ở dòng {idx + 1} (giữa file): {e}")

        if need_truncate:
            f.seek(0)
            f.writelines(valid_lines)
            f.truncate()
            f.flush()
            os.fsync(f.fileno())

    print(f"[*] Primary đã khôi phục từ đĩa: current_version={current_version}, tổng số log={len(write_log)}")


def append_wal_to_disk(entry: Dict[str, Any]):
    """Ghi bền vững 1 bản ghi xuống đĩa (Append-only JSONL)."""
    with open(WAL_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_wal_from_disk()
    start_async_workers()
    yield
    await stop_async_workers()


app = FastAPI(title="Primary Coordinator Node", version="4.0.0", lifespan=lifespan)


class WriteRequest(BaseModel):
    operation: str                          # "PUT" hoặc "DELETE"
    key: str
    value: Optional[str] = None
    consistency: Optional[str] = "SYNC"     # "SYNC", "ASYNC", "QUORUM"
    quorum_w: Optional[int] = None          # Tuỳ chọn ghi đè W cho request này


@app.get("/api/v1/health")
async def health():
    return {
        "status": "ALIVE",
        "node_id": NODE_ID,
        "role": "PRIMARY",
        "current_version": current_version,
        "backups": BACKUP_URLS
    }


@app.post("/api/v1/write")
async def handle_write(req: WriteRequest):
    global current_version

    op = req.operation.upper()
    if op not in ["PUT", "DELETE"]:
        raise HTTPException(status_code=400, detail="Operation phải là PUT hoặc DELETE")
    if op == "PUT" and req.value is None:
        raise HTTPException(status_code=400, detail="Thao tác PUT bắt buộc phải có value")

    mode = (req.consistency or "SYNC").upper()
    if mode not in ["SYNC", "ASYNC", "QUORUM"]:
        raise HTTPException(status_code=400, detail="Consistency phải là SYNC, ASYNC hoặc QUORUM")

    w_required = req.quorum_w if req.quorum_w is not None else DEFAULT_QUORUM_W

    # Khoá tuần tự toàn bộ quá trình cấp phát version + lan truyền để chống đảo thứ tự gói tin
    async with write_lock:
        assigned_version = current_version + 1
        now = time.time()

        # Ở chế độ ASYNC: Primary commit ngay trước khi gửi nên commit_version = assigned_version.
        # Ở chế độ SYNC/QUORUM: Gửi kèm commit_version = current_version (bản đã commit liền trước).
        commit_ver_in_payload = assigned_version if mode == "ASYNC" else current_version

        log_entry = {
            "version": assigned_version,
            "commit_version": commit_ver_in_payload,
            "operation": op,
            "key": req.key,
            "value": req.value if op == "PUT" else None,
            "timestamp": now
        }

        if mode == "ASYNC":
            # Commit ngay trên Primary rồi mới đẩy vào hàng đợi FIFO gửi sang Backup
            current_version = assigned_version
            write_log.append(log_entry)
            append_wal_to_disk(log_entry)
            apply_entry_to_memory(log_entry)

            repl_result = await replicate_entry(
                backup_urls=BACKUP_URLS,
                payload=log_entry,
                mode=mode,
                quorum_w=w_required
            )
        else:
            # Chế độ SYNC và QUORUM: Gửi sang Backup (lưu vào pending) và đợi đủ ACK
            repl_result = await replicate_entry(
                backup_urls=BACKUP_URLS,
                payload=log_entry,
                mode=mode,
                quorum_w=w_required
            )

            # Nếu không đủ ACK -> Không ghi vào write_log/WAL, trả lỗi 503 ngay
            if not repl_result["success"]:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "status": "WRITE_FAILED",
                        "mode": mode,
                        "required_acks": repl_result["required_acks"],
                        "actual_acks": repl_result["acks"],
                        "replicated_nodes": repl_result["replicated_nodes"],
                        "message": "Không đủ số node phản hồi ACK, thao tác đã được huỷ trên Primary."
                    }
                )

            # Đã đủ ACK -> Chính thức Commit trên Primary
            current_version = assigned_version
            write_log.append(log_entry)
            append_wal_to_disk(log_entry)
            apply_entry_to_memory(log_entry)

            # Gửi ngầm thông báo POST /api/v1/commit tới các Backup (không cần chờ)
            asyncio.create_task(broadcast_commit_async(BACKUP_URLS, current_version))

    return {
        "status": "SUCCESS",
        "version": assigned_version,
        "key": req.key,
        "operation": op,
        "value": log_entry["value"],
        "consistency": mode,
        "timestamp": now,
        "acks": repl_result["acks"],
        "replicated_nodes": repl_result["replicated_nodes"]
    }


@app.get("/api/v1/read/{key}")
async def handle_read(key: str):
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
            "deleted": is_deleted
        }
    return {
        "node_id": NODE_ID,
        "key": key,
        "value": None,
        "version": 0,
        "node_version": current_version,
        "found": False,
        "deleted": False
    }


@app.get("/api/v1/catch-up")
async def handle_catch_up(from_version: int = Query(..., ge=0)):
    """Cung cấp danh sách các bản ghi ĐÃ COMMIT (version > from_version) cho Backup."""
    missed_entries = [entry for entry in write_log if entry["version"] > from_version]
    return {
        "current_primary_version": current_version,
        "missed_count": len(missed_entries),
        "entries": missed_entries
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)