import os
import sys
import asyncio
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from typing import Dict, Any, List, Optional


class MockBackupState:
    """Trạng thái giả lập đầy đủ chuẩn Backup Node (SV2) phục vụ kiểm thử SV1."""

    def __init__(self, node_id: str, primary_url: Optional[str] = None):
        self.node_id = node_id
        self.primary_url = primary_url
        self.current_version: int = 0
        self.pending: Dict[int, Dict[str, Any]] = {}
        self.storage: Dict[str, Dict[str, Any]] = {}
        self.committed_log: List[Dict[str, Any]] = []
        self.replicate_order: List[int] = []
        self.lock = asyncio.Lock()

        # Cờ giả lập sự cố cho kịch bản test
        self.delay_sec: float = float(os.getenv("MOCK_DELAY_SEC", "0.0"))
        self.fail_replicate: bool = os.getenv("MOCK_FAIL_REPLICATE", "0") == "1"
        self.drop_commit: bool = os.getenv("MOCK_DROP_COMMIT", "0") == "1"
        self.is_catching_up: bool = os.getenv("MOCK_CATCHING_UP", "0") == "1"
        self.fail_read: bool = os.getenv("MOCK_FAIL_READ", "0") == "1"
        self.read_delay_sec: float = float(os.getenv("MOCK_READ_DELAY_SEC", "0.0"))


def _apply_to_storage(state: MockBackupState, entry: Dict[str, Any]):
    key = entry["key"]
    op = entry["operation"]
    ver = entry["version"]
    if op == "PUT":
        state.storage[key] = {"value": entry["value"], "version": ver, "deleted": False}
    elif op == "DELETE":
        state.storage[key] = {"value": None, "version": ver, "deleted": True}
    state.committed_log.append(entry)


def _commit_pending_up_to(state: MockBackupState, target_commit_v: int):
    for ver in sorted(list(state.pending.keys())):
        if ver <= target_commit_v and ver == state.current_version + 1:
            entry = state.pending.pop(ver)
            _apply_to_storage(state, entry)
            state.current_version = ver


def create_mock_backup_app(
    node_id: str = "backup-1", primary_url: Optional[str] = None
) -> FastAPI:
    state = MockBackupState(node_id=node_id, primary_url=primary_url)
    app = FastAPI(title=f"Mock {node_id}")
    app.state.mock_state = state

    @app.get("/api/v1/health")
    async def health():
        return {
            "status": "CATCHING_UP" if state.is_catching_up else "ALIVE",
            "node_id": state.node_id,
            "role": "BACKUP",
            "current_version": state.current_version,
        }

    @app.post("/api/v1/replicate")
    async def replicate(entry: Dict[str, Any]):
        if state.delay_sec > 0:
            await asyncio.sleep(state.delay_sec)
        if state.fail_replicate:
            raise HTTPException(status_code=500, detail="Simulated backup failure")

        incoming_v = entry["version"]
        commit_v = entry.get("commit_version", 0)

        need_catch_up = False
        catch_up_from = 0

        async with state.lock:
            state.replicate_order.append(incoming_v)
            # Bước 1: Áp dụng pending <= commit_version trước khi kiểm tra gap
            _commit_pending_up_to(state, commit_v)

            if incoming_v <= state.current_version:
                print(
                    f"[{state.node_id}] [REPLICATE-DUP] v={incoming_v} <= local={state.current_version}"
                )
                return {
                    "status": "ACK",
                    "node_id": state.node_id,
                    "applied_version": state.current_version,
                }

            if incoming_v == state.current_version + 1:
                state.pending[incoming_v] = entry
                if commit_v >= incoming_v:
                    _commit_pending_up_to(state, incoming_v)
                print(
                    f"[{state.node_id}] [REPLICATE] v={incoming_v} "
                    f"(commit_v={commit_v}, local={state.current_version}) | "
                    f"{entry.get('operation')} {entry.get('key')}={entry.get('value')}"
                )
                return {
                    "status": "ACK",
                    "node_id": state.node_id,
                    "applied_version": incoming_v,
                }

            # Kiểm tra xem khoảng giữa đã đủ trong pending chưa
            missing = [
                v for v in range(state.current_version + 1, incoming_v) if v not in state.pending
            ]
            if not missing:
                state.pending[incoming_v] = entry
                if commit_v >= incoming_v:
                    _commit_pending_up_to(state, commit_v)
                return {
                    "status": "ACK",
                    "node_id": state.node_id,
                    "applied_version": incoming_v,
                }

            need_catch_up = True
            catch_up_from = state.current_version

        if need_catch_up and state.primary_url:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{state.primary_url}/api/v1/catch-up",
                    params={"from_version": catch_up_from},
                    timeout=3.0,
                )
                if resp.status_code == 200:
                    missed_entries = resp.json().get("entries", [])
                    async with state.lock:
                        for m in missed_entries:
                            mv = m["version"]
                            if mv == state.current_version + 1:
                                state.pending.pop(mv, None)
                                _apply_to_storage(state, m)
                                state.current_version = mv
                        if incoming_v == state.current_version + 1:
                            state.pending[incoming_v] = entry
                            if commit_v >= incoming_v:
                                _commit_pending_up_to(state, incoming_v)
                        elif incoming_v <= state.current_version:
                            pass
                        return {
                            "status": "ACK",
                            "node_id": state.node_id,
                            "applied_version": state.current_version,
                        }

        async with state.lock:
            state.pending[incoming_v] = entry
        return {
            "status": "ACK",
            "node_id": state.node_id,
            "applied_version": incoming_v,
        }

    @app.post("/api/v1/commit")
    async def commit(data: Dict[str, Any]):
        if state.drop_commit:
            raise HTTPException(status_code=500, detail="Simulated commit failure")
        commit_v = data.get("commit_version", 0)
        async with state.lock:
            _commit_pending_up_to(state, commit_v)
            local_v = state.current_version
        print(
            f"[{state.node_id}] [COMMIT] Đã áp dụng các bản ghi pending <= v={commit_v} (local={local_v})"
        )
        return {
            "status": "COMMITTED",
            "node_id": state.node_id,
            "commit_version": local_v,
        }

    @app.get("/api/v1/read/{key:path}")
    async def read_key(key: str):
        if key == "":
            raise HTTPException(status_code=400, detail="Key không được để trống")
        if state.read_delay_sec > 0:
            await asyncio.sleep(state.read_delay_sec)
        if state.is_catching_up:
            raise HTTPException(status_code=503, detail="NODE_CATCHING_UP")
        if state.fail_read:
            raise HTTPException(status_code=503, detail="Simulated read failure")
        async with state.lock:
            local_v = state.current_version
            item = state.storage.get(key)
        if item is not None:
            is_deleted = item.get("deleted", False)
            return {
                "node_id": state.node_id,
                "key": key,
                "value": None if is_deleted else item["value"],
                "version": item["version"],
                "node_version": local_v,
                "found": not is_deleted,
                "deleted": is_deleted,
            }
        return {
            "node_id": state.node_id,
            "key": key,
            "value": None,
            "version": 0,
            "node_version": local_v,
            "found": False,
            "deleted": False,
        }

    return app


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
    node_id = sys.argv[2] if len(sys.argv) > 2 else f"backup-{port}"
    primary_url = sys.argv[3] if len(sys.argv) > 3 else os.getenv("PRIMARY_URL")
    app = create_mock_backup_app(node_id=node_id, primary_url=primary_url)
    uvicorn.run(app, host="127.0.0.1", port=port)
