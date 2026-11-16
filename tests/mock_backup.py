"""Mock Backup tối giản để SV3 thử failover; không có WAL/pending/catch-up."""

import sys

import uvicorn
from fastapi import FastAPI


port = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
node_id = sys.argv[2] if len(sys.argv) > 2 else f"backup-{port}"

app = FastAPI(title=f"Mock {node_id}")
storage = {}
current_version = 0


def read_record(key):
    """Định dạng phản hồi đọc theo giao thức, gồm cả khóa chưa từng có."""
    item = storage.get(key)
    if item is None:
        return {
            "node_id": node_id,
            "key": key,
            "value": None,
            "version": 0,
            "node_version": current_version,
            "found": False,
            "deleted": False,
        }
    deleted = item["deleted"]
    return {
        "node_id": node_id,
        "key": key,
        "value": None if deleted else item["value"],
        "version": item["version"],
        "node_version": current_version,
        "found": not deleted,
        "deleted": deleted,
    }


@app.post("/api/v1/replicate")
async def replicate(entry: dict):
    global current_version
    version = entry.get("version", 0)
    operation = entry.get("operation")
    key = entry.get("key")
    if key is not None and operation in ("PUT", "DELETE"):
        deleted = operation == "DELETE"
        storage[key] = {
            "value": None if deleted else entry.get("value"),
            "version": version,
            "deleted": deleted,
        }
    if isinstance(version, int):
        current_version = max(current_version, version)
    print(
        f"[{node_id}] [REPLICATE] v={version} "
        f"(commit_v={entry.get('commit_version')}) | "
        f"{operation} {key}={entry.get('value')}"
    )
    return {
        "status": "ACK",
        "node_id": node_id,
        "applied_version": version,
    }


@app.post("/api/v1/commit")
async def commit(data: dict):
    print(
        f"[{node_id}] [COMMIT] Đã áp dụng các bản ghi pending "
        f"<= v={data.get('commit_version')}"
    )
    return {
        "status": "COMMITTED",
        "node_id": node_id,
        "commit_version": data.get("commit_version"),
    }


@app.get("/api/v1/health")
async def health():
    return {
        "status": "ALIVE",
        "node_id": node_id,
        "role": "BACKUP",
        "current_version": current_version,
    }


@app.get("/api/v1/read/{key}")
async def read(key: str):
    return read_record(key)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=port)
