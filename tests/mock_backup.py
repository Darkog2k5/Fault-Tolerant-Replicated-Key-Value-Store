import sys
import uvicorn
from fastapi import FastAPI

port = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
node_id = sys.argv[2] if len(sys.argv) > 2 else f"backup-{port}"

app = FastAPI(title=f"Mock {node_id}")

@app.post("/api/v1/replicate")
async def replicate(entry: dict):
    print(
        f"[{node_id}] [REPLICATE] v={entry.get('version')} "
        f"(commit_v={entry.get('commit_version')}) | "
        f"{entry.get('operation')} {entry.get('key')}={entry.get('value')}"
    )
    return {
        "status": "ACK",
        "node_id": node_id,
        "applied_version": entry.get("version")
    }

@app.post("/api/v1/commit")
async def commit(data: dict):
    print(f"[{node_id}] [COMMIT] Đã áp dụng các bản ghi pending <= v={data.get('commit_version')}")
    return {"status": "COMMITTED", "node_id": node_id, "commit_version": data.get("commit_version")}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=port)