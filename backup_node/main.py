import os
import uvicorn
from fastapi import FastAPI

NODE_ID = os.getenv("NODE_ID", "backup-1")
PORT = int(os.getenv("PORT", 8001))
app = FastAPI(title=f"Backup Node {NODE_ID}")

@app.get("/api/v1/health")
async def health():
    return {"status": "ALIVE", "node_id": NODE_ID, "role": "BACKUP", "current_version": 0}

@app.post("/api/v1/replicate")
async def replicate(entry: dict):
    print(f"[{NODE_ID}] [REPLICATE] v={entry.get('version')} (commit_v={entry.get('commit_version')})")
    return {"status": "ACK", "node_id": NODE_ID, "applied_version": entry.get("version")}

@app.post("/api/v1/commit")
async def commit(data: dict):
    print(f"[{NODE_ID}] [COMMIT] commit_version={data.get('commit_version')}")
    return {"status": "COMMITTED", "node_id": NODE_ID, "commit_version": data.get("commit_version")}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)