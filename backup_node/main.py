"""HTTP API cho Backup Replica Node."""

from contextlib import asynccontextmanager
from typing import Any, Literal

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator

from config import (
    CATCHUP_DELAY_MS,
    CATCHUP_HTTP_TIMEOUT,
    DATA_DIR,
    NODE_ID,
    PORT,
    PRIMARY_FAILURES_BEFORE_DEGRADED,
    PRIMARY_URL,
    PENDING_TTL,
    REPLICATE_CATCHUP_DEADLINE,
    SYNC_INTERVAL,
)
from node import BackupNode


class RequestModel(BaseModel):
    model_config = ConfigDict(extra="ignore")


class ReplicateRequest(RequestModel):
    version: int = Field(ge=1)
    commit_version: int = Field(ge=0)
    operation: Literal["PUT", "DELETE"]
    key: str
    # Primary nhận mọi giá trị JSON (chuỗi, số, bool, object, mảng) nên Backup cũng vậy.
    value: Any = None
    timestamp: float | None = None
    entry_id: str | None = None
    commit_entry_id: str | None = None

    @model_validator(mode="after")
    def require_value_for_put(self):
        if self.operation == "PUT" and self.value is None:
            raise ValueError("PUT yêu cầu value")
        return self


class CommitRequest(RequestModel):
    commit_version: int = Field(ge=0)
    commit_entry_id: str | None = None


def create_app(node: BackupNode | None = None) -> FastAPI:
    backup = node or BackupNode(
        node_id=NODE_ID,
        primary_url=PRIMARY_URL,
        data_dir=DATA_DIR,
        catchup_http_timeout=CATCHUP_HTTP_TIMEOUT,
        replicate_catchup_deadline=REPLICATE_CATCHUP_DEADLINE,
        primary_failures_before_degraded=PRIMARY_FAILURES_BEFORE_DEGRADED,
        sync_interval=SYNC_INTERVAL,
        catchup_delay_ms=CATCHUP_DELAY_MS,
        pending_ttl=PENDING_TTL,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await backup.start()
        try:
            yield
        finally:
            await backup.close()

    app = FastAPI(title=f"Backup Node {backup.node_id}", lifespan=lifespan)
    app.state.backup_node = backup

    @app.get("/api/v1/health")
    async def health():
        return await backup.health_state()

    # {key:path} để khóa phân cấp như "users/101/profile" đọc được giống Primary.
    @app.get("/api/v1/read/{key:path}")
    async def read(key: str):
        # Giống Primary và Proxy: khóa rỗng là yêu cầu sai, không phải "không tìm thấy".
        if key == "":
            raise HTTPException(status_code=400, detail="Key không được để trống")
        try:
            return await backup.read(key)
        except RuntimeError as error:
            if str(error) == "NODE_CATCHING_UP":
                raise HTTPException(status_code=503, detail="NODE_CATCHING_UP") from error
            raise

    @app.post("/api/v1/replicate")
    async def replicate(request: ReplicateRequest):
        acknowledged, reason, current_version = await backup.replicate(
            request.model_dump(exclude_unset=True)
        )
        if acknowledged:
            return {
                "status": "ACK",
                "node_id": backup.node_id,
                "applied_version": request.version,
            }
        return JSONResponse(
            status_code=503,
            content={
                "status": "NACK",
                "node_id": backup.node_id,
                "reason": reason,
                "current_version": current_version,
            },
        )

    @app.post("/api/v1/commit")
    async def commit(request: CommitRequest):
        await backup.commit(request.commit_version, request.commit_entry_id)
        return {
            "status": "COMMITTED",
            "node_id": backup.node_id,
            "commit_version": request.commit_version,
        }

    @app.get("/api/v1/debug/state")
    async def debug_state():
        return await backup.debug_state()

    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
