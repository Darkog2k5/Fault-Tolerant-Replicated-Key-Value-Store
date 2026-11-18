import os
import asyncio
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any
from urllib.parse import quote

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Header, Query, Response
from pydantic import BaseModel

import config as cfg

# =====================================================================
# BIẾN TOÀN CỤC ĐỒNG BỘ VỚI PHIÊN MẶC ĐỊNH ("default")
# (Hỗ trợ cả truy cập trực tiếp qua module lẫn qua API /app.state)
# =====================================================================
last_written_version: int = 0
last_read_version: int = 0
local_cache: Dict[str, Dict[str, Any]] = {}


class SessionState:
    """
    Quản lý trạng thái nhất quán hướng Client (Client-Centric Consistency) cho 1 phiên:
    - last_written_version: Phiên bản lớn nhất mà Client đã ghi thành công.
    - last_read_version: Phiên bản node_version lớn nhất mà Client từng đọc.
    - local_cache: Bộ nhớ đệm cục bộ tại Proxy (hỗ trợ cả Tombstone khi DELETE).
    """

    def __init__(
        self,
        session_id: str = "default",
        backing_cache: Optional[Dict[str, Dict[str, Any]]] = None,
    ):
        self.session_id: str = session_id
        self.last_written_version: int = 0
        self.last_read_version: int = 0
        self.local_cache: Dict[str, Dict[str, Any]] = (
            backing_cache if backing_cache is not None else {}
        )

    @property
    def required_version(self) -> int:
        """Phiên bản tối thiểu cần đạt để thoả mãn Read-Your-Writes & Monotonic Reads."""
        return max(self.last_written_version, self.last_read_version)

    def update_on_write(
        self, key: str, value: Any, version: int, operation: str
    ) -> None:
        """Cập nhật trạng thái phiên và local_cache khi ghi thành công (200 OK)."""
        op = operation.upper()
        is_deleted = op == "DELETE"
        if version > self.last_written_version:
            self.last_written_version = version

        existing = self.local_cache.get(key)
        if existing is None or version >= existing.get("version", 0):
            self.local_cache[key] = {
                "value": None if is_deleted else value,
                "version": version,
                "node_version": max(self.required_version, version),
                "found": not is_deleted,
                "deleted": is_deleted,
            }

    def update_on_read(self, key: str, resp_data: Dict[str, Any]) -> None:
        """Cập nhật last_read_version và đồng bộ local_cache khi đọc hợp lệ."""
        node_v = int(resp_data.get("node_version", 0))
        key_v = int(resp_data.get("version", 0))

        if node_v > self.last_read_version:
            self.last_read_version = node_v

        # Chỉ lưu vào local_cache các key đã từng tồn tại hoặc đã bị DELETE (Tombstone)
        if key_v > 0:
            is_deleted = bool(resp_data.get("deleted", False))
            existing = self.local_cache.get(key)
            if existing is None or key_v >= existing.get("version", 0):
                self.local_cache[key] = {
                    "value": None if is_deleted else resp_data.get("value"),
                    "version": key_v,
                    "node_version": max(node_v, key_v),
                    "found": bool(resp_data.get("found", not is_deleted)),
                    "deleted": is_deleted,
                }

    def reset(self) -> None:
        """Đặt lại trạng thái phiên về ban đầu."""
        self.last_written_version = 0
        self.last_read_version = 0
        self.local_cache.clear()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "last_written_version": self.last_written_version,
            "last_read_version": self.last_read_version,
            "required_version": self.required_version,
            "cache_size": len(self.local_cache),
            "local_cache": dict(self.local_cache),
        }


class ProxyState:
    """Quản lý cấu hình, kết nối HTTP, danh sách phiên và thống kê của Client Proxy."""

    def __init__(
        self,
        node_id: str = cfg.NODE_ID,
        primary_url: str = cfg.PRIMARY_URL,
        backup_urls: Optional[List[str]] = None,
        read_timeout: float = cfg.READ_TIMEOUT,
        write_timeout: float = cfg.WRITE_TIMEOUT,
        use_global_default_session: bool = False,
    ):
        self.node_id: str = node_id
        self.primary_url: str = cfg.normalize_url(primary_url)
        self.backup_urls: List[str] = (
            list(cfg.BACKUP_URLS)
            if backup_urls is None
            else [cfg.normalize_url(u) for u in backup_urls if cfg.normalize_url(u)]
        )
        self.read_timeout: float = read_timeout
        self.write_timeout: float = write_timeout
        self.use_global_default_session: bool = use_global_default_session

        self.sessions: Dict[str, SessionState] = {}
        default_backing = local_cache if use_global_default_session else None
        self.sessions["default"] = SessionState(
            session_id="default", backing_cache=default_backing
        )

        self._rr_index: int = 0
        self.lock = asyncio.Lock()
        self.client: Optional[httpx.AsyncClient] = None
        self.stats: Dict[str, int] = self._init_stats()

    @staticmethod
    def _init_stats() -> Dict[str, int]:
        return {
            "total_writes": 0,
            "successful_writes": 0,
            "failed_writes": 0,
            "total_reads": 0,
            "backup_hits": 0,
            "primary_hits": 0,
            "cache_hits": 0,
            "stale_rejections": 0,
            "backup_failovers": 0,
            "primary_fallbacks": 0,
            "failed_reads": 0,
        }

    def sync_from_globals_if_default(self, session: SessionState) -> None:
        """Đồng bộ chiều từ biến toàn cục vào phiên mặc định (nếu test sửa trực tiếp biến module)."""
        if self.use_global_default_session and session.session_id == "default":
            session.last_written_version = last_written_version
            session.last_read_version = last_read_version

    def sync_to_globals_if_default(self, session: SessionState) -> None:
        """Đồng bộ chiều từ phiên mặc định ra biến toàn cục của module."""
        global last_written_version, last_read_version
        if self.use_global_default_session and session.session_id == "default":
            last_written_version = session.last_written_version
            last_read_version = session.last_read_version

    def get_session(self, session_id: Optional[str] = None) -> SessionState:
        sid = (session_id or "default").strip() or "default"
        session = self.sessions.get(sid)
        if session is None:
            session = SessionState(session_id=sid)
            self.sessions[sid] = session
        self.sync_from_globals_if_default(session)
        return session

    def next_ordered_backups(self) -> List[str]:
        """Trả về danh sách Backup URL theo thứ tự Round-Robin cho lần đọc hiện tại."""
        n = len(self.backup_urls)
        if n == 0:
            return []
        start = self._rr_index % n
        self._rr_index = (self._rr_index + 1) % n
        return [self.backup_urls[(start + i) % n] for i in range(n)]

    def reset_all(self) -> None:
        global last_written_version, last_read_version
        for s in self.sessions.values():
            s.reset()
        default_s = self.sessions.get("default")
        self.sessions.clear()
        if default_s is not None:
            self.sessions["default"] = default_s
        else:
            self.sessions["default"] = SessionState(
                session_id="default",
                backing_cache=local_cache if self.use_global_default_session else None,
            )
        if self.use_global_default_session:
            last_written_version = 0
            last_read_version = 0
            local_cache.clear()
        self._rr_index = 0
        self.stats = self._init_stats()


class WriteRequest(BaseModel):
    operation: str                          # "PUT" hoặc "DELETE"
    key: str
    value: Optional[Any] = None             # Hỗ trợ str, số, bool, dict, list
    consistency: Optional[str] = "SYNC"     # "SYNC", "ASYNC", "QUORUM"
    quorum_w: Optional[int] = None          # Tuỳ chọn ghi đè W cho QUORUM


def _extract_error_detail(resp: httpx.Response) -> Any:
    """Trích xuất trường detail từ JSON lỗi của Primary/Backup nếu có."""
    try:
        body = resp.json()
        if isinstance(body, dict) and "detail" in body:
            return body["detail"]
        return body
    except Exception:
        return resp.text or f"HTTP {resp.status_code}"


def create_proxy_app(
    node_id: Optional[str] = None,
    primary_url: Optional[str] = None,
    backup_urls: Optional[List[str]] = None,
    read_timeout: Optional[float] = None,
    write_timeout: Optional[float] = None,
    use_global_default_session: bool = False,
) -> FastAPI:
    """Factory khởi tạo ứng dụng FastAPI cho Smart Client Proxy (SV3)."""
    eff_node_id = node_id if node_id is not None else os.getenv("NODE_ID", cfg.NODE_ID)
    eff_port = int(os.getenv("PORT", str(cfg.PORT)))
    eff_primary = (
        primary_url
        if primary_url is not None
        else os.getenv("PRIMARY_URL", cfg.PRIMARY_URL)
    )
    if backup_urls is None:
        raw_backups = os.getenv("BACKUP_URLS", cfg.BACKUP_URLS_STR)
        eff_backups = cfg.parse_backup_urls(raw_backups, eff_node_id, eff_port)
    else:
        eff_backups = backup_urls

    eff_read_timeout = (
        read_timeout
        if read_timeout is not None
        else float(os.getenv("READ_TIMEOUT", str(cfg.READ_TIMEOUT)))
    )
    eff_write_timeout = (
        write_timeout
        if write_timeout is not None
        else float(os.getenv("WRITE_TIMEOUT", str(cfg.WRITE_TIMEOUT)))
    )

    state = ProxyState(
        node_id=eff_node_id,
        primary_url=eff_primary,
        backup_urls=eff_backups,
        read_timeout=eff_read_timeout,
        write_timeout=eff_write_timeout,
        use_global_default_session=use_global_default_session,
    )

    @asynccontextmanager
    async def lifespan(app_instance: FastAPI):
        limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
        state.client = httpx.AsyncClient(limits=limits)
        try:
            yield
        finally:
            if state.client is not None and not state.client.is_closed:
                await state.client.aclose()
            state.client = None

    proxy_app = FastAPI(
        title="Smart Client Proxy",
        version="4.1.0",
        lifespan=lifespan,
    )
    proxy_app.state.proxy_state = state

    async def _get_http_client() -> httpx.AsyncClient:
        if state.client is not None and not state.client.is_closed:
            return state.client
        limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
        state.client = httpx.AsyncClient(limits=limits)
        return state.client

    @proxy_app.get("/api/v1/health")
    async def health(
        x_session_id: Optional[str] = Header(default=None, alias="X-Session-ID"),
        session_id: Optional[str] = Query(default=None),
    ):
        eff_sid = session_id or x_session_id or "default"
        session = state.get_session(eff_sid)
        return {
            "status": "ALIVE",
            "node_id": state.node_id,
            "role": "PROXY",
            "current_version": session.required_version,
            "last_written_version": session.last_written_version,
            "last_read_version": session.last_read_version,
            "primary_url": state.primary_url,
            "backups": state.backup_urls,
        }

    @proxy_app.get("/api/v1/session")
    async def get_session_info(
        x_session_id: Optional[str] = Header(default=None, alias="X-Session-ID"),
        session_id: Optional[str] = Query(default=None),
    ):
        """API kiểm tra trạng thái phiên và bộ nhớ đệm cục bộ (phục vụ kiểm thử & debug)."""
        eff_sid = session_id or x_session_id or "default"
        session = state.get_session(eff_sid)
        data = session.to_dict()
        data["stats"] = dict(state.stats)
        return data

    @proxy_app.delete("/api/v1/session")
    async def reset_session_info(
        all_sessions: bool = Query(default=False),
        x_session_id: Optional[str] = Header(default=None, alias="X-Session-ID"),
        session_id: Optional[str] = Query(default=None),
    ):
        """API đặt lại trạng thái phiên và thống kê (phục vụ kiểm thử tự động)."""
        async with state.lock:
            if all_sessions:
                state.reset_all()
                return {
                    "status": "RESET_ALL",
                    "session": state.get_session("default").to_dict(),
                }
            eff_sid = session_id or x_session_id or "default"
            session = state.get_session(eff_sid)
            session.reset()
            state.sync_to_globals_if_default(session)
            return {
                "status": "RESET",
                "session": session.to_dict(),
            }

    @proxy_app.get("/api/v1/stats")
    async def get_stats():
        """Trả về số liệu thống kê hoạt động định tuyến, nhất quán và failover của Proxy."""
        total_r = state.stats["total_reads"]
        stale_r = state.stats["stale_rejections"]
        return {
            "node_id": state.node_id,
            "stats": dict(state.stats),
            "stale_rejection_rate": (stale_r / total_r) if total_r > 0 else 0.0,
        }

    @proxy_app.post("/api/v1/write")
    async def handle_write(
        req: WriteRequest,
        response: Response,
        x_session_id: Optional[str] = Header(default=None, alias="X-Session-ID"),
        session_id: Optional[str] = Query(default=None),
    ):
        """
        Chuyển tiếp yêu cầu Ghi (PUT / DELETE) lên Primary Node.
        Nếu nhận 200 OK -> Cập nhật last_written_version và local_cache của phiên.
        """
        op = (req.operation or "").upper()
        if op not in ("PUT", "DELETE"):
            raise HTTPException(
                status_code=400, detail="Operation phải là PUT hoặc DELETE"
            )
        if req.key == "":
            raise HTTPException(status_code=400, detail="Key không được để trống")
        if op == "PUT" and req.value is None:
            raise HTTPException(
                status_code=400, detail="Thao tác PUT bắt buộc phải có value"
            )

        mode = (req.consistency or "SYNC").upper()
        if mode not in ("SYNC", "ASYNC", "QUORUM"):
            raise HTTPException(
                status_code=400,
                detail="Consistency phải là SYNC, ASYNC hoặc QUORUM",
            )

        payload: Dict[str, Any] = {
            "operation": op,
            "key": req.key,
            "value": req.value if op == "PUT" else None,
            "consistency": mode,
        }
        if req.quorum_w is not None:
            payload["quorum_w"] = req.quorum_w

        eff_sid = session_id or x_session_id or "default"
        client = await _get_http_client()
        state.stats["total_writes"] += 1

        try:
            resp = await client.post(
                f"{state.primary_url}/api/v1/write",
                json=payload,
                timeout=state.write_timeout,
            )
        except Exception as e:
            state.stats["failed_writes"] += 1
            print(f"[Proxy] [-] Lỗi kết nối tới Primary khi ghi key='{req.key}': {e}")
            raise HTTPException(
                status_code=503,
                detail={
                    "status": "WRITE_FAILED",
                    "reason": "PRIMARY_UNREACHABLE",
                    "message": f"Không thể kết nối tới Primary Node ({state.primary_url}): {e}",
                },
            )

        if resp.status_code != 200:
            state.stats["failed_writes"] += 1
            raise HTTPException(
                status_code=resp.status_code,
                detail=_extract_error_detail(resp),
            )

        data = resp.json()
        written_version = int(data.get("version", 0))

        async with state.lock:
            session = state.get_session(eff_sid)
            session.update_on_write(
                key=req.key,
                value=data.get("value"),
                version=written_version,
                operation=op,
            )
            state.sync_to_globals_if_default(session)
            state.stats["successful_writes"] += 1

        response.headers["X-Session-ID"] = eff_sid
        response.headers["X-Written-Version"] = str(written_version)
        return data

    @proxy_app.get("/api/v1/read/{key:path}")
    async def handle_read(
        key: str,
        response: Response,
        x_session_id: Optional[str] = Header(default=None, alias="X-Session-ID"),
        session_id: Optional[str] = Query(default=None),
    ):
        """
        Đọc dữ liệu đảm bảo Read-Your-Writes, Monotonic Reads và Tự động Failover:
        1. Tính required_version = max(last_written_version, last_read_version).
        2. Thử đọc từ các Backup Node theo thứ tự Round-Robin (timeout = 2.0s):
           - Nếu timeout / lỗi kết nối / 503 -> Failover sang Backup kế tiếp.
           - Nếu 200 OK và node_version >= required_version -> Hợp lệ, cập nhật last_read_version & trả về.
           - Nếu 200 OK nhưng node_version < required_version -> Từ chối dữ liệu cũ (Stale Read).
        3. Fallback gọi trực tiếp Primary Node nếu các Backup lỗi hoặc trả dữ liệu cũ.
        4. Nếu Primary cũng không khả dụng, phục hồi từ local_cache (nếu có).
        """
        if key == "":
            raise HTTPException(status_code=400, detail="Key không được để trống")

        eff_sid = session_id or x_session_id or "default"
        async with state.lock:
            session = state.get_session(eff_sid)
            required_version = session.required_version
            ordered_backups = state.next_ordered_backups()
            state.stats["total_reads"] += 1

        encoded_key = quote(key, safe="/")
        client = await _get_http_client()
        stale_detected = False
        failover_count = 0

        # Bước 1: Thử lần lượt các Backup Node
        for backup_url in ordered_backups:
            try:
                b_resp = await client.get(
                    f"{backup_url}/api/v1/read/{encoded_key}",
                    timeout=state.read_timeout,
                )
            except Exception as e:
                failover_count += 1
                state.stats["backup_failovers"] += 1
                print(
                    f"[Proxy] [!] Backup {backup_url} lỗi/timeout khi đọc '{key}': {e} -> Failover"
                )
                continue

            if b_resp.status_code != 200:
                failover_count += 1
                state.stats["backup_failovers"] += 1
                print(
                    f"[Proxy] [!] Backup {backup_url} trả HTTP {b_resp.status_code} khi đọc '{key}' -> Failover"
                )
                continue

            b_data = b_resp.json()
            b_node_version = int(b_data.get("node_version", 0))

            if b_node_version < required_version:
                stale_detected = True
                state.stats["stale_rejections"] += 1
                print(
                    f"[Proxy] [STALE] Từ chối dữ liệu cũ từ {b_data.get('node_id', backup_url)}: "
                    f"node_version={b_node_version} < required_version={required_version}"
                )
                continue

            # Dữ liệu từ Backup hợp lệ (node_version >= required_version)
            async with state.lock:
                session = state.get_session(eff_sid)
                session.update_on_read(key, b_data)
                state.sync_to_globals_if_default(session)
                state.stats["backup_hits"] += 1

            response.headers["X-Session-ID"] = eff_sid
            response.headers["X-Proxy-Source"] = "backup"
            response.headers["X-Proxy-Node"] = str(b_data.get("node_id", backup_url))
            response.headers["X-Required-Version"] = str(required_version)
            response.headers["X-Stale-Rejected"] = str(stale_detected).lower()
            response.headers["X-Failover-Count"] = str(failover_count)
            return b_data

        # Bước 2: Fallback đọc trực tiếp từ Primary Node
        state.stats["primary_fallbacks"] += 1
        try:
            p_resp = await client.get(
                f"{state.primary_url}/api/v1/read/{encoded_key}",
                timeout=state.read_timeout,
            )
            if p_resp.status_code == 200:
                p_data = p_resp.json()
                p_node_version = int(p_data.get("node_version", 0))
                if p_node_version >= required_version:
                    async with state.lock:
                        session = state.get_session(eff_sid)
                        session.update_on_read(key, p_data)
                        state.sync_to_globals_if_default(session)
                        state.stats["primary_hits"] += 1

                    response.headers["X-Session-ID"] = eff_sid
                    response.headers["X-Proxy-Source"] = "primary"
                    response.headers["X-Proxy-Node"] = str(
                        p_data.get("node_id", "primary")
                    )
                    response.headers["X-Required-Version"] = str(required_version)
                    response.headers["X-Stale-Rejected"] = str(stale_detected).lower()
                    response.headers["X-Failover-Count"] = str(failover_count)
                    return p_data
                else:
                    stale_detected = True
                    state.stats["stale_rejections"] += 1
                    print(
                        f"[Proxy] [STALE-PRIMARY] Primary trả node_version={p_node_version} "
                        f"< required_version={required_version}"
                    )
        except Exception as e:
            print(
                f"[Proxy] [-] Không thể đọc fallback từ Primary ({state.primary_url}) cho key='{key}': {e}"
            )

        # Bước 3: Fallback lấy từ local_cache của phiên (nếu có)
        async with state.lock:
            session = state.get_session(eff_sid)
            cached = session.local_cache.get(key)
            if cached is not None:
                eff_node_version = max(
                    required_version,
                    int(cached.get("node_version", 0)),
                    int(cached.get("version", 0)),
                )
                if eff_node_version > session.last_read_version:
                    session.last_read_version = eff_node_version
                state.sync_to_globals_if_default(session)
                state.stats["cache_hits"] += 1

                is_deleted = bool(cached.get("deleted", False))
                cache_resp = {
                    "node_id": "local_cache",
                    "key": key,
                    "value": None if is_deleted else cached.get("value"),
                    "version": int(cached.get("version", 0)),
                    "node_version": eff_node_version,
                    "found": not is_deleted,
                    "deleted": is_deleted,
                }
                response.headers["X-Session-ID"] = eff_sid
                response.headers["X-Proxy-Source"] = "local_cache"
                response.headers["X-Proxy-Node"] = "local_cache"
                response.headers["X-Required-Version"] = str(required_version)
                response.headers["X-Stale-Rejected"] = str(stale_detected).lower()
                response.headers["X-Failover-Count"] = str(failover_count)
                return cache_resp

            state.stats["failed_reads"] += 1

        raise HTTPException(
            status_code=503,
            detail={
                "status": "READ_FAILED",
                "key": key,
                "required_version": required_version,
                "message": "Không thể đọc dữ liệu hợp lệ từ các Backup, Primary và key không có trong local_cache.",
            },
        )

    return proxy_app


# Khởi tạo app mặc định ở mức module (tương thích uvicorn proxy:app)
app = create_proxy_app(use_global_default_session=True)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=cfg.PORT)
