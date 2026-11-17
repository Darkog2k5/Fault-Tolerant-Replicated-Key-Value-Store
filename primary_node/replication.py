import asyncio
import httpx
from typing import List, Dict, Any, Optional, Tuple, Set
from config import (
    NODE_ID,
    BACKUP_URLS,
    REPLICATE_TIMEOUT,
    COMMIT_TIMEOUT,
    MAX_ASYNC_QUEUE_SIZE,
)

# Hàng đợi FIFO và Lock riêng cho từng Backup URL để đảm bảo thứ tự version
async_queues: Dict[str, asyncio.Queue] = {}
per_backup_locks: Dict[str, asyncio.Lock] = {}
worker_tasks: List[asyncio.Task] = []
bg_tasks: Set[asyncio.Task] = set()
shared_client: Optional[httpx.AsyncClient] = None


def _get_backup_lock(url: str) -> asyncio.Lock:
    """Lấy hoặc khởi tạo Lock riêng cho một Backup URL."""
    lock = per_backup_locks.get(url)
    if lock is None:
        lock = asyncio.Lock()
        per_backup_locks[url] = lock
    return lock


def _track_bg_task(task: asyncio.Task) -> asyncio.Task:
    """Giữ tham chiếu tới background task để tránh bị GC và dọn sạch khi shutdown."""
    bg_tasks.add(task)
    task.add_done_callback(bg_tasks.discard)
    return task


def _get_shared_client() -> Optional[httpx.AsyncClient]:
    """Trả về shared AsyncClient nếu đang hoạt động."""
    if shared_client is not None and not shared_client.is_closed:
        return shared_client
    return None


def start_async_workers(backup_urls: Optional[List[str]] = None):
    """Khởi chạy shared HTTP client và các background worker FIFO cho từng Backup."""
    global shared_client

    urls = BACKUP_URLS if backup_urls is None else backup_urls

    # Dọn dẹp trạng thái cũ nếu được gọi lại trong test
    for task in worker_tasks:
        if not task.done():
            task.cancel()
    worker_tasks.clear()
    async_queues.clear()
    per_backup_locks.clear()

    if shared_client is None or shared_client.is_closed:
        limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
        shared_client = httpx.AsyncClient(limits=limits)

    for url in urls:
        q: asyncio.Queue = asyncio.Queue(maxsize=MAX_ASYNC_QUEUE_SIZE)
        async_queues[url] = q
        per_backup_locks[url] = asyncio.Lock()
        task = asyncio.create_task(_async_worker_loop(url, q))
        worker_tasks.append(task)


async def stop_async_workers(drain_timeout: float = 1.5):
    """Chờ rút nhanh hàng đợi ASYNC và dọn dẹp workers/HTTP client khi tắt server."""
    global shared_client

    wait_coros = []
    for q in async_queues.values():
        wait_coros.append(asyncio.create_task(q.join()))
    for t in list(bg_tasks):
        if not t.done():
            wait_coros.append(asyncio.shield(t))

    if wait_coros:
        _, pending = await asyncio.wait(wait_coros, timeout=drain_timeout)
        for p in pending:
            if isinstance(p, asyncio.Task) and not p.done():
                p.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    for task in list(worker_tasks):
        if not task.done():
            task.cancel()
    if worker_tasks:
        await asyncio.gather(*worker_tasks, return_exceptions=True)

    for task in list(bg_tasks):
        if not task.done():
            task.cancel()
    if bg_tasks:
        await asyncio.gather(*bg_tasks, return_exceptions=True)

    worker_tasks.clear()
    async_queues.clear()
    per_backup_locks.clear()
    bg_tasks.clear()

    if shared_client is not None and not shared_client.is_closed:
        await shared_client.aclose()
    shared_client = None


async def _send_commit_to_one(
    client: httpx.AsyncClient,
    url: str,
    commit_version: int,
    timeout: float = COMMIT_TIMEOUT,
    retries: int = 3,
) -> bool:
    """Gửi POST /api/v1/commit tới 1 Backup với cơ chế retry nhẹ."""
    payload = {"commit_version": commit_version}
    backoff_delays = (0.05, 0.15)
    for attempt in range(retries):
        try:
            resp = await client.post(
                f"{url}/api/v1/commit", json=payload, timeout=timeout
            )
            if resp.status_code == 200:
                return True
        except Exception as e:
            if attempt == retries - 1:
                print(
                    f"[-] Lỗi gửi /commit (v={commit_version}) tới {url}: {e}"
                )
        if attempt < retries - 1:
            delay = backoff_delays[min(attempt, len(backoff_delays) - 1)]
            await asyncio.sleep(delay)
    return False


async def _async_worker_loop(url: str, queue: asyncio.Queue):
    """Worker lấy từng bản ghi từ hàng đợi của 1 Backup và gửi tuần tự (FIFO)."""
    owned_client: Optional[httpx.AsyncClient] = None
    client = _get_shared_client()
    if client is None:
        owned_client = httpx.AsyncClient()
        client = owned_client

    lock = _get_backup_lock(url)
    try:
        while True:
            item = await queue.get()
            try:
                if isinstance(item, tuple) and len(item) == 2:
                    payload, require_explicit_commit = item
                else:
                    payload, require_explicit_commit = item, False

                active_client = _get_shared_client() or client
                async with lock:
                    ok, _ = await send_replicate_to_one(
                        active_client,
                        url,
                        payload,
                        timeout=REPLICATE_TIMEOUT,
                        retries=2,
                    )
                    if ok and require_explicit_commit:
                        await _send_commit_to_one(
                            active_client,
                            url,
                            payload["version"],
                            timeout=COMMIT_TIMEOUT,
                            retries=3,
                        )

                # Nếu Backup đang lỗi và trong queue dồn nhiều bản ghi mới hơn,
                # xả các bản ghi trung gian và giữ lại bản mới nhất (Backup sẽ dùng /catch-up).
                if not ok and not queue.empty():
                    latest_item = None
                    while not queue.empty():
                        latest_item = queue.get_nowait()
                        queue.task_done()
                    if latest_item is not None:
                        queue.put_nowait(latest_item)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[-] Lỗi không mong muốn trong async worker {url}: {e}")
            finally:
                queue.task_done()
    finally:
        if owned_client is not None and not owned_client.is_closed:
            await owned_client.aclose()


async def send_replicate_to_one(
    client: httpx.AsyncClient,
    url: str,
    payload: Dict[str, Any],
    timeout: float = REPLICATE_TIMEOUT,
    retries: int = 1,
) -> Tuple[bool, Optional[str]]:
    """
    Gửi bản ghi sang 1 Backup Node và đợi ACK (có hỗ trợ retry nhẹ nếu mạng chập chờn).
    Trả về: (True/False, tên node_id của backup)
    """
    for attempt in range(max(1, retries)):
        try:
            resp = await client.post(
                f"{url}/api/v1/replicate", json=payload, timeout=timeout
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "ACK":
                    return True, data.get("node_id", url)
        except Exception as e:
            if attempt == retries - 1:
                print(
                    f"[-] Lỗi kết nối khi replicate v={payload.get('version')} tới {url}: {e}"
                )
            else:
                await asyncio.sleep(0.1)
    return False, None


async def _replicate_sync_to_one(
    url: str,
    payload: Dict[str, Any],
    timeout: float = REPLICATE_TIMEOUT,
) -> Tuple[bool, Optional[str], str]:
    """
    Gửi bản ghi SYNC/QUORUM tới 1 Backup sau khi chờ rút hết hàng đợi ASYNC
    trên chính Backup đó trong phạm vi ngân sách timeout.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    q = async_queues.get(url)
    if q is not None:
        remaining = deadline - loop.time()
        if remaining <= 0:
            return False, None, url
        try:
            await asyncio.wait_for(q.join(), timeout=remaining)
        except asyncio.TimeoutError:
            return False, None, url

    remaining = deadline - loop.time()
    if remaining <= 0:
        return False, None, url

    lock = _get_backup_lock(url)

    async def _locked_send() -> Tuple[bool, Optional[str]]:
        async with lock:
            rem_send = deadline - loop.time()
            if rem_send <= 0:
                return False, None
            client = _get_shared_client()
            if client is not None:
                return await send_replicate_to_one(
                    client, url, payload, timeout=rem_send, retries=1
                )
            async with httpx.AsyncClient() as temp_client:
                return await send_replicate_to_one(
                    temp_client, url, payload, timeout=rem_send, retries=1
                )

    try:
        ok, node_id = await asyncio.wait_for(_locked_send(), timeout=remaining)
        return ok, node_id, url
    except asyncio.TimeoutError:
        return False, None, url


async def broadcast_commit_async(backup_urls: List[str], commit_version: int):
    """
    Gửi ngầm thông báo POST /api/v1/commit {"commit_version": v} tới danh sách Backup
    với cơ chế retry nhẹ.
    """
    if not backup_urls:
        return

    client = _get_shared_client()
    if client is not None:
        tasks = [
            _send_commit_to_one(client, url, commit_version, timeout=COMMIT_TIMEOUT, retries=3)
            for url in backup_urls
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
        return

    async with httpx.AsyncClient() as temp_client:
        tasks = [
            _send_commit_to_one(
                temp_client, url, commit_version, timeout=COMMIT_TIMEOUT, retries=3
            )
            for url in backup_urls
        ]
        await asyncio.gather(*tasks, return_exceptions=True)


def schedule_broadcast_commit(backup_urls: List[str], commit_version: int):
    """Đẩy tác vụ gửi /commit vào background task có theo dõi vòng đời."""
    if not backup_urls:
        return
    task = asyncio.create_task(broadcast_commit_async(backup_urls, commit_version))
    _track_bg_task(task)


async def _finalize_late_replications(
    pending_tasks: Set[asyncio.Task],
    task_to_url: Dict[asyncio.Task, str],
    commit_version: int,
):
    """
    Theo dõi các task replicate về đích muộn sau khi QUORUM đã đạt ngưỡng W;
    nếu Backup phản hồi ACK muộn thì gửi bù POST /api/v1/commit ngay.
    """
    if not pending_tasks:
        return

    late_ack_urls: List[str] = []
    results = await asyncio.gather(*pending_tasks, return_exceptions=True)
    for task, res in zip(pending_tasks, results):
        url = task_to_url.get(task)
        if isinstance(res, tuple) and res[0] is True and url:
            late_ack_urls.append(url)

    if late_ack_urls:
        await broadcast_commit_async(late_ack_urls, commit_version)


def finalize_late_replications(repl_result: Dict[str, Any], commit_version: int):
    """Kích hoạt theo dõi ngầm và gửi commit bù cho các node phản hồi muộn sau QUORUM."""
    pending_tasks: Set[asyncio.Task] = repl_result.pop("_pending_tasks", set())
    task_to_url: Dict[asyncio.Task, str] = repl_result.pop("_task_to_url", {})
    if pending_tasks:
        task = asyncio.create_task(
            _finalize_late_replications(pending_tasks, task_to_url, commit_version)
        )
        _track_bg_task(task)


async def cancel_pending_replications(repl_result: Dict[str, Any]):
    """Huỷ các task đang bay nếu không sử dụng (ví dụ lỗi ghi WAL)."""
    pending_tasks: Set[asyncio.Task] = repl_result.pop("_pending_tasks", set())
    repl_result.pop("_task_to_url", None)
    for task in pending_tasks:
        if not task.done():
            task.cancel()
    if pending_tasks:
        await asyncio.gather(*pending_tasks, return_exceptions=True)


async def replicate_entry(
    backup_urls: List[str],
    payload: Dict[str, Any],
    mode: str = "SYNC",
    quorum_w: int = 2,
    poisoned_pending: bool = False,
) -> Dict[str, Any]:
    """
    Điều phối lan truyền sang các Backup:
    - ASYNC: Đẩy vào hàng đợi FIFO của từng Backup, trả về thành công ngay (acks = 1).
    - SYNC: Đợi TẤT CẢ backup trả về ACK (tổng acks = 1 + len(backup_urls)).
    - QUORUM: Đợi đủ tối thiểu `quorum_w` nodes (tính cả Primary là 1), trả sớm ngay khi đạt ngưỡng.
    """
    mode = mode.upper()

    if mode == "ASYNC":
        item = (dict(payload), bool(poisoned_pending))
        for url in backup_urls:
            q = async_queues.get(url)
            if q is not None:
                if q.full():
                    try:
                        q.get_nowait()
                        q.task_done()
                    except asyncio.QueueEmpty:
                        pass
                q.put_nowait(item)
            else:
                _track_bg_task(asyncio.create_task(_one_off_send(url, payload, poisoned_pending)))
        return {
            "success": True,
            "mode": "ASYNC",
            "acks": 1,
            "required_acks": 1,
            "replicated_nodes": [NODE_ID],
            "replicated_urls": [],
        }

    total_nodes = 1 + len(backup_urls)
    required_acks = total_nodes if mode == "SYNC" else quorum_w

    replicated_nodes: List[str] = [NODE_ID]
    replicated_urls: List[str] = []

    task_to_url: Dict[asyncio.Task, str] = {}
    for url in backup_urls:
        task = asyncio.create_task(
            _replicate_sync_to_one(url, payload, timeout=REPLICATE_TIMEOUT)
        )
        task_to_url[task] = url

    pending: Set[asyncio.Task] = set(task_to_url.keys())
    success: Optional[bool] = None

    while pending:
        if len(replicated_nodes) >= required_acks:
            success = True
            break
        if len(replicated_nodes) + len(pending) < required_acks:
            success = False
            break

        done, pending = await asyncio.wait(
            pending, return_when=asyncio.FIRST_COMPLETED
        )
        for task in done:
            url = task_to_url[task]
            try:
                ok, node_id, _ = task.result()
                if ok and node_id is not None:
                    replicated_nodes.append(node_id)
                    replicated_urls.append(url)
            except Exception as e:
                print(f"[-] Lỗi replicate tới {url}: {e}")

    if success is None:
        success = len(replicated_nodes) >= required_acks

    if not success and pending:
        for task in pending:
            if not task.done():
                task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        pending = set()

    return {
        "success": success,
        "mode": mode,
        "acks": len(replicated_nodes),
        "required_acks": required_acks,
        "replicated_nodes": replicated_nodes,
        "replicated_urls": replicated_urls,
        "_pending_tasks": pending,
        "_task_to_url": task_to_url,
    }


async def _one_off_send(
    url: str, payload: Dict[str, Any], require_explicit_commit: bool = False
):
    """Fallback gửi 1 lần khi worker queue chưa được khởi động."""
    lock = _get_backup_lock(url)
    async with lock:
        async with httpx.AsyncClient() as client:
            ok, _ = await send_replicate_to_one(
                client, url, payload, timeout=REPLICATE_TIMEOUT, retries=2
            )
            if ok and require_explicit_commit:
                await _send_commit_to_one(
                    client, url, payload["version"], timeout=COMMIT_TIMEOUT, retries=3
                )
