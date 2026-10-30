import asyncio
import httpx
from typing import List, Dict, Any, Optional, Tuple
from config import NODE_ID, BACKUP_URLS

# Hàng đợi FIFO riêng cho từng Backup URL để đảm bảo chế độ ASYNC luôn gửi đúng thứ tự version
async_queues: Dict[str, asyncio.Queue] = {}
worker_tasks: List[asyncio.Task] = []


def start_async_workers():
    """Khởi chạy các background worker gửi tuần tự cho từng Backup khi server bật."""
    for url in BACKUP_URLS:
        q: asyncio.Queue = asyncio.Queue()
        async_queues[url] = q
        task = asyncio.create_task(_async_worker_loop(url, q))
        worker_tasks.append(task)


async def stop_async_workers():
    """Dọn dẹp background workers khi tắt server."""
    for task in worker_tasks:
        task.cancel()


async def _async_worker_loop(url: str, queue: asyncio.Queue):
    """Worker lấy từng bản ghi từ hàng đợi của 1 Backup và gửi tuần tự (FIFO)."""
    async with httpx.AsyncClient() as client:
        while True:
            payload = await queue.get()
            try:
                await send_replicate_to_one(client, url, payload, timeout=2.0, retries=2)
            finally:
                queue.task_done()


async def send_replicate_to_one(
    client: httpx.AsyncClient,
    url: str,
    payload: Dict[str, Any],
    timeout: float = 2.0,
    retries: int = 1
) -> Tuple[bool, Optional[str]]:
    """
    Gửi bản ghi sang 1 Backup Node và đợi ACK (có hỗ trợ retry nhẹ nếu mạng chập chờn).
    Trả về: (True/False, tên node_id của backup)
    """
    for attempt in range(retries):
        try:
            resp = await client.post(f"{url}/api/v1/replicate", json=payload, timeout=timeout)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "ACK":
                    return True, data.get("node_id", url)
        except Exception as e:
            if attempt == retries - 1:
                print(f"[-] Lỗi kết nối khi replicate v={payload.get('version')} tới {url}: {e}")
            else:
                await asyncio.sleep(0.1)
    return False, None


async def broadcast_commit_async(backup_urls: List[str], commit_version: int):
    """
    Gửi ngầm thông báo POST /api/v1/commit {"commit_version": v} tới tất cả Backup
    sau khi SYNC hoặc QUORUM đã commit thành công trên Primary. Không cần chờ phản hồi.
    """
    payload = {"commit_version": commit_version}
    async with httpx.AsyncClient() as client:
        tasks = [
            client.post(f"{url}/api/v1/commit", json=payload, timeout=2.0)
            for url in backup_urls
        ]
        await asyncio.gather(*tasks, return_exceptions=True)


async def replicate_entry(
    backup_urls: List[str],
    payload: Dict[str, Any],
    mode: str = "SYNC",
    quorum_w: int = 2
) -> Dict[str, Any]:
    """
    Điều phối lan truyền sang các Backup:
    - ASYNC: Đẩy vào hàng đợi FIFO của từng Backup, trả về thành công ngay (acks = 1).
    - SYNC: Đợi TẤT CẢ backup trả về ACK (tổng acks = 1 + len(backup_urls)).
    - QUORUM: Đợi đủ tối thiểu `quorum_w` nodes (tính cả Primary là 1).
    """
    mode = mode.upper()

    if mode == "ASYNC":
        for url in backup_urls:
            if url in async_queues:
                async_queues[url].put_nowait(payload)
            else:
                asyncio.create_task(_one_off_send(url, payload))
        return {
            "success": True,
            "mode": "ASYNC",
            "acks": 1,
            "required_acks": 1,
            "replicated_nodes": [NODE_ID]
        }

    # Chế độ SYNC và QUORUM: Gửi song song tới các Backup và thu thập ACK
    replicated_nodes = [NODE_ID]
    async with httpx.AsyncClient() as client:
        tasks = [send_replicate_to_one(client, url, payload, timeout=2.0, retries=1) for url in backup_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, tuple) and res[0] is True:
                replicated_nodes.append(res[1])

    total_acks = len(replicated_nodes)
    required_acks = (1 + len(backup_urls)) if mode == "SYNC" else quorum_w

    return {
        "success": total_acks >= required_acks,
        "mode": mode,
        "acks": total_acks,
        "required_acks": required_acks,
        "replicated_nodes": replicated_nodes
    }


async def _one_off_send(url: str, payload: Dict[str, Any]):
    async with httpx.AsyncClient() as client:
        await send_replicate_to_one(client, url, payload, timeout=2.0, retries=2)