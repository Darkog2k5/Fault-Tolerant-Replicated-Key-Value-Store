import os
from typing import List, Set
from urllib.parse import urlparse

NODE_ID = os.getenv("NODE_ID", "primary")
PORT = int(os.getenv("PORT", "8000"))


def _is_self_url(url: str, node_id: str, port: int) -> bool:
    """Kiểm tra xem URL cấu hình trong REPLICAS có trỏ nhầm về chính Primary không."""
    parsed = urlparse(url)
    url_port = parsed.port
    if url_port is None:
        url_port = 443 if parsed.scheme == "https" else 80
    hostname = (parsed.hostname or "").lower()
    self_hosts = {"localhost", "127.0.0.1", "0.0.0.0", node_id.lower()}
    return hostname in self_hosts and url_port == port


def parse_backup_urls(replicas_str: str, node_id: str = NODE_ID, port: int = PORT) -> List[str]:
    """Chuẩn hoá danh sách Backup URL: strip/rstrip('/'), bỏ trùng lặp và bỏ URL tự trỏ về Primary."""
    seen: Set[str] = set()
    urls: List[str] = []
    for raw in replicas_str.split(","):
        cleaned = raw.strip().rstrip("/")
        if not cleaned or cleaned in seen:
            continue
        if _is_self_url(cleaned, node_id, port):
            continue
        seen.add(cleaned)
        urls.append(cleaned)
    return urls


# Danh sách URL của các Backup Replicas (đã chuẩn hoá)
REPLICAS_STR = os.getenv("REPLICAS", "http://localhost:8001,http://localhost:8002")
BACKUP_URLS: List[str] = parse_backup_urls(REPLICAS_STR, NODE_ID, PORT)

# Tổng số node trong cụm (1 Primary + số Backup)
CLUSTER_SIZE = 1 + len(BACKUP_URLS)

# Số lượng node tối thiểu cần xác nhận cho chế độ QUORUM (tính cả Primary = 1)
_raw_default_w = os.getenv("DEFAULT_QUORUM_W")
DEFAULT_QUORUM_W = int(_raw_default_w) if _raw_default_w is not None else min(2, CLUSTER_SIZE)
if DEFAULT_QUORUM_W < 1 or DEFAULT_QUORUM_W > CLUSTER_SIZE:
    raise ValueError(
        f"DEFAULT_QUORUM_W={DEFAULT_QUORUM_W} không hợp lệ cho cụm có {CLUSTER_SIZE} node (phải thuộc [1, {CLUSTER_SIZE}])."
    )

# Timeout & giới hạn hàng đợi lan truyền
REPLICATE_TIMEOUT = float(os.getenv("REPLICATE_TIMEOUT", "2.0"))
COMMIT_TIMEOUT = float(os.getenv("COMMIT_TIMEOUT", "2.0"))
MAX_ASYNC_QUEUE_SIZE = max(1, int(os.getenv("MAX_ASYNC_QUEUE_SIZE", "1000")))

# Thư mục lưu file log tự tạo (không dùng DB có sẵn)
DATA_DIR = os.getenv("DATA_DIR", "./data")
WAL_FILE = os.path.join(DATA_DIR, "primary_wal.jsonl")
