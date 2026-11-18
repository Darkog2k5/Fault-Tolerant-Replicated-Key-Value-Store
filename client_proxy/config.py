import os
from typing import List, Set
from urllib.parse import urlparse

NODE_ID = os.getenv("NODE_ID", "proxy")
PORT = int(os.getenv("PORT", "8080"))


def _is_self_url(url: str, node_id: str, port: int) -> bool:
    """Kiểm tra xem URL cấu hình có trỏ nhầm về chính Client Proxy không."""
    parsed = urlparse(url)
    url_port = parsed.port
    if url_port is None:
        url_port = 443 if parsed.scheme == "https" else 80
    hostname = (parsed.hostname or "").lower()
    self_hosts = {"localhost", "127.0.0.1", "0.0.0.0", node_id.lower()}
    return hostname in self_hosts and url_port == port


def normalize_url(url: str) -> str:
    """Chuẩn hoá một URL đơn lẻ (bỏ khoảng trắng và dấu '/' cuối)."""
    return (url or "").strip().rstrip("/")


def parse_backup_urls(
    backups_str: str, node_id: str = NODE_ID, port: int = PORT
) -> List[str]:
    """Chuẩn hoá danh sách Backup URL: strip/rstrip('/'), bỏ trùng lặp và bỏ URL tự trỏ về Proxy."""
    seen: Set[str] = set()
    urls: List[str] = []
    for raw in (backups_str or "").split(","):
        cleaned = normalize_url(raw)
        if not cleaned or cleaned in seen:
            continue
        if _is_self_url(cleaned, node_id, port):
            continue
        seen.add(cleaned)
        urls.append(cleaned)
    return urls


# Địa chỉ Primary Node và danh sách Backup Replicas
PRIMARY_URL: str = normalize_url(os.getenv("PRIMARY_URL", "http://localhost:8000"))
BACKUP_URLS_STR: str = os.getenv(
    "BACKUP_URLS", "http://localhost:8001,http://localhost:8002"
)
BACKUP_URLS: List[str] = parse_backup_urls(BACKUP_URLS_STR, NODE_ID, PORT)

# Timeout cho thao tác Đọc (mặc định 2.0s theo PROTOCOL_SPEC.md mục 4) và Ghi
READ_TIMEOUT: float = float(os.getenv("READ_TIMEOUT", "2.0"))
WRITE_TIMEOUT: float = float(os.getenv("WRITE_TIMEOUT", "5.0"))

