"""Cấu hình môi trường cho Backup Replica Node."""

import os
from pathlib import Path

NODE_ID = os.getenv("NODE_ID", "backup-1")
PORT = int(os.getenv("PORT", "8001"))
PRIMARY_URL = os.getenv("PRIMARY_URL", "http://localhost:8000").rstrip("/")
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
WAL_FILE = DATA_DIR / "backup_wal.jsonl"

CATCHUP_HTTP_TIMEOUT = float(os.getenv("CATCHUP_HTTP_TIMEOUT", "5.0"))
REPLICATE_CATCHUP_DEADLINE = float(os.getenv("REPLICATE_CATCHUP_DEADLINE", "1.5"))
PRIMARY_FAILURES_BEFORE_DEGRADED = int(
    os.getenv("PRIMARY_FAILURES_BEFORE_DEGRADED", "5")
)
SYNC_INTERVAL = float(os.getenv("SYNC_INTERVAL", "5.0"))
CATCHUP_DELAY_MS = int(os.getenv("CATCHUP_DELAY_MS", "0"))
PENDING_TTL = float(os.getenv("PENDING_TTL", "3.0"))
