import os

NODE_ID = os.getenv("NODE_ID", "primary")
PORT = int(os.getenv("PORT", 8000))

# Danh sách URL của các Backup Replicas
REPLICAS_STR = os.getenv("REPLICAS", "http://localhost:8001,http://localhost:8002")
BACKUP_URLS = [url.strip() for url in REPLICAS_STR.split(",") if url.strip()]

# Số lượng node tối thiểu cần xác nhận cho chế độ QUORUM (Tính cả Primary = 1)
DEFAULT_QUORUM_W = int(os.getenv("DEFAULT_QUORUM_W", 2))

# Thư mục lưu file log tự tạo (không dùng DB có sẵn)
DATA_DIR = os.getenv("DATA_DIR", "./data")
WAL_FILE = os.path.join(DATA_DIR, "primary_wal.jsonl")