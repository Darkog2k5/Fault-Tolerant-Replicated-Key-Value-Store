# Fault-Tolerant Replicated Key-Value Store
> **Course:** Distributed Systems (INT1405) — Semester 1, 2026-2027  
> **Topic:** Track C — Fault-Tolerant Replicated Key-Value Store  

A distributed key-value storage cluster that replicates data across multiple storage nodes to provide high availability, crash recovery via custom Write-Ahead Logging (JSONL), and configurable consistency guarantees (`SYNC`, `QUORUM`, `ASYNC`).

---

## 1. PHÂN CÔNG VAI TRÒ THÀNH VIÊN (MODULE OWNERSHIP)

| Thành viên | Vai trò đảm nhận | Thư mục mã nguồn | Nhiệm vụ chính |
| :--- | :--- | :--- | :--- |
| **Sinh viên 1** | **Primary Coordinator Node & Write Protocol** | `primary_node/` | Tiếp nhận `PUT/DELETE`, cấp phát số phiên bản tăng đơn điệu (`version`), quản lý WAL trên Primary, điều phối lan truyền bản sao (`SYNC`, `ASYNC` FIFO Queue, `QUORUM` + Commit) và API Catch-Up. |
| **Sinh viên 2** | **Backup Replica Nodes & Catch-Up Recovery** | `backup_node/` | Quản lý lưu trữ bền vững (`backup_wal.jsonl`), xử lý pha Prepare (`pending`) & Commit, phát hiện khuyết log (gap) và tự động đồng bộ bù (Catch-Up) sau khi sập. |
| **Sinh viên 3** | **Smart Client Proxy & Client-Centric Consistency** | `client_proxy/` | Cài đặt Middleware tầng Client, quản lý session state & cache, tự động Failover, đảm bảo tính nhất quán *Read-Your-Writes* và *Monotonic Reads*. |

---

## 2. CẤU TRÚC THƯ MỤC DỰ ÁN

    Fault-Tolerant-Replicated-Key-Value-Store/
    ├── docker-compose.yml            # Điều phối khởi động cụm (1 Primary, 2 Backups, 1 Proxy)
    ├── PROTOCOL_SPEC.md              # Đặc tả chi tiết các API & Format JSON (v4.0)
    ├── HUONG_DAN_TRIEN_KHAI_v4.md    # Hướng dẫn triển khai chi tiết & giải quyết Edge Cases
    ├── README.md                     # Giới thiệu & Hướng dẫn chạy dự án
    │
    ├── primary_node/                 # [SV1] Máy chủ Primary Coordinator (Port 8000)
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   ├── config.py
    │   ├── replication.py
    │   └── main.py
    │
    ├── backup_node/                  # [SV2] Máy chủ Backup Replica (Port 8001, 8002)
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── main.py
    │
    ├── client_proxy/                 # [SV3] Client Proxy / Middleware (Port 8080)
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── proxy.py
    │
    └── tests/                        # Bộ kiểm thử độc lập
        ├── mock_backup.py            # Giả lập Backup Nodes (Port 8001, 8002)
        └── mock_client.py            # Giả lập Client test các chế độ ghi, đọc, xóa và catch-up

---

## 3. QUY ƯỚC KẾT NỐI & TÀI LIỆU API

Chi tiết về cấu trúc các gói tin JSON, mã trạng thái và các endpoint được mô tả đầy đủ tại:  
👉 **[PROTOCOL_SPEC.md](PROTOCOL_SPEC.md)**

* **Primary Node:** `http://localhost:8000` (Swagger UI: `http://localhost:8000/docs`)
* **Backup Node 1:** `http://localhost:8001`
* **Backup Node 2:** `http://localhost:8002`
* **Client Proxy:** `http://localhost:8080`

---

## 4. HƯỚNG DẪN KHỞI CHẠY & KIỂM THỬ

### Cách 1: Chạy toàn bộ cụm bằng Docker Compose (Khuyên dùng)

    # 1. Build và khởi chạy ngầm cả 4 containers
    docker compose up --build -d

    # 2. Chạy kịch bản kiểm thử tự động
    python tests/mock_client.py

    # 3. Dừng cụm (giữ nguyên dữ liệu WAL trong volume)
    docker compose down

    # (Tuỳ chọn) Dừng cụm và xoá sạch dữ liệu WAL để reset version về 0
    docker compose down -v

### Cách 2: Chạy trực tiếp bằng Python trên máy (Local Dev)

    pip install -r primary_node/requirements.txt

    # Terminal 1 & 2: Chạy 2 Mock Backups
    python tests/mock_backup.py 8001 backup-1
    python tests/mock_backup.py 8002 backup-2

    # Terminal 3: Chạy Primary Node
    cd primary_node && python main.py

    # Terminal 4: Chạy Client Test
    python tests/mock_client.py