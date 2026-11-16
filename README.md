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
    ├── PROTOCOL_SPEC.md              # Đặc tả chi tiết các API & Format JSON (v4.1)
    ├── README.md                     # Giới thiệu & Hướng dẫn chạy dự án
    ├── REVIEW_SINH_VIEN_2.md         # Review và ghi chú vai trò Backup
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
    │   ├── config.py
    │   ├── node.py
    │   └── main.py
    │
    ├── client_proxy/                 # [SV3] Client Proxy / Middleware (Port 8080)
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── proxy.py
    │
    └── tests/                        # Bộ kiểm thử độc lập
        ├── conftest.py                # Cấu hình chung cho pytest
        ├── test_backup_node.py        # Kiểm thử đơn vị Backup Node
        ├── test_backup_cluster.py     # Kiểm thử tích hợp cụm thật
        ├── cluster_harness.py         # Khởi chạy và dọn dẹp tiến trình cụm
        ├── bench_recovery.py          # Đo thời gian catch-up và độ trễ đọc
        ├── requirements.txt           # Phụ thuộc cho kiểm thử
        ├── mock_backup.py             # Giả lập Backup cho kiểm thử failover
        └── mock_client.py             # Giả lập Client kiểm thử Primary

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

### Cách 3: Chạy Backup thật ở local (không Docker)

Mở bốn terminal tại thư mục gốc dự án. Mỗi Backup phải dùng một `DATA_DIR` riêng để WAL không bị dùng chung.

**PowerShell:**

    # Terminal 1: Primary
    $env:NODE_ID="primary"; $env:PORT="8000"; $env:REPLICAS="http://127.0.0.1:8001,http://127.0.0.1:8002"; $env:DATA_DIR="data/primary"; python primary_node/main.py

    # Terminal 2: Backup 1
    $env:NODE_ID="backup-1"; $env:PORT="8001"; $env:PRIMARY_URL="http://127.0.0.1:8000"; $env:DATA_DIR="data/backup-1"; python backup_node/main.py

    # Terminal 3: Backup 2
    $env:NODE_ID="backup-2"; $env:PORT="8002"; $env:PRIMARY_URL="http://127.0.0.1:8000"; $env:DATA_DIR="data/backup-2"; python backup_node/main.py

**Bash:**

    # Terminal 1: Primary
    NODE_ID=primary PORT=8000 REPLICAS=http://127.0.0.1:8001,http://127.0.0.1:8002 DATA_DIR=data/primary python primary_node/main.py

    # Terminal 2: Backup 1
    NODE_ID=backup-1 PORT=8001 PRIMARY_URL=http://127.0.0.1:8000 DATA_DIR=data/backup-1 python backup_node/main.py

    # Terminal 3: Backup 2
    NODE_ID=backup-2 PORT=8002 PRIMARY_URL=http://127.0.0.1:8000 DATA_DIR=data/backup-2 python backup_node/main.py

Sau khi các node báo sẵn sàng, có thể chạy `python tests/mock_client.py` từ terminal thứ tư. Cấu hình Backup khác được liệt kê ở mục 6.

## 5. KIỂM THỬ BACKUP NODE

    # Cài các thư viện kiểm thử
    python -m pip install -r tests/requirements.txt

    # Chạy toàn bộ kiểm thử đơn vị và tích hợp
    python -m pytest tests -q

    # Chỉ chạy kiểm thử cụm thật hoặc bỏ qua chúng
    python -m pytest tests -q -m integration
    python -m pytest tests -q -m "not integration"

    # Đo thời gian phục hồi với 10 và 100 bản ghi bị lỡ
    python tests/bench_recovery.py --sizes 10,100

Benchmark mặc định dùng `ASYNC`; có thể truyền chế độ khác bằng `--mode`, ví dụ `--mode QUORUM`.

## 6. CẤU HÌNH BACKUP NODE

| Biến môi trường | Mặc định | Mô tả |
| :--- | :--- | :--- |
| `NODE_ID` | `backup-1` | Tên node Backup trong phản hồi API. |
| `PORT` | `8001` | Cổng HTTP của Backup. |
| `PRIMARY_URL` | `http://localhost:8000` | URL Primary dùng cho catch-up và kiểm tra sức khỏe. |
| `DATA_DIR` | `./data` | Thư mục WAL `backup_wal.jsonl` của node. |
| `CATCHUP_HTTP_TIMEOUT` | `5.0` giây | Timeout gọi API của Primary. |
| `REPLICATE_CATCHUP_DEADLINE` | `1.5` giây | Thời gian tối đa chờ catch-up khi phát hiện gap trong `/replicate`. |
| `PRIMARY_FAILURES_BEFORE_DEGRADED` | `5` | Số lần liên tiếp không liên lạc được Primary trước khi chuyển `DEGRADED`. |
| `SYNC_INTERVAL` | `5.0` giây | Chu kỳ kiểm tra Primary và đồng bộ bù nền. |
| `CATCHUP_DELAY_MS` | `0` ms | Độ trễ mô phỏng trước mỗi lần catch-up; hữu ích để trình diễn phản hồi 503. |
| `PENDING_TTL` | `3.0` giây | Thời gian một bản ghi pending được giữ trước khi bị xem là cũ. |

Endpoint `GET /api/v1/debug/state` trả trạng thái node, `current_version`, danh sách version pending, số khóa trong RAM, đường dẫn WAL và thông tin lần catch-up gần nhất.
