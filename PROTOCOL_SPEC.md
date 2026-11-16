# CLUSTER API PROTOCOL SPECIFICATION (v4.1)
## Track C: Fault-Tolerant Replicated Key-Value Store

Bản đặc tả giao diện lập trình ứng dụng (API Contract) và giao thức truyền thông chuẩn hoá giữa 3 thành phần: **Primary Coordinator Node (SV1)**, **Backup Replica Nodes (SV2)** và **Smart Client Proxy (SV3)**.

### Nhật ký cập nhật v4.1

- Bổ sung hành vi NACK, xử lý pending cũ, trạng thái Backup, đồng bộ định kỳ và API debug theo phần Backup đã triển khai.
- Backup hỗ trợ tuỳ chọn `entry_id` và `commit_entry_id`, nhưng việc Primary gửi các trường này vẫn là **ĐỀ XUẤT (chờ SV1)**; cần SV1/SV3 thống nhất trước khi xem là hợp đồng chung.
- Các phần đánh dấu **ĐỀ XUẤT** cần SV1/SV3 đồng ý trước khi đưa vào hợp đồng chung. Không thay đổi format request/response hiện có của Primary.

---

## 1. QUY ƯỚC CỔNG MẠNG & ĐỊNH DANH NODE

| Dịch vụ | Vai trò | `node_id` | Cổng (Port) | URL Nội bộ Docker | URL Máy chủ (Host) |
| :--- | :--- | :---: | :---: | :--- | :--- |
| **Primary Node** | Điều phối & Ghi (SV1) | `primary` | `8000` | `http://primary:8000` | `http://localhost:8000` |
| **Backup Node 1** | Bản sao dự phòng 1 (SV2) | `backup-1` | `8001` | `http://backup1:8001` | `http://localhost:8001` |
| **Backup Node 2** | Bản sao dự phòng 2 (SV2) | `backup-2` | `8002` | `http://backup2:8002` | `http://localhost:8002` |
| **Client Proxy** | Middleware Client (SV3) | `proxy` | `8080` | `http://proxy:8080` | `http://localhost:8080` |

---

## 2. API CỦA PRIMARY COORDINATOR NODE (SINH VIÊN 1)

### 2.1. Ghi dữ liệu (`POST /api/v1/write`)
* **Mục đích:** Tiếp nhận yêu cầu `PUT` hoặc `DELETE`, cấp phát `version` tăng đơn điệu và điều phối nhân bản theo 3 chế độ (`SYNC`, `QUORUM`, `ASYNC`).
* **Request Body (JSON):**

    {
      "operation": "PUT",           // Bắt buộc: "PUT" hoặc "DELETE"
      "key": "user_101",            // Bắt buộc: Khóa dữ liệu
      "value": "Nguyen Van A",      // Bắt buộc nếu là PUT, để null nếu là DELETE
      "consistency": "QUORUM",      // Tuỳ chọn: "SYNC" (mặc định), "ASYNC", hoặc "QUORUM"
      "quorum_w": 2                 // Tuỳ chọn: Số node tối thiểu cần ACK khi chạy QUORUM (mặc định W=2)
    }

* **Response Thành công (`200 OK`):**

    {
      "status": "SUCCESS",
      "version": 12,
      "key": "user_101",
      "operation": "PUT",
      "value": "Nguyen Van A",
      "consistency": "QUORUM",
      "timestamp": 1728800020.123,
      "acks": 2,
      "replicated_nodes": ["primary", "backup-1"]
    }

* **Response Thất bại khi không đủ ACK ở chế độ `SYNC` / `QUORUM` (`503 Service Unavailable`):**

    {
      "detail": {
        "status": "WRITE_FAILED",
        "mode": "SYNC",
        "required_acks": 3,
        "actual_acks": 2,
        "replicated_nodes": ["primary", "backup-1"],
        "message": "Không đủ số node phản hồi ACK, thao tác đã được huỷ trên Primary."
      }
    }

### 2.2. Đọc dữ liệu (`GET /api/v1/read/{key}`)
* **Dùng chung một cấu trúc JSON trả về cho cả Primary (SV1) và Backup (SV2).**
* **Hỗ trợ Tombstone (Bia mộ):** Khi một `key` bị `DELETE` ở phiên bản `v`, hệ thống lưu dấu `{"value": null, "version": v, "deleted": true}` thay vì xóa trắng khỏi bộ nhớ.
* **Response (`200 OK`):**

    {
      "node_id": "primary",
      "key": "user_101",
      "value": "Nguyen Van A",
      "version": 8,                 // Phiên bản cập nhật gần nhất của riêng key này (0 nếu chưa từng tồn tại)
      "node_version": 12,           // Phiên bản tổng mới nhất ĐÃ COMMIT (current_version) của toàn Node
      "found": true,                // false nếu chưa từng tồn tại hoặc đã bị DELETE
      "deleted": false              // true nếu key đã bị xóa bằng lệnh DELETE
    }

### 2.3. Phục hồi dữ liệu sau sự cố (`GET /api/v1/catch-up?from_version={v}`)
* **Mục đích:** Dành cho Backup gọi khi khởi động lại hoặc khi phát hiện bị khuyết version (gap). Trả về danh sách các bản ghi **đã commit** có `version > from_version`.
* **Response (`200 OK`):**

    {
      "current_primary_version": 12,
      "missed_count": 2,
      "entries": [
        {
          "version": 11,
          "commit_version": 10,
          "operation": "PUT",
          "key": "user_101",
          "value": "Nguyen Van A",
          "timestamp": 1728800010.0
        },
        {
          "version": 12,
          "commit_version": 11,
          "operation": "DELETE",
          "key": "temp_key",
          "value": null,
          "timestamp": 1728800020.0
        }
      ]
    }

### 2.4. Kiểm tra sức khỏe (`GET /api/v1/health`)
* **Response (`200 OK`):**

    {
      "status": "ALIVE",            // "ALIVE", "CATCHING_UP" hoặc "DEGRADED"
      "node_id": "primary",
      "role": "PRIMARY",            // "PRIMARY" hoặc "BACKUP"
      "current_version": 12
    }

Với Backup, `status` có ngữ nghĩa cụ thể tại mục 3.3. Proxy phải dựa vào `node_version` để kiểm tra Read-Your-Writes và Monotonic Reads; `status` không thay thế phép so phiên bản.

---

## 3. API CỦA BACKUP REPLICA NODES (SINH VIÊN 2)

### 3.1. Nhận bản sao — Pha Prepare (`POST /api/v1/replicate`)
* **Request Body từ Primary gửi xuống Backup:**

    {
      "version": 8,
      "commit_version": 7,          // Version lớn nhất mà Primary ĐÃ COMMIT tính đến lúc gửi gói tin này
      "operation": "PUT",
      "key": "user_101",
      "value": "Nguyen Van A",
      "timestamp": 1728800020.0,
      "entry_id": "optional-primary-entry-id",
      "commit_entry_id": "optional-committed-entry-id"
    }

  *(Lưu ý: Với `SYNC`/`QUORUM`, `commit_version` bằng version đã commit liền trước. Với `ASYNC`, Primary đã commit trước khi gửi nên `commit_version == version`. Hai trường ID là tuỳ chọn; xem phần ĐỀ XUẤT bên dưới.)*
* **Quy trình xử lý tại Backup:**
  1. **Bước 1:** Áp dụng tiền tố liên tục trong `pending` đến `min(commit_version, incoming_v - 1)` vào `backup_wal.jsonl` và `storage` trước, đồng thời nâng `current_version`. Khi đến version đích, Backup kiểm tra `PENDING_TTL` và `commit_entry_id` nếu các trường này được gửi.
  2. **Bước 2:** Kiểm tra `incoming_v = entry["version"]` so với `current_version` (đã commit):
     * `incoming_v <= current_version`: Bản ghi trùng lặp -> Bỏ qua và trả `ACK`.
     * `incoming_v == current_version + 1`: Đặt vào `pending[incoming_v]` (ghi đè nếu có bản `pending` cũ bị huỷ của lần 503 trước). Nếu `commit_version >= incoming_v` (`ASYNC`), áp dụng luôn xuống WAL và `storage`. Trả `ACK`.
     * `incoming_v > current_version + 1` (Nhảy cóc version): Gọi `GET /api/v1/catch-up?from_version={current_version}` ở ngoài Lock mạng, nạp bù các bản ghi đã commit vào WAL/`storage`, xử lý `entry` hiện tại rồi trả `ACK` nếu đã lấp được gap.
  3. Nếu không lấp được gap, Backup trả NACK; trong `/replicate`, Backup chỉ chờ tác vụ catch-up tối đa `REPLICATE_CATCHUP_DEADLINE` (mặc định 1.5 giây). Primary hiện chờ ACK tối đa 2 giây.
  4. Pending đã chờ quá `PENDING_TTL` (mặc định 3 giây) bị bỏ và Backup lấy lại dữ liệu chuẩn từ Primary. Đây là heuristic: pending cũ vẫn có thể bị commit nếu commit đến trong cửa sổ TTL.
* **Response từ Backup về Primary (`200 OK`):**

    {
      "status": "ACK",
      "node_id": "backup-1",
      "applied_version": 8
    }

* **Response từ chối (`503 Service Unavailable`):**

    {
      "status": "NACK",
      "node_id": "backup-1",
      "reason": "GAP_NOT_FILLED",
      "current_version": 7
    }

  `reason` hiện có thể là `GAP_NOT_FILLED`, `CATCHING_UP` (đã chạm deadline khi tác vụ catch-up vẫn chạy), hoặc `DIVERGED` (Primary báo version thấp hơn lịch sử đã lưu của Backup). Bản ghi trùng version được ACK nếu Backup chưa bị phân kỳ.

> **ĐỀ XUẤT (chờ SV1):** Primary thêm `entry_id` vào log entries gửi qua `/replicate` và trả từ `/catch-up`; gửi `commit_entry_id` trong `/replicate` và `/commit` để Backup nhận biết pending thuộc đúng lần ghi. Backup đã chấp nhận các trường tuỳ chọn này, nhưng bỏ qua kiểm tra danh tính khi chúng vắng mặt. Cần SV1 đồng ý trước khi Primary phát các trường này.

### 3.2. Xác nhận Commit — Pha Commit (`POST /api/v1/commit`)
* **Mục đích:** Ngay sau khi Primary gom đủ ACK và commit thành công một lệnh `SYNC` hoặc `QUORUM`, Primary bắn ngầm thông báo này sang các Backup.
* **Request Body:**

    {
      "commit_version": 8,
      "commit_entry_id": "optional-committed-entry-id"
    }

* **Hành vi tại Backup:** Áp dụng tiền tố liên tục từ `current_version + 1` đến `commit_version`; dừng ở version bị thiếu, không nhảy qua gap. Nếu thiếu pending hoặc phát hiện pending đã cũ, Backup vẫn trả `COMMITTED` rồi bắt đầu catch-up nền. Commit đến muộn hoặc commit có version không lớn hơn `current_version` được bỏ qua (idempotent). Khi Backup đã phân kỳ, commit không thay đổi trạng thái và không khởi chạy catch-up.
* **Response (`200 OK`):**

    {
      "status": "COMMITTED",
      "node_id": "backup-1",
      "commit_version": 8
    }

> **ĐỀ XUẤT (chờ SV1):** `commit_entry_id` là trường tuỳ chọn đã được Backup chấp nhận. Primary cần gửi ID khớp với entry ở `commit_version` để phát hiện pending của lần ghi đã bị huỷ.

### 3.3. Đọc dữ liệu từ Backup (`GET /api/v1/read/{key}`)
* Trả về JSON giống hệt Mục 2.2 (`node_version` chỉ tính các bản ghi đã commit).
* `CATCHING_UP`: khi catch-up đang diễn ra, trả về **`503 Service Unavailable`** (`{"detail": "NODE_CATCHING_UP"}`).
* `DEGRADED`: trả `200 OK` từ dữ liệu cục bộ và kèm `node_version` thật. Trạng thái này xuất hiện sau `PRIMARY_FAILURES_BEFORE_DEGRADED` lần liên tiếp không liên lạc được Primary (mặc định 5), hoặc khi phát hiện Primary có version thấp hơn Backup. Sau khi phát hiện phân kỳ, Backup giữ `DEGRADED` đến khi tiến trình khởi động lại; không nhận replicate và không áp dụng catch-up.
* `ALIVE`: Backup sẵn sàng đọc và xử lý replicate bình thường.

SV3 cần dựa vào `node_version`, không dựa vào `status`, để bảo đảm Read-Your-Writes và Monotonic Reads.

### 3.4. Đồng bộ định kỳ (anti-entropy)

Backup gọi `GET /api/v1/health` của Primary theo chu kỳ `SYNC_INTERVAL` (mặc định 5 giây). Nếu Primary có version mới hơn, Backup gọi catch-up để nạp các bản ghi đã commit còn thiếu. Vòng lặp cũng tiếp tục thử catch-up khi Backup đang `CATCHING_UP` hoặc `DEGRADED`; Backup đã phân kỳ không áp dụng thêm dữ liệu.

### 3.5. Trạng thái debug (`GET /api/v1/debug/state`)

Endpoint này chỉ phục vụ kiểm thử và debug, không thuộc API contract cho Proxy hay Client.

    {
      "status": "ALIVE",
      "current_version": 12,
      "pending": [],
      "key_count": 4,
      "wal_path": "data/backup_wal.jsonl",
      "diverged": false,
      "last_catchup": {
        "time": 1728800020.123,
        "entries_applied": 2,
        "primary_version": 12,
        "last_error": null
      }
    }

`diverged` cho biết Backup đã phát hiện lịch sử của Primary thấp hơn WAL cục bộ hay chưa. Giá trị này chỉ được đặt trong tiến trình hiện tại và reset khi tiến trình khởi động lại.

---

## 4. QUY TẮC NHẤT QUÁN TẠI CLIENT PROXY (SINH VIÊN 3)

1. **Theo dõi Phiên (Session Tracking):** Lưu `last_written_version`, `last_read_version` và `local_cache`.
2. **Khi Ghi (`PUT` / `DELETE`):** Gửi lên `POST {PRIMARY_URL}/api/v1/write`. Nếu nhận `200 OK`, cập nhật `last_written_version = resp["version"]` và lưu vào `local_cache`.
3. **Khi Đọc (`GET`):**
   * Tính phiên bản tối thiểu cần đạt: `required_version = max(last_written_version, last_read_version)`.
   * Gọi `GET /api/v1/read/{key}` tới một Backup Node (`timeout = 2.0s`).
   * **Failover:** Nếu Backup timeout hoặc trả về `503` -> Thử Backup còn lại hoặc gọi thẳng vào Primary.
   * **Kiểm tra Read-Your-Writes & Monotonic Reads:** So sánh trường **`"node_version"`** của node trả về với `required_version`:
     * Nếu `resp["node_version"] < required_version`: Từ chối dữ liệu cũ, gọi đọc trực tiếp từ Primary (hoặc lấy từ `local_cache`).
     * Nếu `resp["node_version"] >= required_version`: Hợp lệ -> Cập nhật `last_read_version = max(last_read_version, resp["node_version"])` và trả kết quả cho Client.
