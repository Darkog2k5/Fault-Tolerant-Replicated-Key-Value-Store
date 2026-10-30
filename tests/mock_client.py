import httpx
import time

PRIMARY_URL = "http://localhost:8000"

def run_tests():
    print("=== 1. KIỂM TRA HEALTH CỦA PRIMARY ===")
    h = httpx.get(f"{PRIMARY_URL}/api/v1/health")
    print("Health:", h.json())

    print("\n=== 2. TEST GHI 3 CHẾ ĐỘ (SYNC, QUORUM, ASYNC) ===")
    for i, mode in enumerate(["SYNC", "QUORUM", "ASYNC"], 1):
        payload = {
            "operation": "PUT",
            "key": f"key_{i}",
            "value": f"val_{mode}",
            "consistency": mode
        }
        t0 = time.time()
        resp = httpx.post(f"{PRIMARY_URL}/api/v1/write", json=payload)
        dt = (time.time() - t0) * 1000
        print(f"[{mode}] HTTP {resp.status_code} ({dt:.1f}ms) -> {resp.json()}")

    print("\n=== 3. TEST ĐỌC DỮ LIỆU ===")
    r = httpx.get(f"{PRIMARY_URL}/api/v1/read/key_1")
    print("Read key_1:", r.json())

    print("\n=== 4. TEST XÓA (TOMBSTONE) & ĐỌC LẠI ===")
    del_resp = httpx.post(f"{PRIMARY_URL}/api/v1/write", json={
        "operation": "DELETE",
        "key": "key_1",
        "consistency": "SYNC"
    })
    print("Delete key_1:", del_resp.json())
    r_after = httpx.get(f"{PRIMARY_URL}/api/v1/read/key_1")
    print("Read key_1 sau khi xóa:", r_after.json())

    print("\n=== 5. TEST API CATCH-UP (CHO SV2) ===")
    catchup_resp = httpx.get(f"{PRIMARY_URL}/api/v1/catch-up?from_version=1")
    print("Catch-up từ version 1:", catchup_resp.json())

if __name__ == "__main__":
    run_tests()
    