import os
import time
import httpx

PRIMARY_URL = os.getenv("PRIMARY_URL", "http://localhost:8000")
PROXY_URL = os.getenv("PROXY_URL", "http://localhost:8080")


def run_tests():
    print("=== 1. KIỂM TRA HEALTH CỦA PRIMARY ===")
    h = httpx.get(f"{PRIMARY_URL}/api/v1/health", timeout=3.0)
    print("Health:", h.json())

    print("\n=== 2. TEST GHI 3 CHẾ ĐỘ TRỰC TIẾP TRÊN PRIMARY (SYNC, QUORUM, ASYNC) ===")
    for i, mode in enumerate(["SYNC", "QUORUM", "ASYNC"], 1):
        payload = {
            "operation": "PUT",
            "key": f"key_{i}",
            "value": f"val_{mode}",
            "consistency": mode,
        }
        t0 = time.time()
        resp = httpx.post(f"{PRIMARY_URL}/api/v1/write", json=payload, timeout=5.0)
        dt = (time.time() - t0) * 1000
        print(f"[{mode}] HTTP {resp.status_code} ({dt:.1f}ms) -> {resp.json()}")

    print("\n=== 3. TEST ĐỌC DỮ LIỆU TRÊN PRIMARY ===")
    r = httpx.get(f"{PRIMARY_URL}/api/v1/read/key_1", timeout=3.0)
    print("Read key_1:", r.json())

    print("\n=== 4. TEST XÓA (TOMBSTONE) & ĐỌC LẠI ===")
    del_resp = httpx.post(
        f"{PRIMARY_URL}/api/v1/write",
        json={"operation": "DELETE", "key": "key_1", "consistency": "SYNC"},
        timeout=5.0,
    )
    print("Delete key_1:", del_resp.json())
    r_after = httpx.get(f"{PRIMARY_URL}/api/v1/read/key_1", timeout=3.0)
    print("Read key_1 sau khi xóa:", r_after.json())

    print("\n=== 5. TEST API CATCH-UP (CHO SV2) ===")
    catchup_resp = httpx.get(f"{PRIMARY_URL}/api/v1/catch-up?from_version=1", timeout=3.0)
    print("Catch-up từ version 1:", catchup_resp.json())

    print("\n=== 6. TEST SMART CLIENT PROXY (CHO SV3) ===")
    try:
        ph = httpx.get(f"{PROXY_URL}/api/v1/health", timeout=2.0)
    except Exception as e:
        print(f"[!] Bỏ qua test Proxy vì {PROXY_URL} chưa khởi chạy ({e})")
        return

    print("Proxy Health:", ph.json())

    for mode in ("SYNC", "QUORUM", "ASYNC"):
        w_resp = httpx.post(
            f"{PROXY_URL}/api/v1/write",
            json={
                "operation": "PUT",
                "key": f"proxy_key_{mode.lower()}",
                "value": f"proxy_val_{mode}",
                "consistency": mode,
            },
            timeout=5.0,
        )
        print(f"[Proxy Write {mode}] HTTP {w_resp.status_code} -> {w_resp.json()}")

    for mode in ("SYNC", "QUORUM", "ASYNC"):
        r_resp = httpx.get(
            f"{PROXY_URL}/api/v1/read/proxy_key_{mode.lower()}", timeout=3.0
        )
        src = r_resp.headers.get("X-Proxy-Source", "unknown")
        print(f"[Proxy Read {mode} via {src}] -> {r_resp.json()}")

    sess_resp = httpx.get(f"{PROXY_URL}/api/v1/session", timeout=2.0)
    print("Proxy Session & Stats:", sess_resp.json())


if __name__ == "__main__":
    run_tests()
