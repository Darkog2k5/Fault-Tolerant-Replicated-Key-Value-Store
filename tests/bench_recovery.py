"""Đo thời gian Backup bắt kịp và độ trễ đọc trong cụm tiến trình thật."""

from __future__ import annotations

import argparse
import math
import statistics
import tempfile
import time

from cluster_harness import Cluster


def parse_sizes(value: str) -> list[int]:
    """Đọc danh sách kích thước benchmark từ tham số dòng lệnh."""
    try:
        sizes = [int(item.strip()) for item in value.split(",") if item.strip()]
    except ValueError as error:
        raise argparse.ArgumentTypeError("sizes phải là các số nguyên, cách nhau bằng dấu phẩy") from error
    if not sizes or any(size <= 0 for size in sizes):
        raise argparse.ArgumentTypeError("mỗi size phải là số nguyên dương")
    return sizes


def percentile95(samples: list[float]) -> float:
    """Tính p95 theo nearest-rank, với mẫu đã đo bằng mili giây."""
    ordered = sorted(samples)
    return ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)]


def read_latency(cluster: Cluster, node: str, key: str, count: int = 200):
    """Đo trung bình và p95 của các lần đọc HTTP liên tiếp."""
    samples = []
    for _ in range(count):
        started = time.perf_counter()
        response = cluster.read(node, key)
        elapsed_ms = (time.perf_counter() - started) * 1000
        if response.status_code != 200:
            raise RuntimeError(f"Đọc {node}/{key} lỗi HTTP {response.status_code}: {response.text}")
        samples.append(elapsed_ms)
    return statistics.mean(samples), percentile95(samples)


def run_size(size: int, mode: str):
    """Tạo cụm mới, làm Backup 1 tụt version rồi đo phục hồi."""
    with tempfile.TemporaryDirectory(prefix="kv-recovery-") as temp_dir:
        cluster = Cluster(temp_dir)
        try:
            cluster.check_runtime()
            cluster.start_primary()
            cluster.wait_health("primary", status="ALIVE")
            cluster.start_backup(1)
            cluster.start_backup(2)
            cluster.wait_health("backup-1", status="ALIVE")
            cluster.wait_health("backup-2", status="ALIVE")
            cluster.stop_backup(1)

            for index in range(size):
                response = cluster.write(
                    f"bench-{index}", f"value-{index}", consistency=mode
                )
                if response.status_code != 200:
                    raise RuntimeError(
                        f"Ghi {mode} thứ {index + 1}/{size} thất bại "
                        f"(HTTP {response.status_code}): {response.text}"
                    )

            target_version = cluster.health("primary")["current_version"]
            started = cluster.start_backup(1)
            cluster.wait_health(
                "backup-1", status="ALIVE", version=target_version, timeout=90.0
            )
            recovery_seconds = time.perf_counter() - started
            cluster.wait_health("backup-2", status="ALIVE", version=target_version)

            key = f"bench-{size - 1}"
            backup_mean, backup_p95 = read_latency(cluster, "backup-1", key)
            primary_mean, primary_p95 = read_latency(cluster, "primary", key)
            return {
                "size": size,
                "mode": mode,
                "recovery": recovery_seconds,
                "backup_mean": backup_mean,
                "backup_p95": backup_p95,
                "primary_mean": primary_mean,
                "primary_p95": primary_p95,
            }
        finally:
            cluster.close()


def main():
    parser = argparse.ArgumentParser(description="Đo phục hồi và độ trễ đọc của Backup Node")
    parser.add_argument(
        "--sizes",
        type=parse_sizes,
        default=[10, 100, 1000],
        help="số bản ghi bị lỡ, phân tách bằng dấu phẩy (mặc định: 10,100,1000)",
    )
    parser.add_argument(
        "--mode",
        choices=("ASYNC", "QUORUM"),
        default="ASYNC",
        help="chế độ ghi trong lúc Backup 1 dừng (SYNC không thể commit khi một Backup dừng)",
    )
    args = parser.parse_args()

    results = [run_size(size, args.mode) for size in args.sizes]
    print("| Bản ghi bị lỡ | Chế độ | Phục hồi (giây) | Backup TB (ms) | Backup p95 (ms) | Primary TB (ms) | Primary p95 (ms) |")
    print("| ---: | :--- | ---: | ---: | ---: | ---: | ---: |")
    for result in results:
        print(
            f"| {result['size']} | {result['mode']} | {result['recovery']:.4f} "
            f"| {result['backup_mean']:.3f} | {result['backup_p95']:.3f} "
            f"| {result['primary_mean']:.3f} | {result['primary_p95']:.3f} |"
        )


if __name__ == "__main__":
    main()
