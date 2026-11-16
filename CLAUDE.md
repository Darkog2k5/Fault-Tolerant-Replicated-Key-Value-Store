# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Course project (Distributed Systems, Track C): a primary-backup replicated key-value store with hand-written JSONL write-ahead logs and three write consistency modes (`SYNC`, `QUORUM`, `ASYNC`). Docs, comments, log messages and test docstrings are written in Vietnamese; keep that convention.

Ownership is split by directory between three students. Stay inside the owner's directory unless asked otherwise, and treat changes to the wire format as something the whole group must agree on:

| Directory | Owner | Status |
| :--- | :--- | :--- |
| `primary_node/` | SV1 — coordinator, version allocation, replication fan-out, `/catch-up` | implemented, no tests of its own |
| `backup_node/` | SV2 — replica WAL, prepare/commit, gap detection, catch-up recovery | implemented, with tests |
| `client_proxy/` | SV3 — session tracking, Read-Your-Writes, Monotonic Reads, failover | stub (only `/health`) |

`PROTOCOL_SPEC.md` (v4.1) is the API contract between the three components; section 4 holds the consistency rules the proxy still has to implement. `REVIEW_SINH_VIEN_2.md` is the backup owner's design review: sections 4–5 explain why the backup behaves as it does, section 6 is the test scenario table, sections 8–9 list open issues outside the backup that the group has not settled.

## Commands

A virtualenv lives in `.venv` (Windows: `.venv\Scripts\python.exe`). There is no linter or build step.

```bash
python -m pip install -r tests/requirements.txt

python -m pytest tests -q                         # everything (~1 min)
python -m pytest tests -q -m "not integration"    # unit tests only, fast
python -m pytest tests -q -m integration          # real-process cluster tests
python -m pytest tests/test_backup_node.py::test_commit_is_idempotent -q   # single test

python tests/bench_recovery.py --sizes 10,100 [--mode QUORUM]   # catch-up time / read latency

docker compose up --build -d      # primary:8000, backup1:8001, backup2:8002, proxy:8080
docker compose down               # keeps WAL volumes
docker compose down -v            # wipes all three WALs (versions restart at 0)
python tests/mock_client.py       # smoke script against a running Primary on :8000
```

Running nodes locally without Docker: each node is `python <dir>/main.py` configured purely by env vars (`NODE_ID`, `PORT`, `DATA_DIR`, plus `REPLICAS` for the primary or `PRIMARY_URL` for a backup). Every node needs its own `DATA_DIR`, otherwise two backups share one WAL file. README sections 4 and 6 have copy-paste command lines and the full backup env-var table.

Each node directory is a flat script directory, not a package: modules import each other as `from config import ...` / `from node import ...`. `tests/conftest.py` puts `backup_node/` on `sys.path`; the primary is only ever exercised as a subprocess started with `cwd=primary_node/`.

Windows: when a node's stdout is redirected to a file, set `PYTHONIOENCODING=utf-8` (the test harness does) — the Primary uses bare `print` with Vietnamese text. The venv's `python.exe` is a launcher that spawns the real interpreter, so anything that suspends or kills a node must act on the whole process tree (see `Cluster._process_tree`).

## Architecture

### Write path (two-phase for SYNC/QUORUM)

The Primary serializes all writes under one `write_lock` and assigns `version = current_version + 1`.

- **SYNC / QUORUM**: the entry is sent to both backups (`/replicate`, prepare) with `commit_version = current_version` (the previous committed version) and the Primary waits up to 2 s, one attempt, for ACKs. Enough ACKs → the Primary appends to its WAL, bumps `current_version`, and fires `/commit` as an un-awaited, un-retried background task. Not enough ACKs → HTTP 503, nothing is logged, and **the same version number is reused by the next write**.
- **ASYNC**: the Primary commits first, then enqueues the entry on a per-backup FIFO worker with `commit_version == version`, so the backup commits on receipt.

ACK counts include the Primary itself: `SYNC` needs `1 + len(backups)` (one backup down fails every SYNC write), `QUORUM` needs `quorum_w` (default 2 = Primary + one backup), `ASYNC` always reports `acks: 1`.

Consequences the backup is built around:

- A backup may hold a prepared entry for a write the Primary aborted. A re-send of the same version overwrites it; if the re-send is missed, the stale entry could be committed by a later `commit_version`. The Primary does not send an `entry_id`, so the backup falls back to `PENDING_TTL` (a pending entry older than 3 s is discarded and re-fetched via catch-up). The `entry_id` / `commit_entry_id` fields in the backup's request models are an optional, forward-compatible proposal — inert until the Primary sends them.
- ASYNC entries go through the queue while SYNC/QUORUM are sent directly, so mixed-mode traffic reaches a backup out of order and routinely triggers gap handling.
- `/commit` messages and ASYNC entries (dropped after 2 failed sends) can be lost with no further write to repair them; the backup's periodic anti-entropy loop covers this.

### Primary node

All state is module-level globals in `primary_node/main.py` (`current_version`, `storage`, `write_log`); `replication.py` holds the fan-out and the ASYNC queues. `/catch-up` is served from the in-memory `write_log`, which is the full committed history rebuilt from the WAL at startup. Primary WAL lines carry the `commit_version` field from the replicate payload, and catch-up returns them as-is; the backup strips it (`_wal_entry`) before writing its own WAL. Unlike the backup, Primary startup tolerates a torn last line but does not check version contiguity.

### Backup node

`backup_node/main.py` is only the FastAPI layer (`create_app(node)` factory, Pydantic models, HTTP status mapping). All logic is in the `BackupNode` class in `node.py`, which takes an injectable `httpx` transport/client — unit tests drive it through `TestClient` with a `FakePrimary` on `httpx.MockTransport`.

Invariants to preserve when changing `node.py`:

- The WAL holds exactly versions `1..current_version`, contiguous; WAL append + fsync happens before the in-memory update. Startup truncates a torn last line but refuses to start on a corrupt middle line or a version gap.
- `current_version` / `node_version` count committed entries only; `pending` never advances them. The proxy's consistency checks depend on this.
- ACK only if the entry is in `pending` or already committed, and the backup has not diverged. Otherwise answer 503 with a top-level body `{"status": "NACK", "reason": "GAP_NOT_FILLED" | "CATCHING_UP" | "DIVERGED", ...}` (the Primary treats anything other than 200 + `ACK` as a failure). Note the read path differs: 503 there uses FastAPI's `{"detail": "NODE_CATCHING_UP"}` shape.
- Catch-up is single-flight (`_get_or_start_catchup`), the HTTP call is made outside `self.lock`, and a replicate request waits for it at most `REPLICATE_CATCHUP_DEADLINE` (1.5 s) so it answers inside the Primary's 2 s timeout.
- State machine: `CATCHING_UP` (reads return 503) → `ALIVE`; after `PRIMARY_FAILURES_BEFORE_DEGRADED` consecutive failures, or when the Primary reports a lower version than the backup, the node becomes `DEGRADED` and serves local data with its true `node_version`. A `DEGRADED` node still ACKs in-order replicates. The divergence flag (Primary behind backup) is sticky until the process restarts: replicate answers `DIVERGED`, commit is a no-op, and catch-up applies nothing.

The NACK body, `DEGRADED` semantics, `PENDING_TTL`, anti-entropy and `GET /api/v1/debug/state` are specified in `PROTOCOL_SPEC.md` section 3.

### Client proxy

`client_proxy/proxy.py` is a stub. `docker-compose.yml` already passes it `PRIMARY_URL` and `BACKUP_URLS`. Backup and Primary `/read` responses share one shape (`version` = the key's version, `node_version` = the node's committed version, tombstones as `found: false, deleted: true`), which is what the proxy's Read-Your-Writes / Monotonic Reads checks are meant to compare against.

### Tests

- `tests/test_backup_node.py` — unit tests, in-process, no sockets. Helpers `make_node` / `open_client` / `start_alive` build a node with short timeouts and a long `pending_ttl`; override per test via keyword options.
- `tests/test_backup_cluster.py` — marked `integration`; `cluster_harness.Cluster` runs a real Primary and two Backups as subprocesses on free localhost ports with per-test data directories, and kills/suspends processes to simulate failures (T7 needs `psutil`). Test names `test_t<N>_...` follow the scenario table in section 6 of `REVIEW_SINH_VIEN_2.md`; tests self-skip when the environment cannot bind sockets or spawn subprocesses. Node logs for a failing test are in `tmp_path/logs/`.
- `tests/mock_backup.py` — an always-ACK backup with no WAL, intended only for proxy failover experiments.
