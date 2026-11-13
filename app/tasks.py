"""任务去重、租约选举、执行接续与预算事务。

进程模型：两个 uvicorn 进程共享同一个 SQLite 文件，协调全部由数据库完成，
不依赖内存锁或第三方队列。

关键不变量：
1. 每个 (canonical_key, epsilon) 至多一个 active 任务（部分唯一索引强制），
   相同查询的并发请求只会有一个执行者，其余成为跟随者复用在途任务。
2. 预算检查与预留放在同一个 BEGIN IMMEDIATE 事务中，跨进程串行化。
3. 加噪结果、任务成功、预留转实扣在同一事务提交；提交前绝不返回，
   响应丢失后重试命中缓存，拿到的是同一份噪声，不会重新扰动、不重复扣费。
4. 执行进程靠心跳续租；租约过期后其他进程才能接管（generation +1），
   存活进程的心跳会持续推高 lease_expires_at，因此不会被接管。
5. 失败只记结果为 failed 并退还预留；不写 query_results，重试才会重新采样噪声。
"""
from __future__ import annotations

import json
import logging
import os
import socket
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from . import budget as budget_mod
from . import config, privacy
from .db import get_conn, write_tx
from .models import canonicalize, dumps_stations

logger = logging.getLogger("dp_transit.tasks")


class BudgetExhausted(Exception):
    """剩余预算不足以支付本次预留。"""


class NotImported(Exception):
    """尚未导入乘车数据。"""


class QueryTimeout(Exception):
    """等待在途任务完成超时。"""


class LeaseLost(Exception):
    """执行租约被其他进程接管。"""


# 进程级工作者标识：主机名 + PID + 随机后缀，重启后必然变化。
WORKER_ID = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _find_cached(conn: sqlite3.Connection, key: str, epsilon: float) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM query_results WHERE canonical_key = ? AND epsilon = ?",
        (key, epsilon),
    ).fetchone()


def _find_active_task(conn: sqlite3.Connection, key: str, epsilon: float) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM tasks WHERE canonical_key = ? AND epsilon = ? AND active = 1",
        (key, epsilon),
    ).fetchone()


def _ensure_imported(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT imported FROM meta WHERE id = 1").fetchone()
    if row is None or row["imported"] != 1:
        raise NotImported("尚未导入乘车 CSV，请先调用 /import")


def _heartbeat(task_id: int, generation: int, stop_event: threading.Event,
               lost_event: threading.Event) -> None:
    """后台续租线程：存活执行期间持续推高租约到期时间。"""
    while not stop_event.wait(config.HEARTBEAT_SECONDS):
        try:
            with get_conn() as conn:
                with write_tx(conn):
                    cur = conn.execute(
                        "UPDATE tasks SET lease_expires_at = ?, updated_at = ? "
                        "WHERE id = ? AND worker_id = ? AND generation = ? AND active = 1",
                        (time.time() + config.LEASE_SECONDS, _utcnow(),
                         task_id, WORKER_ID, generation),
                    )
                    if cur.rowcount == 0:
                        lost_event.set()
                        return
        except Exception:  # 写锁持续不可用等：视为可能失租，主流程提交前会复核
            logger.exception("heartbeat failed for task %s", task_id)
            lost_event.set()
            return


def _elect(key: str, epsilon: float, stations: list[str],
           stations_json: str) -> tuple[str, sqlite3.Row]:
    """一次选举事务，返回 ('leader', task_row) 或 ('follower', task_row)。

    可能抛出 BudgetExhausted / sqlite3.IntegrityError（调用方重试）。
    """
    with get_conn() as conn:
        with write_tx(conn):
            _ensure_imported(conn)
            if _find_cached(conn, key, epsilon) is not None:
                # 事务内再次确认缓存，避免与刚刚提交的执行者重复建任务。
                raise _CachedDuringElection()

            now = time.time()
            task = _find_active_task(conn, key, epsilon)
            if task is not None:
                if task["lease_expires_at"] > now and task["active"] == 1:
                    return "follower", task
                # 租约过期：原子接管（条件 UPDATE 防止与其他接管者竞争）。
                cur = conn.execute(
                    "UPDATE tasks SET worker_id = ?, generation = generation + 1, "
                    "status = 'running', lease_expires_at = ?, updated_at = ?, error = NULL "
                    "WHERE id = ? AND active = 1 AND lease_expires_at <= ?",
                    (WORKER_ID, now + config.LEASE_SECONDS, _utcnow(),
                     task["id"], now),
                )
                if cur.rowcount == 1:
                    row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task["id"],)).fetchone()
                    return "leader", row
                raise _ElectionRetry()

            # 无活跃任务：先验预算，再在同一事务里建任务 + 写预留账本。
            if not budget_mod.has_capacity(conn, epsilon):
                raise BudgetExhausted(
                    f"预算不足：需要预留 {epsilon}，"
                    f"当前剩余 {budget_mod.get_snapshot(conn).remaining}"
                )
            conn.execute(
                "INSERT INTO tasks (canonical_key, epsilon, stations_json, status, active, "
                "worker_id, generation, reserved_epsilon, lease_expires_at, created_at, updated_at) "
                "VALUES (?, ?, ?, 'running', 1, ?, 1, ?, ?, ?, ?)",
                (key, epsilon, stations_json, WORKER_ID, epsilon,
                 now + config.LEASE_SECONDS, _utcnow(), _utcnow()),
            )
            task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.execute(
                "INSERT INTO budget_ledger (task_id, entry_type, epsilon, amount, created_at) "
                "VALUES (?, 'reserve', ?, ?, ?)",
                (task_id, epsilon, epsilon, _utcnow()),
            )
            row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            return "leader", row


class _CachedDuringElection(Exception):
    """选举事务内发现结果已被其他进程提交。"""


class _ElectionRetry(Exception):
    """接管竞争失败，需要重新选举。"""


def _execute_as_leader(task: sqlite3.Row, epsilon: float, stations: list[str]) -> None:
    """领导者执行：统计（只读）→ 加噪 → 与预算转实扣同事务持久化。"""
    task_id = task["id"]
    generation = task["generation"]
    stop_event = threading.Event()
    lost_event = threading.Event()
    heartbeat = threading.Thread(
        target=_heartbeat, args=(task_id, generation, stop_event, lost_event),
        name=f"hb-task-{task_id}", daemon=True,
    )
    heartbeat.start()
    try:
        # 真值统计：独立短连接、只读 SELECT，不占用任何写事务。
        placeholders = ",".join("?" for _ in stations)
        with get_conn() as read_conn:
            row = read_conn.execute(
                f"SELECT COUNT(DISTINCT passenger_id) AS c FROM rides "
                f"WHERE station IN ({placeholders})",
                stations,
            ).fetchone()
        true_count = int(row["c"])
        if config.SIMULATED_EXEC_DELAY > 0:
            # 演示用：在“已预留、未提交”的窗口停留，便于观察在途任务与崩溃接续。
            time.sleep(config.SIMULATED_EXEC_DELAY)
        # 噪声独立采样；真值到此为止，不入库、不记录日志。
        noisy_count = privacy.add_laplace_noise(true_count, epsilon)

        if lost_event.is_set():
            raise LeaseLost(f"task {task_id} 租约已丢失，放弃提交")

        with get_conn() as conn:
            with write_tx(conn):
                # 提交前复核：任务仍活跃且归属未变（双保险，防 ABA）。
                owner = conn.execute(
                    "SELECT * FROM tasks WHERE id = ? AND active = 1", (task_id,)
                ).fetchone()
                if owner is None or owner["worker_id"] != WORKER_ID \
                        or owner["generation"] != generation:
                    raise LeaseLost(f"task {task_id} 归属已变更")
                conn.execute(
                    "INSERT INTO query_results (canonical_key, epsilon, stations_json, "
                    "noisy_count, created_at, created_by_worker) VALUES (?, ?, ?, ?, ?, ?)",
                    (task["canonical_key"], epsilon, task["stations_json"],
                     noisy_count, _utcnow(), WORKER_ID),
                )
                conn.execute(
                    "UPDATE tasks SET status = 'succeeded', active = 0, updated_at = ? "
                    "WHERE id = ?",
                    (_utcnow(), task_id),
                )
                # 冲销预留（release -eps）+ 实扣（commit -eps），与结果同一事务。
                conn.execute(
                    "INSERT INTO budget_ledger (task_id, entry_type, epsilon, amount, created_at) "
                    "VALUES (?, 'release', ?, ?, ?)",
                    (task_id, epsilon, -epsilon, _utcnow()),
                )
                conn.execute(
                    "INSERT INTO budget_ledger (task_id, entry_type, epsilon, amount, created_at) "
                    "VALUES (?, 'commit', ?, ?, ?)",
                    (task_id, epsilon, -epsilon, _utcnow()),
                )
        logger.info("task %s committed by %s", task_id, WORKER_ID)
    except Exception as exc:
        # 失败路径：仅当任务仍归本进程所有时，才标记 failed 并退还预留。
        logger.warning("task %s failed: %r", task_id, exc)
        stop_event.set()
        try:
            with get_conn() as conn:
                with write_tx(conn):
                    cur = conn.execute(
                        "UPDATE tasks SET status = 'failed', active = 0, error = ?, updated_at = ? "
                        "WHERE id = ? AND worker_id = ? AND generation = ? AND active = 1",
                        (repr(exc)[:500], _utcnow(), task_id, WORKER_ID, generation),
                    )
                    if cur.rowcount == 1:
                        conn.execute(
                            "INSERT INTO budget_ledger (task_id, entry_type, epsilon, amount, created_at) "
                            "VALUES (?, 'release', ?, ?, ?)",
                            (task_id, epsilon, -epsilon, _utcnow()),
                        )
        except Exception:
            logger.exception("failure cleanup error for task %s", task_id)
        if isinstance(exc, LeaseLost):
            return  # 接管者负责后续，本请求转入轮询
    else:
        stop_event.set()


def _poll(key: str, epsilon: float, task_id: int,
          deadline: float) -> tuple[Optional[sqlite3.Row], str]:
    """只读轮询，返回 (缓存行或None, 状态)。

    状态取值：cached / failed / lease_expired / waiting。
    """
    with get_conn() as conn:
        cached = _find_cached(conn, key, epsilon)
        if cached is not None:
            return cached, "cached"
        task = conn.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        if task is not None and task["active"] == 0 and task["status"] == "failed":
            return None, "failed"
        if task is not None and task["active"] == 1 \
                and task["lease_expires_at"] <= time.time():
            return None, "lease_expired"
    return None, "waiting"


def submit_query(stations_in: list[str], epsilon: float) -> dict:
    """查询入口：缓存复用 → 选举 → 执行/跟随 → 轮询，全程保证上述不变量。"""
    key, stations = canonicalize(stations_in)
    stations_json = dumps_stations(stations)
    deadline = time.time() + config.QUERY_WAIT_TIMEOUT_SECONDS

    # 快速路径：已持久化的结果直接复用，不建任务、不扣费、不重新扰动。
    with get_conn() as conn:
        _ensure_imported(conn)
        cached = _find_cached(conn, key, epsilon)
        if cached is not None:
            return _result_dict(cached, reused=True, generation=None)

    while time.time() < deadline:
        try:
            role, task = _elect(key, epsilon, stations, stations_json)
        except _CachedDuringElection:
            with get_conn() as conn:
                cached = _find_cached(conn, key, epsilon)
            if cached is not None:
                return _result_dict(cached, reused=True, generation=None)
            continue
        except _ElectionRetry:
            time.sleep(config.POLL_INTERVAL_SECONDS)
            continue

        if role == "leader":
            _execute_as_leader(task, epsilon, stations)

        # 领导者执行完（或跟随者）：只读轮询等待持久化结果。
        while time.time() < deadline:
            cached, state = _poll(key, epsilon, task["id"], deadline)
            if cached is not None:
                gen = task["generation"] if role == "leader" else task["generation"]
                return _result_dict(cached, reused=(role == "follower"), generation=gen)
            if state in ("failed", "lease_expired"):
                # 失败已退还预留，或持有者租约过期：回到外层重新选举/接管。
                break
            time.sleep(config.POLL_INTERVAL_SECONDS)
        # 失败、租约过期或等待超时：重新走选举流程
        time.sleep(config.POLL_INTERVAL_SECONDS)

    raise QueryTimeout("等待查询结果超时")


def _result_dict(row: sqlite3.Row, reused: bool, generation: Optional[int]) -> dict:
    return {
        "status": "completed",
        "noisy_count": float(row["noisy_count"]),
        "epsilon": float(row["epsilon"]),
        "stations": json.loads(row["stations_json"]),
        "canonical_key": row["canonical_key"],
        "reused": reused,
        "worker_id": row["created_by_worker"],
        "task_generation": generation,
        "charged_epsilon": float(row["epsilon"]) if not reused else 0.0,
    }


def list_tasks(limit: int = 100) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM tasks ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    out = []
    for t in rows:
        cached = None
        if t["status"] == "succeeded":
            with get_conn() as conn:
                cr = _find_cached(conn, t["canonical_key"], t["epsilon"])
                cached = float(cr["noisy_count"]) if cr else None
        out.append({
            "task_id": t["id"],
            "canonical_key": t["canonical_key"],
            "epsilon": t["epsilon"],
            "stations": json.loads(t["stations_json"]),
            "status": t["status"],
            "active": bool(t["active"]),
            "worker_id": t["worker_id"],
            "generation": t["generation"],
            "reserved_epsilon": t["reserved_epsilon"],
            "created_at": t["created_at"],
            "updated_at": t["updated_at"],
            "lease_expires_at": t["lease_expires_at"],
            "error": t["error"],
            "noisy_count": cached,
        })
    return out


def get_task(task_id: int) -> Optional[dict]:
    with get_conn() as conn:
        t = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if t is None:
            return None
        cached = None
        if t["status"] == "succeeded":
            cr = _find_cached(conn, t["canonical_key"], t["epsilon"])
            cached = float(cr["noisy_count"]) if cr else None
    return {
        "task_id": t["id"],
        "canonical_key": t["canonical_key"],
        "epsilon": t["epsilon"],
        "stations": json.loads(t["stations_json"]),
        "status": t["status"],
        "active": bool(t["active"]),
        "worker_id": t["worker_id"],
        "generation": t["generation"],
        "reserved_epsilon": t["reserved_epsilon"],
        "created_at": t["created_at"],
        "updated_at": t["updated_at"],
        "lease_expires_at": t["lease_expires_at"],
        "error": t["error"],
        "noisy_count": cached,
    }
