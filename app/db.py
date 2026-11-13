"""SQLite 数据访问层。

设计要点：
- WAL 模式 + busy_timeout：统计/状态读不阻塞写，两个 API 进程可并发读；
  写事务通过 BEGIN IMMEDIATE 串行化，保证跨进程互斥。
- 每个请求/线程使用独立连接（check_same_thread=False 仅用于该线程内），
  不跨线程共享连接。
- schema 初始化幂等，两个进程同时启动也安全。
"""
from __future__ import annotations

import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from . import config

_SCHEMA = """
-- 单例行：控制一次性导入
CREATE TABLE IF NOT EXISTS meta (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    imported        INTEGER NOT NULL DEFAULT 0,
    imported_at     TEXT,
    ride_count      INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO meta(id, imported) VALUES (1, 0);

-- 乘车记录（匿名乘客号、上车站）
CREATE TABLE IF NOT EXISTS rides (
    id              INTEGER PRIMARY KEY,
    passenger_id    TEXT NOT NULL,
    station         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rides_station ON rides(station);

-- 查询结果缓存：canonical_key 与 epsilon 唯一确定一份“加噪结果”。
-- 只存加噪结果，真值不落库、不出接口。
CREATE TABLE IF NOT EXISTS query_results (
    id                  INTEGER PRIMARY KEY,
    canonical_key       TEXT NOT NULL,
    epsilon             REAL NOT NULL,
    stations_json       TEXT NOT NULL,
    noisy_count         REAL NOT NULL,
    created_at          TEXT NOT NULL,
    created_by_worker   TEXT NOT NULL,
    UNIQUE(canonical_key, epsilon)
);

-- 在途/历史任务。每个 (canonical_key, epsilon) 至多有一个活跃任务
-- （active=1），由部分唯一索引在数据库层面保证，跨进程并发 INSERT 会竞态失败。
CREATE TABLE IF NOT EXISTS tasks (
    id                  INTEGER PRIMARY KEY,
    canonical_key       TEXT NOT NULL,
    epsilon             REAL NOT NULL,
    stations_json       TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    active              INTEGER NOT NULL DEFAULT 1,
    worker_id           TEXT NOT NULL,
    generation          INTEGER NOT NULL DEFAULT 1,
    reserved_epsilon    REAL NOT NULL,
    lease_expires_at    REAL NOT NULL,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    error               TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_active_unique
    ON tasks(canonical_key, epsilon) WHERE active = 1;
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);

-- 预算账本：唯一事实来源。amount 带符号：
--   reserve > 0（预留，占用预算），release < 0（冲销预留：成功转实扣或失败退还），
--   commit < 0（实扣）。在途预留 = SUM(reserve+release)，已用 = |SUM(commit)|。
CREATE TABLE IF NOT EXISTS budget_ledger (
    id              INTEGER PRIMARY KEY,
    task_id         INTEGER,
    entry_type      TEXT NOT NULL CHECK (entry_type IN ('reserve', 'commit', 'release')),
    epsilon         REAL NOT NULL,
    amount          REAL NOT NULL,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ledger_task ON budget_ledger(task_id);
"""

_init_lock = threading.Lock()
_initialized: set[Path] = set()


def _connect() -> sqlite3.Connection:
    config.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(config.DB_PATH),
        timeout=config.BUSY_TIMEOUT_MS / 1000.0,
        isolation_level=None,  # 显式事务，关闭 sqlite3 模块的自动 BEGIN
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {config.BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    _ensure_wal(conn)
    return conn


def _ensure_wal(conn: sqlite3.Connection) -> None:
    """启用 WAL（数据库持久属性，只需切换一次）。

    另一进程/连接持写锁时 journal_mode 切换会立即返回 BUSY（busy_timeout
    对该 pragma 不生效），因此带有限重试；已是 WAL 则直接跳过。
    """
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    if str(mode).lower() == "wal":
        return
    deadline = time.monotonic() + config.BUSY_TIMEOUT_MS / 1000.0
    while True:
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            return
        except sqlite3.OperationalError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.1)


def init_db() -> None:
    """幂等建表（进程启动时调用一次）。"""
    with _init_lock:
        if config.DB_PATH in _initialized:
            return
        conn = _connect()
        try:
            conn.executescript(_SCHEMA)
        finally:
            conn.close()
        _initialized.add(config.DB_PATH)


@contextmanager
def get_conn() -> Iterator[sqlite3.Connection]:
    """短连接：请求/后台线程用完即关，避免跨线程复用。"""
    init_db()
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def write_tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """立即写事务：BEGIN IMMEDIATE 立刻获取 RESERVED 锁，
    把预算检查 + 预留/插入等“读-判-写”序列在数据库层串行化。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
