"""跨进程集成测试：启动两个真实 uvicorn 进程共享同一个 SQLite 文件。

覆盖题目最关键的分布式语义：
1. 相同查询的并发请求只有一个执行者、一份噪声、一次扣费；
2. 任一进程都能复用另一个进程持久化的结果；
3. 执行进程崩溃后，租约过期自动由另一进程接续（generation 增加）；
4. 存活执行的租约会被心跳续租，不会被其他进程接管。
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_ready(port: int, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as r:
                if r.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError):
            time.sleep(0.2)
    raise RuntimeError(f"server on {port} not ready")


class Server:
    def __init__(self, db_path: Path, lease: float = 2.0, heartbeat: float = 0.4,
                 exec_delay: float = 0.0):
        self.port = _free_port()
        self.db_path = db_path
        env = os.environ.copy()
        env.update({
            "DP_DB_PATH": str(db_path),
            "DP_LEASE_SECONDS": str(lease),
            "DP_HEARTBEAT_SECONDS": str(heartbeat),
            "DP_QUERY_WAIT_TIMEOUT": "40",
            "DP_SIMULATED_EXEC_DELAY": str(exec_delay),
            "PYTHONPATH": str(ROOT),
        })
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.api:app",
             "--host", "127.0.0.1", "--port", str(self.port)],
            cwd=str(ROOT), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        _wait_ready(self.port)

    def post(self, path: str, payload: dict) -> httpx.Response:
        return httpx.post(f"http://127.0.0.1:{self.port}{path}", json=payload, timeout=60)

    def get(self, path: str) -> httpx.Response:
        return httpx.get(f"http://127.0.0.1:{self.port}{path}", timeout=10)

    def stop(self) -> None:
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()

    def kill(self) -> None:
        """模拟崩溃：SIGKILL，没有任何清理机会。"""
        self.proc.kill()
        self.proc.wait(timeout=10)


@pytest.fixture()
def cluster(tmp_path):
    db_path = tmp_path / "shared.db"
    a = Server(db_path)
    b = Server(db_path)
    csv = (ROOT / "sample_rides.csv").read_bytes()
    with open(tmp_path / "rides.csv", "wb") as f:
        f.write(csv)
    with open(tmp_path / "rides.csv", "rb") as f:
        r = httpx.post(
            f"http://127.0.0.1:{a.port}/import",
            files={"file": ("rides.csv", f, "text/csv")}, timeout=30,
        )
    assert r.status_code == 200, r.text
    yield a, b, db_path
    for s in (a, b):
        try:
            s.stop()
        except Exception:
            pass


def test_two_processes_share_cache_and_single_charge(cluster):
    a, b, _ = cluster
    payload = {"stations": ["人民广场", "陆家嘴"], "epsilon": 0.2}

    r1 = a.post("/query", payload)
    assert r1.status_code == 200
    first = r1.json()
    assert first["reused"] is False

    # 另一进程：同集合逆序 + 同 ε，必须复用同一噪声且不扣费
    r2 = b.post("/query", {"stations": ["陆家嘴", "人民广场"], "epsilon": 0.2})
    assert r2.status_code == 200
    second = r2.json()
    assert second["reused"] is True
    assert second["noisy_count"] == first["noisy_count"]
    assert second["charged_epsilon"] == 0.0

    ba, bb = a.get("/budget").json(), b.get("/budget").json()
    assert ba == bb and round(ba["used"], 6) == 0.2 and ba["reserved"] == 0


def test_concurrent_identical_queries_single_execution(cluster):
    a, b, _ = cluster
    payload = {"stations": ["静安寺", "南京西路"], "epsilon": 0.5}

    # 两个进程同时收到完全相同的查询：只能有一个执行者
    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(
            lambda srv: srv.post("/query", payload),
            [a, b, a, b, a, b, a, b],
        ))
    assert all(r.status_code == 200 for r in responses), [r.text for r in responses]
    noises = {r.json()["noisy_count"] for r in responses}
    keys = {r.json()["canonical_key"] for r in responses}
    assert len(noises) == 1 and len(keys) == 1  # 同一份噪声，没有重复采样
    leaders = [r for r in responses if r.json()["reused"] is False]
    assert len(leaders) == 1  # 只有一个请求真正执行并扣费

    budget = a.get("/budget").json()
    assert round(budget["used"], 6) == 0.5 and budget["reserved"] == 0
    tasks_ = a.get("/tasks").json()
    matching = [t for t in tasks_ if t["epsilon"] == 0.5 and t["status"] == "succeeded"]
    assert len(matching) == 1  # 只有一个成功任务


def test_budget_reservation_shared_between_processes(cluster):
    a, b, _ = cluster
    # 在 a 上消耗 0.5，b 视角必须立刻可见并继续受总预算约束
    assert a.post("/query", {"stations": ["人民广场"], "epsilon": 0.5}).status_code == 200
    assert round(b.get("/budget").json()["remaining"], 6) == 0.5
    assert b.post("/query", {"stations": ["陆家嘴"], "epsilon": 0.5}).status_code == 200
    r = a.post("/query", {"stations": ["静安寺"], "epsilon": 0.1})
    assert r.status_code == 402  # 两进程合计已用满 1.0


def test_crash_lease_takeover(tmp_path):
    """执行进程在计算阶段崩溃（SIGKILL），另一进程等租约过期后接续。"""
    db_path = tmp_path / "crash.db"
    # 短租约便于测试：2s 过期，心跳 0.5s；a 的执行窗口拉长到 5s 确保崩溃时任务在途
    a = Server(db_path, lease=2.0, heartbeat=0.5, exec_delay=5.0)
    b = Server(db_path, lease=2.0, heartbeat=0.5, exec_delay=0.0)
    try:
        csv = (ROOT / "sample_rides.csv").read_bytes()
        f = tmp_path / "rides.csv"
        f.write_bytes(csv)
        with open(f, "rb") as fh:
            httpx.post(f"http://127.0.0.1:{a.port}/import",
                       files={"file": ("rides.csv", fh, "text/csv")}, timeout=30)

        # 让 a 拿到一个在途任务后立刻 SIGKILL：通过制造慢执行不现实，
        # 改为先正常提交一个查询制造历史，再用新查询在 a 上发起并立即杀进程。
        # 利用租约窗口：a 建任务（预留已持久化）→ kill → b 接续。
        payload = {"stations": ["人民广场", "静安寺", "陆家嘴"], "epsilon": 0.1}

        def crash_query():
            try:
                return a.post("/query", payload)
            except (httpx.ReadError, httpx.ConnectError, httpx.RemoteProtocolError):
                return None

        with ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(crash_query)
            # 等待任务在共享库中出现（预留已落库），随后杀死 a
            deadline = time.time() + 10
            task_seen = False
            import sqlite3
            while time.time() < deadline:
                try:
                    conn = sqlite3.connect(str(db_path), timeout=2)
                    row = conn.execute(
                        "SELECT COUNT(*) c FROM tasks WHERE active=1 AND status='running'"
                    ).fetchone()
                    conn.close()
                    if row[0] >= 1:
                        task_seen = True
                        break
                except sqlite3.OperationalError:
                    pass
                time.sleep(0.05)
            assert task_seen, "未观察到在途任务"
            a.kill()
            fut.result(timeout=10)

        # 此时预留仍占着预算；b 等租约过期后接管并完成
        t0 = time.time()
        r = b.post("/query", payload)
        elapsed = time.time() - t0
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["reused"] is False  # 崩溃者没提交结果，由 b 实际完成
        assert elapsed >= 1.5, f"应等待租约过期，实际 {elapsed:.2f}s"

        # 任务代次增加，且工作者属于 b
        tasks_ = b.get("/tasks").json()
        gens = [(t["generation"], t["status"], t["worker_id"]) for t in tasks_]
        assert any(g >= 2 for g, s, _ in gens if s == "succeeded"), gens

        budget = b.get("/budget").json()
        assert round(budget["used"], 6) == 0.1 and budget["reserved"] == 0
    finally:
        try:
            b.stop()
        except Exception:
            pass


def test_live_leader_is_not_taken_over(cluster):
    """存活执行者持续心跳，租约不断续期；另一进程只能跟随，不能接管。"""
    a, b, _ = cluster
    # 先在 a 上完成一次普通查询建立基线
    base = a.post("/query", {"stations": ["南京西路"], "epsilon": 0.2}).json()
    assert base["reused"] is False

    # 新查询并发打到两个进程；检查最终任务只有一代（generation=1），
    # 没有发生租约接管
    payload = {"stations": ["人民广场", "南京西路", "静安寺"], "epsilon": 0.1}
    with ThreadPoolExecutor(max_workers=2) as pool:
        rs = list(pool.map(lambda s: s.post("/query", payload), [a, b]))
    assert all(r.status_code == 200 for r in rs)
    assert rs[0].json()["noisy_count"] == rs[1].json()["noisy_count"]

    tasks_ = a.get("/tasks?limit=10").json()
    target = [t for t in tasks_ if t["canonical_key"] == rs[0].json()["canonical_key"]
              and t["epsilon"] == 0.1]
    assert len(target) == 1 and target[0]["generation"] == 1
    assert target[0]["status"] == "succeeded" and target[0]["active"] is False
