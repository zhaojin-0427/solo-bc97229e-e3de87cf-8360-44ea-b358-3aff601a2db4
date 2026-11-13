"""单进程端到端：导入、查询、复用、预算、拒绝、失败退还、真值不泄露。"""
from __future__ import annotations

import io

from app import tasks
from app.db import get_conn


SAMPLE_CSV = (
    "匿名乘客号,上车站\n"
    "P1,A\nP2,A\nP1,B\nP3,B\nP4,A\nP2,C\nP5,C\nP1,C\nP6,A\nP3,A\n"
)


def _import(client):
    r = client.post(
        "/import",
        files={"file": ("rides.csv", io.BytesIO(SAMPLE_CSV.encode()), "text/csv")},
    )
    assert r.status_code == 200, r.text
    return r.json()


def test_import_once_then_readonly(app_env):
    client, _ = app_env
    info = _import(client)
    assert info["ride_count"] == 10
    assert info["distinct_passengers"] == 6
    assert info["distinct_stations"] == 3

    r = client.post(
        "/import",
        files={"file": ("rides.csv", io.BytesIO(SAMPLE_CSV.encode()), "text/csv")},
    )
    assert r.status_code == 409


def test_query_requires_import(app_env):
    client, _ = app_env
    r = client.post("/query", json={"stations": ["A"], "epsilon": 0.2})
    assert r.status_code == 409


def test_epsilon_validation(app_env):
    client, _ = app_env
    _import(client)
    r = client.post("/query", json={"stations": ["A"], "epsilon": 0.3})
    assert r.status_code == 422
    r = client.post("/query", json={"stations": [], "epsilon": 0.2})
    assert r.status_code == 422
    r = client.post("/query", json={"stations": ["  "], "epsilon": 0.2})
    assert r.status_code == 422


def test_query_returns_noise_never_true_value(app_env):
    client, db_file = app_env
    _import(client)
    # A 站真实去重乘客：P1,P2,P4,P6,P3 = 5
    r = client.post("/query", json={"stations": ["A"], "epsilon": 0.5})
    assert r.status_code == 200, r.text
    body = r.json()
    assert set(body) >= {"noisy_count", "epsilon", "reused", "canonical_key",
                         "worker_id", "charged_epsilon"}
    assert body["reused"] is False
    assert body["charged_epsilon"] == 0.5

    # 数据库中绝不存真值：query_results 只有加噪值
    with get_conn() as conn:
        rows = conn.execute("SELECT noisy_count FROM query_results").fetchall()
    assert len(rows) == 1
    assert rows[0]["noisy_count"] != 5  # 噪声后恰好等于真值的概率为 0（连续分布）


def test_reuse_same_set_same_epsilon_no_recharge_same_noise(app_env):
    client, _ = app_env
    _import(client)
    r1 = client.post("/query", json={"stations": ["A", "B"], "epsilon": 0.2}).json()
    r2 = client.post("/query", json={"stations": ["B", "A"], "epsilon": 0.2}).json()  # 逆序
    r3 = client.post("/query", json={"stations": ["A", "B", "A"], "epsilon": 0.2}).json()  # 重复元素

    assert r1["noisy_count"] == r2["noisy_count"] == r3["noisy_count"]
    assert r1["canonical_key"] == r2["canonical_key"] == r3["canonical_key"]
    assert r2["reused"] is True and r3["reused"] is True
    assert r2["charged_epsilon"] == 0.0 and r3["charged_epsilon"] == 0.0

    b = client.get("/budget").json()
    assert b["used"] == 0.2 and b["reserved"] == 0 and round(b["remaining"], 6) == 0.8


def test_different_epsilon_is_independent_draw_and_separate_charge(app_env):
    client, _ = app_env
    _import(client)
    r1 = client.post("/query", json={"stations": ["A"], "epsilon": 0.1}).json()
    r2 = client.post("/query", json={"stations": ["A"], "epsilon": 0.5}).json()
    assert r1["canonical_key"] == r2["canonical_key"]
    # 独立采样，尺度不同；不做硬性不等断言（连续分布），但预算应分别扣费
    b = client.get("/budget").json()
    assert round(b["used"], 6) == 0.6


def test_budget_exhaustion_reserve_refused(app_env):
    client, _ = app_env
    _import(client)
    # 用满预算：0.5 + 0.5
    client.post("/query", json={"stations": ["A"], "epsilon": 0.5})
    client.post("/query", json={"stations": ["B"], "epsilon": 0.5})
    b = client.get("/budget").json()
    assert round(b["remaining"], 6) == 0.0

    r = client.post("/query", json={"stations": ["C"], "epsilon": 0.1})
    assert r.status_code == 402
    # 被拒绝不产生预留、不产生结果
    b2 = client.get("/budget").json()
    assert b2 == b
    with get_conn() as conn:
        n = conn.execute("SELECT COUNT(*) c FROM query_results").fetchone()["c"]
    assert n == 2


def test_budget_exact_boundary_accepted(app_env):
    client, _ = app_env
    _import(client)
    for eps, st in [(0.5, "A"), (0.2, "B"), (0.2, "C"), (0.1, "D")]:
        r = client.post("/query", json={"stations": [st], "epsilon": eps})
        assert r.status_code == 200, (eps, r.text)
    b = client.get("/budget").json()
    assert round(b["used"], 6) == 1.0 and round(b["remaining"], 6) == 0.0
    r = client.post("/query", json={"stations": ["E"], "epsilon": 0.1})
    assert r.status_code == 402


def test_failed_task_refunds_reservation_and_allows_retry(app_env, monkeypatch):
    client, _ = app_env
    _import(client)

    # 注入故障：第一次计算加噪结果时抛错（模拟执行阶段失败），之后恢复。
    from app import privacy
    real = privacy.add_laplace_noise
    calls = {"n": 0}

    def flaky_noise(true_count, epsilon):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("injected compute failure")
        return real(true_count, epsilon)

    monkeypatch.setattr(privacy, "add_laplace_noise", flaky_noise)
    r = client.post("/query", json={"stations": ["A"], "epsilon": 0.2})
    # 整体请求内部完成失败记账、退还并重新选举重试，最终成功
    assert r.status_code == 200, r.text
    assert calls["n"] == 2  # 恰好失败一次、重试一次
    # 不在此处 monkeypatch.undo()：fixture 统一回收，避免提前恢复 DB_PATH 等夹具补丁

    b = client.get("/budget").json()
    # 只有一次实扣，预留全部被冲销
    assert round(b["used"], 6) == 0.2 and b["reserved"] == 0

    task_list = client.get("/tasks").json()
    statuses = [t["status"] for t in task_list]
    assert "failed" in statuses and "succeeded" in statuses
    failed = next(t for t in task_list if t["status"] == "failed")
    assert failed["error"] and "injected compute failure" in failed["error"]

    # 账本校验：reserve 两次（失败+成功），release 两次，commit 一次
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT entry_type, COUNT(*) c, SUM(amount) s FROM budget_ledger GROUP BY entry_type"
        ).fetchall()
    ledger = {r["entry_type"]: (r["c"], r["s"]) for r in rows}
    assert ledger["reserve"] == (2, 0.4)
    assert ledger["release"] == (2, -0.4)
    assert ledger["commit"] == (1, -0.2)


def test_task_status_endpoints(app_env):
    client, _ = app_env
    _import(client)
    client.post("/query", json={"stations": ["A", "B"], "epsilon": 0.1})
    tasks_ = client.get("/tasks").json()
    assert len(tasks_) == 1
    tid = tasks_[0]["task_id"]
    detail = client.get(f"/tasks/{tid}").json()
    assert detail["status"] == "succeeded" and detail["active"] is False
    assert detail["noisy_count"] is not None
    assert client.get("/tasks/9999").status_code == 404


def test_budget_is_pure_read(app_env):
    client, _ = app_env
    _import(client)
    client.post("/query", json={"stations": ["A"], "epsilon": 0.2})
    # 预算与任务查询在写进行中也能返回（这里直接验证重复读一致性）
    b1 = client.get("/budget").json()
    b2 = client.get("/budget").json()
    assert b1 == b2
