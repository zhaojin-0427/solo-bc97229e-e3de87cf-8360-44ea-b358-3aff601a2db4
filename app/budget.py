"""预算账本聚合与预留校验。"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from . import config

# 浮点累计留一点容差，避免 0.1+0.2 这类二进制误差导致合法预留被拒。
_BUDGET_TOLERANCE = 1e-9


@dataclass(frozen=True)
class BudgetSnapshot:
    total: float
    used: float          # 已实扣（commit 条目之和的绝对值）
    reserved: float      # 当前在途预留（reserve 未被 commit/release 冲销的部分）
    remaining: float     # total - used - reserved

    def as_dict(self) -> dict:
        return {
            "total": round(self.total, 6),
            "used": round(self.used, 6),
            "reserved": round(self.reserved, 6),
            "remaining": round(self.remaining, 6),
        }


def get_snapshot(conn: sqlite3.Connection) -> BudgetSnapshot:
    # 在途预留 = reserve 累计 - 已冲销（成功转实扣或失败退还都会写 release 负条目）。
    row = conn.execute(
        "SELECT COALESCE(SUM(amount), 0) AS reserved FROM budget_ledger "
        "WHERE entry_type IN ('reserve', 'release')"
    ).fetchone()
    reserved = float(row["reserved"])
    row = conn.execute(
        "SELECT COALESCE(SUM(amount), 0) AS used FROM budget_ledger "
        "WHERE entry_type = 'commit'"
    ).fetchone()
    # commit 以负金额记账，已用取绝对值。
    used = abs(float(row["used"]))
    remaining = config.TOTAL_BUDGET - used - reserved
    return BudgetSnapshot(
        total=config.TOTAL_BUDGET, used=used, reserved=reserved, remaining=remaining
    )


def has_capacity(conn: sqlite3.Connection, epsilon: float) -> bool:
    """预留 + 已用之和不得超过总预算（含容差）。"""
    snap = get_snapshot(conn)
    return snap.used + snap.reserved + epsilon <= config.TOTAL_BUDGET + _BUDGET_TOLERANCE
