"""一次性 CSV 导入：匿名乘客号 + 上车站。"""
from __future__ import annotations

import csv
import io

from .db import get_conn, write_tx
from .tasks import _utcnow

# 兼容中英文表头
_PASSENGER_ALIASES = {"passenger_id", "passenger", "rider_id", "匿名乘客号", "乘客号", "乘客id"}
_STATION_ALIASES = {"station", "board_station", "boarding_station", "上车站", "站点", "上车站点"}


class AlreadyImported(Exception):
    """数据只允许导入一次，之后服务进入只读状态。"""


def _resolve_columns(header: list[str]) -> tuple[int, int]:
    lowered = [h.strip().lower() for h in header]
    try:
        p_idx = next(i for i, h in enumerate(lowered) if h in _PASSENGER_ALIASES)
    except StopIteration as exc:
        raise ValueError("CSV 缺少乘客号列（passenger_id / 匿名乘客号）") from exc
    try:
        s_idx = next(i for i, h in enumerate(lowered) if h in _STATION_ALIASES)
    except StopIteration as exc:
        raise ValueError("CSV 缺少上车站列（station / 上车站）") from exc
    return p_idx, s_idx


def parse_csv(content: bytes) -> list[tuple[str, str]]:
    text = content.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration as exc:
        raise ValueError("CSV 为空") from exc
    p_idx, s_idx = _resolve_columns(header)
    rows: list[tuple[str, str]] = []
    for raw in reader:
        if len(raw) <= max(p_idx, s_idx):
            continue
        passenger = raw[p_idx].strip()
        station = raw[s_idx].strip()
        if passenger and station:
            rows.append((passenger, station))
    if not rows:
        raise ValueError("CSV 没有有效数据行")
    return rows


def import_rides(content: bytes) -> dict:
    rows = parse_csv(content)
    with get_conn() as conn:
        with write_tx(conn):
            meta = conn.execute("SELECT imported FROM meta WHERE id = 1").fetchone()
            if meta is not None and meta["imported"] == 1:
                raise AlreadyImported("数据已导入过，服务为只读模式，不允许重复导入")
            conn.executemany(
                "INSERT INTO rides (passenger_id, station) VALUES (?, ?)",
                rows,
            )
            stats = conn.execute(
                "SELECT COUNT(*) AS n, COUNT(DISTINCT passenger_id) AS p, "
                "COUNT(DISTINCT station) AS s FROM rides"
            ).fetchone()
            conn.execute(
                "UPDATE meta SET imported = 1, imported_at = ?, ride_count = ? WHERE id = 1",
                (_utcnow(), len(rows)),
            )
    return {
        "status": "imported",
        "ride_count": stats["n"],
        "distinct_passengers": stats["p"],
        "distinct_stations": stats["s"],
    }
