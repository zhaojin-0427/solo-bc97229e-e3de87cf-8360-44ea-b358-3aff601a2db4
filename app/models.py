"""查询集合规范化与 API 数据模型。"""
from __future__ import annotations

import hashlib
import json
from typing import List, Literal

from pydantic import BaseModel, Field, field_validator

from . import config


def canonicalize(stations: list[str]) -> tuple[str, list[str]]:
    """站点顺序无关：去空白、去重、排序后生成稳定 key。

    返回 (canonical_key, 规范化后的站点列表)。
    """
    cleaned = sorted({s.strip() for s in stations if s and s.strip()})
    key = hashlib.sha256("\x1f".join(cleaned).encode("utf-8")).hexdigest()
    return key, cleaned


class QueryRequest(BaseModel):
    stations: List[str] = Field(..., min_length=1, description="非空站点集合")
    epsilon: float = Field(..., description="隐私预算，仅允许 0.1 / 0.2 / 0.5")

    @field_validator("stations")
    @classmethod
    def _non_empty_station(cls, v: List[str]) -> List[str]:
        cleaned = [s.strip() for s in v if s and s.strip()]
        if not cleaned:
            raise ValueError("stations 必须为非空站点集合")
        return v

    @field_validator("epsilon")
    @classmethod
    def _allowed_epsilon(cls, v: float) -> float:
        if not any(abs(v - e) <= config.EPSILON_TOLERANCE for e in config.ALLOWED_EPSILONS):
            raise ValueError(f"epsilon 仅允许 {config.ALLOWED_EPSILONS}")
        return float(v)


class ImportResponse(BaseModel):
    status: Literal["imported"]
    ride_count: int
    distinct_passengers: int
    distinct_stations: int


class QueryResponse(BaseModel):
    status: Literal["completed"]
    noisy_count: float
    epsilon: float
    stations: List[str]
    canonical_key: str
    reused: bool
    worker_id: str
    task_generation: int | None = None
    charged_epsilon: float


class TaskOut(BaseModel):
    task_id: int
    canonical_key: str
    epsilon: float
    stations: List[str]
    status: str
    active: bool
    worker_id: str
    generation: int
    reserved_epsilon: float
    created_at: str
    updated_at: str
    lease_expires_at: float
    error: str | None = None
    noisy_count: float | None = None


class BudgetResponse(BaseModel):
    total: float
    used: float
    reserved: float
    remaining: float


def dumps_stations(stations: list[str]) -> str:
    return json.dumps(stations, ensure_ascii=False, sort_keys=True)
