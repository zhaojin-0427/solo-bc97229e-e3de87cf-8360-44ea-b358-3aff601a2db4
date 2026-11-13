"""pytest 公共夹具：每个测试使用独立临时 SQLite 库。

不重载模块（避免测试文件顶部的 import 与应用内部持有不同模块实例），
而是直接 monkeypatch 单例 config 的属性；所有 app 子模块均以 config.XXX
形式在调用时读取，因此补丁对全局生效。
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_TMP_DB_DIR = tempfile.mkdtemp(prefix="dp_test_")
import os
os.environ.setdefault("DP_DB_PATH", str(Path(_TMP_DB_DIR) / "bootstrap.db"))
os.environ.setdefault("DP_LEASE_SECONDS", "2")
os.environ.setdefault("DP_HEARTBEAT_SECONDS", "0.3")
os.environ.setdefault("DP_QUERY_WAIT_TIMEOUT", "30")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture()
def app_env(tmp_path, monkeypatch):
    from app import config, db

    db_file = tmp_path / "test.db"
    monkeypatch.setattr(config, "DB_PATH", db_file)
    monkeypatch.setattr(config, "LEASE_SECONDS", 2.0)
    monkeypatch.setattr(config, "HEARTBEAT_SECONDS", 0.3)
    monkeypatch.setattr(config, "QUERY_WAIT_TIMEOUT_SECONDS", 30.0)
    monkeypatch.setattr(config, "POLL_INTERVAL_SECONDS", 0.05)

    db._initialized.clear()
    db.init_db()

    from app.api import app
    with TestClient(app) as client:
        yield client, db_file
