"""运行期配置，全部可通过环境变量覆盖。"""
from __future__ import annotations

import os
from pathlib import Path

# 数据库文件：compose 中两个 API 进程挂载同一个命名卷，共享同一个文件。
DB_PATH = Path(os.getenv("DP_DB_PATH", "/data/dp_transit.db"))

# 总隐私预算 rho。
TOTAL_BUDGET = float(os.getenv("DP_TOTAL_BUDGET", "1.0"))

# 允许的 epsilon 取值。
ALLOWED_EPSILONS = (0.1, 0.2, 0.5)
EPSILON_TOLERANCE = 1e-9

# 租约：执行进程在租约内对任务保持心跳（lease_heartbeat 小于 lease_seconds）。
# 租约过期且没有新心跳后，其他进程才允许接管，存活执行不会被接管。
LEASE_SECONDS = float(os.getenv("DP_LEASE_SECONDS", "15"))
HEARTBEAT_SECONDS = float(os.getenv("DP_HEARTBEAT_SECONDS", "3"))

# 跟随者等待在途任务完成的轮询间隔与最长等待时间。
POLL_INTERVAL_SECONDS = float(os.getenv("DP_POLL_INTERVAL", "0.2"))
QUERY_WAIT_TIMEOUT_SECONDS = float(os.getenv("DP_QUERY_WAIT_TIMEOUT", "120"))

# 单次写事务拿不到写锁时的 busy timeout（SQLite 内置重试）。
BUSY_TIMEOUT_MS = int(os.getenv("DP_BUSY_TIMEOUT_MS", "15000"))

# 导入 CSV 时的批量插入大小。
INSERT_BATCH_SIZE = 1000

# 执行阶段的人为延迟（秒），仅用于演示/测试崩溃接续窗口；生产环境置 0。
SIMULATED_EXEC_DELAY = float(os.getenv("DP_SIMULATED_EXEC_DELAY", "0"))
