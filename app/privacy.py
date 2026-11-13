"""差分隐私机制：独立 Laplace 噪声。"""
from __future__ import annotations

import math
import random

# SystemRandom 基于 os.urandom，避免全局随机流被推断；每次调用独立采样。
_rng = random.SystemRandom()


def laplace_noise(epsilon: float) -> float:
    """采样 Lap(0, 1/epsilon)。

    逆变换采样：u ~ U(-0.5, 0.5)，噪声 = -scale * sign(u) * ln(1 - 2|u|)。
    本接口的计数查询敏感度为 1（去重乘客计数在单条记录变化下至多变化 1），
    故尺度参数 b = 1/epsilon。
    """
    if epsilon <= 0:
        raise ValueError("epsilon must be positive")
    u = _rng.uniform(-0.5, 0.5)
    scale = 1.0 / epsilon
    return -scale * math.copysign(math.log(1.0 - 2.0 * abs(u)), u)


def add_laplace_noise(true_count: int, epsilon: float) -> float:
    """返回加噪计数。真值仅在计算瞬间存在，不入库、不返回。"""
    return float(true_count) + laplace_noise(epsilon)
