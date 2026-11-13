"""Laplace 机制与规范化逻辑的单元测试。"""
import math
import statistics

from app.privacy import add_laplace_noise, laplace_noise


def test_laplace_distribution_shape():
    eps = 0.5
    samples = [laplace_noise(eps) for _ in range(20000)]
    # 均值接近 0
    assert abs(statistics.mean(samples)) < 0.1
    # Laplace 方差 = 2 b^2 = 2 / eps^2 = 8
    assert abs(statistics.variance(samples) - 2 / eps ** 2) < 1.0


def test_laplace_scale_changes_with_epsilon():
    s01 = statistics.variance([laplace_noise(0.1) for _ in range(5000)])
    s05 = statistics.variance([laplace_noise(0.5) for _ in range(5000)])
    # ε 越大噪声越小：Var(0.1)=200 远大于 Var(0.5)=8
    assert s01 > s05 * 10


def test_noisy_count_differs_from_true_almost_surely():
    # 独立噪声，重复采样几乎不可能相等
    vals = {add_laplace_noise(42, 0.2) for _ in range(20)}
    assert len(vals) > 1


def test_canonicalization_order_invariant():
    from app.models import canonicalize
    k1, s1 = canonicalize(["人民广场", "南京西路", "陆家嘴"])
    k2, s2 = canonicalize(["陆家嘴", "人民广场", "南京西路"])
    assert k1 == k2 and s1 == s2


def test_canonicalization_dedup_and_trim():
    from app.models import canonicalize
    k1, s1 = canonicalize(["A", "A", " B "])
    k2, s2 = canonicalize(["B", "A"])
    assert k1 == k2 and s1 == ["A", "B"]


def test_different_sets_different_keys():
    from app.models import canonicalize
    k1, _ = canonicalize(["A", "B"])
    k2, _ = canonicalize(["A", "C"])
    assert k1 != k2
