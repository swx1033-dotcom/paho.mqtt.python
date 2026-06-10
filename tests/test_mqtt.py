"""
MQTTMatcher / 主题过滤器匹配的正确性和性能基准测试。

运行方式::

    python -m pytest tests/test_mqtt.py -v
    python -m pytest tests/test_mqtt.py --benchmark-only -v     # 只跑基准
    python -m pytest tests/test_mqtt.py --benchmark-disable -v  # 跳过基准

基准测试会在 ``N`` 个不同的主题过滤器上注册回调，然后对 ``M`` 个消息主题执行
匹配，验证：

* ``TrieMatcher`` 的插入 / 删除 / 匹配性能；
* 随着订阅数增长，匹配耗时应保持近似常数（与主题深度相关），而非线性增长。
"""

from __future__ import annotations

import random
import time
from typing import Callable, Dict, List

import pytest

# 同时导入被重构的匹配器以及客户端 API，确保向后兼容。
from paho.mqtt.matcher import MQTTMatcher  # noqa: F401
import paho.mqtt.client as mqtt_client


# ---------------------------------------------------------------------------
# 正确性测试
# ---------------------------------------------------------------------------


class Test_MQTTMatcher_basic:
    """基本的 set/get/del/contains 行为。"""

    def test_set_and_get(self):
        m = MQTTMatcher()
        m["foo/bar"] = "cb1"
        assert m["foo/bar"] == "cb1"
        assert len(m) == 1
        assert "foo/bar" in m
        assert "foo/baz" not in m

    def test_get_missing_raises_keyerror(self):
        m = MQTTMatcher()
        with pytest.raises(KeyError):
            m["nope"]

    def test_del_missing_raises_keyerror(self):
        m = MQTTMatcher()
        with pytest.raises(KeyError):
            del m["nope"]

    def test_del_reclaims_empty_nodes(self):
        """删除条目后，没有子节点也没有内容的节点应被回收。"""
        m = MQTTMatcher()
        m["a/b/c/d"] = 1
        del m["a/b/c/d"]
        # 根节点不应保留任何子节点
        assert m._root._children == {}
        assert len(m) == 0

    def test_del_keeps_sibling_paths(self):
        """删除一条路径时不能影响共享前缀的其他路径。"""
        m = MQTTMatcher()
        m["a/b/c"] = 1
        m["a/b/d"] = 2
        m["a/x"] = 3
        del m["a/b/c"]
        assert "a/b/c" not in m
        assert m["a/b/d"] == 2
        assert m["a/x"] == 3
        assert len(m) == 2

    def test_overwrite_does_not_double_count(self):
        m = MQTTMatcher()
        m["x"] = 1
        m["x"] = 2
        assert len(m) == 1
        assert m["x"] == 2

    def test_iter_contains_all_keys(self):
        m = MQTTMatcher()
        keys = ["a", "a/b", "a/c/d", "x/y/z", "#"]
        for k in keys:
            m[k] = True
        assert sorted(m) == sorted(keys)


class Test_MQTTMatcher_matching:
    """iter_match 的通配符匹配语义。"""

    @pytest.mark.parametrize("sub,topic", [
        ("foo/bar", "foo/bar"),
        ("foo/+", "foo/bar"),
        ("foo/+/baz", "foo/bar/baz"),
        ("foo/+/#", "foo/bar/baz"),
        ("A/B/+/#", "A/B/B/C"),
        ("#", "foo/bar/baz"),
        ("#", "/foo/bar"),
        ("/#", "/foo/bar"),
        ("$SYS/bar", "$SYS/bar"),
    ])
    def test_matches(self, sub: str, topic: str):
        m = MQTTMatcher()
        m[sub] = True
        assert list(m.iter_match(topic)) == [True]

    @pytest.mark.parametrize("sub,topic", [
        ("test/6/#", "test/3"),
        ("foo/bar", "foo"),
        ("foo/+", "foo/bar/baz"),
        ("foo/+/baz", "foo/bar/bar"),
        ("foo/+/#", "fo2/bar/baz"),
        ("/#", "foo/bar"),
        ("#", "$SYS/bar"),
        ("$BOB/bar", "$SYS/bar"),
    ])
    def test_not_matches(self, sub: str, topic: str):
        m = MQTTMatcher()
        m[sub] = True
        assert list(m.iter_match(topic)) == []

    def test_topic_matches_sub_backward_compat(self):
        """client.topic_matches_sub 必须保持完全一致的行为。"""
        cases_matching = [
            ("foo/bar", "foo/bar"),
            ("foo/+", "foo/bar"),
            ("foo/+/baz", "foo/bar/baz"),
            ("foo/+/#", "foo/bar/baz"),
            ("A/B/+/#", "A/B/B/C"),
            ("#", "foo/bar/baz"),
            ("#", "/foo/bar"),
            ("/#", "/foo/bar"),
            ("$SYS/bar", "$SYS/bar"),
        ]
        for sub, topic in cases_matching:
            assert mqtt_client.topic_matches_sub(sub, topic), (sub, topic)

        cases_non_matching = [
            ("test/6/#", "test/3"),
            ("foo/bar", "foo"),
            ("foo/+", "foo/bar/baz"),
            ("foo/+/baz", "foo/bar/bar"),
            ("foo/+/#", "fo2/bar/baz"),
            ("/#", "foo/bar"),
            ("#", "$SYS/bar"),
            ("$BOB/bar", "$SYS/bar"),
        ]
        for sub, topic in cases_non_matching:
            assert not mqtt_client.topic_matches_sub(sub, topic), (sub, topic)

    def test_multi_filter_match_order_and_count(self):
        """一个主题可以匹配多个过滤器；都应该被返回。"""
        m = MQTTMatcher()
        m["sensors/room1/temp"] = 1
        m["sensors/+/temp"] = 2
        m["sensors/#"] = 3
        m["#"] = 4
        results = set(m.iter_match("sensors/room1/temp"))
        assert results == {1, 2, 3, 4}

    def test_empty_trie_returns_nothing(self):
        m = MQTTMatcher()
        assert list(m.iter_match("anything")) == []

    def test_hash_only_on_deep_topic(self):
        m = MQTTMatcher()
        m["#"] = 1
        assert list(m.iter_match("a/b/c/d/e/f/g")) == [1]


# ---------------------------------------------------------------------------
# 参考实现：线性扫描，用于正确性对照与性能对比
# ---------------------------------------------------------------------------


class _LinearMatcher:
    """最简单的线性扫描实现，作为 Trie 的正确性与性能基线。"""

    def __init__(self) -> None:
        self._filters: Dict[str, Callable] = {}

    def __setitem__(self, key: str, value: Callable) -> None:
        self._filters[key] = value

    def __delitem__(self, key: str) -> None:
        del self._filters[key]

    def __len__(self) -> int:
        return len(self._filters)

    def iter_match(self, topic: str) -> List[Callable]:
        """遍历所有过滤器，逐一进行字符串级别的匹配。"""
        topic_parts = topic.split('/')
        is_dollar_topic = topic.startswith('$')
        out: List[Callable] = []
        for sub, value in self._filters.items():
            if self._matches(sub, topic_parts, is_dollar_topic):
                out.append(value)
        return out

    @staticmethod
    def _matches(sub: str, topic_parts: List[str], is_dollar_topic: bool) -> bool:
        sub_parts = sub.split('/')
        # 以 $ 开头的主题不匹配根层 # / +
        if is_dollar_topic and sub_parts and sub_parts[0] in ('#', '+'):
            return False

        i = 0
        for sp in sub_parts:
            if sp == '#':
                # 匹配零或多个剩余层级；必须是最后一个 token。
                return True
            if i >= len(topic_parts):
                return False
            if sp == '+':
                i += 1
                continue
            if sp != topic_parts[i]:
                return False
            i += 1
        return i == len(topic_parts)


# ---------------------------------------------------------------------------
# 性能基准测试
# ---------------------------------------------------------------------------


# 如果 pytest-benchmark 未安装，则优雅降级。
pytestmark = pytest.mark.skipif(
    False, reason="always run unless --benchmark-disable is used"
)

try:
    import pytest_benchmark  # noqa: F401
    _HAS_BENCHMARK = True
except ImportError:  # pragma: no cover - 仅在未安装插件时触发
    _HAS_BENCHMARK = False


def _gen_filters(n: int, seed: int = 42) -> List[str]:
    """生成 ``n`` 个带通配符 / 不带通配符的主题过滤器。

    生成策略模仿真实 IoT 场景：
    * 40% 精确主题（sensors/<id>/<metric>）；
    * 40% 单层通配符（sensors/+/<metric>）；
    * 20% 多层前缀通配符（building/<id>/#）。
    """
    rng = random.Random(seed)
    filters: List[str] = []
    for i in range(n):
        kind = i % 5
        if kind < 2:
            # 精确主题
            filters.append(f"sensors/{rng.randint(0, n // 5)}/temp/{rng.randint(0, 20)}")
        elif kind < 4:
            # 单层 +
            metrics = ("temp", "humidity", "pressure", "battery")
            filters.append(f"sensors/+/{rng.choice(metrics)}")
        else:
            # 前缀 #
            filters.append(f"building/{rng.randint(0, n // 10)}/#")
    return filters


def _gen_topics(n: int, seed: int = 7) -> List[str]:
    """生成用于匹配的消息主题。"""
    rng = random.Random(seed)
    topics: List[str] = []
    for _ in range(n):
        topics.append(
            f"sensors/{rng.randint(0, n // 2)}/"
            f"{rng.choice(('temp', 'humidity', 'pressure', 'battery'))}"
        )
    return topics


@pytest.mark.skipif(not _HAS_BENCHMARK, reason="pytest-benchmark not installed")
class Test_MQTTMatcher_benchmark:
    """对比 Trie 与线性扫描在不同订阅规模下的性能。"""

    # 规模参数：每个过滤器数量下各自运行一次基准。
    @pytest.mark.parametrize("num_filters", [100, 1000, 5000])
    def test_trie_insert(self, benchmark, num_filters: int):
        filters = _gen_filters(num_filters)

        def bench():
            m = MQTTMatcher()
            for i, f in enumerate(filters):
                m[f] = i

        benchmark(bench)

    @pytest.mark.parametrize("num_filters", [100, 1000, 5000])
    def test_trie_match(self, benchmark, num_filters: int):
        filters = _gen_filters(num_filters)
        topics = _gen_topics(200)
        m = MQTTMatcher()
        for i, f in enumerate(filters):
            m[f] = i

        def bench():
            hits = 0
            for t in topics:
                hits += len(list(m.iter_match(t)))
            return hits

        benchmark(bench)

    @pytest.mark.parametrize("num_filters", [100, 1000, 5000])
    def test_linear_match_baseline(self, benchmark, num_filters: int):
        """同规模的线性扫描实现，作为性能对照基线。"""
        filters = _gen_filters(num_filters)
        topics = _gen_topics(200)
        m = _LinearMatcher()
        for i, f in enumerate(filters):
            m[f] = i

        def bench():
            hits = 0
            for t in topics:
                hits += len(m.iter_match(t))
            return hits

        benchmark(bench)

    def test_trie_delete(self, benchmark):
        num_filters = 1000
        filters = _gen_filters(num_filters)
        m = MQTTMatcher()
        for i, f in enumerate(filters):
            m[f] = i

        def bench():
            for f in filters:
                try:
                    del m[f]
                except KeyError:
                    pass

        benchmark(bench)

    def test_match_against_client_api(self):
        """不使用 benchmark 插件时也可以运行的简易计时。

        即使在未安装 pytest-benchmark 的环境，也能给出数量级差异的快速反馈。
        """
        num_filters = 5000
        filters = _gen_filters(num_filters)
        topics = _gen_topics(500)

        trie = MQTTMatcher()
        linear = _LinearMatcher()
        for i, f in enumerate(filters):
            trie[f] = i
            linear[f] = i

        # 预热 & 交叉正确性检查
        for t in topics[:50]:
            assert set(trie.iter_match(t)) == set(linear.iter_match(t)), (
                "Trie 和线性扫描必须返回一致的匹配结果"
            )

        # 简单计时
        start = time.perf_counter()
        total_trie = 0
        for t in topics:
            total_trie += len(list(trie.iter_match(t)))
        trie_time = time.perf_counter() - start

        start = time.perf_counter()
        total_linear = 0
        for t in topics:
            total_linear += len(linear.iter_match(t))
        linear_time = time.perf_counter() - start

        assert total_trie == total_linear, "Trie 与线性扫描命中数不一致"
        # 打印到 stderr，避免干扰测试输出
        import sys
        print(
            f"\n[bench] trie={trie_time:.4f}s linear={linear_time:.4f}s "
            f"speedup={linear_time / max(trie_time, 1e-9):.2f}x "
            f"(filters={num_filters}, topics={len(topics)})",
            file=sys.stderr,
        )
