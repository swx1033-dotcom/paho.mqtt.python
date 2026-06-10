"""
MQTT 主题过滤器匹配器。

提供 :class:`MQTTMatcher`，用于管理带有通配符 (``+`` / ``#``) 的 MQTT 主题过滤器，
并基于前缀树 (Trie / Prefix Tree) 高效地查找与某个主题名称匹配的所有过滤器。

设计目标
--------
* 插入 / 删除 / 匹配的时间复杂度均与 **主题层级** 相关，与已注册过滤器数量无关；
* 使用迭代式（显式栈）而不是递归生成器，避免深递归、减少 Python 生成器对象开销；
* 仅在必要时保留子节点：删除条目后回收空节点，节省内存。

MQTT 通配符规则
----------------
* ``/`` 是主题层级分隔符；
* ``+`` 匹配任意 **单个** 层级；
* ``#`` 匹配任意 **零或多个** 层级，只能作为最后一个层级；
* 以 ``$`` 开头的主题 **不** 匹配根级别的 ``#`` 或 ``+`` 过滤器。
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, List, Tuple


class MQTTMatcher:
    """管理 MQTT 主题过滤器并高效匹配主题名称。

    示例::

        matcher = MQTTMatcher()
        matcher["sensors/+/temp"] = handle_temp
        matcher["sensors/room1/#"] = handle_room1

        for callback in matcher.iter_match("sensors/room1/temp"):
            callback(...)
    """

    __slots__ = ("_root", "_size")

    class _Node:
        """Trie 中的单个节点，对应主题过滤器的一个层级。"""

        __slots__ = ("_children", "_content")

        def __init__(self) -> None:
            # 使用普通 dict：由于层级数量小，线性查找 + 哈希的组合已足够快，
            # 且避免了额外的对象开销。
            self._children: Dict[str, "MQTTMatcher._Node"] = {}
            # 当此节点对应某个过滤器的末尾时，_content 不为 None。
            self._content: Any = None

        def __repr__(self) -> str:  # pragma: no cover - 仅用于调试
            return f"_Node(children={list(self._children)}, content={self._content!r})"

    def __init__(self) -> None:
        self._root = MQTTMatcher._Node()
        self._size = 0

    def __len__(self) -> int:
        """返回已注册的过滤器数量。"""
        return self._size

    def __contains__(self, key: str) -> bool:
        """判断某个过滤器是否已被注册。"""
        try:
            self[key]
        except KeyError:
            return False
        return True

    def __setitem__(self, key: str, value: Any) -> None:
        """将过滤器 ``key`` 与 ``value`` 关联，插入到前缀树中。"""
        node = self._root
        for sym in key.split('/'):
            # setdefault 的常见路径；避免额外分配。
            child = node._children.get(sym)
            if child is None:
                child = MQTTMatcher._Node()
                node._children[sym] = child
            node = child
        if node._content is None:
            self._size += 1
        node._content = value

    def __getitem__(self, key: str) -> Any:
        """按过滤器字符串精确取出之前存储的值。"""
        node = self._root
        for sym in key.split('/'):
            try:
                node = node._children[sym]
            except KeyError:
                raise KeyError(key) from None
        if node._content is None:
            raise KeyError(key)
        return node._content

    def __delitem__(self, key: str) -> None:
        """删除某个过滤器，并回收空节点。"""
        # 先一路遍历到目标节点，记录路径以便回溯清理。
        path: List[Tuple[MQTTMatcher._Node, str]] = []
        node = self._root
        for sym in key.split('/'):
            try:
                next_node = node._children[sym]
            except KeyError:
                raise KeyError(key) from None
            path.append((node, sym))
            node = next_node

        if node._content is None:
            raise KeyError(key)

        node._content = None
        self._size -= 1

        # 从最深节点向上回溯：如果子节点既没有内容也没有子节点，则删除。
        for parent, sym in reversed(path):
            child = parent._children[sym]
            if child._content is None and not child._children:
                del parent._children[sym]
            else:
                break

    def iter_match(self, topic: str) -> Iterator[Any]:
        """迭代返回所有与 ``topic`` 匹配的过滤器对应的值。

        使用显式栈模拟前序遍历：

        * 在每个层级依次尝试：精确匹配的子节点、``+`` 子节点；
        * 在每个节点都检查是否存在 ``#`` 子节点，因为 ``#`` 匹配任意剩余层级；
        * 以 ``$`` 开头的主题不会与根层的 ``#`` / ``+`` 匹配。

        与原始递归版本相比，这个实现：

        * 不依赖 Python 生成器的递归 yield from，每向下遍历一层少一次
          ``StopIteration`` / 生成器切换开销；
        * 栈中只保存必要的 ``(node, index)``，避免嵌套 ``for content in rec(...)``
          产生的中间生成器对象。
        """
        # 预先一次性拆分主题，避免递归过程中反复切片。
        parts: List[str] = topic.split('/')
        n = len(parts)
        # 以 ``$`` 开头的主题不匹配根层通配符。
        normal_root = not topic.startswith('$')

        # 栈元素: (node, index_at_which_it_was_pushed)
        # 使用 tuple 而非自定义对象，减少内存分配与属性查找开销。
        stack: List[Tuple[MQTTMatcher._Node, int]] = [(self._root, 0)]
        # 将结果缓存在列表中以减少 Python 迭代器 / 生成器对象开销。
        results: List[Any] = []

        while stack:
            node, i = stack.pop()

            # ``#`` 子节点可以匹配任意剩余层级（包括零个）。
            hash_child = node._children.get('#')
            if hash_child is not None:
                # 根层 ``#`` 对 ``$...`` 主题无效。
                if normal_root or i > 0:
                    if hash_child._content is not None:
                        results.append(hash_child._content)

            if i == n:
                # 到达主题末尾：若当前节点对应一个完整过滤器，则产出。
                if node._content is not None:
                    results.append(node._content)
                continue

            part = parts[i]
            next_idx = i + 1

            # 先压 ``+``，再压精确匹配，这样精确匹配会先被处理
            # （LIFO 栈）。顺序不影响正确性，但保持与原实现类似的
            # 行为有助于向后兼容。
            plus_child = node._children.get('+')
            if plus_child is not None:
                if normal_root or i > 0:
                    stack.append((plus_child, next_idx))

            exact_child = node._children.get(part)
            if exact_child is not None:
                stack.append((exact_child, next_idx))

        # 一次性 yield，避免在循环中反复 yield 产生的微小开销。
        yield from results

    def __iter__(self) -> Iterator[str]:
        """迭代返回所有已注册的过滤器字符串 (深度优先)。

        主要用于测试 / 调试。
        """
        stack: List[Tuple[MQTTMatcher._Node, str]] = [(self._root, "")]
        while stack:
            node, prefix = stack.pop()
            if node._content is not None:
                yield prefix
            # 为了稳定的迭代顺序，按字母排序子节点。
            for sym in sorted(node._children.keys(), reverse=True):
                child = node._children[sym]
                new_prefix = sym if prefix == "" else prefix + "/" + sym
                stack.append((child, new_prefix))
