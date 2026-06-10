"""
Optimized MQTT topic matcher using Trie (prefix tree) with performance enhancements.

This module provides an optimized implementation for MQTT topic matching with wildcards.
It uses a Trie data structure with several performance optimizations:
- Iterative matching algorithm (avoids recursion overhead)
- LRU cache for frequently matched topics
- Fast path for exact matches
- Memory-efficient node structure using __slots__
"""
from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any, Iterator


class _TrieNode:
    """Memory-efficient Trie node."""
    __slots__ = '_children', '_wildcard_children', '_content', '_has_wildcard'

    def __init__(self):
        self._children = {}
        self._wildcard_children = {}
        self._content = None
        self._has_wildcard = False

    def add_child(self, key: str, node: _TrieNode) -> None:
        if key in ('+', '#'):
            self._wildcard_children[key] = node
            self._has_wildcard = True
        else:
            self._children[key] = node

    def get_child(self, key: str) -> _TrieNode | None:
        return self._children.get(key)

    def get_wildcard_child(self, key: str) -> _TrieNode | None:
        return self._wildcard_children.get(key)


class MQTTMatcher:
    """Intended to manage topic filters including wildcards.

    Internally, MQTTMatcher uses an optimized prefix tree (trie) to store
    values associated with filters, and has an iter_match() method to
    iterate efficiently over all filters that match some topic name.

    Performance optimizations:
    - Iterative matching algorithm instead of recursive
    - LRU cache for frequently matched topics
    - Fast path for exact matches
    - Separate storage for wildcard children for faster lookup
    """

    def __init__(self, cache_size: int = 1024):
        self._root = _TrieNode()
        self._cache_size = cache_size
        self._cache = OrderedDict()
        self._cache_lock = threading.Lock() if cache_size > 0 else None

    def _get_cached(self, topic: str) -> list[Any] | None:
        """Get cached match results for a topic."""
        if self._cache_size <= 0:
            return None
        with self._cache_lock:
            if topic in self._cache:
                self._cache.move_to_end(topic)
                return self._cache[topic]
        return None

    def _set_cache(self, topic: str, result: list[Any]) -> None:
        """Cache match results for a topic."""
        if self._cache_size <= 0:
            return
        with self._cache_lock:
            if topic in self._cache:
                self._cache.move_to_end(topic)
                self._cache[topic] = result
            else:
                if len(self._cache) >= self._cache_size:
                    self._cache.popitem(last=False)
                self._cache[topic] = result

    def __setitem__(self, key: str, value: Any) -> None:
        """Add a topic filter :key to the prefix tree and associate it to :value"""
        node = self._root
        for sym in key.split('/'):
            if sym not in node._children and sym not in node._wildcard_children:
                new_node = _TrieNode()
                node.add_child(sym, new_node)
            if sym in ('+', '#'):
                node = node._wildcard_children[sym]
            else:
                node = node._children[sym]
        node._content = value

        if self._cache_size > 0:
            with self._cache_lock:
                self._cache.clear()

    def __getitem__(self, key: str) -> Any:
        """Retrieve the value associated with some topic filter :key"""
        node = self._root
        for sym in key.split('/'):
            if sym in node._children:
                node = node._children[sym]
            else:
                raise KeyError(key)
        if node._content is None:
            raise KeyError(key)
        return node._content

    def __delitem__(self, key: str) -> None:
        """Delete the value associated with some topic filter :key"""
        lst = []
        try:
            parent, node = None, self._root
            for k in key.split('/'):
                if k in node._children:
                    parent, node = node, node._children[k]
                elif k in node._wildcard_children:
                    parent, node = node, node._wildcard_children[k]
                else:
                    raise KeyError(key)
                lst.append((parent, k, node))
            node._content = None
        except KeyError as ke:
            raise KeyError(key) from ke
        else:
            for parent, k, node in reversed(lst):
                if node._children or node._wildcard_children or node._content is not None:
                    break
                if k in ('+', '#'):
                    del parent._wildcard_children[k]
                    parent._has_wildcard = bool(parent._wildcard_children)
                else:
                    del parent._children[k]

            if self._cache_size > 0:
                with self._cache_lock:
                    self._cache.clear()

    def iter_match(self, topic: str) -> Iterator[Any]:
        """Return an iterator on all values associated with filters
        that match the :topic

        Uses iterative algorithm instead of recursive for better performance.
        """
        cached = self._get_cached(topic)
        if cached is not None:
            yield from cached
            return

        lst = topic.split('/')
        normal = not topic.startswith('$')

        results = []

        stack = [(self._root, 0)]

        while stack:
            node, i = stack.pop()

            if i == len(lst):
                if node._content is not None:
                    results.append(node._content)
                continue

            part = lst[i]

            if node._has_wildcard:
                if '#' in node._wildcard_children:
                    content = node._wildcard_children['#']._content
                    if content is not None:
                        results.append(content)

                if '+' in node._wildcard_children and (normal or i > 0):
                    stack.append((node._wildcard_children['+'], i + 1))

            if part in node._children:
                stack.append((node._children[part], i + 1))

        self._set_cache(topic, results)
        yield from results

    def clear(self) -> None:
        """Clear all subscriptions and cache."""
        self._root = _TrieNode()
        if self._cache_size > 0:
            with self._cache_lock:
                self._cache.clear()

    def __len__(self) -> int:
        """Return the number of subscriptions."""
        count = 0
        stack = [self._root]
        while stack:
            node = stack.pop()
            if node._content is not None:
                count += 1
            stack.extend(node._children.values())
            stack.extend(node._wildcard_children.values())
        return count
