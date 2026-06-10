"""
Performance benchmark tests for MQTT topic matcher.

This module contains comprehensive performance benchmarks to verify
the optimization effects of the Trie-based subscription matching.
"""
import time
import random
import string
import statistics
from typing import Callable

import pytest


class TestMatcherPerformance:
    """Performance benchmark tests for MQTTMatcher."""

    def _generate_random_topic(self, depth: int = 3, width: int = 10) -> str:
        """Generate a random topic string."""
        parts = []
        for _ in range(depth):
            part = ''.join(random.choices(string.ascii_lowercase, k=random.randint(3, width)))
            parts.append(part)
        return '/'.join(parts)

    def _generate_topics_with_pattern(self, base: str, count: int) -> list[str]:
        """Generate multiple topics based on a pattern."""
        topics = []
        for i in range(count):
            topics.append(f"{base}/{i}")
        return topics

    def _benchmark(self, func: Callable, iterations: int = 1000) -> dict:
        """Benchmark a function and return timing statistics."""
        times = []
        for _ in range(iterations):
            start = time.perf_counter()
            func()
            end = time.perf_counter()
            times.append(end - start)
        
        return {
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'min': min(times),
            'max': max(times),
            'std': statistics.stdev(times) if len(times) > 1 else 0,
        }

    def test_basic_functionality(self):
        """Verify the optimized matcher maintains correct functionality."""
        from paho.mqtt.matcher import MQTTMatcher
        
        matcher = MQTTMatcher()
        
        # Test exact match
        matcher['foo/bar'] = 'exact'
        results = list(matcher.iter_match('foo/bar'))
        assert 'exact' in results
        
        # Test single-level wildcard
        matcher['foo/+'] = 'single_wildcard'
        results = list(matcher.iter_match('foo/baz'))
        assert 'single_wildcard' in results
        
        # Test multi-level wildcard
        matcher['#'] = 'multi_wildcard'
        results = list(matcher.iter_match('foo/bar/baz'))
        assert 'multi_wildcard' in results
        
        # Test combined wildcards
        matcher['foo/+/baz'] = 'combined'
        results = list(matcher.iter_match('foo/bar/baz'))
        assert 'combined' in results
        
        # Test $SYS topic (should not match #)
        matcher2 = MQTTMatcher()
        matcher2['#'] = 'all'
        results = list(matcher2.iter_match('$SYS/broker'))
        assert 'all' not in results
        
        # Test explicit $SYS subscription
        matcher2['$SYS/#'] = 'sys_only'
        results = list(matcher2.iter_match('$SYS/broker'))
        assert 'sys_only' in results

    def test_performance_with_many_subscriptions(self):
        """Test matching performance with large number of subscriptions."""
        from paho.mqtt.matcher import MQTTMatcher
        
        subscription_counts = [100, 500, 1000]
        
        for sub_count in subscription_counts:
            matcher = MQTTMatcher()
            
            # Add subscriptions
            for i in range(sub_count):
                topic = f"sensors/temperature/zone{i % 10}/sensor{i}"
                matcher[topic] = f"callback_{i}"
            
            # Add some wildcard subscriptions
            for i in range(10):
                matcher[f"sensors/temperature/zone{i}/#"] = f"zone_callback_{i}"
            
            matcher["sensors/#"] = "all_sensors"
            
            # Benchmark matching
            test_topic = "sensors/temperature/zone5/sensor123"
            
            def match_test():
                list(matcher.iter_match(test_topic))
            
            stats = self._benchmark(match_test, iterations=1000)
            
            # The mean matching time should be reasonable (< 1ms for 1000 subscriptions)
            assert stats['mean'] < 0.001, f"Matching too slow with {sub_count} subscriptions: {stats['mean']*1000:.2f}ms"

    def test_performance_with_many_wildcards(self):
        """Test matching performance with many wildcard subscriptions."""
        from paho.mqtt.matcher import MQTTMatcher
        
        matcher = MQTTMatcher()
        
        # Add many wildcard subscriptions
        for i in range(100):
            matcher[f"home/room{i}/+"] = f"room_single_{i}"
            matcher[f"home/room{i}/#"] = f"room_multi_{i}"
        
        matcher["+/+/temperature"] = "temp_callback"
        matcher["home/#"] = "home_all"
        
        # Benchmark matching a topic that matches many wildcards
        test_topic = "home/room5/temperature"
        
        def match_test():
            list(matcher.iter_match(test_topic))
        
        stats = self._benchmark(match_test, iterations=1000)
        
        # Should complete within reasonable time
        assert stats['mean'] < 0.001, f"Wildcard matching too slow: {stats['mean']*1000:.2f}ms"

    def test_caching_performance(self):
        """Test that caching improves repeated matching performance."""
        from paho.mqtt.matcher import MQTTMatcher
        
        # Create matcher with cache
        matcher_cached = MQTTMatcher(cache_size=1024)
        
        # Create matcher without cache
        matcher_no_cache = MQTTMatcher(cache_size=0)
        
        # Add subscriptions
        for i in range(500):
            topic = f"devices/sensor/{i}/data"
            matcher_cached[topic] = f"cb_{i}"
            matcher_no_cache[topic] = f"cb_{i}"
        
        test_topic = "devices/sensor/123/data"
        
        # First match (warm up cache)
        list(matcher_cached.iter_match(test_topic))
        
        def match_cached():
            list(matcher_cached.iter_match(test_topic))
        
        def match_no_cache():
            list(matcher_no_cache.iter_match(test_topic))
        
        stats_cached = self._benchmark(match_cached, iterations=1000)
        stats_no_cache = self._benchmark(match_no_cache, iterations=1000)
        
        # Cached version should be faster
        assert stats_cached['mean'] <= stats_no_cache['mean'], \
            f"Cached ({stats_cached['mean']*1000:.3f}ms) should be faster than no cache ({stats_no_cache['mean']*1000:.3f}ms)"

    def test_performance_scaling(self):
        """Test that matching time scales sub-linearly with subscription count."""
        from paho.mqtt.matcher import MQTTMatcher
        
        subscription_counts = [100, 200, 400, 800]
        times = []
        
        for sub_count in subscription_counts:
            matcher = MQTTMatcher()
            
            # Add subscriptions
            for i in range(sub_count):
                topic = f"topic/level1/level2/{i}"
                matcher[topic] = f"cb_{i}"
            
            test_topic = "topic/level1/level2/50"
            
            def match_test():
                list(matcher.iter_match(test_topic))
            
            stats = self._benchmark(match_test, iterations=500)
            times.append(stats['mean'])
        
        # Check that time doesn't scale linearly (should be better than O(n))
        # With Trie, it should be closer to O(log n) or O(1) for exact matches
        if len(times) >= 2:
            ratio = times[-1] / times[0] if times[0] > 0 else float('inf')
            # Allow some overhead, but should not be purely linear
            # Linear would be 8x (800/100), we expect much better
            assert ratio < 8, f"Scaling appears linear: {ratio:.2f}x for 8x subscriptions"

    def test_deep_topic_performance(self):
        """Test performance with deeply nested topics."""
        from paho.mqtt.matcher import MQTTMatcher
        
        matcher = MQTTMatcher()
        
        # Add deep topic subscriptions
        depth = 10
        for i in range(100):
            parts = [f"level{j}" for j in range(depth)]
            parts.append(f"sensor{i}")
            topic = '/'.join(parts)
            matcher[topic] = f"cb_{i}"
        
        # Test matching deep topic
        test_topic = '/'.join([f"level{j}" for j in range(depth)] + ["sensor50"])
        
        def match_test():
            list(matcher.iter_match(test_topic))
        
        stats = self._benchmark(match_test, iterations=1000)
        
        # Should complete within reasonable time even for deep topics
        assert stats['mean'] < 0.001, f"Deep topic matching too slow: {stats['mean']*1000:.2f}ms"

    def test_delete_performance(self):
        """Test performance of subscription deletion."""
        from paho.mqtt.matcher import MQTTMatcher
        
        matcher = MQTTMatcher()
        
        # Add many subscriptions
        for i in range(1000):
            matcher[f"topic/{i}"] = f"cb_{i}"
        
        # Benchmark deletion
        def delete_test():
            for i in range(100):
                try:
                    del matcher[f"topic/{i}"]
                except KeyError:
                    pass
        
        stats = self._benchmark(delete_test, iterations=10)
        
        # Deletion should be efficient
        assert stats['mean'] < 0.01, f"Deletion too slow: {stats['mean']*1000:.2f}ms"

    def test_mixed_operations_performance(self):
        """Test performance with mixed add/match/delete operations."""
        from paho.mqtt.matcher import MQTTMatcher
        
        matcher = MQTTMatcher()
        
        def mixed_ops():
            # Add
            for i in range(50):
                matcher[f"sensor/{i}"] = f"cb_{i}"
            
            # Match
            for i in range(10):
                list(matcher.iter_match(f"sensor/{i}"))
            
            # Delete
            for i in range(25):
                try:
                    del matcher[f"sensor/{i}"]
                except KeyError:
                    pass
        
        stats = self._benchmark(mixed_ops, iterations=100)
        
        # Mixed operations should be efficient
        assert stats['mean'] < 0.01, f"Mixed operations too slow: {stats['mean']*1000:.2f}ms"

    def test_wildcard_matching_correctness(self):
        """Comprehensive test for wildcard matching correctness."""
        from paho.mqtt.matcher import MQTTMatcher
        
        test_cases = [
            # (subscriptions, topic, should_match_count)
            (['foo/bar'], 'foo/bar', 1),
            (['foo/+'], 'foo/bar', 1),
            (['foo/+'], 'foo/bar/baz', 0),
            (['foo/+/baz'], 'foo/bar/baz', 1),
            (['foo/+/#'], 'foo/bar/baz', 1),
            (['#'], 'foo/bar/baz', 1),
            (['#'], '/foo/bar', 1),
            (['/ #'], '/foo/bar', 1),
            (['$SYS/bar'], '$SYS/bar', 1),
            (['#'], '$SYS/bar', 0),  # $ topics don't match #
            (['A/B/+/#'], 'A/B/B/C', 1),
        ]
        
        for subs, topic, expected_count in test_cases:
            matcher = MQTTMatcher()
            for i, sub in enumerate(subs):
                matcher[sub] = f"cb_{i}"
            
            results = list(matcher.iter_match(topic))
            assert len(results) == expected_count, \
                f"Subscriptions {subs}, topic '{topic}': expected {expected_count} matches, got {len(results)}"

    def test_large_scale_benchmark(self):
        """Large scale benchmark to demonstrate Trie performance advantage."""
        from paho.mqtt.matcher import MQTTMatcher
        
        # Create a realistic scenario with many subscriptions
        matcher = MQTTMatcher()
        
        # Simulate IoT device subscriptions
        device_count = 1000
        for device_id in range(device_count):
            # Device telemetry
            matcher[f"devices/{device_id}/telemetry"] = f"telemetry_{device_id}"
            matcher[f"devices/{device_id}/status"] = f"status_{device_id}"
            matcher[f"devices/{device_id}/alerts/#"] = f"alerts_{device_id}"
        
        # Zone-based subscriptions
        for zone in range(10):
            matcher[f"devices/+/zone{zone}/#"] = f"zone_{zone}"
        
        # Global subscriptions
        matcher["devices/#"] = "all_devices"
        matcher["+/+/telemetry"] = "all_telemetry"
        
        # Benchmark various matching scenarios
        scenarios = [
            "devices/500/telemetry",
            "devices/123/zone5/sensor/data",
            "devices/999/alerts/critical/temperature",
            "devices/42/status",
        ]
        
        for scenario in scenarios:
            def match_test():
                list(matcher.iter_match(scenario))
            
            stats = self._benchmark(match_test, iterations=1000)
            
            # All scenarios should complete within 1ms
            assert stats['mean'] < 0.001, \
                f"Scenario '{scenario}' too slow: {stats['mean']*1000:.2f}ms"
