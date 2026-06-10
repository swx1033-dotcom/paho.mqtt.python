#!/usr/bin/env python3
"""Simple test to verify the optimized MQTTMatcher works correctly."""
import sys
sys.path.insert(0, 'src')

from paho.mqtt.matcher import MQTTMatcher

def test_basic():
    m = MQTTMatcher()
    
    # Test exact match
    m['foo/bar'] = 'test1'
    assert list(m.iter_match('foo/bar')) == ['test1'], "Exact match failed"
    
    # Test single wildcard
    m['foo/+'] = 'test2'
    assert 'test2' in list(m.iter_match('foo/baz')), "Single wildcard failed"
    
    # Test multi wildcard
    m['#'] = 'test3'
    assert 'test3' in list(m.iter_match('foo/bar/baz')), "Multi wildcard failed"
    
    # Test $SYS topic
    m2 = MQTTMatcher()
    m2['#'] = 'all'
    assert 'all' not in list(m2.iter_match('$SYS/bar')), "$SYS should not match #"
    
    m2['$SYS/#'] = 'sys'
    assert 'sys' in list(m2.iter_match('$SYS/bar')), "$SYS explicit match failed"
    
    print("All basic tests passed!")

def test_performance():
    import time
    
    m = MQTTMatcher()
    
    # Add 1000 subscriptions
    for i in range(1000):
        m[f'sensors/zone{i%10}/sensor{i}'] = f'cb_{i}'
    
    m['sensors/#'] = 'all_sensors'
    
    # Benchmark
    start = time.perf_counter()
    for _ in range(1000):
        list(m.iter_match('sensors/zone5/sensor123'))
    end = time.perf_counter()
    
    avg_ms = (end - start) / 1000 * 1000
    print(f"Average match time (1000 subs): {avg_ms:.3f}ms")
    assert avg_ms < 1.0, f"Too slow: {avg_ms:.3f}ms"
    
    print("Performance test passed!")

if __name__ == '__main__':
    test_basic()
    test_performance()
    print("\nAll tests passed successfully!")
