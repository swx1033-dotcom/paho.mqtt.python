import time

import paho.mqtt.client as client


def _build_subscription_filters() -> list[str]:
    exact_filters = [
        f"bench/site-{index}/room-{index % 32}/sensor-{index % 8}"
        for index in range(4000)
    ]
    single_level_wildcards = [
        f"bench/site-{index}/+/status"
        for index in range(1024)
    ]
    multi_level_wildcards = [
        f"bench/+/room-{index}/#"
        for index in range(128)
    ]
    return exact_filters + single_level_wildcards + multi_level_wildcards


def _linear_matches(topic_filters: list[str], topic: str) -> tuple[str, ...]:
    return tuple(
        topic_filter
        for topic_filter in topic_filters
        if client.topic_matches_sub(topic_filter, topic)
    )


def _measure(iterations: int, func) -> float:
    start = time.perf_counter()
    for _ in range(iterations):
        func()
    return time.perf_counter() - start


def test_subscription_trie_tracks_subscribe_and_unsubscribe() -> None:
    mqttc = client.Client(client.CallbackAPIVersion.VERSION2)

    mqttc._track_subscribe_request(7, ["bench/1/+", "bench/2/#", "bench/2/#", "bench/3/exact"])
    mqttc._confirm_subscribe_request(
        7,
        [
            client.ReasonCode(client.SUBACK >> 4, identifier=1),
            client.ReasonCode(client.SUBACK >> 4, identifier=2),
            client.ReasonCode(client.SUBACK >> 4, identifier=2),
            client.ReasonCode(client.SUBACK >> 4, identifier=0x80),
        ],
    )

    assert mqttc._matching_subscriptions("bench/1/value") == ("bench/1/+",)
    assert mqttc._matching_subscriptions("bench/3/exact") == ()
    assert mqttc._matching_subscriptions("bench/2/value/deep") == ("bench/2/#",)

    mqttc._track_unsubscribe_request(8, ["bench/2/#"])
    mqttc._confirm_unsubscribe_request(8, [])
    assert mqttc._matching_subscriptions("bench/2/value/deep") == ("bench/2/#",)

    mqttc._track_unsubscribe_request(9, ["bench/2/#"])
    mqttc._confirm_unsubscribe_request(9, [])
    assert mqttc._matching_subscriptions("bench/2/value/deep") == ()


def test_subscription_trie_benchmark() -> None:
    topic_filters = _build_subscription_filters()
    topic = "bench/site-512/room-10/status"

    mqttc = client.Client(client.CallbackAPIVersion.VERSION2)
    mqttc._commit_subscriptions(topic_filters)

    expected = _linear_matches(topic_filters, topic)
    assert set(mqttc._matching_subscriptions(topic)) == set(expected)

    iterations = 4
    trie_elapsed = 0.0
    linear_elapsed = 0.0
    while linear_elapsed < 0.05 and iterations < 256:
        trie_elapsed = _measure(iterations, lambda: mqttc._matching_subscriptions(topic))
        linear_elapsed = _measure(iterations, lambda: _linear_matches(topic_filters, topic))
        iterations *= 2

    trie_elapsed = _measure(iterations, lambda: mqttc._matching_subscriptions(topic))
    linear_elapsed = _measure(iterations, lambda: _linear_matches(topic_filters, topic))

    assert trie_elapsed < linear_elapsed
