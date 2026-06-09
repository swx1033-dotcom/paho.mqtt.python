#!/usr/bin/env python3

"""
selectors_multi_client.py — 使用 selectors 模块管理多个 MQTT 客户端

本示例展示了如何同时管理两个不同的 MQTT 客户端，使用 Python 的 selectors
模块（而非底层的 select.select()）进行 I/O 多路复用。

关键特性：
- 使用 selectors.DefaultSelector 注册/注销每个客户端的 socket
- 通过 on_socket_open / on_socket_close / on_socket_register_write /
  on_socket_unregister_write 回调实现与 selector 的解耦集成
- 周期性调用 loop_misc() 处理 keepalive 和重连逻辑
- 使用 selector.select(timeout) 并以最小剩余定时任务时间为超时上限
- 正确处理客户端断开与重连时的 selector 注册/注销

常见陷阱说明见代码内注释（标注为「陷阱」）。
"""

import selectors
import socket
import time
import uuid

import paho.mqtt.client as mqtt

BROKER = "mqtt.eclipseprojects.io"
PORT = 1883
KEEPALIVE = 60

CLIENT1_ID = "paho-selectors-multi-1/" + str(uuid.uuid4())
CLIENT1_TOPIC = CLIENT1_ID
CLIENT2_ID = "paho-selectors-multi-2/" + str(uuid.uuid4())
CLIENT2_TOPIC = CLIENT2_ID

# 固定间隔调用 loop_misc()，确保每个客户端的 keepalive 和内部定时器得到及时处理。
# 设置为 keepalive 的一半，保证在 keepalive 超时前有至少两次机会发送 PINGREQ。
MISC_INTERVAL = min(1.0, KEEPALIVE / 2)


class SelectorsMqttManager:
    """
    使用 selectors 模块管理多个 MQTT 客户端的 I/O 循环。

    职责：
    - 维护 selector 注册表，将每个客户端的 socket 注册到 selector 上监听读/写事件
    - 响应客户端的回调（socket_open / socket_close / register_write / unregister_write）
    - 在事件循环中定期调用每个客户端的 loop_misc()
    """

    def __init__(self):
        self.sel = selectors.DefaultSelector()
        # socket -> client 的映射。
        #
        # 陷阱：selector 的 key.data 也可以存储用户数据，但 selectors 的事件循环
        # 中 FileObj 作为 key 使用，跨平台行为存在差异（部分平台使用 fd 而非
        # socket 对象）。因此额外维护一个字典来映射 socket -> client 是最可靠的做法。
        self._sock_to_client: dict = {}

        # 记录每个客户端最后一次调用 loop_misc() 的时间，用于计算下次调度的超时。
        self._last_misc: dict[int, float] = {}

        # 标记是否正在运行。当所有客户端都已断开且不应重连时设为 False。
        self._running = False

    def _register_read(self, client, sock):
        """
        将 socket 注册到 selector，监听可读事件。

        陷阱：必须先检查 socket 是否已经注册，重复注册同一个 socket 会导致
        KeyError。同样，modify() 可以替代 register() 来修改监听事件，但
        需要确认 selector 实现支持（DefaultSelector 在 Linux 上通常是 EpollSelector，
        支持 modify）。
        """
        key = self.sel.get_key(sock)
        if key is None:
            self.sel.register(sock, selectors.EVENT_READ, client)
        else:
            # socket 已注册，改为同时监听读写事件
            self.sel.modify(sock, key.events | selectors.EVENT_READ)
        self._sock_to_client[sock] = client

    def _unregister_read(self, client, sock):
        """
        从 selector 中移除 socket 的可读监听。

        陷阱：不要简单地调用 self.sel.unregister(sock)，因为可能还需要监听
        可写事件。应当只移除 EVENT_READ 标志位。如果读写都不再需要，则完全注销。
        """
        key = self.sel.get_key(sock)
        if key is None:
            return
        new_events = key.events & ~selectors.EVENT_READ
        if new_events:
            self.sel.modify(sock, new_events)
        else:
            self.sel.unregister(sock)
            self._sock_to_client.pop(sock, None)

    def _register_write(self, client, sock):
        """将 socket 注册到 selector，增加可写事件监听。"""
        key = self.sel.get_key(sock)
        if key is None:
            self.sel.register(sock, selectors.EVENT_WRITE, client)
        else:
            self.sel.modify(sock, key.events | selectors.EVENT_WRITE)
        self._sock_to_client[sock] = client

    def _unregister_write(self, client, sock):
        """
        从 selector 中移除 socket 的可写监听。

        陷阱：loop_write() 调用后，内部可能仍有待发送数据。on_socket_unregister_write
        仅在 _registered_write 为 True 时被调用，所以这里做幂等处理是安全的。
        """
        key = self.sel.get_key(sock)
        if key is None:
            return
        new_events = key.events & ~selectors.EVENT_WRITE
        if new_events:
            self.sel.modify(sock, new_events)
        else:
            self.sel.unregister(sock)
            self._sock_to_client.pop(sock, None)

    # ------------------------------------------------------------------
    # 回调：由 paho-mqtt 客户端在 I/O 状态变化时调用
    # ------------------------------------------------------------------

    def on_socket_open(self, client, userdata, sock):
        """当客户端 socket 打开时，注册可读事件。"""
        print(f"[{client.client_id}] Socket opened, fd={sock.fileno()}")
        self._register_read(client, sock)
        self._last_misc[id(client)] = time.time()

    def on_socket_close(self, client, userdata, sock):
        """
        当客户端 socket 即将关闭时，从 selector 完全注销。

        陷阱：on_socket_close 可能被多次调用（例如显式 disconnect 后再由析构触发）。
        应当做幂等处理——重复调用 unregister 是安全的，因为我们在 _unregister_*
        中已经通过 get_key() 做了检查。
        """
        print(f"[{client.client_id}] Socket closed, fd={sock.fileno()}")
        self._unregister_read(client, sock)
        self._unregister_write(client, sock)
        self._last_misc.pop(id(client), None)

    def on_socket_register_write(self, client, userdata, sock):
        """当客户端有待发送数据时，注册可写事件。"""
        self._register_write(client, sock)

    def on_socket_unregister_write(self, client, userdata, sock):
        """当客户端数据发送完毕后，移除可写事件。"""
        self._unregister_write(client, sock)

    # ------------------------------------------------------------------
    # 事件循环
    # ------------------------------------------------------------------

    def _get_timeout(self):
        """
        计算 selector.select() 的超时时间。

        超时时间 = 距离下一次 loop_misc 应该被调用的最小剩余时间。

        陷阱：
        1. 如果没有任何客户端在运行，返回 None（永久阻塞）会导致事件循环无法退出。
           因此应始终返回一个有限值或 0。
        2. loop_misc 除了处理 keepalive 外，还负责检查 PINGRESP 超时和消息重传。
           即使 keepalive 较大（如 60s），也应以较短的间隔调用 loop_misc，
           否则会延迟 PINGRESP 超时检测和 QoS>0 消息的确认。
        """
        now = time.time()
        min_remaining = MISC_INTERVAL  # 默认值：下一个周期
        for client_id, last in list(self._last_misc.items()):
            elapsed = now - last
            remaining = max(0, MISC_INTERVAL - elapsed)
            if remaining < min_remaining:
                min_remaining = remaining

        # 陷阱：select 的超时不能为负，也不能小于 0。
        # 另一方面，超时设为 0（非阻塞轮询）会导致 CPU 空转，仅在需要立即
        # 处理 loop_misc 时才使用。
        return max(0, min_remaining)

    def _call_loop_misc_all(self):
        """
        遍历所有客户端，在需要时调用 loop_misc()。

        陷阱：loop_misc() 必须在持有 socket 的客户端上调用。
        如果 socket 为 None 但 _last_misc 中仍有记录（例如正在重连中），
        应该跳过或清理。loop_misc() 自身会检查 _sock 的存在性并返回
        MQTT_ERR_NO_CONN，因此调用是安全的，但需要及时清理状态。
        """
        now = time.time()
        for sock, client in list(self._sock_to_client.items()):
            client_id = id(client)
            last = self._last_misc.get(client_id, 0)
            if now - last >= MISC_INTERVAL:
                client.loop_misc()
                self._last_misc[client_id] = now

    def run(self):
        """
        主事件循环。

        陷阱：
        1. 不要在回调中直接修改 selector 的注册表（如注册/注销），因为 select()
           返回的 (key, events) 列表在遍历时如果变更注册表，可能导致迭代行为
           不确定。selectors 模块在内部已经做好了保护（遍历的是快照），但
           在回调链中需要警惕重入问题（例如 loop_read 触发消息回调，消息回调中
           又调用 publish，publish 触发 register_write）。本示例中回调仅修改
           selector 状态，实际的 loop_read / loop_write 在事件循环中调用，避开了
           此问题。
        2. 即使 selector 返回了可写事件，也不意味着一定有数据要写。必须先用
           want_write() 确认。
        3. 对已关闭的 socket 调用 fileno() 会抛出 OSError。如果回调中 socket
           已经无效，应捕获并妥善处理。
        """
        self._running = True
        print("Selector event loop started. Press Ctrl+C to exit.")
        print(f"Client 1: {CLIENT1_ID}")
        print(f"Client 2: {CLIENT2_ID}")

        try:
            while self._running:
                timeout = self._get_timeout()
                events = self.sel.select(timeout=timeout)

                for key, mask in events:
                    sock = key.fileobj
                    client = key.data

                    # 陷阱：socket 可能已经断开，需要做防御性检查。
                    if sock.fileno() < 0:
                        continue

                    if mask & selectors.EVENT_READ:
                        # 只有在 socket 可读时才调用 loop_read()。
                        # 底层使用非阻塞 I/O，如果没有数据会返回 MQTT_ERR_SUCCESS。
                        client.loop_read()

                    if mask & selectors.EVENT_WRITE:
                        # 陷阱：event mask 可能同时包含 READ 和 WRITE。
                        # 先处理读事件，再处理写事件，避免读缓冲区满导致协议死锁。
                        if client.want_write():
                            client.loop_write()
                        else:
                            # 理论上不会发生（register_write 回调只在有数据时触发），
                            # 但做一次安全解除以保持状态一致。
                            self._unregister_write(client, sock)

                # 处理所有客户端的定时任务（keepalive、重传等）
                self._call_loop_misc_all()

                # 检查是否所有客户端都已断开且无重连计划
                if not self._sock_to_client and not self._last_misc:
                    print("All clients disconnected. Exiting.")
                    self._running = False

        except KeyboardInterrupt:
            print("Interrupted by user.")
        finally:
            self.sel.close()


# ======================================================================
# 客户端配置：两个角色不同的 MQTT 客户端
# ======================================================================

class PublisherClient:
    """发布者客户端：定期向自己的 topic 发送消息"""

    def __init__(self, manager, client_id, topic):
        self.manager = manager
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        self.topic = topic
        self.disconnected = (False, None)
        self.publish_count = 0
        self.last_publish = 0.0

        # 设置客户端回调
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish

        # 将 socket 生命周期事件委托给 SelectorsMqttManager
        #
        # 陷阱：每个客户端实例必须独立绑定这 4 个回调。如果两个客户端共用同一个
        # 回调处理函数，需要在回调中通过 client 参数区分哪个客户端触发了事件。
        self.client.on_socket_open = manager.on_socket_open
        self.client.on_socket_close = manager.on_socket_close
        self.client.on_socket_register_write = manager.on_socket_register_write
        self.client.on_socket_unregister_write = manager.on_socket_unregister_write

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            print(f"[{client.client_id}] Connected to broker.")
        else:
            print(f"[{client.client_id}] Connection failed: {reason_code}")

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        print(f"[{client.client_id}] Disconnected: {reason_code}")
        self.disconnected = (True, reason_code)

    def on_publish(self, client, userdata, mid, reason_code, properties):
        print(f"[{client.client_id}] Published message {mid}")

    def connect(self):
        print(f"[{self.client.client_id}] Connecting...")
        self.client.connect(BROKER, PORT, KEEPALIVE)
        # 设置发送缓冲区大小，模拟大消息场景以触发 write 注册需求
        self.client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)

    def publish_if_needed(self, now):
        if not self.disconnected[0] and now - self.last_publish >= 5.0:
            payload = b"Hello from publisher " * 1000  # 较大的 payload
            self.client.publish(self.topic, payload, qos=1)
            self.publish_count += 1
            self.last_publish = now
            print(f"[{self.client.client_id}] Publishing #{self.publish_count} "
                  f"({len(payload)} bytes)")

    def disconnect(self):
        self.client.disconnect()


class SubscriberClient:
    """订阅者客户端：订阅发布者的 topic 并接收消息"""

    def __init__(self, manager, client_id, subscribe_topic):
        self.manager = manager
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        self.subscribe_topic = subscribe_topic
        self.disconnected = (False, None)
        self.message_count = 0

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

        self.client.on_socket_open = manager.on_socket_open
        self.client.on_socket_close = manager.on_socket_close
        self.client.on_socket_register_write = manager.on_socket_register_write
        self.client.on_socket_unregister_write = manager.on_socket_unregister_write

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            print(f"[{client.client_id}] Connected. Subscribing to {self.subscribe_topic}")
            client.subscribe(self.subscribe_topic, qos=1)
        else:
            print(f"[{client.client_id}] Connection failed: {reason_code}")

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        print(f"[{client.client_id}] Disconnected: {reason_code}")
        self.disconnected = (True, reason_code)

    def on_message(self, client, userdata, msg):
        self.message_count += 1
        print(f"[{client.client_id}] Received #{self.message_count}: "
              f"topic={msg.topic}, len={len(msg.payload)} bytes")

    def connect(self):
        print(f"[{self.client.client_id}] Connecting...")
        self.client.connect(BROKER, PORT, KEEPALIVE)
        self.client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)

    def disconnect(self):
        self.client.disconnect()


# ======================================================================
# 主程序入口
# ======================================================================

def main():
    """
    演示流程：
    1. 创建 SelectorsMqttManager 作为 I/O 管理器
    2. 创建发布者和订阅者两个客户端
    3. 连接两个客户端
    4. 进入事件循环：
       - selector.select() 等待 I/O 事件
       - 处理可读/可写事件，调用 loop_read/loop_write
       - 定期调用 loop_misc 处理 keepalive
       - 发布者每 5 秒发送一条消息
    5. 发布 5 条消息后，优雅断开所有客户端连接

    陷阱总结：
    - 重复注册：使用 get_key() 检查已有注册状态后再做 modify 或 register
    - socket 失效：在回调中通过 fileno() < 0 检查 socket 是否有效
    - 竞态条件：loop_write 后会通过 want_write() 重新触发 register/unregister
      回调，必须确保回调是幂等的
    - 内存泄漏：断开连接后确保从 _sock_to_client 和 _last_misc 中清理条目
    - 超时计算：永远不要让 select() 的 timeout 大于最小定时任务剩余时间，
      否则 keepalive 会延迟导致 broker 断开连接
    """
    manager = SelectorsMqttManager()

    publisher = PublisherClient(manager, CLIENT1_ID, CLIENT1_TOPIC)
    subscriber = SubscriberClient(manager, CLIENT2_ID, CLIENT1_TOPIC)

    publisher.connect()
    subscriber.connect()

    # 陷阱：connect() 之后 socket 已经打开并注册到 selector，但 CONNACK 尚未到达。
    # MQTT 协议要求在 socket 打开后 CONNACK 返回前不能发布消息，因此我们在
    # on_connect 回调中才开始订阅，在主循环中通过 disconnected 标记控制发布时间。

    start_time = time.time()
    max_publishes = 5

    try:
        while not (publisher.disconnected[0] and subscriber.disconnected[0]):
            timeout = manager._get_timeout()
            events = manager.sel.select(timeout=timeout)

            for key, mask in events:
                sock = key.fileobj
                client = key.data

                if sock.fileno() < 0:
                    continue

                if mask & selectors.EVENT_READ:
                    client.loop_read()

                if mask & selectors.EVENT_WRITE:
                    if client.want_write():
                        client.loop_write()
                    else:
                        manager._unregister_write(client, sock)

            manager._call_loop_misc_all()

            now = time.time()

            # 发布者：每 5 秒发一条消息，达到上限后断开
            publisher.publish_if_needed(now)
            if publisher.publish_count >= max_publishes and not publisher.disconnected[0]:
                print(f"[{publisher.client.client_id}] Reached {max_publishes} publishes, disconnecting.")
                publisher.disconnect()

            # 订阅者：收到一定数量消息后断开
            if subscriber.message_count >= max_publishes and not subscriber.disconnected[0]:
                print(f"[{subscriber.client.client_id}] Received enough messages, disconnecting.")
                subscriber.disconnect()

            # 安全退出：两个客户端都确认断开后退出
            if publisher.disconnected[0] and subscriber.disconnected[0]:
                print("Both clients disconnected. Exiting event loop.")
                break

            # 陷阱：如果运行时间过长且 selector 中没有已注册的 socket，
            # select() 将永远阻塞（如果 timeout=None）。这里 timeout 由
            # _get_timeout() 返回一个有限值，保证循环可以定期检查退出条件。
            if not manager.sel.get_map():
                print("No sockets registered. Exiting.")
                break

    except KeyboardInterrupt:
        print("Interrupted by user.")
        if not publisher.disconnected[0]:
            publisher.disconnect()
        if not subscriber.disconnected[0]:
            subscriber.disconnect()
    finally:
        manager.sel.close()
        print("Selector closed.")

    print(f"Finished. Publisher sent {publisher.publish_count} messages, "
          f"subscriber received {subscriber.message_count} messages.")


if __name__ == "__main__":
    main()