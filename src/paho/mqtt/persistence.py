from __future__ import annotations

import sqlite3
import threading
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Sequence


class SessionPersistence(ABC):
    @abstractmethod
    def open(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def save_out_message(self, mid: int, topic: str, payload: bytes, qos: int,
                         retain: bool, dup: bool, state: int, timestamp: float,
                         properties: Optional[bytes] = None) -> None:
        pass

    @abstractmethod
    def save_in_message(self, mid: int, topic: str, payload: bytes, qos: int,
                        retain: bool, dup: bool, state: int, timestamp: float,
                        properties: Optional[bytes] = None) -> None:
        pass

    @abstractmethod
    def remove_out_message(self, mid: int) -> None:
        pass

    @abstractmethod
    def remove_in_message(self, mid: int) -> None:
        pass

    @abstractmethod
    def update_out_message_state(self, mid: int, state: int) -> None:
        pass

    @abstractmethod
    def update_out_message_dup(self, mid: int, dup: bool) -> None:
        pass

    @abstractmethod
    def get_out_messages(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_in_messages(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def save_subscription(self, topic: str, qos: int, options: Optional[bytes] = None) -> None:
        pass

    @abstractmethod
    def remove_subscription(self, topic: str) -> None:
        pass

    @abstractmethod
    def get_subscriptions(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def save_last_mid(self, mid: int) -> None:
        pass

    @abstractmethod
    def get_last_mid(self) -> int:
        pass

    @abstractmethod
    def clear_session(self) -> None:
        pass


class SQLitePersistence(SessionPersistence):
    def __init__(self, client_id: str, db_path: Optional[str] = None):
        self._client_id = client_id
        if db_path is None:
            self._db_path = f"paho_mqtt_session_{client_id}.db"
        else:
            self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.RLock()

    def open(self) -> None:
        with self._lock:
            if self._conn is not None:
                return
            self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._create_tables()

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _create_tables(self) -> None:
        assert self._conn is not None
        with self._conn:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS out_messages (
                    mid INTEGER PRIMARY KEY,
                    topic TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    qos INTEGER NOT NULL,
                    retain INTEGER NOT NULL,
                    dup INTEGER NOT NULL,
                    state INTEGER NOT NULL,
                    timestamp REAL NOT NULL,
                    properties BLOB
                )
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS in_messages (
                    mid INTEGER PRIMARY KEY,
                    topic TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    qos INTEGER NOT NULL,
                    retain INTEGER NOT NULL,
                    dup INTEGER NOT NULL,
                    state INTEGER NOT NULL,
                    timestamp REAL NOT NULL,
                    properties BLOB
                )
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS subscriptions (
                    topic TEXT PRIMARY KEY,
                    qos INTEGER NOT NULL,
                    options BLOB
                )
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS session_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)

    def save_out_message(self, mid: int, topic: str, payload: bytes, qos: int,
                         retain: bool, dup: bool, state: int, timestamp: float,
                         properties: Optional[bytes] = None) -> None:
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute(
                    "INSERT OR REPLACE INTO out_messages (mid, topic, payload, qos, retain, dup, state, timestamp, properties) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (mid, topic, payload, qos, int(retain), int(dup), state, timestamp, properties)
                )

    def save_in_message(self, mid: int, topic: str, payload: bytes, qos: int,
                        retain: bool, dup: bool, state: int, timestamp: float,
                        properties: Optional[bytes] = None) -> None:
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute(
                    "INSERT OR REPLACE INTO in_messages (mid, topic, payload, qos, retain, dup, state, timestamp, properties) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (mid, topic, payload, qos, int(retain), int(dup), state, timestamp, properties)
                )

    def remove_out_message(self, mid: int) -> None:
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute("DELETE FROM out_messages WHERE mid = ?", (mid,))

    def remove_in_message(self, mid: int) -> None:
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute("DELETE FROM in_messages WHERE mid = ?", (mid,))

    def update_out_message_state(self, mid: int, state: int) -> None:
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute("UPDATE out_messages SET state = ? WHERE mid = ?", (state, mid))

    def update_out_message_dup(self, mid: int, dup: bool) -> None:
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute("UPDATE out_messages SET dup = ? WHERE mid = ?", (int(dup), mid))

    def get_out_messages(self) -> List[Dict[str, Any]]:
        with self._lock:
            assert self._conn is not None
            cursor = self._conn.execute(
                "SELECT mid, topic, payload, qos, retain, dup, state, timestamp, properties FROM out_messages ORDER BY mid"
            )
            result = []
            for row in cursor:
                result.append({
                    "mid": row[0],
                    "topic": row[1],
                    "payload": row[2],
                    "qos": row[3],
                    "retain": bool(row[4]),
                    "dup": bool(row[5]),
                    "state": row[6],
                    "timestamp": row[7],
                    "properties": row[8],
                })
            return result

    def get_in_messages(self) -> List[Dict[str, Any]]:
        with self._lock:
            assert self._conn is not None
            cursor = self._conn.execute(
                "SELECT mid, topic, payload, qos, retain, dup, state, timestamp, properties FROM in_messages ORDER BY mid"
            )
            result = []
            for row in cursor:
                result.append({
                    "mid": row[0],
                    "topic": row[1],
                    "payload": row[2],
                    "qos": row[3],
                    "retain": bool(row[4]),
                    "dup": bool(row[5]),
                    "state": row[6],
                    "timestamp": row[7],
                    "properties": row[8],
                })
            return result

    def save_subscription(self, topic: str, qos: int, options: Optional[bytes] = None) -> None:
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute(
                    "INSERT OR REPLACE INTO subscriptions (topic, qos, options) VALUES (?, ?, ?)",
                    (topic, qos, options)
                )

    def remove_subscription(self, topic: str) -> None:
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute("DELETE FROM subscriptions WHERE topic = ?", (topic,))

    def get_subscriptions(self) -> List[Dict[str, Any]]:
        with self._lock:
            assert self._conn is not None
            cursor = self._conn.execute("SELECT topic, qos, options FROM subscriptions")
            result = []
            for row in cursor:
                result.append({
                    "topic": row[0],
                    "qos": row[1],
                    "options": row[2],
                })
            return result

    def save_last_mid(self, mid: int) -> None:
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute(
                    "INSERT OR REPLACE INTO session_meta (key, value) VALUES (?, ?)",
                    ("last_mid", str(mid))
                )

    def get_last_mid(self) -> int:
        with self._lock:
            assert self._conn is not None
            cursor = self._conn.execute(
                "SELECT value FROM session_meta WHERE key = ?", ("last_mid",)
            )
            row = cursor.fetchone()
            if row is None:
                return 0
            return int(row[0])

    def clear_session(self) -> None:
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute("DELETE FROM out_messages")
                self._conn.execute("DELETE FROM in_messages")
                self._conn.execute("DELETE FROM subscriptions")
                self._conn.execute("DELETE FROM session_meta")

    def save_all_out_messages(self, messages: Sequence[Dict[str, Any]]) -> None:
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute("DELETE FROM out_messages")
                for msg in messages:
                    self._conn.execute(
                        "INSERT INTO out_messages (mid, topic, payload, qos, retain, dup, state, timestamp, properties) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (msg["mid"], msg["topic"], msg["payload"], msg["qos"],
                         int(msg["retain"]), int(msg["dup"]), msg["state"],
                         msg["timestamp"], msg.get("properties"))
                    )

    def save_all_in_messages(self, messages: Sequence[Dict[str, Any]]) -> None:
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute("DELETE FROM in_messages")
                for msg in messages:
                    self._conn.execute(
                        "INSERT INTO in_messages (mid, topic, payload, qos, retain, dup, state, timestamp, properties) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (msg["mid"], msg["topic"], msg["payload"], msg["qos"],
                         int(msg["retain"]), int(msg["dup"]), msg["state"],
                         msg["timestamp"], msg.get("properties"))
                    )

    def save_all_subscriptions(self, subscriptions: Sequence[Dict[str, Any]]) -> None:
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute("DELETE FROM subscriptions")
                for sub in subscriptions:
                    self._conn.execute(
                        "INSERT INTO subscriptions (topic, qos, options) VALUES (?, ?, ?)",
                        (sub["topic"], sub["qos"], sub.get("options"))
                    )
