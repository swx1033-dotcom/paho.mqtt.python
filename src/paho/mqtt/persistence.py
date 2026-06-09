import sqlite3
import json
import threading
import os
from typing import Optional, Dict, List, Any, Tuple
from collections import OrderedDict


class SessionPersistence:
    """Abstract base class for session persistence implementations."""

    def open(self, client_id: str) -> None:
        """Open the persistence store for a client."""
        pass

    def close(self) -> None:
        """Close the persistence store."""
        pass

    def save_session_state(self, last_mid: int, out_messages: Dict[int, Any], in_messages: Dict[int, Any], subscriptions: List[Tuple[str, Any]]) -> None:
        """Save the complete session state."""
        pass

    def load_session_state(self) -> Tuple[int, OrderedDict, OrderedDict, List[Tuple[str, Any]]]:
        """Load the complete session state. Returns (last_mid, out_messages, in_messages, subscriptions)."""
        pass

    def clear_session_state(self) -> None:
        """Clear all session state."""
        pass

    def save_out_message(self, mid: int, message: Any) -> None:
        """Save a single outgoing message."""
        pass

    def delete_out_message(self, mid: int) -> None:
        """Delete a single outgoing message."""
        pass

    def save_in_message(self, mid: int, message: Any) -> None:
        """Save a single incoming message."""
        pass

    def delete_in_message(self, mid: int) -> None:
        """Delete a single incoming message."""
        pass

    def add_subscription(self, topic: str, qos: Any) -> None:
        """Add a subscription to persistence."""
        pass

    def remove_subscription(self, topic: str) -> None:
        """Remove a subscription from persistence."""
        pass

    def clear_subscriptions(self) -> None:
        """Clear all subscriptions from persistence."""
        pass


class SQLiteSessionPersistence(SessionPersistence):
    """SQLite-based session persistence with transaction support and thread-safe concurrent access."""

    def __init__(self, db_path: Optional[str] = None, create_dir: bool = True):
        """Initialize SQLite persistence.

        :param db_path: Path to the SQLite database file. If None, uses
               `{client_id}.db` in the current directory.
        :param create_dir: If True, create the directory containing the database
               file if it doesn't exist.
        """
        self.db_path = db_path
        self.create_dir = create_dir
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.RLock()
        self._client_id: str = ""

    def open(self, client_id: str) -> None:
        """Open the database connection and create tables if they don't exist."""
        self._client_id = client_id

        if self.db_path is None:
            db_path = f"{client_id}_session.db"
        else:
            db_path = self.db_path

        if self.create_dir:
            db_dir = os.path.dirname(db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)

        with self._lock:
            self._conn = sqlite3.connect(
                db_path,
                check_same_thread=False,
                detect_types=sqlite3.PARSE_DECLTYPES
            )
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._create_tables()

    def close(self) -> None:
        """Close the database connection."""
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _create_tables(self) -> None:
        """Create the necessary tables if they don't exist."""
        assert self._conn is not None
        cursor = self._conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS session_metadata (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_mid INTEGER NOT NULL DEFAULT 0
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS out_messages (
                mid INTEGER PRIMARY KEY,
                timestamp REAL NOT NULL,
                state INTEGER NOT NULL,
                dup INTEGER NOT NULL,
                topic TEXT NOT NULL,
                payload BLOB NOT NULL,
                qos INTEGER NOT NULL,
                retain INTEGER NOT NULL,
                info_json TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS in_messages (
                mid INTEGER PRIMARY KEY,
                timestamp REAL NOT NULL,
                state INTEGER NOT NULL,
                dup INTEGER NOT NULL,
                topic TEXT NOT NULL,
                payload BLOB NOT NULL,
                qos INTEGER NOT NULL,
                retain INTEGER NOT NULL,
                info_json TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                topic TEXT PRIMARY KEY,
                qos INTEGER NOT NULL,
                options_json TEXT
            )
        """)

        cursor.execute("INSERT OR IGNORE INTO session_metadata (id, last_mid) VALUES (1, 0)")
        self._conn.commit()

    def _serialize_message(self, message: Any) -> Dict[str, Any]:
        """Serialize an MQTT message to a dictionary for storage."""
        return {
            'timestamp': message.timestamp,
            'state': message.state,
            'dup': 1 if message.dup else 0,
            'topic': message.topic if hasattr(message, 'topic') else message._topic.decode('utf-8'),
            'payload': message.payload,
            'qos': message.qos,
            'retain': 1 if message.retain else 0,
            'info_json': self._serialize_info(message.info) if message.info else None,
        }

    def _serialize_info(self, info: Any) -> Optional[str]:
        """Serialize message info to JSON."""
        if info is None:
            return None
        try:
            return json.dumps({
                'mid': info.mid,
                'rc': info.rc if hasattr(info, 'rc') else 0,
                'published': info.is_published() if hasattr(info, 'is_published') else False,
            })
        except (TypeError, ValueError):
            return None

    def _deserialize_message(self, row: sqlite3.Row) -> Dict[str, Any]:
        """Deserialize a message from a database row."""
        return {
            'mid': row[0],
            'timestamp': row[1],
            'state': row[2],
            'dup': bool(row[3]),
            'topic': row[4],
            'payload': row[5],
            'qos': row[6],
            'retain': bool(row[7]),
            'info_json': row[8],
        }

    def save_session_state(self, last_mid: int, out_messages: Dict[int, Any], in_messages: Dict[int, Any], subscriptions: List[Tuple[str, Any]]) -> None:
        """Save the complete session state in a single transaction."""
        with self._lock:
            assert self._conn is not None
            with self._conn:
                cursor = self._conn.cursor()
                cursor.execute("UPDATE session_metadata SET last_mid = ?", (last_mid,))
                cursor.execute("DELETE FROM out_messages")
                cursor.execute("DELETE FROM in_messages")
                cursor.execute("DELETE FROM subscriptions")

                for mid, message in out_messages.items():
                    data = self._serialize_message(message)
                    cursor.execute("""
                        INSERT INTO out_messages
                        (mid, timestamp, state, dup, topic, payload, qos, retain, info_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        mid,
                        data['timestamp'],
                        data['state'],
                        data['dup'],
                        data['topic'],
                        data['payload'],
                        data['qos'],
                        data['retain'],
                        data['info_json'],
                    ))

                for mid, message in in_messages.items():
                    data = self._serialize_message(message)
                    cursor.execute("""
                        INSERT INTO in_messages
                        (mid, timestamp, state, dup, topic, payload, qos, retain, info_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        mid,
                        data['timestamp'],
                        data['state'],
                        data['dup'],
                        data['topic'],
                        data['payload'],
                        data['qos'],
                        data['retain'],
                        data['info_json'],
                    ))

                for topic, qos in subscriptions:
                    if isinstance(qos, int):
                        cursor.execute("""
                            INSERT INTO subscriptions (topic, qos) VALUES (?, ?)
                        """, (topic, qos))
                    else:
                        import pickle
                        options_bytes = pickle.dumps(qos)
                        cursor.execute("""
                            INSERT INTO subscriptions (topic, qos, options_json) VALUES (?, ?, ?)
                        """, (topic, 0, options_bytes))

    def load_session_state(self) -> Tuple[int, OrderedDict, OrderedDict, List[Tuple[str, Any]]]:
        """Load the complete session state from the database."""
        with self._lock:
            assert self._conn is not None
            cursor = self._conn.cursor()

            cursor.execute("SELECT last_mid FROM session_metadata WHERE id = 1")
            row = cursor.fetchone()
            last_mid = row[0] if row else 0

            out_messages = OrderedDict()
            cursor.execute("SELECT mid, timestamp, state, dup, topic, payload, qos, retain, info_json FROM out_messages ORDER BY mid")
            for row in cursor.fetchall():
                data = self._deserialize_message(row)
                out_messages[data['mid']] = data

            in_messages = OrderedDict()
            cursor.execute("SELECT mid, timestamp, state, dup, topic, payload, qos, retain, info_json FROM in_messages ORDER BY mid")
            for row in cursor.fetchall():
                data = self._deserialize_message(row)
                in_messages[data['mid']] = data

            subscriptions = []
            cursor.execute("SELECT topic, qos, options_json FROM subscriptions")
            import pickle
            for topic, qos, options_bytes in cursor.fetchall():
                if options_bytes is not None:
                    try:
                        options = pickle.loads(options_bytes)
                        subscriptions.append((topic, options))
                    except (pickle.UnpicklingError, EOFError):
                        subscriptions.append((topic, qos))
                else:
                    subscriptions.append((topic, qos))

            return last_mid, out_messages, in_messages, subscriptions

    def clear_session_state(self) -> None:
        """Clear all session state from the database."""
        with self._lock:
            assert self._conn is not None
            with self._conn:
                cursor = self._conn.cursor()
                cursor.execute("UPDATE session_metadata SET last_mid = 0")
                cursor.execute("DELETE FROM out_messages")
                cursor.execute("DELETE FROM in_messages")
                cursor.execute("DELETE FROM subscriptions")

    def save_out_message(self, mid: int, message: Any) -> None:
        """Save a single outgoing message."""
        with self._lock:
            assert self._conn is not None
            data = self._serialize_message(message)
            with self._conn:
                self._conn.execute("""
                    REPLACE INTO out_messages
                    (mid, timestamp, state, dup, topic, payload, qos, retain, info_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    mid,
                    data['timestamp'],
                    data['state'],
                    data['dup'],
                    data['topic'],
                    data['payload'],
                    data['qos'],
                    data['retain'],
                    data['info_json'],
                ))

    def delete_out_message(self, mid: int) -> None:
        """Delete a single outgoing message."""
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute("DELETE FROM out_messages WHERE mid = ?", (mid,))

    def save_in_message(self, mid: int, message: Any) -> None:
        """Save a single incoming message."""
        with self._lock:
            assert self._conn is not None
            data = self._serialize_message(message)
            with self._conn:
                self._conn.execute("""
                    REPLACE INTO in_messages
                    (mid, timestamp, state, dup, topic, payload, qos, retain, info_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    mid,
                    data['timestamp'],
                    data['state'],
                    data['dup'],
                    data['topic'],
                    data['payload'],
                    data['qos'],
                    data['retain'],
                    data['info_json'],
                ))

    def delete_in_message(self, mid: int) -> None:
        """Delete a single incoming message."""
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute("DELETE FROM in_messages WHERE mid = ?", (mid,))

    def add_subscription(self, topic: str, qos: Any) -> None:
        """Add a subscription to persistence."""
        with self._lock:
            assert self._conn is not None
            with self._conn:
                if isinstance(qos, int):
                    self._conn.execute("""
                        REPLACE INTO subscriptions (topic, qos) VALUES (?, ?)
                    """, (topic, qos))
                else:
                    import pickle
                    options_bytes = pickle.dumps(qos)
                    self._conn.execute("""
                        REPLACE INTO subscriptions (topic, qos, options_json) VALUES (?, ?, ?)
                    """, (topic, 0, options_bytes))

    def remove_subscription(self, topic: str) -> None:
        """Remove a subscription from persistence."""
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute("DELETE FROM subscriptions WHERE topic = ?", (topic,))

    def clear_subscriptions(self) -> None:
        """Clear all subscriptions from persistence."""
        with self._lock:
            assert self._conn is not None
            with self._conn:
                self._conn.execute("DELETE FROM subscriptions")


class NullSessionPersistence(SessionPersistence):
    """Null implementation that doesn't persist anything - for backward compatibility."""

    def save_session_state(self, last_mid: int, out_messages: Dict[int, Any], in_messages: Dict[int, Any], subscriptions: List[Tuple[str, Any]]) -> None:
        pass

    def load_session_state(self) -> Tuple[int, OrderedDict, OrderedDict, List[Tuple[str, Any]]]:
        return 0, OrderedDict(), OrderedDict(), []

    def clear_session_state(self) -> None:
        pass

    def save_out_message(self, mid: int, message: Any) -> None:
        pass

    def delete_out_message(self, mid: int) -> None:
        pass

    def save_in_message(self, mid: int, message: Any) -> None:
        pass

    def delete_in_message(self, mid: int) -> None:
        pass

    def add_subscription(self, topic: str, qos: Any) -> None:
        pass

    def remove_subscription(self, topic: str) -> None:
        pass

    def clear_subscriptions(self) -> None:
        pass
