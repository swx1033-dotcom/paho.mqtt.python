__version__ = "2.1.1.dev0"


class MQTTException(Exception):
    pass


from .persistence import (
    SessionPersistence,
    SQLiteSessionPersistence,
    NullSessionPersistence,
)

__all__ = [
    'MQTTException',
    'SessionPersistence',
    'SQLiteSessionPersistence',
    'NullSessionPersistence',
]
