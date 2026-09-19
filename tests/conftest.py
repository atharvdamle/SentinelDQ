"""Fixtures shared by the ingestion tests."""

MOCK_EVENT = {
    "id": "12345",
    "type": "PushEvent",
    "repo": {"id": 98765, "name": "test/repo", "url": "https://api.github.com/repos/test/repo"},
    "actor": {
        "id": 11111,
        "login": "testuser",
        "url": "https://api.github.com/users/testuser",
        "avatar_url": "https://avatars.githubusercontent.com/u/11111",
    },
    "payload": {"ref": "refs/heads/main", "head": "abcdef123", "before": "123456789", "push_id": 987654321},
    "public": True,
    "created_at": "2025-10-20T12:00:00Z",
}


def stops_after(consumer, *messages):
    """A `poll` side effect that yields each message, then asks for a stop.

    Shutdown in a container is a signal setting `_running = False`, never the
    KeyboardInterrupt these tests used to raise.
    """
    queue = list(messages)

    def poll(timeout=None):
        if queue:
            return queue.pop(0)
        consumer.stop()
        return None

    return poll
