from __future__ import annotations

import json
import threading
import time


def test_publish_and_subscribe_round_trip(pg):
    """Publish a message and receive it via subscribe() in a background thread."""
    received: list[tuple[str, str]] = []

    def listener():
        # subscribe with a 3-second timeout so the thread exits cleanly.
        pg.pubsub.subscribe(
            ["test_channel"],
            lambda ch, payload: received.append((ch, payload)),
            timeout=3.0,
        )

    t = threading.Thread(target=listener, daemon=True)
    t.start()

    # Give the listener time to execute LISTEN before we publish.
    time.sleep(0.3)
    pg.pubsub.publish("test_channel", "hello world")

    t.join(timeout=4.0)

    assert len(received) == 1
    channel, payload = received[0]
    assert channel == "test_channel"
    assert payload == "hello world"


def test_publish_dict_message(pg):
    received: list[dict] = []

    def listener():
        def cb(ch, payload):
            received.append(json.loads(payload))

        pg.pubsub.subscribe(["dict_channel"], cb, timeout=3.0)

    t = threading.Thread(target=listener, daemon=True)
    t.start()
    time.sleep(0.3)
    pg.pubsub.publish("dict_channel", {"event": "user.signup", "id": 42})
    t.join(timeout=4.0)

    assert len(received) == 1
    assert received[0] == {"event": "user.signup", "id": 42}


def test_multiple_channels(pg):
    received: list[str] = []

    def listener():
        pg.pubsub.subscribe(
            ["chan_a", "chan_b"],
            lambda ch, _: received.append(ch),
            timeout=3.0,
        )

    t = threading.Thread(target=listener, daemon=True)
    t.start()
    time.sleep(0.3)
    pg.pubsub.publish("chan_a", "msg1")
    pg.pubsub.publish("chan_b", "msg2")
    t.join(timeout=5.0)

    assert "chan_a" in received
    assert "chan_b" in received
