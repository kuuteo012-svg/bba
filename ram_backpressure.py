"""Bounded RAM ingress. Spoiler/text never wait behind queued media.

There is deliberately no task per received message and no unbounded backlog.
Overload policy: reject lower-priority incoming work, or replace the oldest
work of the same/worse priority; always call task_done for a displaced item.
"""
from __future__ import annotations

import asyncio
import logging

_log = logging.getLogger("autobot.ingress_queue")


def priority(env) -> int:
    message = getattr(env, "message", None)
    if message is None:
        return 4
    entities = getattr(message, "entities", None) or ()
    if any(type(e).__name__ == "MessageEntitySpoiler" for e in entities):
        return 0
    text = str(getattr(message, "message", None) or getattr(message, "text", None) or "").strip()
    if text:
        return 1
    media = getattr(message, "media", None)
    if media is None:
        return 1
    document = getattr(media, "document", None) or getattr(message, "document", None)
    mime = str(getattr(document, "mime_type", "") or "").lower()
    if getattr(message, "photo", None) is not None or mime.startswith("image/"):
        return 2
    return 3


class PrioritizedIngressQueue(asyncio.Queue):
    """Maximum memory-bound queue with priority and responsible task accounting."""

    def __init__(self, maxsize: int = 128):
        super().__init__(maxsize=max(1, int(maxsize)))
        self.dropped_count = 0
        self.rejected_count = 0
        self._on_drop = None

    def set_drop_callback(self, callback):
        self._on_drop = callback

    def _evict(self, index):
        item = self._queue[index]
        del self._queue[index]
        self.task_done()
        self.dropped_count += 1
        if self._on_drop is not None:
            try:
                self._on_drop(item)
            except Exception:
                # A diagnostic/cleanup callback must never discard an incoming
                # Telegram event or corrupt asyncio.Queue's task accounting.
                _log.exception("[IngressQueue] drop callback failed")
        return item

    def drop_oldest(self):
        if not self._queue:
            return None
        worst = max(priority(item) for item in self._queue)
        index = next(i for i, item in enumerate(self._queue) if priority(item) == worst)
        return self._evict(index)

    def put_nowait(self, item):
        if self.full():
            worst = max(priority(old) for old in self._queue)
            if priority(item) > worst:
                self.rejected_count += 1
                raise asyncio.QueueFull
            self.drop_oldest()
        return super().put_nowait(item)

    async def put(self, item):
        return self.put_nowait(item)

    def _get(self):
        best = min(priority(item) for item in self._queue)
        index = next(i for i, item in enumerate(self._queue) if priority(item) == best)
        item = self._queue[index]
        del self._queue[index]
        return item
