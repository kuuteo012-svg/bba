"""Push-only Telegram admission: bounded RAM dedup and a single priority queue.

No history RPC, polling, disk inbox, backfill, or persistence worker lives here.
"""
from __future__ import annotations
import asyncio
import hashlib
import inspect
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Iterable, Optional
from config import Config
from ram_backpressure import PrioritizedIngressQueue

_log = logging.getLogger("autobot." + __name__)
RAW_INGEST_QUEUE = PrioritizedIngressQueue(maxsize=Config.INGEST_QUEUE_MAXSIZE)

def _message_dedup_key(chat_id: int, message: Any, edited: bool) -> tuple:
    edit_dt = getattr(message, "edit_date", None)
    edit_ts = int(edit_dt.timestamp()) if edit_dt else 0
    entity_signature = tuple(
        (type(entity).__name__, getattr(entity, "offset", None),
         getattr(entity, "length", None), getattr(entity, "url", None))
        for entity in (getattr(message, "entities", None) or [])
    )
    text = str(getattr(message, "message", None) or getattr(message, "text", None) or "")
    text_hash = hashlib.blake2s(text.encode("utf-8", "surrogatepass"), digest_size=12).digest()
    media = getattr(message, "media", None)
    media_object = (getattr(media, "document", None) or getattr(media, "photo", None)
                    or getattr(media, "webpage", None))
    media_signature = (type(media).__name__, getattr(media_object, "id", None)) if media else None
    return int(chat_id), int(message.id), edit_ts, entity_signature, text_hash, media_signature


def _ingress_age_limit(chat_id: int, cfg: Any) -> float:
    channel = getattr(Config, "CHANNEL_CONFIG", {}).get(int(chat_id), {})
    if "xx88code.com" in str(channel.get("url", "")).lower():
        return max(0.0, float(getattr(cfg, "xx88_max_age_seconds", 40.0)))
    ocr_only = set(getattr(Config, "OCR_MEDIA_ONLY_CHANNEL_IDS", ()))
    if int(chat_id) in ocr_only:
        return max(0.0, float(getattr(cfg, "ocr_max_age_seconds", 45.0)))
    return max(0.0, float(getattr(cfg, "max_age_seconds", 120.0)))


@dataclass
class Envelope:
    chat_id: int
    message: Any
    edited: bool
    source: str
    seen_perf: float
    age_s: float

class OfferResult(str, Enum):

    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    FILTERED = "filtered"
    INVALID = "invalid"
    SINK_FAILED = "sink_failed"


@dataclass
class ReceiverConfig:
    dedup_size: int = 10000
    dedupe_ttl_seconds: float = 240.0
    age_drop_enabled: bool = True
    max_age_seconds: float = 120.0
    xx88_max_age_seconds: float = 40.0
    ocr_max_age_seconds: float = 45.0

    @classmethod
    def from_config(cls, cfg):
        return cls(
            dedup_size=min(10000, max(1, int(getattr(cfg, "TELEGRAM_INGRESS_DEDUPE_MAX_ENTRIES", 10000)))),
            dedupe_ttl_seconds=min(240.0, max(1.0, float(getattr(cfg, "TELEGRAM_INGRESS_DEDUPE_TTL_SEC", 240.0)))),
            age_drop_enabled=bool(getattr(cfg, "TELEGRAM_INGRESS_AGE_DROP_ENABLED", True)),
            max_age_seconds=float(getattr(cfg, "INGRESS_MAX_AGE_SEC", 120.0)),
            xx88_max_age_seconds=float(getattr(cfg, "INGRESS_XX88_MAX_AGE_SEC", 40.0)),
            ocr_max_age_seconds=float(getattr(cfg, "INGRESS_OCR_MAX_AGE_SEC", 45.0)),
        )

class TelegramReceiver:
    def __init__(self, client, chats: Iterable[int], sink: Callable, *, accept=None,
                 cfg=None, log=None, clock=time.monotonic):
        self._chat_set = set(map(int, chats))
        self._sink = sink
        self._accept = accept
        self._cfg = cfg or ReceiverConfig()
        self._log = log or _log
        self._clock = clock
        self._seen = OrderedDict()
        self._pending_ingress = set()
        self._st = dict(push=0, dup=0, rejected=0, age_drop=0, error=0)

    def _prune_seen(self):
        cutoff = self._clock() - self._cfg.dedupe_ttl_seconds
        while self._seen and (next(iter(self._seen.values())) <= cutoff
                              or len(self._seen) > self._cfg.dedup_size):
            self._seen.popitem(last=False)

    def _message_age(self, message, edited=False):
        date = (getattr(message, "edit_date", None) if edited else None) or getattr(message, "date", None)
        if date is None:
            return -1.0
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - date).total_seconds())

    async def _offer_detailed(self, chat_id, message, *, edited=False, source="push"):
        try:
            cid = int(chat_id)
        except (TypeError, ValueError):
            return OfferResult.INVALID
        if cid not in self._chat_set or message is None or getattr(message, "id", None) is None:
            return OfferResult.INVALID
        if self._accept is not None and not self._accept(message):
            self._st["rejected"] += 1
            return OfferResult.FILTERED
        key = _message_dedup_key(cid, message, edited)
        self._prune_seen()
        if key in self._seen or key in self._pending_ingress:
            self._st["dup"] += 1
            return OfferResult.DUPLICATE
        age = self._message_age(message, edited)
        if self._cfg.age_drop_enabled and age > _ingress_age_limit(cid, self._cfg):
            self._st["age_drop"] += 1
            self._log.info("INGRESS_DROP_OLD chat=%s message=%s age=%.1f", cid, message.id, age)
            return OfferResult.FILTERED
        env = Envelope(cid, message, edited, source, time.perf_counter(), age)
        self._pending_ingress.add(key)
        try:
            result = self._sink(env)
            if inspect.isawaitable(result):
                await result
        except asyncio.QueueFull:
            self._st["rejected"] += 1
            return OfferResult.SINK_FAILED
        finally:
            self._pending_ingress.discard(key)
        self._seen[key] = self._clock()
        self._prune_seen()
        self._st["push"] += 1
        return OfferResult.ACCEPTED

    async def on_push(self, event, *, edited=False):
        try:
            return await self._offer_detailed(getattr(event, "chat_id", None),
                                             getattr(event, "message", None), edited=edited)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._st["error"] += 1
            self._log.exception("[Telegram] push admission failed")
            return OfferResult.SINK_FAILED
