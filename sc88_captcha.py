"""Conservative OCR for an SC88 CAPTCHA image; does not submit or refresh it."""
from __future__ import annotations

import math
import re

import cv2
import numpy as np


def recognize_sc88_captcha(png: bytes, extractor, *, min_confidence: float = .90):
    if not png:
        return None
    image = cv2.imdecode(np.frombuffer(png, dtype=np.uint8), cv2.IMREAD_COLOR)
    if image is None or image.size == 0:
        return None
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    enlarged = cv2.resize(image, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
    monochrome = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)
    readings = extractor.recognize_code_regions([image, enlarged, monochrome])
    if len(readings) != 3:
        return None
    accepted = {}
    for text, confidence in readings:
        raw = str(text or '').strip()
        try:
            score = float(confidence)
        except (TypeError, ValueError):
            continue
        if (re.fullmatch(r'[A-Za-z0-9]{4,8}', raw) and math.isfinite(score)
                and min_confidence <= score <= 1.0):
            accepted.setdefault(raw, []).append(score)
    agreed = [(code, scores) for code, scores in accepted.items() if len(scores) >= 2]
    if len(agreed) != 1:
        return None
    code, scores = agreed[0]
    return {'text': code, 'confidence': min(scores), 'votes': len(scores)}
