"""Event model and watermark tracking.

Timestamps are integer milliseconds since the epoch. An event carries a `key`, a numeric `value`,
and a `kind` which is either `data` (counted by aggregations) or `punct` (a punctuation marker that
advances time without contributing a value). Keeping `punct` explicit means a stream can express
"no more data before T" without inventing a fake value.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator

from .errors import ParseError, ValidationError

DATA = "data"
PUNCT = "punct"
KINDS = (DATA, PUNCT)

# NaN / Infinity / -Infinity are Python json extensions, not standard JSON. They are rejected
# before decoding so the error column can point at the token's first character; `-Infinity`
# must be tried before `Infinity` so the column lands on the minus sign.
_NONSTANDARD_CONSTANT = re.compile(r"-Infinity|NaN|Infinity")


def is_finite_number(value: object) -> bool:
    """True for a real number (int/float, not bool) the engine's float domain can hold.

    NaN and infinities are False; so is an integer too large to convert to a float
    (``math.isfinite`` raises ``OverflowError`` on those instead of answering).
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def _find_nonstandard_constant(text: str) -> re.Match[str] | None:
    """First NaN/Infinity token outside a string literal, or None.

    A plain regex would also hit tokens inside string values (``{"key":"NaN"}`` is perfectly
    valid input), so string literals — including escaped quotes — are skipped explicitly.
    """
    index = 0
    while index < len(text):
        if text[index] == '"':
            index += 1
            while index < len(text):
                if text[index] == "\\":
                    index += 2
                elif text[index] == '"':
                    index += 1
                    break
                else:
                    index += 1
            continue
        match = _NONSTANDARD_CONSTANT.match(text, index)
        if match is not None:
            return match
        index += 1
    return None


@dataclass(frozen=True, slots=True)
class Event:
    timestamp: int
    key: str
    value: float = 0.0
    kind: str = DATA

    def to_document(self) -> dict[str, Any]:
        return {"timestamp": self.timestamp, "key": self.key, "value": self.value, "kind": self.kind}


@dataclass(slots=True)
class WatermarkTracker:
    """Tracks how far time has provably advanced.

    The watermark is `max_seen - max_out_of_orderness`. An event whose timestamp is below the
    current watermark is *late*: by default it is counted and dropped, never silently applied to a
    window that was already emitted.
    """

    max_out_of_orderness: int = 0
    _max_seen: int | None = None
    late_dropped: int = field(default=0, init=False)
    observed: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        if self.max_out_of_orderness < 0:
            raise ValidationError("max_out_of_orderness must be >= 0", value=self.max_out_of_orderness)

    @property
    def max_seen(self) -> int | None:
        return self._max_seen

    @property
    def current(self) -> int | None:
        if self._max_seen is None:
            return None
        return self._max_seen - self.max_out_of_orderness

    def observe(self, timestamp: int) -> bool:
        """Record a timestamp; return True when it is late (below the current watermark)."""
        current = self.current
        late = current is not None and timestamp < current
        self.observed += 1
        if late:
            self.late_dropped += 1
        if self._max_seen is None or timestamp > self._max_seen:
            self._max_seen = timestamp
        return late

    def advance_to(self, timestamp: int) -> None:
        """Punctuation: time itself has advanced to `timestamp`."""
        if self._max_seen is None or timestamp > self._max_seen:
            self._max_seen = timestamp

    def is_closed(self, end: int) -> bool:
        """A window ending at `end` (exclusive) may emit once the watermark reaches `end`."""
        current = self.current
        return current is not None and current >= end


def parse_event_line(text: str, *, line: int | None = None) -> Event:
    """Parse one JSON object into an Event, reporting a position on every failure."""
    stripped = text.strip()
    if not stripped:
        raise ParseError("empty line", line=line, column=1)
    constant = _find_nonstandard_constant(stripped)
    if constant is not None:
        raise ParseError(
            f"non-standard JSON constant: {constant.group(0)}",
            line=line,
            column=constant.start() + 1,
        )
    try:
        document = json.loads(stripped)
    except json.JSONDecodeError as error:
        raise ParseError(f"invalid JSON: {error.msg}", line=line, column=error.colno) from error
    if not isinstance(document, dict):
        raise ParseError("event must be a JSON object", line=line, column=1)
    unknown = sorted(set(document) - {"timestamp", "key", "value", "kind"})
    if unknown:
        raise ParseError(f"unknown field(s): {', '.join(unknown)}", line=line, column=1)
    if "timestamp" not in document:
        raise ParseError("missing field: timestamp", line=line, column=1)
    if "key" not in document:
        raise ParseError("missing field: key", line=line, column=1)
    timestamp = document["timestamp"]
    if isinstance(timestamp, bool) or not isinstance(timestamp, int):
        raise ParseError("timestamp must be an integer (milliseconds)", line=line, column=1)
    key = document["key"]
    if not isinstance(key, str) or not key:
        raise ParseError("key must be a non-empty string", line=line, column=1)
    value = document.get("value", 0.0)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ParseError("value must be a number", line=line, column=1)
    if not is_finite_number(value):
        # e.g. 1e400: syntactically valid JSON, but outside the finite domain this engine
        # accepts and emits. Checked for punct events too, so every entry point agrees.
        raise ParseError("value must be a finite number", line=line, column=1)
    kind = document.get("kind", DATA)
    if kind not in KINDS:
        raise ParseError(f"kind must be one of {', '.join(KINDS)}", line=line, column=1)
    return Event(timestamp=timestamp, key=key, value=float(value), kind=kind)


def parse_event_lines(lines: Iterable[str]) -> Iterator[Event]:
    for number, text in enumerate(lines, start=1):
        yield parse_event_line(text, line=number)
