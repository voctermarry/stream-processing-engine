"""The stateful pipeline: assign events to windows, aggregate per key, emit on watermark.

Emission is driven by the watermark, not by input order, so the same input produces the same output
regardless of how the lines are grouped. `add` returns whatever closed during that call; `flush`
returns the remainder. Results are always ordered by `(window.start, key)`.

A running pipeline can be snapshotted with `checkpoint` and reconstructed with `restore`: the
document captures the event-time clock, every keyed window's pending values, and the identities of
timers whose output was already committed. Restoring those identities is what stops an emitted
window from firing a second time after recovery.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from .errors import ValidationError
from .events import DATA, Event, WatermarkTracker
from .windows import Session, Sliding, Tumbling, Window, merge_sessions, session, sliding, tumbling

CHECKPOINT_VERSION = 1

_CHECKPOINT_FIELDS = {
    "version",
    "windowing",
    "aggregation",
    "maxOutOfOrderness",
    "allowedLateness",
    "watermark",
    "state",
    "emitted",
}
_WATERMARK_FIELDS = {"maxSeen", "lateDropped", "observed"}
_STATE_FIELDS = {"start", "end", "key", "values"}
_EMITTED_FIELDS = {"start", "end", "key"}

Aggregator = Callable[[list[float]], float]

AGGREGATORS: dict[str, Aggregator] = {
    "count": lambda values: float(len(values)),
    "sum": lambda values: float(sum(values)),
    "min": lambda values: float(min(values)),
    "max": lambda values: float(max(values)),
    "mean": lambda values: float(sum(values) / len(values)),
}


@dataclass(frozen=True, slots=True)
class Result:
    window: Window
    key: str
    aggregation: str
    value: float
    count: int

    def to_document(self) -> dict[str, object]:
        return {
            "window": self.window.to_document(),
            "key": self.key,
            "aggregation": self.aggregation,
            "value": self.value,
            "count": self.count,
        }


@dataclass(slots=True)
class Pipeline:
    windowing: Tumbling | Sliding | Session
    aggregation: str = "sum"
    max_out_of_orderness: int = 0
    allowed_lateness: int = 0
    watermark: WatermarkTracker = field(init=False)
    _values: dict[tuple[int, int, str], list[float]] = field(default_factory=dict, init=False)
    _emitted: set[tuple[int, int, str]] = field(default_factory=set, init=False)

    def __post_init__(self) -> None:
        if self.aggregation not in AGGREGATORS:
            raise ValidationError(f"unknown aggregation: {self.aggregation}", known=sorted(AGGREGATORS))
        if self.allowed_lateness < 0:
            raise ValidationError("allowed_lateness must be >= 0", value=self.allowed_lateness)
        self.watermark = WatermarkTracker(max_out_of_orderness=self.max_out_of_orderness)

    # -- input ---------------------------------------------------------------
    def add(self, event: Event) -> list[Result]:
        if event.kind != DATA:
            self.watermark.advance_to(event.timestamp)
            return self._emit(force_end=None)
        if self.watermark.observe(event.timestamp):
            return []  # late: counted by the tracker, never applied to a closed window
        for window in self._windows_for(event.timestamp):
            self._values.setdefault((window.start, window.end, event.key), []).append(event.value)
        return self._emit(force_end=None)

    def run(self, events: Iterable[Event]) -> list[Result]:
        results: list[Result] = []
        for event in events:
            results.extend(self.add(event))
        results.extend(self.flush())
        return results

    def flush(self) -> list[Result]:
        return self._emit(force_end=True)

    # -- checkpoint / recovery ----------------------------------------------
    def checkpoint(self) -> dict[str, Any]:
        """Capture everything needed to continue deterministically in a new instance.

        The document holds the configuration, the event-time clock (including late-drop counters),
        one entry per keyed window with its pending values, and the identities of timers whose
        output was already committed. Committed identities are listed separately from state because
        merged session windows emit under an identity that is not itself a state key. The document
        contains no output records: committed writes stay committed purely through those identities.
        """
        return {
            "version": CHECKPOINT_VERSION,
            "windowing": _windowing_document(self.windowing),
            "aggregation": self.aggregation,
            "maxOutOfOrderness": self.max_out_of_orderness,
            "allowedLateness": self.allowed_lateness,
            "watermark": {
                "maxSeen": self.watermark.max_seen,
                "lateDropped": self.watermark.late_dropped,
                "observed": self.watermark.observed,
            },
            "state": [
                {"start": start, "end": end, "key": aggregation_key, "values": list(values)}
                for (start, end, aggregation_key), values in sorted(self._values.items())
            ],
            "emitted": [
                {"start": start, "end": end, "key": aggregation_key}
                for start, end, aggregation_key in sorted(self._emitted)
            ],
        }

    def checkpoint_text(self) -> str:
        """Canonical JSON rendering of `checkpoint`: same conventions as CLI output."""
        return json.dumps(self.checkpoint(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    @classmethod
    def restore(cls, document: dict[str, Any]) -> Pipeline:
        """Rebuild a pipeline from a `checkpoint` document (version must match exactly)."""
        if not isinstance(document, dict):
            raise ValidationError("checkpoint must be a JSON object")
        _reject_unknown(document, _CHECKPOINT_FIELDS)
        version = document.get("version")
        if version != CHECKPOINT_VERSION:
            raise ValidationError(
                f"unsupported checkpoint version: {version!r}",
                supported=CHECKPOINT_VERSION,
            )
        aggregation = _require_str(document, "aggregation")
        if aggregation not in AGGREGATORS:
            raise ValidationError(f"unknown aggregation: {aggregation}", known=sorted(AGGREGATORS))
        max_out_of_orderness = _require_int(document, "maxOutOfOrderness")
        allowed_lateness = _require_int(document, "allowedLateness")
        windowing = _windowing_from_document(_require_mapping(document, "windowing"))

        pipeline = cls(
            windowing=windowing,
            aggregation=aggregation,
            max_out_of_orderness=max_out_of_orderness,
            allowed_lateness=allowed_lateness,
        )

        clock = _require_mapping(document, "watermark")
        _reject_unknown(clock, _WATERMARK_FIELDS, context="watermark")
        max_seen = clock.get("maxSeen")
        if max_seen is not None and (isinstance(max_seen, bool) or not isinstance(max_seen, int)):
            raise ValidationError("watermark.maxSeen must be an integer or null")
        late_dropped = _require_int(clock, "lateDropped", context="watermark")
        observed = _require_int(clock, "observed", context="watermark")
        tracker = WatermarkTracker(max_out_of_orderness=max_out_of_orderness)
        tracker._max_seen = max_seen
        tracker.late_dropped = late_dropped
        tracker.observed = observed
        pipeline.watermark = tracker

        entries = document.get("state")
        if not isinstance(entries, list):
            raise ValidationError("checkpoint state must be a list")
        values_by_key: dict[tuple[int, int, str], list[float]] = {}
        for position, entry in enumerate(entries):
            if not isinstance(entry, dict):
                raise ValidationError("checkpoint state entries must be objects", index=position)
            _reject_unknown(entry, _STATE_FIELDS, context="state", index=position)
            start = _require_int(entry, "start", context="state")
            end = _require_int(entry, "end", context="state")
            key = _require_str(entry, "key", context="state")
            if end <= start:
                raise ValidationError("state window end must be greater than start", start=start, end=end)
            raw_values = entry.get("values")
            if not isinstance(raw_values, list) or any(
                isinstance(value, bool) or not isinstance(value, (int, float)) for value in raw_values
            ):
                raise ValidationError("state values must be a list of numbers", index=position)
            identity = (start, end, key)
            if identity in values_by_key:
                raise ValidationError("duplicate state entry in checkpoint", start=start, end=end, key=key)
            values_by_key[identity] = [float(value) for value in raw_values]

        committed = document.get("emitted")
        if not isinstance(committed, list):
            raise ValidationError("checkpoint emitted must be a list")
        emitted: set[tuple[int, int, str]] = set()
        for position, entry in enumerate(committed):
            if not isinstance(entry, dict):
                raise ValidationError("checkpoint emitted entries must be objects", index=position)
            _reject_unknown(entry, _EMITTED_FIELDS, context="emitted", index=position)
            start = _require_int(entry, "start", context="emitted")
            end = _require_int(entry, "end", context="emitted")
            key = _require_str(entry, "key", context="emitted")
            if end <= start:
                raise ValidationError("emitted window end must be greater than start", start=start, end=end)
            identity = (start, end, key)
            if identity in emitted:
                raise ValidationError("duplicate emitted entry in checkpoint", start=start, end=end, key=key)
            emitted.add(identity)

        pipeline._values = values_by_key
        pipeline._emitted = emitted
        return pipeline

    @classmethod
    def restore_text(cls, text: str) -> Pipeline:
        """Parse canonical checkpoint text and restore; malformed JSON is a ValidationError."""
        try:
            document = json.loads(text)
        except (json.JSONDecodeError, TypeError) as error:
            raise ValidationError(f"invalid checkpoint JSON: {error}") from error
        return cls.restore(document)

    # -- internals -----------------------------------------------------------
    def _windows_for(self, timestamp: int) -> list[Window]:
        if isinstance(self.windowing, Session):
            return self.windowing.assign(timestamp)
        return self.windowing.assign(timestamp)

    def _emit(self, force_end: bool | None) -> list[Result]:
        if isinstance(self.windowing, Session):
            return self._emit_sessions(force_end)
        results: list[Result] = []
        for key in sorted(self._values):
            start, end, aggregation_key = key
            if key in self._emitted:
                continue
            closed = force_end or self.watermark.is_closed(end + self.allowed_lateness)
            if not closed:
                continue
            results.append(self._result(start, end, aggregation_key))
            self._emitted.add(key)
        return sorted(results, key=lambda result: (result.window.start, result.key))

    def _emit_sessions(self, force_end: bool | None) -> list[Result]:
        results: list[Result] = []
        gap = self.windowing.gap
        for aggregation_key in sorted({key[2] for key in self._values}):
            windows = [Window(key[0], key[1]) for key in self._values if key[2] == aggregation_key]
            merged = merge_sessions(windows, gap)
            for window in merged:
                values: list[float] = []
                for key, stored in self._values.items():
                    if key[2] != aggregation_key:
                        continue
                    if window.start <= key[0] and key[1] <= window.end:
                        values.extend(stored)
                identity = (window.start, window.end, aggregation_key)
                if identity in self._emitted or not values:
                    continue
                closed = force_end or self.watermark.is_closed(window.end + gap + self.allowed_lateness)
                if not closed:
                    continue
                results.append(self._result(window.start, window.end, aggregation_key, values))
                self._emitted.add(identity)
        return sorted(results, key=lambda result: (result.window.start, result.key))

    def _result(self, start: int, end: int, key: str, values: list[float] | None = None) -> Result:
        stored = values if values is not None else self._values[(start, end, key)]
        return Result(
            window=Window(start, end),
            key=key,
            aggregation=self.aggregation,
            value=AGGREGATORS[self.aggregation](stored),
            count=len(stored),
        )


def _windowing_document(windowing: Tumbling | Sliding | Session) -> dict[str, Any]:
    if isinstance(windowing, Tumbling):
        return {"type": "tumbling", "size": windowing.size, "offset": windowing.offset}
    if isinstance(windowing, Sliding):
        return {
            "type": "sliding",
            "size": windowing.size,
            "slide": windowing.slide,
            "offset": windowing.offset,
        }
    return {"type": "session", "gap": windowing.gap}


def _windowing_from_document(document: dict[str, Any]) -> Tumbling | Sliding | Session:
    kind = _require_str(document, "type", context="windowing")
    if kind == "tumbling":
        _reject_unknown(document, {"type", "size", "offset"}, context="windowing")
        size = _require_int(document, "size", context="windowing")
        offset = _require_int(document, "offset", context="windowing")
        return tumbling(size, offset)
    if kind == "sliding":
        _reject_unknown(document, {"type", "size", "slide", "offset"}, context="windowing")
        size = _require_int(document, "size", context="windowing")
        slide = _require_int(document, "slide", context="windowing")
        offset = _require_int(document, "offset", context="windowing")
        return sliding(size, slide, offset)
    if kind == "session":
        _reject_unknown(document, {"type", "gap"}, context="windowing")
        return session(_require_int(document, "gap", context="windowing"))
    raise ValidationError(f"unknown windowing type: {kind}", value=kind)


def _require_mapping(document: dict[str, Any], name: str) -> dict[str, Any]:
    value = document.get(name)
    if not isinstance(value, dict):
        raise ValidationError(f"checkpoint {name} must be an object")
    return value


def _reject_unknown(
    document: dict[str, Any],
    allowed: set[str],
    *,
    context: str | None = None,
    index: int | None = None,
) -> None:
    unknown = sorted(set(document) - allowed)
    if unknown:
        raise ValidationError(
            f"unknown checkpoint field(s) in {context or 'root'}: {', '.join(unknown)}",
            index=index,
        )


def _require_str(document: dict[str, Any], name: str, *, context: str | None = None) -> str:
    value = document.get(name)
    if not isinstance(value, str):
        raise ValidationError(f"checkpoint {context + '.' if context else ''}{name} must be a string")
    return value


def _require_int(document: dict[str, Any], name: str, *, context: str | None = None) -> int:
    value = document.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"checkpoint {context + '.' if context else ''}{name} must be an integer")
    return value
