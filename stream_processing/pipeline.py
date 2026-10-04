"""The stateful pipeline: assign events to windows, aggregate per key, emit on watermark.

Emission is driven by the watermark, not by input order, so the same input produces the same output
regardless of how the lines are grouped. `add` returns whatever closed during that call; `flush`
returns the remainder. Results are always ordered by `(window.start, key)`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterable

from .errors import ValidationError
from .events import DATA, Event, WatermarkTracker
from .windows import Session, Sliding, Tumbling, Window, merge_sessions

Aggregator = Callable[[list[float]], float]


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)

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

    def snapshot_state(self) -> dict[str, object]:
        """Plain-data capture of keyed window state and the emitted-record ledger.

        Configuration (windowing, aggregation, bounds) is not part of the state snapshot: it is
        recorded alongside the checkpoint and used to construct the pipeline before `restore_state`.
        """
        return {
            "watermark": self.watermark.snapshot(),
            "values": [
                {"start": start, "end": end, "key": aggregation_key, "values": list(values)}
                for (start, end, aggregation_key), values in sorted(self._values.items())
            ],
            "emitted": [[start, end, aggregation_key] for start, end, aggregation_key in sorted(self._emitted)],
        }

    def restore_state(self, state: dict[str, object]) -> None:
        """Rebuild mutable pipeline state from a `snapshot_state` document.

        Validated before any assignment so a malformed snapshot never partially mutates the
        pipeline. The caller is responsible for constructing the pipeline with the configuration
        the snapshot was taken under; the watermark snapshot's out-of-orderness must agree.
        """
        watermark_state = state.get("watermark")
        values_state = state.get("values")
        emitted_state = state.get("emitted")
        if not isinstance(watermark_state, dict):
            raise ValidationError("checkpoint pipeline state must carry a watermark object")
        if not isinstance(values_state, list) or not isinstance(emitted_state, list):
            raise ValidationError("checkpoint values and emitted state must be lists")
        values: dict[tuple[int, int, str], list[float]] = {}
        for entry in values_state:
            if not isinstance(entry, dict):
                raise ValidationError("each keyed-state entry must be an object")
            start = entry.get("start")
            end = entry.get("end")
            key = entry.get("key")
            stored = entry.get("values")
            if not _is_int(start) or not _is_int(end) or not isinstance(key, str) or not key:
                raise ValidationError("keyed-state entry needs integer start/end and a non-empty key")
            if end <= start:
                raise ValidationError("restored window end must be greater than start", start=start, end=end)
            if not isinstance(stored, list) or not all(_is_number(value) for value in stored):
                raise ValidationError("keyed-state values must be a list of numbers")
            identity = (start, end, key)
            if identity in values:
                raise ValidationError("duplicate keyed-state entry", start=start, end=end, key=key)
            values[identity] = [float(value) for value in stored]
        emitted: set[tuple[int, int, str]] = set()
        for identity in emitted_state:
            if not isinstance(identity, list) or len(identity) != 3:
                raise ValidationError("each emitted identity must be [start, end, key]")
            start, end, key = identity
            if not _is_int(start) or not _is_int(end) or not isinstance(key, str) or not key:
                raise ValidationError("emitted identity needs integer start/end and a non-empty key")
            triple = (start, end, key)
            if triple in emitted:
                raise ValidationError("duplicate emitted identity", start=start, end=end, key=key)
            emitted.add(triple)
        tracker = WatermarkTracker(max_out_of_orderness=self.max_out_of_orderness)
        tracker.restore(watermark_state)
        if tracker.max_out_of_orderness != self.max_out_of_orderness:
            raise ValidationError(
                "checkpoint max_out_of_orderness does not match the restoring pipeline",
                checkpoint=tracker.max_out_of_orderness,
                configured=self.max_out_of_orderness,
            )
        self.watermark = tracker
        self._values = values
        self._emitted = emitted

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
