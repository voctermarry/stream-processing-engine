"""The stateful pipeline: assign events to windows, aggregate per key, emit on watermark.

Emission is driven by the watermark, not by input order, so the same input produces the same output
regardless of how the lines are grouped. `add` returns whatever closed during that call; `flush`
returns the remainder. Results are always ordered by `(window.start, key)`.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Callable, Iterable

from .errors import ValidationError
from .events import DATA, Event, WatermarkTracker, is_finite_number
from .windows import Session, Sliding, Tumbling, Window, merge_sessions

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
        # Validate before touching any state: a rejected event leaves the watermark, the
        # observed / late_dropped counters, the window values and the emitted set exactly as
        # they were. Punctuation values are checked too -- they are never aggregated, but no
        # entry point may accept what another rejects.
        if not is_finite_number(event.value):
            raise ValidationError("event value must be a finite number", value=repr(event.value))
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
                # window.end is lastTimestamp + 1, and an event exactly `gap` after the last one
                # would still merge -- so the session closes only once the watermark reaches
                # lastTimestamp + gap + 1 + allowed_lateness.
                closed = force_end or self.watermark.is_closed(window.end + gap + self.allowed_lateness)
                if not closed:
                    continue
                results.append(self._result(window.start, window.end, aggregation_key, values))
                self._emitted.add(identity)
        return sorted(results, key=lambda result: (result.window.start, result.key))

    def _result(self, start: int, end: int, key: str, values: list[float] | None = None) -> Result:
        stored = values if values is not None else self._values[(start, end, key)]
        try:
            value = AGGREGATORS[self.aggregation](stored)
        except OverflowError as error:
            # sum/mean over huge Python ints (directly constructed events) can overflow float.
            raise self._overflow_error(start, end, key) from error
        if not math.isfinite(value):
            # Every input was finite, yet the aggregate itself escaped the finite domain
            # (e.g. 1e308 + 1e308): refuse the window result instead of emitting Infinity.
            raise self._overflow_error(start, end, key)
        return Result(
            window=Window(start, end),
            key=key,
            aggregation=self.aggregation,
            value=value,
            count=len(stored),
        )

    def _overflow_error(self, start: int, end: int, key: str) -> ValidationError:
        return ValidationError(
            "aggregation result is not finite",
            aggregation=self.aggregation,
            key=key,
            window={"start": start, "end": end},
        )
