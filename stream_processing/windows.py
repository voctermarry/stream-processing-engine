"""Window assignment: tumbling, sliding and session windows over event time.

Every assigner answers one question: for an event at timestamp `t`, which half-open windows
`[start, end)` does it belong to? Session windows are the exception -- they grow by merging, so they
expose `assign_session` instead of a fixed list.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from .errors import ValidationError, WindowError


@dataclass(frozen=True, slots=True, order=True)
class Window:
    start: int
    end: int

    def __post_init__(self) -> None:
        if self.end <= self.start:
            raise WindowError("window end must be greater than start", start=self.start, end=self.end)

    @property
    def size(self) -> int:
        return self.end - self.start

    def contains(self, timestamp: int) -> bool:
        return self.start <= timestamp < self.end

    def to_document(self) -> dict[str, int]:
        return {"start": self.start, "end": self.end}


@dataclass(frozen=True, slots=True)
class Tumbling:
    """Fixed, non-overlapping windows of `size` ms, aligned to `offset`."""

    size: int
    offset: int = 0

    def __post_init__(self) -> None:
        if self.size <= 0:
            raise ValidationError("tumbling size must be > 0", value=self.size)

    def assign(self, timestamp: int) -> list[Window]:
        start = self.offset + ((timestamp - self.offset) // self.size) * self.size
        return [Window(start, start + self.size)]

    def boundaries(self, first: int, last: int) -> list[Window]:
        """Every window that intersects `[first, last]`, in ascending order."""
        if last < first:
            raise ValidationError("last must be >= first", first=first, last=last)
        first_start = self.assign(first)[0].start
        windows: list[Window] = []
        start = first_start
        while start <= last:
            windows.append(Window(start, start + self.size))
            start += self.size
        return windows


@dataclass(frozen=True, slots=True)
class Sliding:
    """Windows of `size` ms starting every `slide` ms; overlapping when slide < size."""

    size: int
    slide: int
    offset: int = 0

    def __post_init__(self) -> None:
        if self.size <= 0:
            raise ValidationError("sliding size must be > 0", value=self.size)
        if self.slide <= 0:
            raise ValidationError("sliding slide must be > 0", value=self.slide)

    def assign(self, timestamp: int) -> list[Window]:
        latest = self.offset + ((timestamp - self.offset) // self.slide) * self.slide
        starts = [latest - index * self.slide for index in range((self.size + self.slide - 1) // self.slide)]
        return [Window(start, start + self.size) for start in sorted(starts) if start <= timestamp < start + self.size]


@dataclass(frozen=True, slots=True)
class Session:
    """Gap-based windows: events whose timestamps differ by at most `gap` ms share one session."""

    gap: int

    def __post_init__(self) -> None:
        if self.gap <= 0:
            raise ValidationError("session gap must be > 0", value=self.gap)

    def assign(self, timestamp: int) -> list[Window]:
        return [Window(timestamp, timestamp + 1)]


def merge_sessions(windows: Iterable[Window], gap: int) -> list[Window]:
    """Merge windows whose event-time gap is at most `gap`, in ascending order.

    Each event is a half-open point window ``[t, t+1)``, so two adjacent windows touch when the
    distance from one end to the next start is strictly less than ``gap``: ``t2 - (t1 + 1) < gap``
    is exactly ``t2 - t1 <= gap``. A timestamp difference of ``gap + 1`` must therefore stay split.
    """
    if gap <= 0:
        raise ValidationError("session gap must be > 0", value=gap)
    ordered: Sequence[Window] = sorted(windows)
    merged: list[Window] = []
    for window in ordered:
        if merged and window.start - merged[-1].end < gap:
            previous = merged[-1]
            merged[-1] = Window(previous.start, max(previous.end, window.end))
        else:
            merged.append(window)
    return merged


def tumbling(size: int, offset: int = 0) -> Tumbling:
    return Tumbling(size, offset)


def sliding(size: int, slide: int, offset: int = 0) -> Sliding:
    return Sliding(size, slide, offset)


def session(gap: int) -> Session:
    return Session(gap)
