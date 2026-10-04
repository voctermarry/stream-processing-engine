"""Exception hierarchy.

Every failure carries a stable name so callers (and the CLI's JSON error document) can match on
`type` instead of on message text. `ParseError` additionally carries `line`/`column` because it is
raised while reading user input; the other errors report the semantic object that failed.
"""

from __future__ import annotations


class StreamProcessingError(Exception):
    """Base class for every error this package raises on purpose."""

    kind = "stream_processing_error"

    def __init__(self, message: str, **context: object) -> None:
        super().__init__(message)
        self.message = message
        self.context = {key: value for key, value in context.items() if value is not None}

    def to_document(self) -> dict[str, object]:
        document: dict[str, object] = {"error": self.kind, "message": self.message}
        document.update(self.context)
        return document


class ParseError(StreamProcessingError):
    """Malformed input: bad JSON, missing fields, wrong types, unknown enum values."""

    kind = "parse_error"

    def __init__(self, message: str, *, line: int | None = None, column: int | None = None, **context: object) -> None:
        super().__init__(message, line=line, column=column, **context)


class ValidationError(StreamProcessingError):
    """Semantically invalid request: unknown window kind, non-positive size, empty input, ..."""

    kind = "validation_error"


class WindowError(StreamProcessingError):
    """A window operation that cannot be satisfied, e.g. merging windows across keys."""

    kind = "window_error"


class OutputError(StreamProcessingError):
    """The output target cannot be used: it collides with an input, or the write failed."""

    kind = "output_error"
