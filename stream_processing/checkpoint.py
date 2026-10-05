"""Persistent checkpoints for ``run``: one canonical, versioned JSON document.

A checkpoint is a *single line* of canonical JSON (sorted keys, no whitespace) atomically
replaced after every successfully processed input line. It captures everything a fresh process
needs to continue without re-counting a single event:

  * the exact processing configuration (window spec, aggregation, lateness / disorder bounds),
  * how many input lines were consumed and a SHA-256 of their exact on-disk prefix,
  * the full keyed window state (per-window values and the emitted-record identities),
  * the watermark position and the observed / late-drop counters,
  * every result line already produced but not yet committed to the final output file,
  * a lifecycle status (``running`` while lines remain, ``complete`` after a successful finish).

Failure taxonomy, shared with the rest of the package:

  * unreadable checkpoint or failed atomic write  -> ``output_error``
  * malformed JSON                                 -> ``parse_error`` (with line/column)
  * anything structurally or semantically invalid  -> ``validation_error``

A checkpoint that fails validation is never applied and never overwritten.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from typing import Any, Sequence

from .errors import OutputError, ParseError, ValidationError
from .events import WatermarkTracker, is_finite_number
from .pipeline import Pipeline

CHECKPOINT_FORMAT = "stream-processing-checkpoint"
CHECKPOINT_VERSION = 1

STATUS_RUNNING = "running"
STATUS_COMPLETE = "complete"
_STATUSES = (STATUS_RUNNING, STATUS_COMPLETE)


@dataclass(frozen=True, slots=True)
class CheckpointState:
    """Everything a checkpoint document holds, already validated."""

    status: str
    config: dict[str, Any]
    consumed: int
    prefix_sha256: str
    # keyed state: (window.start, window.end, key) -> values in arrival order
    values: dict[tuple[int, int, str], list[float]]
    emitted: frozenset[tuple[int, int, str]]
    max_seen: int | None
    observed: int
    late_dropped: int
    pending: tuple[str, ...]

    def to_document(self) -> dict[str, Any]:
        return {
            "format": CHECKPOINT_FORMAT,
            "version": CHECKPOINT_VERSION,
            "status": self.status,
            "config": self.config,
            "consumed": self.consumed,
            "prefixSha256": self.prefix_sha256,
            "state": {
                "values": [
                    [start, end, key, list(self.values[(start, end, key)])]
                    for start, end, key in sorted(self.values)
                ],
                "emitted": [list(identity) for identity in sorted(self.emitted)],
                "watermarkMaxSeen": self.max_seen,
                "observed": self.observed,
                "lateDropped": self.late_dropped,
            },
            "pending": list(self.pending),
        }


# -----------------------------------------------------------------------------------------------
# Hashing
# -----------------------------------------------------------------------------------------------


def prefix_digest(prefix: str | bytes) -> str:
    """SHA-256 hex digest of a consumed input prefix exactly as it was read.

    The CLI reads input with newline translation disabled, so CRLF stays byte-for-byte visible
    and tampering with a consumed prefix (including its line endings) changes the digest.
    """
    if isinstance(prefix, str):
        prefix = prefix.encode("utf-8")
    return hashlib.sha256(prefix).hexdigest()


# -----------------------------------------------------------------------------------------------
# Atomic persistence
# -----------------------------------------------------------------------------------------------


def render_checkpoint(state: CheckpointState) -> str:
    """Canonical single-line rendering (newline appended by the writer)."""
    return json.dumps(
        state.to_document(), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def save_checkpoint(path: str, state: CheckpointState) -> None:
    """Atomically replace ``path`` with the checkpoint; a failure never leaves a partial target."""
    try:
        payload = render_checkpoint(state) + "\n"
    except ValueError as error:  # allow_nan=False on a non-finite aggregate (exotic input)
        raise OutputError("checkpoint state is not JSON-serializable", value=path) from error
    directory = os.path.dirname(os.path.abspath(path)) or "."
    handle = None
    temporary = None
    try:
        handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=directory, delete=False, newline="\n")
        temporary = handle.name
        handle.write(payload)
        handle.close()
        handle = None
        os.replace(temporary, path)
    except OSError as error:
        if handle is not None:
            handle.close()
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)
        raise OutputError(f"cannot write checkpoint: {error.strerror or error}", value=path) from error


# -----------------------------------------------------------------------------------------------
# Validation
# -----------------------------------------------------------------------------------------------


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: object) -> bool:
    # bool is an int subclass; NaN/Infinity are non-standard JSON and never occur in a real state,
    # and an integer too large for a float cannot be restored into the engine's numeric domain.
    return is_finite_number(value)


def _reject_json_constant(value: str) -> None:
    """``parse_constant`` hook: refuse the non-standard NaN / Infinity tokens Python accepts."""
    raise ValueError(f"non-standard JSON constant: {value}")


def _reject_extra(document: dict[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(set(document) - allowed)
    if unknown:
        raise ValidationError(f"checkpoint has unknown field(s) in {where}", fields=unknown)


def _validate_config(document: object) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise ValidationError("checkpoint config must be an object")
    _reject_extra(document, {"window", "aggregation", "allowedLateness", "maxOutOfOrderness"}, "config")
    required = ("window", "aggregation", "allowedLateness", "maxOutOfOrderness")
    missing = sorted(name for name in required if name not in document)
    if missing:
        raise ValidationError("checkpoint config is missing field(s)", fields=missing)
    window = document["window"]
    aggregation = document["aggregation"]
    lateness = document["allowedLateness"]
    out_of_orderness = document["maxOutOfOrderness"]
    if not isinstance(window, str) or not window:
        raise ValidationError("checkpoint config window must be a non-empty string")
    if not isinstance(aggregation, str) or not aggregation:
        raise ValidationError("checkpoint config aggregation must be a non-empty string")
    if not _is_int(lateness) or lateness < 0:
        raise ValidationError("checkpoint config allowedLateness must be a non-negative integer")
    if not _is_int(out_of_orderness) or out_of_orderness < 0:
        raise ValidationError("checkpoint config maxOutOfOrderness must be a non-negative integer")
    return {
        "window": window,
        "aggregation": aggregation,
        "allowedLateness": lateness,
        "maxOutOfOrderness": out_of_orderness,
    }


def _validate_identity(entry: object, where: str) -> tuple[int, int, str]:
    if not isinstance(entry, list) or len(entry) != 3:
        raise ValidationError(f"checkpoint {where} entries must be [start,end,key] triples")
    start, end, key = entry
    if not _is_int(start):
        raise ValidationError(f"checkpoint {where} start must be an integer")
    if not _is_int(end):
        raise ValidationError(f"checkpoint {where} end must be an integer")
    if end <= start:
        raise ValidationError(f"checkpoint {where} window end must be greater than start")
    if not isinstance(key, str) or not key:
        raise ValidationError(f"checkpoint {where} key must be a non-empty string")
    return start, end, key  # type: ignore[return-value]


def _validate_state(document: object) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise ValidationError("checkpoint state must be an object")
    _reject_extra(document, {"values", "emitted", "watermarkMaxSeen", "observed", "lateDropped"}, "state")
    required = ("values", "emitted", "watermarkMaxSeen", "observed", "lateDropped")
    missing = sorted(name for name in required if name not in document)
    if missing:
        raise ValidationError("checkpoint state is missing field(s)", fields=missing)

    raw_values = document["values"]
    if not isinstance(raw_values, list):
        raise ValidationError("checkpoint state values must be a list")
    values: dict[tuple[int, int, str], list[float]] = {}
    for entry in raw_values:
        if not isinstance(entry, list) or len(entry) != 4:
            raise ValidationError("checkpoint state values entries must be [start,end,key,values]")
        start, end, key = _validate_identity(entry[:3], "state values")
        stored = entry[3]
        if not isinstance(stored, list) or not all(_is_number(item) for item in stored):
            raise ValidationError("checkpoint state values must be a list of numbers")
        identity = (start, end, key)
        if identity in values:
            raise ValidationError("checkpoint state has a duplicate window/key entry")
        values[identity] = [float(item) for item in stored]

    raw_emitted = document["emitted"]
    if not isinstance(raw_emitted, list):
        raise ValidationError("checkpoint state emitted must be a list")
    emitted: set[tuple[int, int, str]] = set()
    for entry in raw_emitted:
        identity = _validate_identity(entry, "state emitted")
        if identity in emitted:
            raise ValidationError("checkpoint state has a duplicate emitted entry")
        emitted.add(identity)
    # Note: for session windows the values are keyed per-event (ts, ts+1) while emitted records
    # carry *merged* window identities, so no key-membership relation is asserted here.

    max_seen = document["watermarkMaxSeen"]
    if max_seen is not None and not _is_int(max_seen):
        raise ValidationError("checkpoint watermarkMaxSeen must be an integer or null")
    observed = document["observed"]
    late_dropped = document["lateDropped"]
    if not _is_int(observed) or observed < 0:
        raise ValidationError("checkpoint observed must be a non-negative integer")
    if not _is_int(late_dropped) or late_dropped < 0:
        raise ValidationError("checkpoint lateDropped must be a non-negative integer")
    return {
        "values": values,
        "emitted": frozenset(emitted),
        "watermarkMaxSeen": max_seen,
        "observed": observed,
        "lateDropped": late_dropped,
    }


def load_checkpoint(path: str) -> CheckpointState:
    """Read and strictly validate a checkpoint document without applying it to anything."""
    try:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
    except OSError as error:
        raise OutputError(f"cannot read checkpoint: {error.strerror or error}", value=path) from error
    try:
        document = json.loads(text, parse_constant=_reject_json_constant)
    except json.JSONDecodeError as error:
        raise ParseError(
            f"invalid checkpoint JSON: {error.msg}", line=error.lineno, column=error.colno
        ) from error
    except ValueError as error:
        # Non-standard NaN / Infinity token rejected by parse_constant; the document is one line.
        raise ParseError(str(error), line=1, column=1) from error
    if not isinstance(document, dict):
        raise ValidationError("checkpoint must be a JSON object")

    allowed = {"format", "version", "status", "config", "consumed", "prefixSha256", "state", "pending"}
    _reject_extra(document, allowed, "checkpoint")
    required = ("format", "version", "status", "config", "consumed", "prefixSha256", "state", "pending")
    missing = sorted(name for name in required if name not in document)
    if missing:
        raise ValidationError("checkpoint is missing field(s)", fields=missing)

    if document["format"] != CHECKPOINT_FORMAT:
        raise ValidationError("unknown checkpoint format", value=document["format"])
    if document["version"] != CHECKPOINT_VERSION:
        raise ValidationError(
            "unsupported checkpoint version",
            value=document["version"],
            supported=CHECKPOINT_VERSION,
        )
    status = document["status"]
    if status not in _STATUSES:
        raise ValidationError("checkpoint status must be running or complete", value=status)

    config = _validate_config(document["config"])
    consumed = document["consumed"]
    if not _is_int(consumed) or consumed < 0:
        raise ValidationError("checkpoint consumed must be a non-negative integer")
    digest = document["prefixSha256"]
    if not isinstance(digest, str) or len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValidationError("checkpoint prefixSha256 must be 64 lowercase hex characters")

    state = _validate_state(document["state"])

    pending = document["pending"]
    if not isinstance(pending, list) or not all(isinstance(line, str) for line in pending):
        raise ValidationError("checkpoint pending must be a list of strings")

    return CheckpointState(
        status=status,
        config=config,
        consumed=consumed,
        prefix_sha256=digest,
        values=state["values"],
        emitted=state["emitted"],
        max_seen=state["watermarkMaxSeen"],
        observed=state["observed"],
        late_dropped=state["lateDropped"],
        pending=tuple(pending),
    )


# -----------------------------------------------------------------------------------------------
# Pipeline binding
# -----------------------------------------------------------------------------------------------


def config_document(
    *, window: str, aggregation: str, allowed_lateness: int, max_out_of_orderness: int
) -> dict[str, Any]:
    return {
        "window": window,
        "aggregation": aggregation,
        "allowedLateness": allowed_lateness,
        "maxOutOfOrderness": max_out_of_orderness,
    }


def capture_state(
    pipeline: Pipeline,
    *,
    config: dict[str, Any],
    consumed: int,
    prefix_sha256: str,
    pending: Sequence[str],
    status: str,
) -> CheckpointState:
    """Snapshot a pipeline's public-ish internal state after a successfully processed line."""
    if status not in _STATUSES:
        raise ValidationError("checkpoint status must be running or complete", value=status)
    return CheckpointState(
        status=status,
        config=config,
        consumed=consumed,
        prefix_sha256=prefix_sha256,
        values={identity: list(values) for identity, values in pipeline._values.items()},
        emitted=frozenset(pipeline._emitted),
        max_seen=pipeline.watermark.max_seen,
        observed=pipeline.watermark.observed,
        late_dropped=pipeline.watermark.late_dropped,
        pending=tuple(pending),
    )


def restore_state(pipeline: Pipeline, state: CheckpointState) -> None:
    """Rebind a freshly constructed pipeline to a validated checkpoint's state."""
    tracker: WatermarkTracker = pipeline.watermark
    tracker._max_seen = state.max_seen
    tracker.observed = state.observed
    tracker.late_dropped = state.late_dropped
    pipeline._values = {identity: list(values) for identity, values in state.values.items()}
    pipeline._emitted = set(state.emitted)
