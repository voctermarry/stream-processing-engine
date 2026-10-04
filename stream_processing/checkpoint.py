"""Product-grade durable checkpoints and resumable ``run`` execution.

A checkpoint is one canonical single-line JSON document (sorted keys, compact separators, trailing
newline) that captures everything a restart needs to continue a ``run`` byte-for-byte identically:

  * ``format`` / ``version`` -- a fixed format marker and integer version,
  * ``config`` -- the window spec, aggregation, allowed lateness and out-of-orderness in effect,
  * ``status`` -- ``running`` while lines remain, ``completed`` after the final flush,
  * ``consumed`` -- how many input lines were successfully parsed and processed,
  * ``prefixDigest`` -- sha256 over the exact consumed bytes, proving the prefix is unchanged,
  * ``state`` -- keyed window/session state, the watermark and late counters (pipeline snapshot),
  * ``emitted`` -- every canonical result record already produced, in emission order; until the
    final result file is committed these are the uncommitted outputs, re-materialized on resume.

The document is replaced atomically (temp file in the same directory + ``os.replace``) after every
successfully processed input line. Recovery verifies the consumed prefix byte-for-byte before any
further line is processed; no verification failure ever touches the existing output or checkpoint.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from typing import Any, Sequence

from .errors import OutputError, ParseError, ValidationError
from .events import parse_event_line
from .pipeline import Pipeline
from .windows import parse_window_spec

CHECKPOINT_FORMAT = "stream-processing-checkpoint"
CHECKPOINT_VERSION = 1

STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
_STATUSES = (STATUS_RUNNING, STATUS_COMPLETED)


def canonical(document: dict[str, Any]) -> str:
    """The same canonical rendering the CLI result writer uses (must stay byte-identical)."""
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


@dataclass(frozen=True, slots=True)
class RunConfig:
    """The configuration a ``run`` was started with; resumed runs must match it exactly."""

    window: str
    aggregation: str
    allowed_lateness: int
    max_out_of_orderness: int

    def to_document(self) -> dict[str, Any]:
        return {
            "window": self.window,
            "aggregation": self.aggregation,
            "allowedLateness": self.allowed_lateness,
            "maxOutOfOrderness": self.max_out_of_orderness,
        }

    @classmethod
    def from_document(cls, document: dict[str, Any]) -> "RunConfig":
        window = document.get("window")
        aggregation = document.get("aggregation")
        lateness = document.get("allowedLateness")
        disorder = document.get("maxOutOfOrderness")
        if not isinstance(window, str) or not window:
            raise ValidationError("checkpoint config must carry a window spec")
        if not isinstance(aggregation, str) or not aggregation:
            raise ValidationError("checkpoint config must carry an aggregation")
        if not _is_int(lateness) or lateness < 0:
            raise ValidationError("checkpoint allowedLateness must be a non-negative integer", value=lateness)
        if not _is_int(disorder) or disorder < 0:
            raise ValidationError("checkpoint maxOutOfOrderness must be a non-negative integer", value=disorder)
        return cls(
            window=window,
            aggregation=aggregation,
            allowed_lateness=lateness,
            max_out_of_orderness=disorder,
        )


# -------------------------------------------------------------------------------------------------
# Input line splitting (mirrors the engine's read().splitlines() but keeps byte offsets)
# -------------------------------------------------------------------------------------------------


def split_input(data: bytes) -> tuple[list[str], list[int]]:
    """Split UTF-8 input the way ``str.splitlines`` does and report each line's end byte offset.

    Returned offsets are cumulative byte positions immediately *after* each logical line (including
    its terminator), so ``data[:offsets[k]]`` is exactly the consumed prefix of the first ``k+1``
    lines. Using ``splitlines(keepends=True)`` reuses Python's own boundary rules -- including
    ``\\r\\n`` and the less common separators -- instead of re-implementing them.
    """
    text = data.decode("utf-8")
    pieces = text.splitlines(keepends=True)
    lines = text.splitlines()
    offsets: list[int] = []
    cumulative = 0
    for piece in pieces:
        cumulative += len(piece.encode("utf-8"))
        offsets.append(cumulative)
    return lines, offsets


def read_input_bytes(path: str) -> bytes:
    try:
        with open(path, "rb") as handle:
            return handle.read()
    except OSError as error:
        raise OutputError(f"cannot read input: {error.strerror or error}", value=path) from error


# -------------------------------------------------------------------------------------------------
# Atomic file writers
# -------------------------------------------------------------------------------------------------


def _atomic_write(path: str, payload: str, *, kind: str) -> None:
    """Write ``payload`` to a temp file in the target directory then atomically replace ``path``."""
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
        raise OutputError(f"cannot write {kind}: {error.strerror or error}", value=path) from error


def write_checkpoint_atomic(path: str, document: dict[str, Any]) -> None:
    """Atomically replace the checkpoint file with one canonical JSON line."""
    _atomic_write(path, canonical(document) + "\n", kind="checkpoint")


def write_output_atomic(path: str, lines: Sequence[str]) -> None:
    """Atomically replace the final result file -- the same payload shape as the CLI writer."""
    _atomic_write(path, "".join(f"{line}\n" for line in lines), kind="output")


# -------------------------------------------------------------------------------------------------
# Checkpoint parsing / validation
# -------------------------------------------------------------------------------------------------


def _build_document(config: RunConfig, status: str, consumed: int, prefix_digest: str,
                    state: dict[str, object], emitted: Sequence[str]) -> dict[str, Any]:
    return {
        "format": CHECKPOINT_FORMAT,
        "version": CHECKPOINT_VERSION,
        "config": config.to_document(),
        "status": status,
        "consumed": consumed,
        "prefixDigest": prefix_digest,
        "state": state,
        "emitted": list(emitted),
    }


def read_checkpoint(path: str) -> dict[str, Any]:
    """Read and parse a checkpoint file, mapping failures onto the stable error kinds.

    Unreadable file -> ``output_error``; malformed JSON -> ``parse_error`` with line/column; an
    unknown format, unsupported version or structurally invalid document -> ``validation_error``.
    """
    try:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
    except OSError as error:
        raise OutputError(f"cannot read checkpoint: {error.strerror or error}", value=path) from error
    try:
        document = json.loads(text)
    except json.JSONDecodeError as error:
        raise ParseError(
            f"invalid checkpoint JSON: {error.msg}", line=error.lineno, column=error.colno
        ) from error
    if not isinstance(document, dict):
        raise ValidationError("checkpoint must be a JSON object")
    if document.get("format") != CHECKPOINT_FORMAT:
        raise ValidationError("unknown checkpoint format", value=document.get("format"))
    version = document.get("version")
    if not _is_int(version):
        raise ValidationError("checkpoint version must be an integer", value=version)
    if version != CHECKPOINT_VERSION:
        raise ValidationError("unsupported checkpoint version", value=version, supported=CHECKPOINT_VERSION)
    _validate_document(document)
    return document


def _validate_document(document: dict[str, Any]) -> None:
    required = (
        "format", "version", "config", "status", "consumed", "prefixDigest",
        "state", "emitted",
    )
    missing = sorted(name for name in required if name not in document)
    if missing:
        raise ValidationError("checkpoint is missing field(s)", fields=missing)
    unknown = sorted(set(document) - set(required))
    if unknown:
        raise ValidationError("checkpoint has unknown field(s)", fields=unknown)
    config = document["config"]
    if not isinstance(config, dict):
        raise ValidationError("checkpoint config must be an object")
    RunConfig.from_document(config)
    if document["status"] not in _STATUSES:
        raise ValidationError("checkpoint status must be running or completed", value=document["status"])
    consumed = document["consumed"]
    if not _is_int(consumed) or consumed < 0:
        raise ValidationError("checkpoint consumed must be a non-negative integer", value=consumed)
    digest = document["prefixDigest"]
    if not isinstance(digest, str) or not digest:
        raise ValidationError("checkpoint prefixDigest must be a string", value=digest)
    state = document["state"]
    if not isinstance(state, dict):
        raise ValidationError("checkpoint state must be an object")
    if not isinstance(state.get("watermark"), dict):
        raise ValidationError("checkpoint state must carry a watermark object")
    if not isinstance(state.get("values"), list) or not isinstance(state.get("emitted"), list):
        raise ValidationError("checkpoint state needs values and emitted lists")
    emitted = document["emitted"]
    if not isinstance(emitted, list) or not all(isinstance(line, str) for line in emitted):
        raise ValidationError("checkpoint emitted ledger must be a list of strings")


# -------------------------------------------------------------------------------------------------
# Prefix verification
# -------------------------------------------------------------------------------------------------


def verify_prefix(data: bytes, document: dict[str, Any], *, whole: bool) -> tuple[list[str], list[int]]:
    """Prove the bytes recorded as consumed in ``document`` still match the current input.

    ``whole`` indicates a completed checkpoint (the digest covers the whole file and consumed must
    equal the line count); otherwise the digest covers the prefix of ``consumed`` lines. Raises
    ``validation_error`` on any drift and returns the parsed logical lines plus byte offsets.
    """
    lines, offsets = split_input(data)
    consumed = document["consumed"]
    if consumed > len(lines):
        raise ValidationError(
            "checkpoint offset is beyond the current input", consumed=consumed, lines=len(lines)
        )
    if whole:
        prefix = data
        if consumed != len(lines):
            raise ValidationError(
                "checkpoint offset is beyond the current input", consumed=consumed, lines=len(lines)
            )
    else:
        prefix = data[: offsets[consumed - 1]] if consumed > 0 else b""
    if hashlib.sha256(prefix).hexdigest() != document["prefixDigest"]:
        raise ValidationError("input prefix verification failed: consumed prefix changed")
    return lines, offsets


# -------------------------------------------------------------------------------------------------
# Resumable execution
# -------------------------------------------------------------------------------------------------


def _snapshot_document(config: RunConfig, status: str, consumed: int, prefix: bytes,
                       pipeline: Pipeline, emitted: Sequence[str]) -> dict[str, Any]:
    return _build_document(
        config=config,
        status=status,
        consumed=consumed,
        prefix_digest=hashlib.sha256(prefix).hexdigest(),
        state=pipeline.snapshot_state(),
        emitted=emitted,
    )


def _build_pipeline(config: RunConfig) -> Pipeline:
    return Pipeline(
        windowing=parse_window_spec(config.window),
        aggregation=config.aggregation,
        max_out_of_orderness=config.max_out_of_orderness,
        allowed_lateness=config.allowed_lateness,
    )


def run_with_checkpoint(
    *,
    input_path: str,
    output_path: str,
    checkpoint_path: str,
    config: RunConfig,
    resume_document: dict[str, Any] | None,
) -> None:
    """Run a ``run`` with a durable per-line checkpoint and an atomically replaced result file.

    A fresh start (``resume_document`` is None) begins at line one and creates the checkpoint. A
    resume reconstructs the pipeline and the uncommitted emitted ledger, verifies the consumed
    prefix, and continues at the next line. On success the full result file is atomically replaced
    first and a ``completed`` checkpoint is written second; resuming that state is idempotent.

    A parse error on a subsequent line leaves the output file and previous checkpoint untouched:
    the exception propagates with the checkpoint parked at the last successfully processed line.
    """
    data = read_input_bytes(input_path)

    if resume_document is not None and resume_document["status"] == STATUS_COMPLETED:
        # Idempotent completion: verify the whole input, re-materialize the identical result file
        # atomically, and refresh the completed checkpoint. Nothing is recomputed.
        verify_prefix(data, resume_document, whole=True)
        write_output_atomic(output_path, resume_document["emitted"])
        write_checkpoint_atomic(checkpoint_path, resume_document)
        return

    pipeline = _build_pipeline(config)
    lines, offsets = split_input(data)
    consumed = 0
    emitted: list[str] = []

    if resume_document is not None:
        lines, offsets = verify_prefix(data, resume_document, whole=False)
        consumed = resume_document["consumed"]
        pipeline.restore_state(resume_document["state"])
        emitted = list(resume_document["emitted"])

    # One durable checkpoint after every successfully parsed and processed line.
    index = consumed
    while index < len(lines):
        event = parse_event_line(lines[index], line=index + 1)
        for result in pipeline.add(event):
            emitted.append(canonical(result.to_document()))
        index += 1
        document = _snapshot_document(
            config, STATUS_RUNNING, index, data[: offsets[index - 1]], pipeline, emitted
        )
        write_checkpoint_atomic(checkpoint_path, document)

    # Final flush surfaces every still-open window exactly as an uninterrupted run would.
    for result in pipeline.flush():
        emitted.append(canonical(result.to_document()))

    # Commit the complete result first; only then record completion, so a crash in between leaves a
    # resumable running checkpoint that still reproduces the full output.
    write_output_atomic(output_path, emitted)
    completed = _snapshot_document(config, STATUS_COMPLETED, len(lines), data, pipeline, emitted)
    write_checkpoint_atomic(checkpoint_path, completed)
