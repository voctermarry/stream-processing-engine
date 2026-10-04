"""Command line entry point.

Four subcommands, each with a stable JSON document on stdout (and nothing else):

  describe    -- what this build can do, so a caller never has to guess
  windows     -- the window boundaries a spec produces for a time range
  run         -- process JSONL events, print one result document per line
  replay      -- process events twice and reconcile the two runs field by field

Exit codes: 0 success, 2 input/usage error, 3 a report was produced but disagreeed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from typing import Any, Sequence

from . import __version__
from .checkpoint import (
    CHECKPOINT_FORMAT,
    CHECKPOINT_VERSION,
    STATUS_COMPLETE,
    STATUS_RUNNING,
    capture_state,
    config_document,
    load_checkpoint,
    prefix_digest,
    restore_state,
    save_checkpoint,
)
from .errors import OutputError, StreamProcessingError, ValidationError
from .events import parse_event_line
from .pipeline import AGGREGATORS, Pipeline
from .windows import Session, Sliding, Tumbling, session, sliding, tumbling

EXIT_OK = 0
EXIT_ERROR = 2
EXIT_REPORT_MISMATCH = 3


def canonical(document: dict[str, Any]) -> str:
    """One canonical rendering: sorted keys, no spaces, trailing newline added by the writer."""
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _parse_window(spec: str) -> Tumbling | Sliding | Session:
    parts = spec.split(":")
    kind = parts[0]
    try:
        numbers = [int(part) for part in parts[1:]]
    except ValueError as error:
        raise ValidationError(f"window numbers must be integers: {spec}", value=spec) from error
    if kind == "tumbling" and len(numbers) == 1:
        return tumbling(numbers[0])
    if kind == "tumbling" and len(numbers) == 2:
        return tumbling(numbers[0], numbers[1])
    if kind == "sliding" and len(numbers) == 2:
        return sliding(numbers[0], numbers[1])
    if kind == "sliding" and len(numbers) == 3:
        return sliding(numbers[0], numbers[1], numbers[2])
    if kind == "session" and len(numbers) == 1:
        return session(numbers[0])
    raise ValidationError(
        "window spec must be tumbling:<size>[:<offset>], sliding:<size>:<slide>[:<offset>] or session:<gap>",
        value=spec,
    )


def _read_lines(path: str) -> list[str]:
    if path == "-":
        return sys.stdin.read().splitlines()
    try:
        with open(path, encoding="utf-8") as handle:
            return handle.read().splitlines()
    except OSError as error:
        raise ValidationError(f"cannot read input: {error.strerror or error}", value=path) from error


def _write(path: str | None, lines: Sequence[str]) -> None:
    payload = "".join(f"{line}\n" for line in lines)
    if path is None or path == "-":
        sys.stdout.write(payload)
        return
    directory = os.path.dirname(os.path.abspath(path)) or "."
    handle = None
    temporary = None
    try:
        handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=directory, delete=False, newline="\n")
        temporary = handle.name
        handle.write(payload)
        handle.close()
        handle = None
        os.replace(temporary, path)  # atomic: a failure never leaves a half-written target
    except OSError as error:
        if handle is not None:
            handle.close()
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)
        raise OutputError(f"cannot write output: {error.strerror or error}", value=path) from error


def _assert_distinct(inputs: Sequence[str | None], output: str | None) -> None:
    if not output or output == "-":
        return
    target = os.path.abspath(output)
    for source in inputs:
        if source and source != "-" and os.path.abspath(source) == target:
            raise OutputError("output path collides with an input path", value=output)


def _collect(events_path: str, window_spec: str, aggregation: str, lateness: int, out_of_orderness: int) -> list[str]:
    pipeline = Pipeline(
        windowing=_parse_window(window_spec),
        aggregation=aggregation,
        max_out_of_orderness=out_of_orderness,
        allowed_lateness=lateness,
    )
    lines: list[str] = []
    for number, text in enumerate(_read_lines(events_path), start=1):
        event = parse_event_line(text, line=number)
        for result in pipeline.add(event):
            lines.append(canonical(result.to_document()))
    for result in pipeline.flush():
        lines.append(canonical(result.to_document()))
    return lines


def _command_describe(_: argparse.Namespace) -> int:
    document = {
        "name": "stream-processing-engine",
        "version": __version__,
        "aggregations": sorted(AGGREGATORS),
        "windowings": ["session:<gap>", "sliding:<size>:<slide>[:<offset>]", "tumbling:<size>[:<offset>]"],
        "eventFields": ["timestamp", "key", "value", "kind"],
        "eventKinds": ["data", "punct"],
        "timestampUnit": "milliseconds",
        "checkpoint": {
            "format": CHECKPOINT_FORMAT,
            "version": CHECKPOINT_VERSION,
            "resume": True,
            "options": ["--checkpoint", "--resume"],
        },
        "exitCodes": {"ok": EXIT_OK, "error": EXIT_ERROR, "reportMismatch": EXIT_REPORT_MISMATCH},
    }
    sys.stdout.write(canonical(document) + "\n")
    return EXIT_OK


def _command_windows(args: argparse.Namespace) -> int:
    windowing = _parse_window(args.window)
    if not isinstance(windowing, (Tumbling, Sliding)):
        raise ValidationError("windows requires a tumbling or sliding spec", value=args.window)
    if not isinstance(windowing, Tumbling):
        raise ValidationError("windows currently supports tumbling specs only", value=args.window)
    for window in windowing.boundaries(args.first, args.last):
        sys.stdout.write(canonical(window.to_document()) + "\n")
    return EXIT_OK


# Sentinel for argparse: a config flag the caller did not pass on the command line.
_UNPROVIDED = object()

DEFAULT_WINDOW = "tumbling:1000"
DEFAULT_AGGREGATION = "sum"
DEFAULT_LATENESS = 0
DEFAULT_OUT_OF_ORDERNESS = 0


def _read_input_text(path: str) -> str:
    """Read the whole input with newline translation disabled.

    Keeping CR/LF verbatim makes the consumed-prefix digest sensitive to line-ending tampering and
    lets the line segmentation agree exactly with ``str.splitlines`` (used by the non-checkpointed
    path), including isolated carriage returns.
    """
    try:
        with open(path, encoding="utf-8", newline="") as handle:
            return handle.read()
    except OSError as error:
        raise ValidationError(f"cannot read input: {error.strerror or error}", value=path) from error


def _resolved_config(args: argparse.Namespace, saved: dict[str, Any] | None) -> tuple[str, str, int, int]:
    """Resolve window/aggregation/lateness/disorder, rejecting conflicts with a resumed checkpoint.

    Without ``--resume`` the command line wins (with its documented defaults). With ``--resume`` the
    checkpoint owns the configuration: passing any of the four flags explicitly is a conflict.
    """
    if saved is None:
        return (
            args.window if args.window is not _UNPROVIDED else DEFAULT_WINDOW,
            args.aggregation if args.aggregation is not _UNPROVIDED else DEFAULT_AGGREGATION,
            args.allowed_lateness if args.allowed_lateness is not _UNPROVIDED else DEFAULT_LATENESS,
            args.max_out_of_orderness
            if args.max_out_of_orderness is not _UNPROVIDED
            else DEFAULT_OUT_OF_ORDERNESS,
        )
    conflicts: list[str] = []
    if args.window is not _UNPROVIDED and args.window != saved["window"]:
        conflicts.append("window")
    if args.aggregation is not _UNPROVIDED and args.aggregation != saved["aggregation"]:
        conflicts.append("aggregation")
    if args.allowed_lateness is not _UNPROVIDED and args.allowed_lateness != saved["allowedLateness"]:
        conflicts.append("allowed-lateness")
    if args.max_out_of_orderness is not _UNPROVIDED and args.max_out_of_orderness != saved["maxOutOfOrderness"]:
        conflicts.append("max-out-of-orderness")
    if conflicts:
        raise ValidationError(
            "resume configuration conflicts with the checkpoint",
            fields=conflicts,
            checkpoint=saved,
        )
    return saved["window"], saved["aggregation"], saved["allowedLateness"], saved["maxOutOfOrderness"]


def _run_with_checkpoint(
    args: argparse.Namespace,
    *,
    resume: bool,
) -> int:
    checkpoint_path = args.resume if resume else args.checkpoint

    # Resume is a strict validation phase: any failure must leave output and checkpoint untouched,
    # so everything is read and checked before the first write happens.
    saved = load_checkpoint(checkpoint_path) if resume else None
    if resume and saved is not None and saved.status not in (STATUS_RUNNING, STATUS_COMPLETE):
        raise ValidationError("checkpoint status must be running or complete", value=saved.status)  # defensive

    window_spec, aggregation, lateness, out_of_orderness = _resolved_config(args, saved.config if saved else None)
    windowing = _parse_window(window_spec)  # a saved-but-syntactically-invalid spec is a validation_error
    pipeline = Pipeline(
        windowing=windowing,
        aggregation=aggregation,
        max_out_of_orderness=out_of_orderness,
        allowed_lateness=lateness,
    )
    config = config_document(
        window=window_spec,
        aggregation=aggregation,
        allowed_lateness=lateness,
        max_out_of_orderness=out_of_orderness,
    )

    raw = _read_input_text(args.input)
    segments = raw.splitlines(keepends=True)
    total = len(segments)

    if saved is not None:
        if saved.consumed > total:
            raise ValidationError(
                "checkpoint offset is beyond the current input",
                consumed=saved.consumed,
                lines=total,
            )
        consumed_prefix = "".join(segments[: saved.consumed])
        if prefix_digest(consumed_prefix) != saved.prefix_sha256:
            raise ValidationError("checkpoint input prefix verification failed", consumed=saved.consumed)
        restore_state(pipeline, saved)
        consumed = saved.consumed
        pending = list(saved.pending)
    else:
        consumed = 0
        pending = []

    def persist(status: str) -> None:
        digest = prefix_digest("".join(segments[:consumed]))
        state = capture_state(
            pipeline,
            config=config,
            consumed=consumed,
            prefix_sha256=digest,
            pending=pending,
            status=status,
        )
        save_checkpoint(checkpoint_path, state)

    # A completed run resumed again is idempotent: rewrite the same committed result file and
    # refresh the completion marker; no prefix event is re-counted.
    if saved is not None and saved.status == STATUS_COMPLETE:
        if consumed != total:
            raise ValidationError("input grew after the checkpoint completed", consumed=consumed, lines=total)
        pending_complete = list(saved.pending)
        _write(args.output, pending_complete)
        digest = prefix_digest("".join(segments[:consumed]))
        save_checkpoint(
            checkpoint_path,
            capture_state(
                pipeline,
                config=config,
                consumed=consumed,
                prefix_sha256=digest,
                pending=pending_complete,
                status=STATUS_COMPLETE,
            ),
        )
        return EXIT_OK

    # Process every line strictly past the consumed prefix. Results accumulate in `pending`; they
    # are committed to the final file only on successful completion, while the checkpoint after each
    # line already durably holds them, so a crash between lines loses nothing.
    for number in range(consumed + 1, total + 1):
        text = segments[number - 1].rstrip("\r\n")
        event = parse_event_line(text, line=number)
        for result in pipeline.add(event):
            pending.append(canonical(result.to_document()))
        consumed = number
        persist(STATUS_RUNNING)

    for result in pipeline.flush():
        pending.append(canonical(result.to_document()))
    consumed = total

    # Finish order: the full result file is atomically replaced exactly as an uninterrupted run
    # would write it, and only then is the completion-state checkpoint left behind.
    _write(args.output, pending)
    digest = prefix_digest("".join(segments[:consumed]))
    save_checkpoint(
        checkpoint_path,
        capture_state(
            pipeline,
            config=config,
            consumed=consumed,
            prefix_sha256=digest,
            pending=pending,
            status=STATUS_COMPLETE,
        ),
    )
    return EXIT_OK


def _command_run(args: argparse.Namespace) -> int:
    if args.checkpoint is not None and args.resume is not None:
        raise ValidationError("--checkpoint and --resume are mutually exclusive")
    persistent = args.checkpoint is not None or args.resume is not None
    if persistent:
        # Durable recovery needs a seekable, re-readable input and a non-standard sink. Reading
        # stdin or streaming to stdout cannot be resume-verified, so both are rejected up front.
        if args.input == "-":
            raise ValidationError("checkpointed run requires a regular --input file, not stdin")
        if not args.output or args.output == "-":
            raise ValidationError("checkpointed run requires an explicit file --output, not stdout")
        active_checkpoint = args.resume if args.resume is not None else args.checkpoint
        _assert_distinct([args.input, active_checkpoint], args.output)
        if os.path.abspath(args.input) == os.path.abspath(active_checkpoint):
            raise OutputError("checkpoint path collides with the input path", value=active_checkpoint)
        if args.resume is None and os.path.exists(args.checkpoint):
            raise ValidationError(
                "checkpoint file already exists; use --resume to continue", value=args.checkpoint
            )
        return _run_with_checkpoint(args, resume=args.resume is not None)
    _assert_distinct([args.input], args.output)
    window_spec, aggregation, lateness, out_of_orderness = _resolved_config(args, None)
    lines = _collect(args.input, window_spec, aggregation, lateness, out_of_orderness)
    _write(args.output, lines)
    return EXIT_OK


def _command_replay(args: argparse.Namespace) -> int:
    _assert_distinct([args.input, args.compare], args.output)
    first = _collect(args.input, args.window, args.aggregation, args.allowed_lateness, args.max_out_of_orderness)
    second = _collect(args.input, args.window, args.aggregation, args.allowed_lateness, args.max_out_of_orderness)
    report = {
        "input": args.input,
        "window": args.window,
        "aggregation": args.aggregation,
        "lines": len(first),
        "identical": first == second,
    }
    if args.compare:
        with open(args.compare, encoding="utf-8") as handle:
            expected = handle.read().splitlines()
        report["matchesReference"] = expected == first
        report["referenceLines"] = len(expected)
    _write(args.output, [canonical(report)])
    identical = report.get("matchesReference", report["identical"])
    return EXIT_OK if identical else EXIT_REPORT_MISMATCH


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="stream-processing-engine", description="event-time stream processing engine")
    parser.add_argument("--version", action="version", version=f"stream-processing-engine {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    describe = subparsers.add_parser("describe", help="print engine capabilities as JSON")
    describe.set_defaults(handler=_command_describe)

    windows = subparsers.add_parser("windows", help="print window boundaries for a range")
    windows.add_argument("--window", required=True)
    windows.add_argument("--from", dest="first", type=int, required=True)
    windows.add_argument("--to", dest="last", type=int, required=True)
    windows.set_defaults(handler=_command_windows)

    run = subparsers.add_parser("run", help="process JSONL events")
    run.add_argument("--input", required=True)
    # Defaults are a sentinel (not the documented default) so --resume can tell an explicitly
    # passed configuration from an omitted one: a resumed run inherits the checkpoint's config and
    # rejects any explicit flag that disagrees, instead of silently overriding it.
    run.add_argument("--window", default=_UNPROVIDED)
    run.add_argument("--aggregation", default=_UNPROVIDED, choices=sorted(AGGREGATORS))
    run.add_argument("--allowed-lateness", type=int, default=_UNPROVIDED, dest="allowed_lateness")
    run.add_argument("--max-out-of-orderness", type=int, default=_UNPROVIDED, dest="max_out_of_orderness")
    run.add_argument("--output")
    run.add_argument("--checkpoint", help="persist state to this file after every processed line")
    run.add_argument("--resume", help="continue from this checkpoint and keep updating it")
    run.set_defaults(handler=_command_run)

    replay = subparsers.add_parser("replay", help="process twice and reconcile")
    replay.add_argument("--input", required=True)
    replay.add_argument("--window", default="tumbling:1000")
    replay.add_argument("--aggregation", default="sum", choices=sorted(AGGREGATORS))
    replay.add_argument("--allowed-lateness", type=int, default=0)
    replay.add_argument("--max-out-of-orderness", type=int, default=0)
    replay.add_argument("--compare")
    replay.add_argument("--output")
    replay.set_defaults(handler=_command_replay)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except StreamProcessingError as error:
        sys.stderr.write(canonical(error.to_document()) + "\n")
        return EXIT_ERROR


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
