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
    RunConfig,
    read_checkpoint,
    run_with_checkpoint,
)
from .errors import OutputError, StreamProcessingError, ValidationError
from .events import parse_event_line
from .pipeline import AGGREGATORS, Pipeline
from .windows import Session, Sliding, Tumbling, parse_window_spec

EXIT_OK = 0
EXIT_ERROR = 2
EXIT_REPORT_MISMATCH = 3

DEFAULT_WINDOW = "tumbling:1000"
DEFAULT_AGGREGATION = "sum"
DEFAULT_LATENESS = 0
DEFAULT_DISORDER = 0


def canonical(document: dict[str, Any]) -> str:
    """One canonical rendering: sorted keys, no spaces, trailing newline added by the writer."""
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _parse_window(spec: str) -> Tumbling | Sliding | Session:
    # Shared grammar lives with the window model so checkpoint resume builds the identical assigner.
    return parse_window_spec(spec)


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
        "exitCodes": {"ok": EXIT_OK, "error": EXIT_ERROR, "reportMismatch": EXIT_REPORT_MISMATCH},
        "checkpoint": {
            "format": CHECKPOINT_FORMAT,
            "version": CHECKPOINT_VERSION,
            "resumeSupported": True,
            "options": ["run --checkpoint <path>", "run --resume <path>"],
        },
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


def _command_run(args: argparse.Namespace) -> int:
    _assert_distinct([args.input], args.output)
    checkpoint_path = args.checkpoint or args.resume
    if checkpoint_path is None:
        # Legacy path: output, errors and exit codes are unchanged when the new options are absent.
        window = args.window or DEFAULT_WINDOW
        aggregation = args.aggregation or DEFAULT_AGGREGATION
        lateness = args.allowed_lateness if args.allowed_lateness is not None else DEFAULT_LATENESS
        disorder = args.max_out_of_orderness if args.max_out_of_orderness is not None else DEFAULT_DISORDER
        lines = _collect(args.input, window, aggregation, lateness, disorder)
        _write(args.output, lines)
        return EXIT_OK

    if args.checkpoint and args.resume:
        raise ValidationError("--checkpoint and --resume are mutually exclusive")
    # A durable run needs a seekable, verifiable regular input and an explicit file output: stdin
    # cannot be prefix-verified on restart and stdout cannot be atomically replaced.
    if args.input == "-" or not os.path.isfile(args.input):
        raise ValidationError("checkpointed run requires a regular file as --input", value=args.input)
    if not args.output or args.output == "-":
        raise ValidationError("checkpointed run requires an explicit non-stdout --output")
    target = os.path.abspath(args.output)
    if os.path.abspath(checkpoint_path) == target:
        raise ValidationError("checkpoint path must differ from --output", value=checkpoint_path)
    if os.path.abspath(checkpoint_path) == os.path.abspath(args.input):
        raise ValidationError("checkpoint path must differ from --input", value=checkpoint_path)

    if args.checkpoint:
        # A fresh checkpointed run never silently clobbers durable state: an existing checkpoint at
        # this path must be continued with --resume, not overwritten.
        if os.path.exists(checkpoint_path):
            raise ValidationError("checkpoint already exists; use --resume to continue it", value=checkpoint_path)
        config = RunConfig(
            window=args.window or DEFAULT_WINDOW,
            aggregation=args.aggregation or DEFAULT_AGGREGATION,
            allowed_lateness=args.allowed_lateness if args.allowed_lateness is not None else DEFAULT_LATENESS,
            max_out_of_orderness=(
                args.max_out_of_orderness if args.max_out_of_orderness is not None else DEFAULT_DISORDER
            ),
        )
        resume_document = None
    else:
        # Resume: malformed/unreadable checkpoints surface here, before any output or state change.
        resume_document = read_checkpoint(checkpoint_path)
        config = _resolve_resume_config(args, RunConfig.from_document(resume_document["config"]))

    run_with_checkpoint(
        input_path=args.input,
        output_path=args.output,
        checkpoint_path=checkpoint_path,
        config=config,
        resume_document=resume_document,
    )
    return EXIT_OK


def _resolve_resume_config(args: argparse.Namespace, saved: RunConfig) -> RunConfig:
    """Adopt the checkpointed configuration; any explicitly passed, differing value is a conflict.

    Omitting a config flag means "use what the checkpoint recorded"; passing the identical value is
    harmless; passing a different value is a validation_error rather than a silent override.
    """
    conflicts: dict[str, object] = {}
    if args.window is not None and args.window != saved.window:
        conflicts["window"] = {"passed": args.window, "checkpoint": saved.window}
    if args.aggregation is not None and args.aggregation != saved.aggregation:
        conflicts["aggregation"] = {"passed": args.aggregation, "checkpoint": saved.aggregation}
    if args.allowed_lateness is not None and args.allowed_lateness != saved.allowed_lateness:
        conflicts["allowedLateness"] = {
            "passed": args.allowed_lateness,
            "checkpoint": saved.allowed_lateness,
        }
    if args.max_out_of_orderness is not None and args.max_out_of_orderness != saved.max_out_of_orderness:
        conflicts["maxOutOfOrderness"] = {
            "passed": args.max_out_of_orderness,
            "checkpoint": saved.max_out_of_orderness,
        }
    if conflicts:
        raise ValidationError("resume configuration conflicts with the checkpoint", **conflicts)
    return saved


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
    run.add_argument("--window", default=None)
    run.add_argument("--aggregation", default=None, choices=sorted(AGGREGATORS))
    run.add_argument("--allowed-lateness", type=int, default=None)
    run.add_argument("--max-out-of-orderness", type=int, default=None)
    run.add_argument("--output")
    run.add_argument("--checkpoint", help="persist a durable checkpoint after each line to this file")
    run.add_argument("--resume", help="continue from a checkpoint file written by --checkpoint")
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
