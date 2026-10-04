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


def _command_run(args: argparse.Namespace) -> int:
    _assert_distinct([args.input], args.output)
    lines = _collect(args.input, args.window, args.aggregation, args.allowed_lateness, args.max_out_of_orderness)
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
    run.add_argument("--window", default="tumbling:1000")
    run.add_argument("--aggregation", default="sum", choices=sorted(AGGREGATORS))
    run.add_argument("--allowed-lateness", type=int, default=0)
    run.add_argument("--max-out-of-orderness", type=int, default=0)
    run.add_argument("--output")
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
