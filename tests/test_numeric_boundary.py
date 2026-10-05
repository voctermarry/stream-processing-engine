"""Numeric boundary: the engine only accepts and produces finitely representable standard JSON.

Two failure shapes are pinned here, across every public entry point:

  * non-standard constants (``NaN`` / ``Infinity`` / ``-Infinity``) and standard syntax with a
    non-finite value (``1e400``, oversized integers) are ``parse_error`` at the JSONL boundary,
    with the original 1-based line number and -- for the constants -- the column of the token's
    first character; a directly constructed ``Event`` handed to ``Pipeline.add`` is a
    ``ValidationError`` that mutates nothing;
  * an aggregation whose finite inputs overflow the float domain (``1e308 + 1e308`` in ``sum``
    or ``mean``) is a ``validation_error`` raised before the window result exists: stdout stays
    clean, a pre-existing ``--output`` file is preserved, a checkpointed run commits nothing and
    holds the last fully processed line, and ``replay`` fails with exit code 2 and no report.

Finite-input behavior (windowing, watermarks, late drops, ordering, canonical bytes) is pinned
elsewhere and must not move.
"""

from __future__ import annotations

import contextlib
import io
import json
import math
import os
import sys
import tempfile
import unittest

from stream_processing.cli import EXIT_ERROR, EXIT_OK, canonical, main
from stream_processing.errors import ParseError, ValidationError
from stream_processing.events import Event, parse_event_line
from stream_processing.pipeline import Pipeline
from stream_processing.windows import tumbling


def run_cli(argv: list[str], stdin: str | None = None) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        if stdin is not None:
            original_stdin = sys.stdin
            sys.stdin = io.StringIO(stdin)
            try:
                code = main(argv)
            finally:
                sys.stdin = original_stdin
        else:
            code = main(argv)
    return code, out.getvalue(), err.getvalue()


def line(document: dict) -> str:
    return canonical(document)


def data(ts: int, key: str, value: float) -> str:
    return line({"timestamp": ts, "key": key, "value": value, "kind": "data"})


def punct(ts: int) -> str:
    return line({"timestamp": ts, "key": "clock", "value": 0, "kind": "punct"})


def write_file(path: str, lines: list[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as handle:
        handle.write("".join(f"{text}\n" for text in lines))


def read_bytes(path: str) -> bytes:
    with open(path, "rb") as handle:
        return handle.read()


class ParseBoundaryTests(unittest.TestCase):
    def test_non_standard_constants_are_parse_errors_at_the_token(self) -> None:
        for token in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(token=token):
                text = f'{{"timestamp": 10, "key": "a", "value": {token}}}'
                with self.assertRaises(ParseError) as caught:
                    parse_event_line(text, line=7)
                error = caught.exception
                self.assertEqual(error.kind, "parse_error")
                self.assertEqual(error.context["line"], 7)
                self.assertEqual(error.context["column"], text.index(token) + 1)

    def test_constant_outside_the_value_field_is_still_a_parse_error(self) -> None:
        text = '{"timestamp": NaN, "key": "a"}'
        with self.assertRaises(ParseError) as caught:
            parse_event_line(text, line=3)
        self.assertEqual(caught.exception.context["column"], text.index("NaN") + 1)

    def test_constant_spelling_inside_a_string_is_data_not_a_token(self) -> None:
        event = parse_event_line('{"timestamp": 1, "key": "NaN Infinity", "value": 2.5}')
        self.assertEqual((event.key, event.value), ("NaN Infinity", 2.5))

    def test_json_syntax_with_non_finite_value_is_a_parse_error(self) -> None:
        for token in ("1e400", "-1e400", "1" + "0" * 400):
            with self.subTest(token=token):
                with self.assertRaises(ParseError) as caught:
                    parse_event_line(f'{{"timestamp": 1, "key": "a", "value": {token}}}', line=2)
                self.assertEqual(caught.exception.kind, "parse_error")
                self.assertEqual(caught.exception.context["line"], 2)

    def test_punct_value_is_validated_like_data(self) -> None:
        with self.assertRaises(ParseError):
            parse_event_line('{"timestamp": 1, "key": "clock", "value": Infinity, "kind": "punct"}')


class DirectConstructionTests(unittest.TestCase):
    def test_non_finite_value_is_rejected_without_touching_state(self) -> None:
        for bad in (math.inf, -math.inf, math.nan, 10**400):
            with self.subTest(bad=repr(bad)):
                pipeline = Pipeline(windowing=tumbling(100), aggregation="sum")
                pipeline.add(Event(timestamp=10, key="a", value=1.0))
                pipeline.add(Event(timestamp=20, key="a", value=2.0))
                snapshot = (
                    pipeline.watermark.max_seen,
                    pipeline.watermark.observed,
                    pipeline.watermark.late_dropped,
                    {key: list(values) for key, values in pipeline._values.items()},
                    set(pipeline._emitted),
                )
                for kind in ("data", "punct"):
                    with self.assertRaises(ValidationError):
                        pipeline.add(Event(timestamp=30, key="a", value=bad, kind=kind))
                current = (
                    pipeline.watermark.max_seen,
                    pipeline.watermark.observed,
                    pipeline.watermark.late_dropped,
                    {key: list(values) for key, values in pipeline._values.items()},
                    set(pipeline._emitted),
                )
                self.assertEqual(current, snapshot)


class CliBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = self.directory.name

    def tearDown(self) -> None:
        self.directory.cleanup()

    def path(self, name: str) -> str:
        return os.path.join(self.root, name)

    def test_run_reports_constant_with_original_line_and_column(self) -> None:
        bad = '{"timestamp": 20, "key": "a", "value": -Infinity}'
        input_path = self.path("constant.jsonl")
        write_file(input_path, [data(10, "a", 1.0), bad, punct(500)])
        code, out, err = run_cli(["run", "--input", input_path, "--window", "tumbling:100"])
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(out, "")
        document = json.loads(err)
        self.assertEqual(document["error"], "parse_error")
        self.assertEqual(document["line"], 2)
        self.assertEqual(document["column"], bad.index("-Infinity") + 1)

    def test_run_rejects_overflowing_literal_on_stdin(self) -> None:
        payload = "\n".join([data(10, "a", 1.0), '{"timestamp": 20, "key": "a", "value": 1e400}'])
        code, out, err = run_cli(["run", "--input", "-", "--window", "tumbling:100"], stdin=payload)
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(out, "")
        self.assertEqual((json.loads(err)["error"], json.loads(err)["line"]), ("parse_error", 2))

    def test_aggregation_overflow_is_a_validation_error_with_clean_stdout(self) -> None:
        for aggregation in ("sum", "mean"):
            with self.subTest(aggregation=aggregation):
                input_path = self.path(f"overflow-{aggregation}.jsonl")
                write_file(input_path, [data(10, "a", 1e308), data(20, "a", 1e308), punct(500)])
                code, out, err = run_cli(
                    ["run", "--input", input_path, "--window", "tumbling:100",
                     "--aggregation", aggregation]
                )
                self.assertEqual(code, EXIT_ERROR)
                self.assertEqual(out, "", "no partial result may reach stdout")
                self.assertEqual(json.loads(err)["error"], "validation_error")

    def test_aggregation_overflow_at_flush_preserves_an_existing_output(self) -> None:
        target = self.path("preexisting.out")
        good = self.path("good.jsonl")
        write_file(good, [data(10, "a", 1.0), punct(500)])
        code, _, err = run_cli(["run", "--input", good, "--output", target, "--window", "tumbling:100"])
        self.assertEqual((code, err), (EXIT_OK, ""), err)
        before = read_bytes(target)

        # No punctuation: the overflowing window is closed only by the end-of-input flush.
        overflowing = self.path("overflowing.jsonl")
        write_file(overflowing, [data(10, "a", 1e308), data(20, "a", 1e308)])
        code, out, err = run_cli(
            ["run", "--input", overflowing, "--output", target, "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(out, "")
        self.assertEqual(json.loads(err)["error"], "validation_error")
        self.assertEqual(read_bytes(target), before, "a failed run must not touch the output")

    def test_replay_fails_with_exit_two_and_no_report(self) -> None:
        for name, events in (
            ("constant", [data(10, "a", 1.0), '{"timestamp": 20, "key": "a", "value": NaN}']),
            ("overflow", [data(10, "a", 1e308), data(20, "a", 1e308), punct(500)]),
        ):
            with self.subTest(name=name):
                input_path = self.path(f"replay-{name}.jsonl")
                output_path = self.path(f"replay-{name}.out")
                write_file(input_path, events)
                code, out, err = run_cli(
                    ["replay", "--input", input_path, "--output", output_path,
                     "--window", "tumbling:100"]
                )
                self.assertEqual(code, EXIT_ERROR)
                self.assertEqual(out, "", "no reconciliation report on a numeric failure")
                self.assertFalse(os.path.exists(output_path))
                self.assertIn(json.loads(err)["error"], ("parse_error", "validation_error"))


class CheckpointBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = self.directory.name

    def tearDown(self) -> None:
        self.directory.cleanup()

    def path(self, name: str) -> str:
        return os.path.join(self.root, name)

    def test_constant_holds_checkpoint_at_last_good_line_and_resume_completes(self) -> None:
        events = [data(10, "a", 1.0), data(20, "a", 2.0), punct(500)]
        crashed = self.path("crashed.jsonl")
        write_file(crashed, [events[0], '{"timestamp": 20, "key": "a", "value": Infinity}', events[2]])
        out, cp = self.path("out.json"), self.path("cp.json")
        code, _, err = run_cli(
            ["run", "--input", crashed, "--output", out, "--checkpoint", cp, "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual((json.loads(err)["error"], json.loads(err)["line"]), ("parse_error", 2))
        self.assertFalse(os.path.exists(out))
        running = json.loads(read_bytes(cp))
        self.assertEqual((running["status"], running["consumed"]), ("running", 1))

        fixed = self.path("fixed.jsonl")
        write_file(fixed, events)
        code, _, err = run_cli(
            ["run", "--input", fixed, "--output", out, "--resume", cp, "--window", "tumbling:100"]
        )
        self.assertEqual((code, err), (EXIT_OK, ""), err)
        plain = self.path("plain.jsonl")
        write_file(plain, events)
        code, reference, _ = run_cli(["run", "--input", plain, "--window", "tumbling:100"])
        self.assertEqual(code, EXIT_OK)
        self.assertEqual(read_bytes(out), reference.encode("utf-8"))

    def test_overflow_commits_nothing_and_saves_no_non_finite_state(self) -> None:
        events = [data(10, "a", 1e308), data(20, "a", 1e308), punct(500)]
        input_path = self.path("overflow.jsonl")
        write_file(input_path, events)
        out, cp = self.path("overflow.out"), self.path("overflow.cp")
        code, _, err = run_cli(
            ["run", "--input", input_path, "--output", out, "--checkpoint", cp, "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "validation_error")
        self.assertFalse(os.path.exists(out), "the final output is committed only on success")
        raw = read_bytes(cp).decode("utf-8")
        self.assertNotIn("Infinity", raw)
        self.assertNotIn("NaN", raw)
        running = json.loads(raw)
        # The overflow surfaced while processing line 3, so the checkpoint holds line 2.
        self.assertEqual((running["status"], running["consumed"]), ("running", 2))
        for _, _, _, stored in running["state"]["values"]:
            self.assertTrue(all(math.isfinite(item) for item in stored))

        # The checkpoint is intact for the standard resume machinery: the same input fails the
        # same way (never a crash), and the checkpoint is left byte-identical.
        before = read_bytes(cp)
        code, _, err = run_cli(
            ["run", "--input", input_path, "--output", out, "--resume", cp, "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "validation_error")
        self.assertFalse(os.path.exists(out))
        self.assertEqual(read_bytes(cp), before)

    def test_constant_in_resumed_suffix_keeps_absolute_line_number(self) -> None:
        events = [data(10, "a", 1.0), data(20, "a", 2.0), punct(500)]
        crashed = self.path("suffix-crash.jsonl")
        write_file(crashed, [events[0], '{"broken":'])
        out, cp = self.path("suffix.out"), self.path("suffix.cp")
        code, _, _ = run_cli(
            ["run", "--input", crashed, "--output", out, "--checkpoint", cp, "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_ERROR)

        resumed = self.path("suffix-resumed.jsonl")
        write_file(resumed, [events[0], events[1], '{"timestamp": 30, "key": "a", "value": NaN}'])
        code, _, err = run_cli(
            ["run", "--input", resumed, "--output", out, "--resume", cp, "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_ERROR)
        document = json.loads(err)
        self.assertEqual((document["error"], document["line"]), ("parse_error", 3))
        self.assertFalse(os.path.exists(out))
        running = json.loads(read_bytes(cp))
        self.assertEqual((running["status"], running["consumed"]), ("running", 2))


if __name__ == "__main__":
    unittest.main()
