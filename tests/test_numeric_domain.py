"""Numeric domain boundary: only standard, finitely representable JSON numbers cross it.

Three seams are pinned here:

  * ``parse_event_line`` (and therefore every execution mode) rejects the non-standard
    NaN / Infinity / -Infinity constants with the column on the token's first character,
    and rejects syntactically valid numbers like ``1e400`` that decode to a non-finite float;
  * ``Pipeline.add`` rejects a caller-constructed event whose value is non-finite without
    touching the watermark, the counters, the window values or the emitted set;
  * a sum/mean that overflows to a non-finite aggregate fails with ``validation_error``
    before the window result exists: stdout stays empty, an existing ``--output`` file is
    preserved, a checkpoint stays at the last fully processed line, and replay exits 2
    with no report.
"""

from __future__ import annotations

import contextlib
import io
import json
import math
import os
import tempfile
import unittest

from stream_processing.cli import EXIT_ERROR, EXIT_OK, canonical, main
from stream_processing.errors import ParseError, ValidationError
from stream_processing.events import Event, parse_event_line
from stream_processing.pipeline import Pipeline
from stream_processing.windows import tumbling


def run_cli(argv: list[str]) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = main(argv)
    return code, out.getvalue(), err.getvalue()


def data(ts: int, key: str, value: float) -> str:
    return canonical({"timestamp": ts, "key": key, "value": value, "kind": "data"})


class ParseConstantTests(unittest.TestCase):
    def test_nan_infinity_tokens_are_parse_errors_at_the_token(self) -> None:
        cases = [
            ('{"timestamp":1,"key":"a","value":NaN}', "NaN"),
            ('{"timestamp":1,"key":"a","value":Infinity}', "Infinity"),
            ('{"timestamp":1,"key":"a","value":-Infinity}', "-Infinity"),
            ('{"timestamp":1,"key":"a","value":1,"kind":NaN}', "NaN"),
        ]
        for text, token in cases:
            with self.subTest(text=text):
                with self.assertRaises(ParseError) as caught:
                    parse_event_line(text, line=4)
                context = caught.exception.context
                self.assertEqual(context["line"], 4)
                self.assertEqual(context["column"], text.index(token) + 1)

    def test_minus_infinity_column_points_at_the_minus(self) -> None:
        text = '{"timestamp":1,"key":"a","value":-Infinity}'
        with self.assertRaises(ParseError) as caught:
            parse_event_line(text)
        self.assertEqual(caught.exception.context["column"], text.index("-") + 1)

    def test_tokens_inside_strings_are_not_constants(self) -> None:
        event = parse_event_line('{"timestamp":1,"key":"NaN Infinity -Infinity","value":2}')
        self.assertEqual((event.key, event.value), ("NaN Infinity -Infinity", 2.0))

    def test_decoded_non_finite_value_is_a_parse_error(self) -> None:
        for literal in ("1e400", "-1e400", "1E400", "1e+400"):
            with self.subTest(literal=literal):
                with self.assertRaises(ParseError) as caught:
                    parse_event_line(f'{{"timestamp":1,"key":"a","value":{literal}}}')
                self.assertEqual(caught.exception.kind, "parse_error")

    def test_punct_value_is_validated_too(self) -> None:
        with self.assertRaises(ParseError):
            parse_event_line('{"timestamp":1,"key":"c","value":Infinity,"kind":"punct"}')
        with self.assertRaises(ParseError):
            parse_event_line('{"timestamp":1,"key":"c","value":1e400,"kind":"punct"}')

    def test_existing_validation_order_is_unchanged(self) -> None:
        with self.assertRaises(ParseError) as caught:
            parse_event_line('{"timestamp":1.5,"key":"a","value":1}')
        self.assertIn("timestamp", caught.exception.message)
        with self.assertRaises(ParseError) as caught:
            parse_event_line('{"timestamp":1,"key":"a","value":"x"}')
        self.assertIn("number", caught.exception.message)


class PipelineAddTests(unittest.TestCase):
    def test_non_finite_value_is_rejected_without_any_state_change(self) -> None:
        for kind in ("data", "punct"):
            with self.subTest(kind=kind):
                pipeline = Pipeline(windowing=tumbling(100))
                pipeline.add(Event(timestamp=10, key="a", value=1.0))
                snapshot = (
                    pipeline.watermark.max_seen,
                    pipeline.watermark.observed,
                    pipeline.watermark.late_dropped,
                    dict(pipeline._values),
                    set(pipeline._emitted),
                )
                for bad in (math.inf, -math.inf, math.nan):
                    with self.assertRaises(ValidationError):
                        pipeline.add(Event(timestamp=20, key="a", value=bad, kind=kind))
                self.assertEqual(
                    (
                        pipeline.watermark.max_seen,
                        pipeline.watermark.observed,
                        pipeline.watermark.late_dropped,
                        dict(pipeline._values),
                        set(pipeline._emitted),
                    ),
                    snapshot,
                )


class OverflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = self.directory.name
        self.events = os.path.join(self.root, "events.jsonl")
        with open(self.events, "w", encoding="utf-8") as handle:
            handle.write(data(10, "a", 1e308) + "\n")
            handle.write(data(20, "a", 1e308) + "\n")
            handle.write(data(5000, "b", 1.0) + "\n")

    def tearDown(self) -> None:
        self.directory.cleanup()

    def path(self, name: str) -> str:
        return os.path.join(self.root, name)

    def test_sum_overflow_is_a_validation_error_with_empty_stdout(self) -> None:
        code, out, err = run_cli(["run", "--input", self.events, "--window", "tumbling:100"])
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(out, "")
        self.assertEqual(json.loads(err)["error"], "validation_error")

    def test_mean_overflow_preserves_an_existing_output_file(self) -> None:
        target = self.path("keep.out")
        with open(target, "w", encoding="utf-8") as handle:
            handle.write("original\n")
        code, out, err = run_cli(
            ["run", "--input", self.events, "--window", "tumbling:100",
             "--aggregation", "mean", "--output", target]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(out, "")
        self.assertEqual(json.loads(err)["error"], "validation_error")
        with open(target, encoding="utf-8") as handle:
            self.assertEqual(handle.read(), "original\n")

    def test_checkpoint_run_commits_nothing_and_holds_last_good_line(self) -> None:
        out, cp = self.path("ovf.out"), self.path("ovf.cp")
        code, stdout, err = run_cli(
            ["run", "--input", self.events, "--window", "tumbling:100",
             "--output", out, "--checkpoint", cp]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(err)["error"], "validation_error")
        self.assertFalse(os.path.exists(out))
        # The overflow surfaces while line 3 is processed, so the checkpoint holds line 2
        # and the saved state is itself finite, loadable JSON.
        with open(cp, encoding="utf-8") as handle:
            saved = json.load(handle)
        self.assertEqual((saved["status"], saved["consumed"]), ("running", 2))
        self.assertEqual(saved["pending"], [])

    def test_replay_overflow_fails_with_exit_two_and_no_report(self) -> None:
        code, out, err = run_cli(["replay", "--input", self.events, "--window", "tumbling:100"])
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(out, "")
        self.assertEqual(json.loads(err)["error"], "validation_error")

    def test_nan_line_halts_checkpoint_run_and_repaired_input_resumes(self) -> None:
        broken = self.path("broken.jsonl")
        with open(broken, "w", encoding="utf-8") as handle:
            handle.write(data(10, "a", 1.0) + "\n")
            handle.write('{"timestamp":20,"key":"a","value":NaN}\n')
            handle.write(data(5000, "b", 2.0) + "\n")
        out, cp = self.path("fix.out"), self.path("fix.cp")
        code, _, err = run_cli(
            ["run", "--input", broken, "--window", "tumbling:100", "--output", out, "--checkpoint", cp]
        )
        self.assertEqual(code, EXIT_ERROR)
        document = json.loads(err)
        self.assertEqual((document["error"], document["line"]), ("parse_error", 2))
        self.assertFalse(os.path.exists(out))
        with open(cp, encoding="utf-8") as handle:
            self.assertEqual(json.load(handle)["consumed"], 1)

        repaired = self.path("repaired.jsonl")
        with open(repaired, "w", encoding="utf-8") as handle:
            handle.write(data(10, "a", 1.0) + "\n")
            handle.write(data(20, "a", 3.0) + "\n")
            handle.write(data(5000, "b", 2.0) + "\n")
        code, _, err = run_cli(
            ["run", "--input", repaired, "--window", "tumbling:100", "--output", out, "--resume", cp]
        )
        self.assertEqual((code, err), (EXIT_OK, ""))
        code, plain, _ = run_cli(["run", "--input", repaired, "--window", "tumbling:100"])
        self.assertEqual(code, EXIT_OK)
        with open(out, encoding="utf-8") as handle:
            self.assertEqual(handle.read(), plain)


if __name__ == "__main__":
    unittest.main()
