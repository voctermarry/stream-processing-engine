"""CLI contract: stable JSON on stdout, documented exit codes, atomic output."""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest

from stream_processing.cli import EXIT_ERROR, EXIT_OK, EXIT_REPORT_MISMATCH, canonical, main


def run_cli(argv: list[str]) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = main(argv)
    return code, out.getvalue(), err.getvalue()


class DescribeTests(unittest.TestCase):
    def test_describe_is_json_and_lists_contract_fields(self) -> None:
        code, out, err = run_cli(["describe"])
        self.assertEqual((code, err), (EXIT_OK, ""))
        document = json.loads(out)
        self.assertEqual(document["name"], "stream-processing-engine")
        self.assertEqual(document["exitCodes"], {"ok": 0, "error": 2, "reportMismatch": 3})
        self.assertIn("session:<gap>", document["windowings"])

    def test_canonical_form_is_sorted_and_compact(self) -> None:
        self.assertEqual(canonical({"b": 1, "a": [1, 2]}), '{"a":[1,2],"b":1}')


class RunTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.events = os.path.join(self.directory.name, "events.jsonl")
        with open(self.events, "w", encoding="utf-8") as handle:
            handle.write('{"timestamp":10,"key":"a","value":1}\n')
            handle.write('{"timestamp":20,"key":"a","value":2}\n')
            handle.write('{"timestamp":110,"key":"a","value":4}\n')

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_run_prints_one_canonical_document_per_line(self) -> None:
        code, out, err = run_cli(["run", "--input", self.events, "--window", "tumbling:100", "--aggregation", "sum"])
        self.assertEqual((code, err), (EXIT_OK, ""))
        documents = [json.loads(line) for line in out.splitlines()]
        self.assertEqual([document["value"] for document in documents], [3.0, 4.0])
        self.assertEqual([document["count"] for document in documents], [2, 1])

    def test_output_is_written_atomically_and_stdout_stays_empty(self) -> None:
        target = os.path.join(self.directory.name, "results.jsonl")
        code, out, _ = run_cli(["run", "--input", self.events, "--window", "tumbling:100", "--output", target])
        self.assertEqual(code, EXIT_OK)
        self.assertEqual(out, "")
        with open(target, encoding="utf-8") as handle:
            self.assertEqual(len(handle.read().splitlines()), 2)

    def test_output_colliding_with_input_is_rejected_before_reading(self) -> None:
        with open(self.events, encoding="utf-8") as handle:
            before = handle.read()
        code, _, err = run_cli(["run", "--input", self.events, "--window", "tumbling:100", "--output", self.events])
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "output_error")
        with open(self.events, encoding="utf-8") as handle:
            self.assertEqual(handle.read(), before)

    def test_parse_error_reports_line_and_column(self) -> None:
        broken = os.path.join(self.directory.name, "broken.jsonl")
        with open(broken, "w", encoding="utf-8") as handle:
            handle.write('{"timestamp":10,"key":"a"}\n')
            handle.write('{"timestamp":20,"key":}\n')
        code, _, err = run_cli(["run", "--input", broken])
        self.assertEqual(code, EXIT_ERROR)
        document = json.loads(err)
        self.assertEqual(document["error"], "parse_error")
        self.assertEqual(document["line"], 2)

    def test_bad_window_spec_is_a_validation_error(self) -> None:
        code, _, err = run_cli(["run", "--input", self.events, "--window", "tumbling:0"])
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "validation_error")


class ReplayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.events = os.path.join(self.directory.name, "events.jsonl")
        with open(self.events, "w", encoding="utf-8") as handle:
            handle.write('{"timestamp":10,"key":"a","value":1}\n')
            handle.write('{"timestamp":110,"key":"a","value":2}\n')

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_replay_reports_identical_runs(self) -> None:
        code, out, _ = run_cli(["replay", "--input", self.events, "--window", "tumbling:100"])
        self.assertEqual(code, EXIT_OK)
        document = json.loads(out)
        self.assertTrue(document["identical"])

    def test_replay_against_a_different_reference_returns_three(self) -> None:
        reference = os.path.join(self.directory.name, "reference.jsonl")
        with open(reference, "w", encoding="utf-8") as handle:
            handle.write('{"different":true}\n')
        code, out, _ = run_cli(["replay", "--input", self.events, "--window", "tumbling:100", "--compare", reference])
        self.assertEqual(code, EXIT_REPORT_MISMATCH)
        document = json.loads(out)
        self.assertFalse(document["matchesReference"])
        self.assertEqual(document["referenceLines"], 1)


class WindowsCommandTests(unittest.TestCase):
    def test_boundaries_are_listed(self) -> None:
        code, out, _ = run_cli(["windows", "--window", "tumbling:100", "--from", "50", "--to", "250"])
        self.assertEqual(code, EXIT_OK)
        self.assertEqual([json.loads(line)["start"] for line in out.splitlines()], [0, 100, 200])

    def test_session_spec_is_rejected_here(self) -> None:
        code, _, err = run_cli(["windows", "--window", "session:10", "--from", "0", "--to", "10"])
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "validation_error")


if __name__ == "__main__":
    unittest.main()
