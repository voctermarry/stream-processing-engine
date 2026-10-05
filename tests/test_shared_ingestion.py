"""Shared-ingestion regression: every execution entry point walks one processing path.

The CLI's plain ``run``, checkpointed ``run``, resumed ``run`` and both ``replay`` passes all
route events through the same ingestion / result-collection flow. These tests pin that
contract from the public entry points only:

  * for one JSONL input and one window/aggregation/lateness/disorder configuration, the plain
    run, the uninterrupted checkpointed run and the crash-then-resume run produce
    **byte-identical** result files (and stdout for a plain run equals the file bytes);
  * file input and stdin input are equivalent for a plain run;
  * ``replay`` agrees with ``run`` on record count, content and exit-code semantics;
  * failures keep their boundaries: parse errors carry the original line/column, a failed
    plain run never disturbs an existing output, a failed checkpointed run never commits
    results and leaves the checkpoint on the last successful line, and a failed resume
    validation touches neither output nor checkpoint.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
import unittest.mock

from stream_processing.cli import EXIT_ERROR, EXIT_OK, EXIT_REPORT_MISMATCH, canonical, main


def run_cli(argv: list[str], stdin: str | None = None) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        if stdin is None:
            code = main(argv)
        else:
            with unittest.mock.patch("sys.stdin", io.StringIO(stdin)):
                code = main(argv)
    return code, out.getvalue(), err.getvalue()


def line(document: dict) -> str:
    return canonical(document)


def data(ts: int, key: str, value: float) -> str:
    return line({"timestamp": ts, "key": key, "value": value, "kind": "data"})


def punct(ts: int) -> str:
    return line({"timestamp": ts, "key": "clock", "value": 0, "kind": "punct"})


# A stream that exercises every seam of the shared flow: per-event emissions at the watermark,
# a same-instant multi-key batch, out-of-order-but-timely data, a late drop, and windows that
# only the single end-of-input flush can emit.
EVENTS = [
    data(10, "alpha", 1.0),
    data(20, "beta", 10.0),
    punct(300),              # closes [0,100) for both keys in one batch
    data(410, "alpha", 2.0),
    data(420, "beta", 20.0),
    data(430, "alpha", 4.0),
    punct(500),              # closes [400,500) for both keys
    data(710, "alpha", 8.0),
    data(90, "beta", 99.0),  # late: watermark is far past 100 -> counted and dropped
    data(910, "beta", 5.0),
]                           # no closing punct: flush alone emits [700,800) and [900,1000)

BAD_LINE = '{"timestamp":950,"key":'

CONFIGURATIONS = {
    "tumbling": ["--window", "tumbling:100"],
    "sliding": ["--window", "sliding:300:100"],
    "session": ["--window", "session:100"],
    "lateness-count": ["--window", "tumbling:100", "--allowed-lateness", "50", "--aggregation", "count"],
    "moo-mean": ["--window", "tumbling:100", "--max-out-of-orderness", "200", "--aggregation", "mean"],
}


def write_file(path: str, lines: list[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write("".join(f"{text}\n" for text in lines))


def read_bytes(path: str) -> bytes:
    with open(path, "rb") as handle:
        return handle.read()


class EntryPointEquivalenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = self.directory.name
        self.input_path = os.path.join(self.root, "events.jsonl")
        write_file(self.input_path, EVENTS)

    def tearDown(self) -> None:
        self.directory.cleanup()

    def output(self, tag: str) -> str:
        return os.path.join(self.root, f"{tag}.out.jsonl")

    def plain_run(self, config: list[str], tag: str) -> bytes:
        target = self.output(f"plain-{tag}")
        code, out, err = run_cli(["run", "--input", self.input_path, "--output", target, *config])
        self.assertEqual((code, err), (EXIT_OK, ""))
        return read_bytes(target)

    def checkpointed_run(self, config: list[str], tag: str) -> bytes:
        target = self.output(f"cp-{tag}")
        checkpoint = os.path.join(self.root, f"cp-{tag}.checkpoint.json")
        code, _, err = run_cli(
            ["run", "--input", self.input_path, "--output", target, "--checkpoint", checkpoint, *config]
        )
        self.assertEqual((code, err), (EXIT_OK, ""))
        self.assertEqual(json.loads(read_bytes(checkpoint))["status"], "complete")
        return read_bytes(target)

    def crash_then_resume(self, config: list[str], tag: str, crash_after: int) -> bytes:
        target = self.output(f"resumed-{tag}")
        checkpoint = os.path.join(self.root, f"resumed-{tag}.checkpoint.json")
        crashed = os.path.join(self.root, f"resumed-{tag}.crash.jsonl")
        write_file(crashed, [*EVENTS[:crash_after], BAD_LINE])
        code, _, err = run_cli(
            ["run", "--input", crashed, "--output", target, "--checkpoint", checkpoint, *config]
        )
        self.assertEqual(code, EXIT_ERROR, err)
        self.assertEqual(json.loads(err)["error"], "parse_error")
        code, _, err = run_cli(
            ["run", "--input", self.input_path, "--output", target, "--resume", checkpoint, *config]
        )
        self.assertEqual((code, err), (EXIT_OK, ""))
        return read_bytes(target)

    def test_all_entry_points_produce_byte_identical_results(self) -> None:
        for tag, config in CONFIGURATIONS.items():
            with self.subTest(tag=tag):
                expected = self.plain_run(config, tag)
                self.assertGreater(len(expected), 0)
                self.assertEqual(self.checkpointed_run(config, tag), expected)
                # Interrupt after a data line, after a punct-emission line and after the last line.
                for crash_after in (1, 3, len(EVENTS)):
                    with self.subTest(tag=tag, crash_after=crash_after):
                        self.assertEqual(
                            self.crash_then_resume(config, f"{tag}-k{crash_after}", crash_after), expected
                        )

    def test_plain_run_stdout_equals_file_output(self) -> None:
        config = CONFIGURATIONS["tumbling"]
        code, out, err = run_cli(["run", "--input", self.input_path, *config])
        self.assertEqual((code, err), (EXIT_OK, ""))
        self.assertEqual(out.encode("utf-8"), self.plain_run(config, "stdout"))

    def test_stdin_input_matches_file_input(self) -> None:
        config = CONFIGURATIONS["tumbling"]
        with open(self.input_path, encoding="utf-8") as handle:
            payload = handle.read()
        code, out, err = run_cli(["run", "--input", "-", *config], stdin=payload)
        self.assertEqual((code, err), (EXIT_OK, ""))
        self.assertEqual(out.encode("utf-8"), self.plain_run(config, "stdin"))

    def test_replay_agrees_with_run_on_content_count_and_exit_code(self) -> None:
        for tag, config in CONFIGURATIONS.items():
            with self.subTest(tag=tag):
                expected = self.plain_run(config, tag)
                reference = self.output(f"reference-{tag}")
                write_file(reference, expected.decode("utf-8").splitlines())
                code, out, err = run_cli(["replay", "--input", self.input_path, "--compare", reference, *config])
                self.assertEqual((code, err), (EXIT_OK, ""))
                report = json.loads(out)
                self.assertTrue(report["identical"])
                self.assertTrue(report["matchesReference"])
                self.assertEqual(report["lines"], len(expected.decode("utf-8").splitlines()))
                self.assertEqual(report["referenceLines"], report["lines"])

    def test_replay_mismatch_keeps_exit_code_three(self) -> None:
        reference = self.output("other-reference")
        write_file(reference, [data(1, "x", 1.0)])
        code, out, _ = run_cli(
            ["replay", "--input", self.input_path, "--compare", reference, *CONFIGURATIONS["tumbling"]]
        )
        self.assertEqual(code, EXIT_REPORT_MISMATCH)
        report = json.loads(out)
        self.assertTrue(report["identical"])  # the two internal passes still agree
        self.assertFalse(report["matchesReference"])


class FailureBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = self.directory.name
        self.config = ["--window", "tumbling:100"]

    def tearDown(self) -> None:
        self.directory.cleanup()

    def paths(self, tag: str) -> tuple[str, str, str]:
        return (
            os.path.join(self.root, f"{tag}.jsonl"),
            os.path.join(self.root, f"{tag}.out.jsonl"),
            os.path.join(self.root, f"{tag}.checkpoint.json"),
        )

    def make_running_checkpoint(self, tag: str, good_lines: int) -> tuple[str, str, str]:
        """A checkpointed run that fails parsing line ``good_lines + 1``; returns the paths."""
        input_path, output_path, checkpoint_path = self.paths(tag)
        crashed = os.path.join(self.root, f"{tag}.crash.jsonl")
        write_file(crashed, [*EVENTS[:good_lines], BAD_LINE])
        code, _, err = run_cli(
            ["run", "--input", crashed, "--output", output_path,
             "--checkpoint", checkpoint_path, *self.config]
        )
        self.assertEqual(code, EXIT_ERROR, err)
        return input_path, output_path, checkpoint_path

    def test_plain_run_parse_error_preserves_existing_output(self) -> None:
        input_path, output_path, _ = self.paths("plain-fail")
        write_file(input_path, [*EVENTS[:4], BAD_LINE, *EVENTS[4:]])
        write_file(output_path, ["previous", "content"])
        before = read_bytes(output_path)
        code, _, err = run_cli(["run", "--input", input_path, "--output", output_path, *self.config])
        self.assertEqual(code, EXIT_ERROR)
        document = json.loads(err)
        self.assertEqual(document["error"], "parse_error")
        self.assertEqual(document["line"], 5)
        self.assertIsInstance(document["column"], int)
        self.assertEqual(read_bytes(output_path), before)

    def test_checkpointed_run_parse_error_commits_nothing_and_stops_at_last_good_line(self) -> None:
        _, output_path, checkpoint_path = self.make_running_checkpoint("cp-fail", good_lines=4)
        self.assertFalse(os.path.exists(output_path), "a failed run must not commit final results")
        checkpoint = json.loads(read_bytes(checkpoint_path))
        self.assertEqual(checkpoint["status"], "running")
        self.assertEqual(checkpoint["consumed"], 4)
        # The lines emitted before the failure are durably held in the checkpoint, in order.
        self.assertEqual(
            [(json.loads(text)["window"]["start"], json.loads(text)["key"]) for text in checkpoint["pending"]],
            [(0, "alpha"), (0, "beta")],
        )

    def test_resumed_parse_error_reports_the_original_file_line_number(self) -> None:
        input_path, output_path, checkpoint_path = self.make_running_checkpoint("lineno", good_lines=3)
        # Resume over an input whose line 6 is broken: the error must name line 6, not a
        # resume-relative offset, and the checkpoint must stay on the last good line (5).
        broken = [*EVENTS[:5], BAD_LINE, *EVENTS[5:]]
        write_file(input_path, broken)
        code, _, err = run_cli(
            ["run", "--input", input_path, "--output", output_path, "--resume", checkpoint_path, *self.config]
        )
        self.assertEqual(code, EXIT_ERROR)
        document = json.loads(err)
        self.assertEqual(document["error"], "parse_error")
        self.assertEqual(document["line"], 6)
        self.assertFalse(os.path.exists(output_path))
        checkpoint = json.loads(read_bytes(checkpoint_path))
        self.assertEqual(checkpoint["status"], "running")
        self.assertEqual(checkpoint["consumed"], 5)

    def test_tampered_prefix_resume_touches_neither_output_nor_checkpoint(self) -> None:
        input_path, output_path, checkpoint_path = self.make_running_checkpoint("tamper", good_lines=3)
        checkpoint_before = read_bytes(checkpoint_path)
        tampered = [data(11, "alpha", 1.0), *EVENTS[1:]]  # change an already-consumed line
        write_file(input_path, tampered)
        code, _, err = run_cli(
            ["run", "--input", input_path, "--output", output_path, "--resume", checkpoint_path, *self.config]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "validation_error")
        self.assertFalse(os.path.exists(output_path))
        self.assertEqual(read_bytes(checkpoint_path), checkpoint_before)

    def test_offset_beyond_input_resume_touches_neither_output_nor_checkpoint(self) -> None:
        input_path, output_path, checkpoint_path = self.make_running_checkpoint("beyond", good_lines=4)
        checkpoint_before = read_bytes(checkpoint_path)
        write_file(input_path, EVENTS[:2])  # shorter than the consumed offset
        code, _, err = run_cli(
            ["run", "--input", input_path, "--output", output_path, "--resume", checkpoint_path, *self.config]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "validation_error")
        self.assertFalse(os.path.exists(output_path))
        self.assertEqual(read_bytes(checkpoint_path), checkpoint_before)

    def test_grown_input_after_completion_is_rejected_without_writes(self) -> None:
        input_path, output_path, checkpoint_path = self.paths("grew")
        write_file(input_path, EVENTS[:5])
        code, _, err = run_cli(
            ["run", "--input", input_path, "--output", output_path, "--checkpoint", checkpoint_path, *self.config]
        )
        self.assertEqual((code, err), (EXIT_OK, ""))
        checkpoint_before = read_bytes(checkpoint_path)
        output_before = read_bytes(output_path)
        write_file(input_path, EVENTS)  # the input grows after the checkpoint completed
        code, _, err = run_cli(
            ["run", "--input", input_path, "--output", output_path, "--resume", checkpoint_path, *self.config]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "validation_error")
        self.assertEqual(read_bytes(checkpoint_path), checkpoint_before)
        self.assertEqual(read_bytes(output_path), output_before)

    def test_conflicting_resume_config_is_rejected_without_writes(self) -> None:
        input_path, output_path, checkpoint_path = self.make_running_checkpoint("conflict", good_lines=3)
        checkpoint_before = read_bytes(checkpoint_path)
        write_file(input_path, EVENTS)
        code, _, err = run_cli(
            ["run", "--input", input_path, "--output", output_path, "--resume", checkpoint_path,
             "--window", "tumbling:500"]
        )
        self.assertEqual(code, EXIT_ERROR)
        document = json.loads(err)
        self.assertEqual(document["error"], "validation_error")
        self.assertEqual(document["fields"], ["window"])
        self.assertFalse(os.path.exists(output_path))
        self.assertEqual(read_bytes(checkpoint_path), checkpoint_before)

    def test_replay_parse_error_matches_run_parse_error(self) -> None:
        input_path, _, _ = self.paths("replay-fail")
        write_file(input_path, [*EVENTS[:2], BAD_LINE])
        code, out, err = run_cli(["replay", "--input", input_path, *self.config])
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(out, "")
        document = json.loads(err)
        self.assertEqual(document["error"], "parse_error")
        self.assertEqual(document["line"], 3)
        self.assertIsInstance(document["column"], int)


if __name__ == "__main__":
    unittest.main()
