"""Cross-entry equivalence for the shared ingestion path.

The refactor behind these tests routes every execution mode — plain ``run`` (file or stdin),
``run --checkpoint`` to completion, ``run --resume`` from a running checkpoint, and both passes of
``replay`` — through one parse -> add -> watermark-driven collect -> flush sequence. These tests
pin the externally visible consequence of that unification:

  * identical JSONL input + configuration produces *byte-for-byte* identical canonical results no
    matter which entry point produced them, across tumbling / sliding / session windows, late data,
    out-of-orderness, lateness and every aggregation;
  * the single end-of-input flush and the per-line watermark collection keep identical counts,
    ordering and exit codes;
  * failure boundaries stay where the contract puts them: parse errors carry the *original* line
    number on every path, a plain failed run never touches an existing output, a checkpointed parse
    error commits nothing and stops at the last good line, and a failed resume validation leaves
    both output and checkpoint byte-identical.

Everything here drives the public CLI; the shared helpers are accepted only via these entries.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest

from stream_processing.cli import EXIT_ERROR, EXIT_OK, EXIT_REPORT_MISMATCH, canonical, main


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


BAD_LINE = '{"timestamp":900,"key":'


# Every seam the shared path has to cross identically: per-key windows closing in one watermark
# batch, out-of-order-but-timely data, late drops, and windows only the single end flush can emit.
TUMBLING_EVENTS = [
    data(10, "alpha", 1.0),
    data(20, "beta", 10.0),
    punct(300),
    data(410, "alpha", 2.0),
    data(420, "beta", 20.0),
    data(430, "alpha", 4.0),
    punct(500),
    data(710, "alpha", 8.0),
    data(900, "beta", 9.0),
]

SLIDING_EVENTS = [data(50, "k", 1.0), data(150, "k", 2.0), data(250, "k", 3.0), punct(1000)]
SESSION_EVENTS = [
    data(10, "a", 1.0),
    data(15, "a", 2.0),
    data(500, "a", 4.0),
    punct(700),
]
LATE_EVENTS = [data(100, "k", 1.0), data(50, "k", 99.0), punct(300)]

MATRIX = (
    ("tumbling-sum", TUMBLING_EVENTS, ["--window", "tumbling:100"]),
    ("tumbling-count-lateness", TUMBLING_EVENTS,
     ["--window", "tumbling:100", "--aggregation", "count", "--allowed-lateness", "50"]),
    ("tumbling-mean-disorder", TUMBLING_EVENTS,
     ["--window", "tumbling:100", "--aggregation", "mean", "--max-out-of-orderness", "200"]),
    ("sliding-sum", SLIDING_EVENTS, ["--window", "sliding:300:100"]),
    ("sliding-min-offset", SLIDING_EVENTS,
     ["--window", "sliding:300:100:50", "--aggregation", "min"]),
    ("session-sum", SESSION_EVENTS, ["--window", "session:100"]),
    ("session-max", SESSION_EVENTS, ["--window", "session:100", "--aggregation", "max"]),
    ("late-drops", LATE_EVENTS, ["--window", "tumbling:100"]),
)


def write_file(path: str, lines: list[str], newline: str = "\n") -> None:
    with open(path, "w", encoding="utf-8", newline="") as handle:
        handle.write("".join(f"{text}{newline}" for text in lines))


def read_bytes(path: str) -> bytes:
    with open(path, "rb") as handle:
        return handle.read()


class EntryEquivalenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = self.directory.name

    def tearDown(self) -> None:
        self.directory.cleanup()

    def path(self, name: str) -> str:
        return os.path.join(self.root, name)

    # -- byte-identical results, every entry point, every configuration -------------------------

    def assert_all_entries_byte_identical(self, tag: str, events: list[str], config: list[str]) -> None:
        input_path = self.path(f"{tag}.jsonl")
        write_file(input_path, events)

        # Reference: a plain run rendered to stdout.
        code, stdout_out, err = run_cli(["run", "--input", input_path, *config])
        self.assertEqual((code, err), (EXIT_OK, ""), err)
        reference = stdout_out.encode("utf-8")
        self.assertTrue(reference, f"{tag}: reference should emit at least one line")
        for document_line in reference.decode("utf-8").splitlines():
            parsed = json.loads(document_line)
            self.assertEqual(canonical(parsed), document_line, f"{tag}: output must stay canonical")

        # A plain run writing to a file is the same bytes.
        plain_file = self.path(f"{tag}-plain.out")
        code, _, err = run_cli(["run", "--input", input_path, "--output", plain_file, *config])
        self.assertEqual((code, err), (EXIT_OK, ""), err)
        self.assertEqual(read_bytes(plain_file), reference, f"{tag}: file run drifted")

        # stdin goes through the same reader and the same ingestion path.
        code, stdin_out, err = run_cli(["run", "--input", "-", *config], stdin="".join(f"{x}\n" for x in events))
        self.assertEqual((code, err), (EXIT_OK, ""), err)
        self.assertEqual(stdin_out.encode("utf-8"), reference, f"{tag}: stdin run drifted")

        # A checkpointed run taken uninterrupted to completion.
        cp_complete_out = self.path(f"{tag}-cp.out")
        cp_complete = self.path(f"{tag}.cp")
        code, _, err = run_cli(
            ["run", "--input", input_path, "--output", cp_complete_out,
             "--checkpoint", cp_complete, *config]
        )
        self.assertEqual((code, err), (EXIT_OK, ""), err)
        self.assertEqual(read_bytes(cp_complete_out), reference, f"{tag}: checkpoint run drifted")
        self.assertEqual(json.loads(read_bytes(cp_complete))["status"], "complete")

        # A crash after every possible good line, then resume to completion — the resumed file must
        # be byte-identical, and the checkpoint must finish complete with all lines consumed.
        for crash_after in range(1, len(events) + 1):
            with self.subTest(tag=tag, crash_after=crash_after):
                crashed = self.path(f"{tag}-k{crash_after}-crash.jsonl")
                write_file(crashed, [*events[:crash_after], BAD_LINE])
                resumed_out = self.path(f"{tag}-k{crash_after}.out")
                resumed_cp = self.path(f"{tag}-k{crash_after}.cp")
                code, _, err = run_cli(
                    ["run", "--input", crashed, "--output", resumed_out,
                     "--checkpoint", resumed_cp, *config]
                )
                self.assertEqual(code, EXIT_ERROR, err)
                self.assertFalse(os.path.exists(resumed_out))
                self.assertEqual(json.loads(read_bytes(resumed_cp))["consumed"], crash_after)
                code, _, err = run_cli(
                    ["run", "--input", input_path, "--output", resumed_out,
                     "--resume", resumed_cp, *config]
                )
                self.assertEqual((code, err), (EXIT_OK, ""), err)
                self.assertEqual(read_bytes(resumed_out), reference, "resumed output must match")
                final = json.loads(read_bytes(resumed_cp))
                self.assertEqual(final["status"], "complete")
                self.assertEqual(final["consumed"], len(events))

    def test_every_configuration_is_byte_identical_across_entries(self) -> None:
        for tag, events, config in MATRIX:
            with self.subTest(tag=tag):
                self.assert_all_entries_byte_identical(tag, events, config)

    # -- replay rides the same path twice --------------------------------------------------------

    def test_replay_both_passes_match_plain_run_and_reference(self) -> None:
        for tag, events, config in MATRIX:
            with self.subTest(tag=tag):
                input_path = self.path(f"{tag}-replay.jsonl")
                write_file(input_path, events)
                code, plain_out, err = run_cli(["run", "--input", input_path, *config])
                self.assertEqual((code, err), (EXIT_OK, ""), err)
                reference_path = self.path(f"{tag}-reference.out")
                with open(reference_path, "w", encoding="utf-8", newline="\n") as handle:
                    handle.write(plain_out)

                code, out, err = run_cli(
                    ["replay", "--input", input_path, "--compare", reference_path, *config]
                )
                self.assertEqual((code, err), (EXIT_OK, ""), err)
                report = json.loads(out)
                self.assertTrue(report["identical"])
                self.assertTrue(report["matchesReference"])
                self.assertEqual(report["lines"], len(plain_out.splitlines()))
                self.assertEqual(report["referenceLines"], report["lines"])

    def test_replay_mismatch_keeps_exit_code_three_and_count_semantics(self) -> None:
        events = TUMBLING_EVENTS
        input_path = self.path("replay-mismatch.jsonl")
        write_file(input_path, events)
        wrong = self.path("wrong-reference.out")
        with open(wrong, "w", encoding="utf-8") as handle:
            handle.write('{"aggregation":"sum","different":true}\n')
        code, out, err = run_cli(
            ["replay", "--input", input_path, "--window", "tumbling:100", "--compare", wrong]
        )
        self.assertEqual(code, EXIT_REPORT_MISMATCH)
        self.assertEqual(err, "")  # a produced report is not an error document
        report = json.loads(out)
        self.assertTrue(report["identical"])  # the two shared-path passes still agree
        self.assertFalse(report["matchesReference"])
        self.assertEqual(report["lines"], 6)
        self.assertEqual(report["referenceLines"], 1)

    # -- original line numbers survive on every path --------------------------------------------

    def test_parse_error_on_stdin_keeps_original_line_and_column(self) -> None:
        payload = "\n".join([data(10, "a", 1.0), '{"timestamp":20,"key":}', data(30, "a", 3.0)])
        code, _, err = run_cli(["run", "--input", "-", "--window", "tumbling:100"], stdin=payload)
        self.assertEqual(code, EXIT_ERROR)
        document = json.loads(err)
        self.assertEqual(document["error"], "parse_error")
        self.assertEqual(document["line"], 2)
        self.assertIsInstance(document["column"], int)

    def test_parse_error_during_resumed_suffix_keeps_absolute_line_number(self) -> None:
        input_path = self.path("suffix-lines.jsonl")
        write_file(input_path, TUMBLING_EVENTS)
        crashed = self.path("suffix-crash.jsonl")
        write_file(crashed, [*TUMBLING_EVENTS[:3], BAD_LINE, *TUMBLING_EVENTS[4:]])
        out, cp = self.path("suffix.out"), self.path("suffix.cp")
        code, _, err = run_cli(
            ["run", "--input", crashed, "--output", out, "--checkpoint", cp,
             "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual((json.loads(err)["error"], json.loads(err)["line"]), ("parse_error", 4))
        # Repairing and resuming reaches the same result as a plain run.
        code, _, err = run_cli(
            ["run", "--input", input_path, "--output", out, "--resume", cp,
             "--window", "tumbling:100"]
        )
        self.assertEqual((code, err), (EXIT_OK, ""), err)
        code, plain, _ = run_cli(["run", "--input", input_path, "--window", "tumbling:100"])
        self.assertEqual(read_bytes(out), plain.encode("utf-8"))

    # -- failure boundaries stay intact ----------------------------------------------------------

    def test_failed_plain_run_preserves_an_existing_output_file(self) -> None:
        good = self.path("good.jsonl")
        write_file(good, [data(10, "a", 1.0), punct(500)])
        target = self.path("preexisting.out")
        code, _, err = run_cli(["run", "--input", good, "--output", target, "--window", "tumbling:100"])
        self.assertEqual((code, err), (EXIT_OK, ""), err)
        before = read_bytes(target)

        broken = self.path("broken.jsonl")
        write_file(broken, [data(10, "a", 1.0), '{"oops":', data(30, "a", 3.0)])
        code, _, err = run_cli(["run", "--input", broken, "--output", target, "--window", "tumbling:100"])
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "parse_error")
        self.assertEqual(read_bytes(target), before, "a failed plain run must not touch the output")

    def test_checkpoint_parse_error_commits_nothing_and_holds_last_good_line(self) -> None:
        crashed = self.path("hold.jsonl")
        write_file(crashed, [*TUMBLING_EVENTS[:5], BAD_LINE])
        out, cp = self.path("hold.out"), self.path("hold.cp")
        code, _, err = run_cli(
            ["run", "--input", crashed, "--output", out, "--checkpoint", cp,
             "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["line"], 6)
        self.assertFalse(os.path.exists(out), "final results are committed only on success")
        running = json.loads(read_bytes(cp))
        self.assertEqual((running["status"], running["consumed"]), ("running", 5))
        # Records already emitted are buffered once, in emission order.
        self.assertEqual(
            [(json.loads(x)["window"]["start"], json.loads(x)["key"]) for x in running["pending"]],
            [(0, "alpha"), (0, "beta")],
        )

    def test_failed_resume_validation_changes_neither_output_nor_checkpoint(self) -> None:
        input_path = self.path("guard.jsonl")
        write_file(input_path, TUMBLING_EVENTS)
        crashed = self.path("guard-crash.jsonl")
        write_file(crashed, [*TUMBLING_EVENTS[:3], BAD_LINE])
        out, cp = self.path("guard.out"), self.path("guard.cp")
        code, _, err = run_cli(
            ["run", "--input", crashed, "--output", out, "--checkpoint", cp,
             "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_ERROR, err)
        checkpoint_before = read_bytes(cp)

        def reject(altered_input: str, label: str, extra: list[str] | None = None) -> None:
            attempted_out = self.path(f"guard-{label}.out")
            argv = ["run", "--input", altered_input, "--output", attempted_out,
                    "--resume", cp, "--window", "tumbling:100"]
            if extra:
                argv.extend(extra)
            code, _, err = run_cli(argv)
            self.assertEqual(code, EXIT_ERROR, label)
            self.assertEqual(json.loads(err)["error"], "validation_error", label)
            self.assertFalse(os.path.exists(attempted_out), label)
            self.assertEqual(read_bytes(cp), checkpoint_before, label)

        # A byte changed inside the consumed prefix.
        tampered = self.path("guard-tampered.jsonl")
        write_file(tampered, [data(11, "alpha", 1.0), *TUMBLING_EVENTS[1:]])
        reject(tampered, "tamper")
        # Offset beyond the current input.
        tiny = self.path("guard-tiny.jsonl")
        write_file(tiny, TUMBLING_EVENTS[:1])
        reject(tiny, "beyond")
        # Configuration conflict with the saved checkpoint.
        reject(input_path, "conflict", ["--aggregation", "count"])
        # Input grew after a *completed* checkpoint.
        complete_out = self.path("guard-complete.out")
        code, _, err = run_cli(
            ["run", "--input", input_path, "--output", complete_out, "--resume", cp,
             "--window", "tumbling:100"]
        )
        self.assertEqual((code, err), (EXIT_OK, ""), err)
        complete_cp_before = read_bytes(cp)
        grew = self.path("guard-grew.jsonl")
        write_file(grew, [*TUMBLING_EVENTS, data(1234, "gamma", 7.0)])
        checkpoint_before = complete_cp_before
        reject(grew, "grew")

    def test_resume_complete_is_idempotent_without_re_emitting(self) -> None:
        input_path = self.path("idem.jsonl")
        write_file(input_path, TUMBLING_EVENTS)
        out, cp = self.path("idem.out"), self.path("idem.cp")
        code, _, err = run_cli(
            ["run", "--input", input_path, "--output", out, "--checkpoint", cp,
             "--window", "tumbling:100"]
        )
        self.assertEqual((code, err), (EXIT_OK, ""), err)
        result_before, cp_before = read_bytes(out), read_bytes(cp)
        for repetition in range(2):
            again = self.path(f"idem-again-{repetition}.out")
            code, _, err = run_cli(
                ["run", "--input", input_path, "--output", again, "--resume", cp,
                 "--window", "tumbling:100"]
            )
            self.assertEqual((code, err), (EXIT_OK, ""), err)
            self.assertEqual(read_bytes(again), result_before)
        self.assertEqual(read_bytes(cp), cp_before, "complete marker must be refreshed identically")


if __name__ == "__main__":
    unittest.main()
