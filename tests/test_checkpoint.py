"""Product-grade durable checkpoint / resume contract for ``run``.

Unlike ``test_recovery`` (which simulates checkpoints test-side by replaying prefixes), these tests
exercise the real durable feature end to end through the public CLI: ``--checkpoint`` writes a
versioned single-line JSON document after every successfully processed line and ``--resume``
verifies the consumed prefix and continues. The headline invariant is checked at *every* possible
interruption offset and over tumbling/sliding/session windows, every aggregation and non-default
lateness/out-of-orderness:

    resumed output == an uninterrupted run under the same configuration, byte for byte.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest

from stream_processing.checkpoint import (
    CHECKPOINT_FORMAT,
    CHECKPOINT_VERSION,
    STATUS_COMPLETED,
    STATUS_RUNNING,
    RunConfig,
    canonical,
    read_checkpoint,
    split_input,
)
from stream_processing.cli import EXIT_ERROR, EXIT_OK, main
from stream_processing.errors import ParseError

WINDOW = "tumbling:100"

FIXTURE = [
    '{"timestamp":10,"key":"alpha","value":1.0}',
    '{"timestamp":20,"key":"beta","value":10.0}',
    '{"timestamp":300,"key":"clock","kind":"punct"}',  # emits [0,100) for both keys
    '{"timestamp":410,"key":"alpha","value":2.0}',
    '{"timestamp":420,"key":"beta","value":20.0}',
    '{"timestamp":430,"key":"alpha","value":4.0}',
    '{"timestamp":500,"key":"clock","kind":"punct"}',  # emits [400,500) after a mid-stream barrier
    '{"timestamp":710,"key":"alpha","value":8.0}',
    '{"timestamp":720,"key":"beta","value":80.0}',
    '{"timestamp":800,"key":"clock","kind":"punct"}',  # emits [700,800)
    '{"timestamp":910,"key":"alpha","value":16.0}',
    '{"timestamp":920,"key":"beta","value":160.0}',  # [900,1000) only via the final flush
]

BAD_LINE = "{this is not json"


def run_cli(argv: list[str]) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = main(argv)
    return code, out.getvalue(), err.getvalue()


class CheckpointHarness:
    """A temp workspace plus the operations a checkpointed run needs."""

    def __init__(self, events: list[str] | None = None) -> None:
        self._directory = tempfile.TemporaryDirectory()
        directory = self._directory.name
        self.input = os.path.join(directory, "events.jsonl")
        self.output = os.path.join(directory, "results.jsonl")
        self.checkpoint = os.path.join(directory, "state.ckpt")
        if events is not None:
            self.write_input(events)

    def cleanup(self) -> None:
        self._directory.cleanup()

    def path(self, name: str) -> str:
        return os.path.join(self._directory.name, name)

    def write_input(self, lines: list[str], *, newline: str = "\n") -> None:
        with open(self.input, "w", encoding="utf-8", newline="") as handle:
            handle.write((newline.join(lines) + newline) if lines else "")

    def reset_durable(self) -> None:
        for path in (self.checkpoint, self.output):
            if os.path.exists(path):
                os.remove(path)

    def write_input_bytes(self, data: bytes) -> None:
        with open(self.input, "wb") as handle:
            handle.write(data)

    def read_output(self) -> bytes:
        with open(self.output, "rb") as handle:
            return handle.read()

    def read_checkpoint_text(self) -> str:
        with open(self.checkpoint, encoding="utf-8") as handle:
            return handle.read()

    def checkpoint_doc(self) -> dict:
        return json.loads(self.read_checkpoint_text())

    def base_argv(self, **overrides: object) -> list[str]:
        argv = [
            "run",
            "--input",
            self.input,
            "--output",
            self.output,
            "--window",
            str(overrides.get("window", WINDOW)),
            "--aggregation",
            str(overrides.get("aggregation", "sum")),
            "--allowed-lateness",
            str(overrides.get("allowed_lateness", 0)),
            "--max-out-of-orderness",
            str(overrides.get("max_out_of_orderness", 0)),
        ]
        return argv

    def run_reference(self, **overrides: object) -> bytes:
        """An ordinary, uninterrupted run (no checkpoint options)."""
        code, _, err = run_cli(self.base_argv(**overrides))
        assert code == EXIT_OK, err
        return self.read_output()

    def stall_after(self, consumed: int, **overrides: object) -> None:
        """Produce a real RUNNING checkpoint parked after ``consumed`` good lines.

        A malformed line immediately after the prefix makes the engine stop exactly there: the
        checkpoint captures the good prefix while the result file is never created.
        """
        events = FIXTURE[:consumed] + [BAD_LINE] + FIXTURE[consumed:]
        self.write_input(events)
        self.reset_durable()
        code, _, err = run_cli([*self.base_argv(**overrides), "--checkpoint", self.checkpoint])
        document = json.loads(err)
        assert code == EXIT_ERROR and document["error"] == "parse_error", err
        assert document["line"] == consumed + 1

    def resume(self, **overrides: object) -> tuple[int, str, str]:
        return run_cli([*self.base_argv(**overrides), "--resume", self.checkpoint])


class ByteIdentityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = CheckpointHarness(FIXTURE)

    def tearDown(self) -> None:
        self.harness.cleanup()

    def _assert_identity_at_every_split(self, **overrides: object) -> None:
        reference = self.harness.run_reference(**overrides)
        self.assertGreater(len(reference), 0)
        for consumed in range(1, len(FIXTURE) + 1):
            # Fresh running checkpoint parked after `consumed` lines, then repair and resume.
            self.harness.stall_after(consumed, **overrides)
            self.harness.write_input(FIXTURE)
            code, _, err = self.harness.resume(**overrides)
            self.assertEqual(code, EXIT_OK, f"split {consumed}: {err}")
            self.assertEqual(
                self.harness.read_output(),
                reference,
                f"split {consumed} diverged from an uninterrupted run",
            )

    def test_tumbling_sum_is_byte_identical_at_every_barrier(self) -> None:
        # Barriers here land on data events, punct advances, window emissions and the final flush.
        self._assert_identity_at_every_split()

    def test_sliding_windows_resume_byte_identically(self) -> None:
        self._assert_identity_at_every_split(window="sliding:300:100")

    def test_session_merges_resume_byte_identically(self) -> None:
        self._assert_identity_at_every_split(window="session:100")

    def test_every_aggregation_resumes_byte_identically(self) -> None:
        for aggregation in ("count", "sum", "min", "max", "mean"):
            self._assert_identity_at_every_split(aggregation=aggregation)

    def test_lateness_and_out_of_orderness_resume_byte_identically(self) -> None:
        self._assert_identity_at_every_split(
            window="tumbling:100", allowed_lateness=100, max_out_of_orderness=50
        )

    def test_crlf_input_uses_byte_exact_prefix_boundaries(self) -> None:
        # The digest must cover exact bytes, including CRLF terminators; splitlines must agree.
        reference = self.harness.run_reference()
        consumed = 5
        self.harness.write_input(FIXTURE[:consumed] + [BAD_LINE] + FIXTURE[consumed:], newline="\r\n")
        code, _, err = run_cli([*self.harness.base_argv(), "--checkpoint", self.harness.checkpoint])
        self.assertEqual(code, EXIT_ERROR, err)
        self.assertEqual(self.harness.checkpoint_doc()["consumed"], consumed)
        self.harness.write_input(FIXTURE, newline="\r\n")
        code, _, err = self.harness.resume()
        self.assertEqual(code, EXIT_OK, err)
        self.assertEqual(self.harness.read_output(), reference)

    def test_multi_segment_resume_converges(self) -> None:
        # Fail, resume a little, fail again, resume to the end: every segment must stay coherent.
        reference = self.harness.run_reference()
        first, second = 4, 8
        self.harness.stall_after(first)
        self.harness.write_input(FIXTURE[:second] + [BAD_LINE] + FIXTURE[second:])
        code, _, err = self.harness.resume()
        self.assertEqual(code, EXIT_ERROR, err)
        self.assertEqual(self.harness.checkpoint_doc()["consumed"], second)
        self.assertFalse(os.path.exists(self.harness.output))
        self.harness.write_input(FIXTURE)
        code, _, err = self.harness.resume()
        self.assertEqual(code, EXIT_OK, err)
        self.assertEqual(self.harness.read_output(), reference)


class CheckpointDocumentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = CheckpointHarness(FIXTURE)

    def tearDown(self) -> None:
        self.harness.cleanup()

    def test_checkpoint_is_one_canonical_json_line(self) -> None:
        self.harness.stall_after(3)
        raw = self.harness.read_checkpoint_text()
        self.assertTrue(raw.endswith("\n"))
        self.assertEqual(raw.count("\n"), 1, "checkpoint must be a single line")
        document = json.loads(raw)
        self.assertEqual(raw, canonical(document) + "\n")
        self.assertEqual(document["format"], CHECKPOINT_FORMAT)
        self.assertEqual(document["version"], CHECKPOINT_VERSION)
        self.assertEqual(document["status"], STATUS_RUNNING)
        self.assertEqual(document["consumed"], 3)

    def test_checkpoint_captures_config_state_watermark_and_emitted_ledger(self) -> None:
        self.harness.stall_after(3, window="tumbling:100", allowed_lateness=7, max_out_of_orderness=3)
        document = self.harness.checkpoint_doc()
        self.assertEqual(
            document["config"],
            {"window": "tumbling:100", "aggregation": "sum", "allowedLateness": 7, "maxOutOfOrderness": 3},
        )
        self.assertRegex(document["prefixDigest"], r"^[0-9a-f]{64}$")
        watermark = document["state"]["watermark"]
        self.assertEqual(watermark["maxOutOfOrderness"], 3)
        self.assertEqual(watermark["maxSeen"], 300)  # advanced by the punctuation
        self.assertEqual(watermark["lateDropped"], 0)
        self.assertEqual(watermark["observed"], 2)  # only data events are "observed"
        # Two keyed [0,100) windows were already emitted at wm 300 and live in the ledger.
        self.assertEqual(len(document["emitted"]), 2)
        emitted_keys = [(json.loads(line)["window"]["start"], json.loads(line)["key"]) for line in document["emitted"]]
        self.assertEqual(emitted_keys, [(0, "alpha"), (0, "beta")])

    def test_prefix_digest_covers_exact_consumed_bytes(self) -> None:
        self.harness.stall_after(2)
        document = self.harness.checkpoint_doc()
        with open(self.harness.input, "rb") as handle:
            data = handle.read()
        _, offsets = split_input(data)
        import hashlib

        self.assertEqual(
            document["prefixDigest"], hashlib.sha256(data[: offsets[1]]).hexdigest()
        )

    def test_completed_checkpoint_is_left_after_success_and_is_resumable(self) -> None:
        reference = self.harness.run_reference()
        code, _, err = run_cli([*self.harness.base_argv(), "--checkpoint", self.harness.checkpoint])
        self.assertEqual(code, EXIT_OK, err)
        document = self.harness.checkpoint_doc()
        self.assertEqual(document["status"], STATUS_COMPLETED)
        self.assertEqual(document["consumed"], len(FIXTURE))
        self.assertEqual(self.harness.read_output(), reference)
        # Resuming a completed checkpoint is idempotent and reproduces the same bytes.
        code, _, err = self.harness.resume()
        self.assertEqual(code, EXIT_OK, err)
        self.assertEqual(self.harness.read_output(), reference)
        self.assertEqual(self.harness.checkpoint_doc()["status"], STATUS_COMPLETED)

    def test_empty_input_completes_with_zero_consumed(self) -> None:
        self.harness.write_input([])
        code, _, err = run_cli([*self.harness.base_argv(), "--checkpoint", self.harness.checkpoint])
        self.assertEqual(code, EXIT_OK, err)
        self.assertEqual(self.harness.read_output(), b"")
        document = self.harness.checkpoint_doc()
        self.assertEqual(document["status"], STATUS_COMPLETED)
        self.assertEqual(document["consumed"], 0)
        code, _, err = self.harness.resume()
        self.assertEqual(code, EXIT_OK, err)
        self.assertEqual(self.harness.read_output(), b"")


class FailureSemanticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = CheckpointHarness(FIXTURE)

    def tearDown(self) -> None:
        self.harness.cleanup()

    def _err(self, argv: list[str]) -> dict:
        code, _, err = run_cli(argv)
        self.assertEqual(code, EXIT_ERROR)
        return json.loads(err)

    def test_checkpoint_requires_regular_input_and_file_output(self) -> None:
        base = ["run", "--window", WINDOW]
        for argv, message in (
            (["run", "--input", "-", "--checkpoint", "x", "--output", self.harness.output], "stdin+checkpoint"),
            (["run", "--input", "-", "--resume", "x", "--output", self.harness.output], "stdin+resume"),
            (["run", "--input", self.harness.input, "--checkpoint", "x"], "checkpoint+stdout"),
            (["run", "--input", self.harness.input, "--checkpoint", "x", "--output", "-"], "checkpoint+dash"),
            (["run", "--input", self.harness.input, "--resume", "x"], "resume+stdout"),
        ):
            document = self._err(argv)
            self.assertEqual(document["error"], "validation_error", message)

    def test_checkpoint_and_resume_are_mutually_exclusive(self) -> None:
        document = self._err(
            [
                "run",
                "--input",
                self.harness.input,
                "--output",
                self.harness.output,
                "--checkpoint",
                self.harness.path("a"),
                "--resume",
                self.harness.path("b"),
            ]
        )
        self.assertEqual(document["error"], "validation_error")

    def test_checkpoint_path_must_differ_from_input_and_output(self) -> None:
        document = self._err(
            ["run", "--input", self.harness.input, "--output", self.harness.output,
             "--checkpoint", self.harness.output]
        )
        self.assertEqual(document["error"], "validation_error")
        document = self._err(
            ["run", "--input", self.harness.input, "--output", self.harness.output,
             "--checkpoint", self.harness.input]
        )
        self.assertEqual(document["error"], "validation_error")

    def test_fresh_checkpoint_refuses_to_clobber_an_existing_file(self) -> None:
        with open(self.harness.checkpoint, "w", encoding="utf-8") as handle:
            handle.write("{}\n")
        document = self._err(
            ["run", "--input", self.harness.input, "--output", self.harness.output,
             "--checkpoint", self.harness.checkpoint]
        )
        self.assertEqual(document["error"], "validation_error")
        with open(self.harness.checkpoint, encoding="utf-8") as handle:
            self.assertEqual(handle.read(), "{}\n")

    def test_unwritable_checkpoint_is_an_output_error(self) -> None:
        document = self._err(
            ["run", "--input", self.harness.input, "--output", self.harness.output,
             "--checkpoint", os.path.join(self.harness.path("no-such-dir"), "c")]
        )
        self.assertEqual(document["error"], "output_error")

    def test_unreadable_checkpoint_is_an_output_error(self) -> None:
        document = self._err(
            ["run", "--input", self.harness.input, "--output", self.harness.output,
             "--resume", self.harness.path("missing.ckpt")]
        )
        self.assertEqual(document["error"], "output_error")

    def test_malformed_checkpoint_json_is_a_parse_error_with_position(self) -> None:
        with open(self.harness.checkpoint, "w", encoding="utf-8") as handle:
            handle.write('{"format":"x"\n{not json')
        document = self._err(
            ["run", "--input", self.harness.input, "--output", self.harness.output,
             "--resume", self.harness.checkpoint]
        )
        self.assertEqual(document["error"], "parse_error")
        self.assertEqual(document["line"], 2)
        self.assertIn("column", document)

    def test_unknown_format_bad_version_and_bad_shape_are_validation_errors(self) -> None:
        cases = [
            canonical({"format": "else", "version": 1}) + "\n",
            canonical({"format": CHECKPOINT_FORMAT, "version": 999}) + "\n",
            canonical({"format": CHECKPOINT_FORMAT, "version": "1"}) + "\n",
            "[1,2,3]\n",
        ]
        for payload in cases:
            with open(self.harness.checkpoint, "w", encoding="utf-8") as handle:
                handle.write(payload)
            document = self._err(
                ["run", "--input", self.harness.input, "--output", self.harness.output,
                 "--resume", self.harness.checkpoint]
            )
            self.assertEqual(document["error"], "validation_error", payload)

    def test_field_and_type_violations_are_validation_errors(self) -> None:
        self.harness.stall_after(3)
        good = json.loads(self.harness.read_checkpoint_text())
        # (mutator, label)
        def replace_state_bad_watermark(doc: dict) -> None:
            doc["state"]["watermark"] = []

        mutations = [
            lambda d: d.pop("consumed"),
            lambda d: d.update(consumed=-1),
            lambda d: d.update(consumed="3"),
            lambda d: d.update(consumed=True),
            lambda d: d.update(status="paused"),
            lambda d: d.update(prefixDigest=123),
            lambda d: d.update(extra=1),
            lambda d: d.update(config={"window": "tumbling:100"}),
            lambda d: d.update(emitted=["ok", 3]),
            replace_state_bad_watermark,
        ]
        for mutate in mutations:
            document = json.loads(json.dumps(good))
            mutate(document)
            with open(self.harness.checkpoint, "w", encoding="utf-8") as handle:
                handle.write(canonical(document) + "\n")
            reported = self._err(
                ["run", "--input", self.harness.input, "--output", self.harness.output,
                 "--resume", self.harness.checkpoint]
            )
            self.assertEqual(reported["error"], "validation_error", mutate)

    def test_resume_configuration_conflicts_are_rejected_not_applied(self) -> None:
        self.harness.stall_after(
            3, window="tumbling:100", aggregation="sum", allowed_lateness=5, max_out_of_orderness=7
        )
        checkpoint_before = self.harness.read_checkpoint_text()
        sentinel = "DO-NOT-TOUCH\n"
        with open(self.harness.output, "w", encoding="utf-8") as handle:
            handle.write(sentinel)
        self.harness.write_input(FIXTURE)
        for flag, value in (
            ("--window", "tumbling:50"),
            ("--aggregation", "count"),
            ("--allowed-lateness", "6"),
            ("--max-out-of-orderness", "8"),
        ):
            document = self._err(
                ["run", "--input", self.harness.input, "--output", self.harness.output,
                 "--resume", self.harness.checkpoint, flag, value]
            )
            self.assertEqual(document["error"], "validation_error", flag)
            self.assertEqual(self.harness.read_checkpoint_text(), checkpoint_before)
            self.assertEqual(self.harness.read_output(), sentinel.encode("utf-8"))

    def test_identical_explicit_config_and_omitted_config_are_accepted(self) -> None:
        self.harness.stall_after(3)
        self.harness.write_input(FIXTURE)
        reference = self.harness.run_reference()
        code, _, err = run_cli(
            ["run", "--input", self.harness.input, "--output", self.harness.output,
             "--resume", self.harness.checkpoint]
        )
        self.assertEqual(code, EXIT_OK, err)
        self.assertEqual(self.harness.read_output(), reference)

    def test_consumed_prefix_tamper_is_rejected_and_leaves_artifacts_untouched(self) -> None:
        self.harness.stall_after(4)
        checkpoint_before = self.harness.read_checkpoint_text()
        sentinel = b"KEEP\n"
        with open(self.harness.output, "wb") as handle:
            handle.write(sentinel)
        # Change a byte strictly inside the consumed prefix (line 1) while keeping line count.
        tampered = [FIXTURE[0].replace('"value":1.0', '"value":9.0'), *FIXTURE[1:]]
        self.harness.write_input(tampered)
        document = self._err(
            ["run", "--input", self.harness.input, "--output", self.harness.output,
             "--resume", self.harness.checkpoint]
        )
        self.assertEqual(document["error"], "validation_error")
        self.assertIn("prefix", document["message"])
        self.assertEqual(self.harness.read_checkpoint_text(), checkpoint_before)
        self.assertEqual(self.harness.read_output(), sentinel)

    def test_offset_beyond_current_input_is_a_validation_error(self) -> None:
        self.harness.stall_after(5)
        self.harness.write_input(FIXTURE[:2])  # fewer lines than the checkpoint consumed
        document = self._err(
            ["run", "--input", self.harness.input, "--output", self.harness.output,
             "--resume", self.harness.checkpoint]
        )
        self.assertEqual(document["error"], "validation_error")

    def test_parse_error_on_continuation_parks_the_checkpoint_and_keeps_output(self) -> None:
        # A running checkpoint after three good lines, with a pre-existing output on disk that the
        # failed continuation must not touch.
        self.harness.stall_after(3)
        sentinel = b"PRE-EXISTING OUTPUT\n"
        with open(self.harness.output, "wb") as handle:
            handle.write(sentinel)
        parked = self.harness.read_checkpoint_text()
        # Next line (4) is malformed; lines beyond it are never reached.
        self.harness.write_input(FIXTURE[:3] + [BAD_LINE] + FIXTURE[4:])
        document = self._err(
            ["run", "--input", self.harness.input, "--output", self.harness.output,
             "--resume", self.harness.checkpoint]
        )
        self.assertEqual(document["error"], "parse_error")
        self.assertEqual(document["line"], 4)
        # The checkpoint stays parked at the last successful line and the output is untouched.
        self.assertEqual(self.harness.checkpoint_doc()["consumed"], 3)
        self.assertEqual(self.harness.read_checkpoint_text(), parked)
        self.assertEqual(self.harness.read_output(), sentinel)
        # Repairing line 4 then resumes cleanly to the byte-identical full result.
        self.harness.write_input(FIXTURE)
        code, _, err = self.harness.resume()
        self.assertEqual(code, EXIT_OK, err)
        self.assertEqual(self.harness.read_output(), self.harness.run_reference())

    def test_validation_failures_do_not_create_output(self) -> None:
        self.harness.stall_after(3)
        self.assertFalse(os.path.exists(self.harness.output))
        self.harness.write_input(FIXTURE[:1])  # offset beyond input
        self._err(
            ["run", "--input", self.harness.input, "--output", self.harness.output,
             "--resume", self.harness.checkpoint]
        )
        self.assertFalse(os.path.exists(self.harness.output))


class LegacyAndDescribeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = CheckpointHarness(FIXTURE)

    def tearDown(self) -> None:
        self.harness.cleanup()

    def test_run_without_checkpoint_options_is_unchanged(self) -> None:
        # stdin -> stdout still works exactly as before.
        import sys

        payload = "\n".join(FIXTURE) + "\n"
        out, err = io.StringIO(), io.StringIO()
        original_stdin = sys.stdin
        sys.stdin = io.StringIO(payload)
        try:
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = main(["run", "--input", "-", "--window", WINDOW])
        finally:
            sys.stdin = original_stdin
        self.assertEqual((code, err.getvalue()), (EXIT_OK, ""))
        file_reference = self.harness.run_reference()
        self.assertEqual(out.getvalue().encode("utf-8"), file_reference)

    def test_describe_advertises_checkpoint_version_and_resume_support(self) -> None:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main(["describe"])
        self.assertEqual((code, err.getvalue()), (EXIT_OK, ""))
        document = json.loads(out.getvalue())
        # Every original field is retained...
        for field_name in (
            "name", "version", "aggregations", "windowings", "eventFields",
            "eventKinds", "timestampUnit", "exitCodes",
        ):
            self.assertIn(field_name, document)
        # ...and the checkpoint contract is now public.
        self.assertEqual(
            document["checkpoint"],
            {
                "format": CHECKPOINT_FORMAT,
                "version": CHECKPOINT_VERSION,
                "resumeSupported": True,
                "options": ["run --checkpoint <path>", "run --resume <path>"],
            },
        )


class DirectCheckpointModuleTests(unittest.TestCase):
    def test_read_checkpoint_reports_parse_error_type_directly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "c.ckpt")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("{broken")
            with self.assertRaises(ParseError) as caught:
                read_checkpoint(path)
            self.assertIsNotNone(caught.exception.context.get("line"))
            self.assertIsNotNone(caught.exception.context.get("column"))

    def test_run_config_round_trips(self) -> None:
        config = RunConfig("session:250", "mean", 9, 4)
        self.assertEqual(RunConfig.from_document(config.to_document()), config)


if __name__ == "__main__":
    unittest.main()
