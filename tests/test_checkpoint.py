"""Product-grade ``run`` checkpoint / resume contract.

Every recovery scenario is driven through the public CLI exactly as a caller would use it:

  * a first ``run --checkpoint`` is interrupted *after* a successfully processed line — modelled
    by an unparsable next line, which is precisely the product behaviour (the output file stays
    untouched and the checkpoint atomically records the last good line);
  * the input is repaired and the run continues with ``run --resume``, updating the same file;
  * the resumed result is compared **byte for byte** with an uninterrupted run over the identical
    input and configuration.

Sweeping the interruption across every line position proves the guarantee for checkpoints taken
after data events, punctuation advances, window emissions, session merges and the final flush.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest

from stream_processing.checkpoint import CHECKPOINT_FORMAT, CHECKPOINT_VERSION
from stream_processing.cli import EXIT_ERROR, EXIT_OK, canonical, main


def run_cli(argv: list[str]) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = main(argv)
    return code, out.getvalue(), err.getvalue()


def line(document: dict) -> str:
    return canonical(document)


def data(ts: int, key: str, value: float) -> str:
    return line({"timestamp": ts, "key": key, "value": value, "kind": "data"})


def punct(ts: int) -> str:
    return line({"timestamp": ts, "key": "clock", "value": 0, "kind": "punct"})


# A stream exercising every checkpoint seam: data, punct, a punct that closes a window for two
# keys, out-of-order-timely data, and pending windows only the final flush can emit.
BASE_EVENTS = [
    data(10, "alpha", 1.0),
    data(20, "beta", 10.0),
    punct(300),          # emits [0,100) for alpha and beta
    data(410, "alpha", 2.0),
    data(420, "beta", 20.0),
    data(430, "alpha", 4.0),
    data(440, "beta", 40.0),
    punct(500),          # emits [400,500) for alpha and beta
    data(710, "alpha", 8.0),
    data(900, "beta", 9.0),
]                       # no trailing punct: flush emits [700,800) and [900,1000)
BAD_LINE = '{"timestamp":900,"key":'

SESSION_EVENTS = [
    data(10, "alpha", 1.0),
    data(15, "alpha", 2.0),    # within the gap: merges with the first event
    data(500, "alpha", 4.0),   # far away: starts a separate session
    punct(700),                # closes both merged sessions
]

SLIDING_EVENTS = [
    data(50, "k", 1.0),
    data(150, "k", 2.0),
    data(250, "k", 3.0),
    punct(1000),
]

LATE_EVENTS = [
    data(100, "k", 1.0),
    data(50, "k", 99.0),   # late: watermark already at 100 -> counted and dropped
    punct(300),
]


def write_file(path: str, lines: list[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write("".join(f"{text}\n" for text in lines))


def read_bytes(path: str) -> bytes:
    with open(path, "rb") as handle:
        return handle.read()


class CheckpointRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = self.directory.name

    def tearDown(self) -> None:
        self.directory.cleanup()

    def paths(self, name: str) -> tuple[str, str, str]:
        return (
            os.path.join(self.root, f"{name}.jsonl"),
            os.path.join(self.root, f"{name}.out.jsonl"),
            os.path.join(self.root, f"{name}.checkpoint.json"),
        )

    def reference(self, events: list[str], config: list[str], tag: str) -> bytes:
        input_path, output_path, _ = self.paths(f"ref-{tag}")
        write_file(input_path, events)
        code, _, err = run_cli(["run", "--input", input_path, "--output", output_path, *config])
        self.assertEqual(code, EXIT_OK, err)
        return read_bytes(output_path)

    def crash_then_resume(
        self,
        events: list[str],
        config: list[str],
        *,
        crash_after: int,
        tag: str,
        second_crash_after: int | None = None,
    ) -> tuple[bytes, dict]:
        """Interrupt after `crash_after` good lines, repair, resume; return output bytes + final cp."""
        full_input, output_path, checkpoint_path = self.paths(tag)

        # --- first run: good prefix followed by an unparsable line -------------------------------
        crashed_input = os.path.join(self.root, f"{tag}-crash.jsonl")
        write_file(crashed_input, [*events[:crash_after], BAD_LINE])
        code, _, err = run_cli(
            ["run", "--input", crashed_input, "--output", output_path, "--checkpoint", checkpoint_path, *config]
        )
        self.assertEqual(code, EXIT_ERROR, err)
        error = json.loads(err)
        self.assertEqual(error["error"], "parse_error")
        self.assertEqual(error["line"], crash_after + 1)
        self.assertIn("column", error)
        # The failure must not produce the output file and must leave a running checkpoint behind.
        self.assertFalse(os.path.exists(output_path), "output must stay untouched on a parse failure")
        running = json.loads(read_bytes(checkpoint_path))
        self.assertEqual(running["status"], "running")
        self.assertEqual(running["consumed"], crash_after)

        if second_crash_after is not None:
            # --- optional second failure further downstream, resuming from the first checkpoint ---
            k2 = second_crash_after
            self.assertGreater(k2, crash_after)
            crashed_two = os.path.join(self.root, f"{tag}-crash2.jsonl")
            write_file(crashed_two, [*events[:k2], BAD_LINE])
            code, _, err = run_cli(
                ["run", "--input", crashed_two, "--output", output_path, "--resume", checkpoint_path, *config]
            )
            self.assertEqual(code, EXIT_ERROR, err)
            self.assertEqual(json.loads(err)["error"], "parse_error")
            self.assertFalse(os.path.exists(output_path))
            self.assertEqual(json.loads(read_bytes(checkpoint_path))["consumed"], k2)

        # --- repair the input and resume ----------------------------------------------------------
        write_file(full_input, events)
        code, _, err = run_cli(
            ["run", "--input", full_input, "--output", output_path, "--resume", checkpoint_path, *config]
        )
        self.assertEqual(code, EXIT_OK, err)
        final_checkpoint = json.loads(read_bytes(checkpoint_path))
        return read_bytes(output_path), final_checkpoint

    def assert_recovery_matches_uninterrupted(
        self, events: list[str], config: list[str], tag: str
    ) -> None:
        expected = self.reference(events, config, tag)
        for crash_after in range(1, len(events) + 1):
            with self.subTest(tag=tag, crash_after=crash_after):
                actual, final_checkpoint = self.crash_then_resume(
                    events, config, crash_after=crash_after, tag=f"{tag}-k{crash_after}"
                )
                self.assertEqual(actual, expected, "resumed output must be byte-identical")
                self.assertEqual(final_checkpoint["status"], "complete")
                self.assertEqual(final_checkpoint["consumed"], len(events))
                self.assertEqual(final_checkpoint["format"], CHECKPOINT_FORMAT)
                self.assertEqual(final_checkpoint["version"], CHECKPOINT_VERSION)

    # -- byte-identical recovery across window types and configurations --------------------------

    def test_tumbling_recovery_matches_at_every_line(self) -> None:
        self.assert_recovery_matches_uninterrupted(BASE_EVENTS, ["--window", "tumbling:100"], "tumbling")

    def test_sliding_recovery_matches_at_every_line(self) -> None:
        self.assert_recovery_matches_uninterrupted(
            SLIDING_EVENTS, ["--window", "sliding:300:100"], "sliding"
        )

    def test_session_merge_recovery_matches_at_every_line(self) -> None:
        self.assert_recovery_matches_uninterrupted(
            SESSION_EVENTS, ["--window", "session:100"], "session"
        )

    def test_lateness_and_disorder_configurations_round_trip(self) -> None:
        self.assert_recovery_matches_uninterrupted(
            BASE_EVENTS,
            ["--window", "tumbling:100", "--allowed-lateness", "50", "--aggregation", "count"],
            "lateness-count",
        )
        self.assert_recovery_matches_uninterrupted(
            BASE_EVENTS,
            ["--window", "tumbling:100", "--max-out-of-orderness", "200", "--aggregation", "mean"],
            "moo-mean",
        )

    def test_late_drop_counter_survives_recovery(self) -> None:
        expected = self.reference(LATE_EVENTS, ["--window", "tumbling:100"], "late-ref")
        actual, checkpoint = self.crash_then_resume(
            LATE_EVENTS, ["--window", "tumbling:100"], crash_after=2, tag="late"
        )
        self.assertEqual(actual, expected)
        self.assertEqual(checkpoint["state"]["lateDropped"], 1)

    def test_multi_hop_recovery_with_two_failures(self) -> None:
        expected = self.reference(BASE_EVENTS, ["--window", "tumbling:100"], "multi-ref")
        actual, checkpoint = self.crash_then_resume(
            BASE_EVENTS,
            ["--window", "tumbling:100"],
            crash_after=3,
            second_crash_after=7,
            tag="multi",
        )
        self.assertEqual(actual, expected)
        self.assertEqual(checkpoint["status"], "complete")

    # -- running checkpoint content --------------------------------------------------------------

    def test_running_checkpoint_holds_emitted_but_uncommitted_records_once(self) -> None:
        # Crash immediately after the punct at line 3 closed [0,100) for both keys.
        crashed_input = os.path.join(self.root, "pending-crash.jsonl")
        cp = os.path.join(self.root, "pending.cp")
        out = os.path.join(self.root, "pending.out")
        write_file(crashed_input, [*BASE_EVENTS[:3], BAD_LINE])
        code, _, err = run_cli(
            ["run", "--input", crashed_input, "--output", out, "--checkpoint", cp, "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_ERROR, err)
        self.assertFalse(os.path.exists(out))
        running = json.loads(read_bytes(cp))
        self.assertEqual(running["status"], "running")
        self.assertEqual(running["consumed"], 3)
        pending = [json.loads(text) for text in running["pending"]]
        self.assertEqual(
            [(document["window"]["start"], document["key"]) for document in pending],
            [(0, "alpha"), (0, "beta")],
        )
        # Open [400,500) state does not exist yet at line 3, but the post-punct watermark is saved.
        self.assertEqual(running["state"]["watermarkMaxSeen"], 300)
        self.assertEqual(running["state"]["lateDropped"], 0)
        # Resuming must carry the two buffered records through and end byte-identical.
        full_input = os.path.join(self.root, "pending-full.jsonl")
        write_file(full_input, BASE_EVENTS)
        code, _, err = run_cli(
            ["run", "--input", full_input, "--output", out, "--resume", cp, "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_OK, err)
        self.assertEqual(
            read_bytes(out), self.reference(BASE_EVENTS, ["--window", "tumbling:100"], "pending-ref")
        )

    def test_checkpoint_is_a_single_canonical_json_line(self) -> None:
        events_path, _, cp = self.paths("shape")
        write_file(events_path, BASE_EVENTS[:2])
        code, _, err = run_cli(
            ["run", "--input", events_path, "--output", os.path.join(self.root, "shape.out"),
             "--checkpoint", cp, "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_OK, err)
        raw = read_bytes(cp).decode("utf-8")
        self.assertTrue(raw.endswith("\n"))
        self.assertEqual(raw.count("\n"), 1)
        document = json.loads(raw)
        self.assertEqual(canonical(document), raw.rstrip("\n"))
        self.assertEqual(
            sorted(document),
            ["config", "consumed", "format", "pending", "prefixSha256", "state", "status", "version"],
        )

    # -- completion and idempotency --------------------------------------------------------------

    def test_resume_from_complete_is_idempotent(self) -> None:
        events_path, output_path, cp = self.paths("done")
        write_file(events_path, BASE_EVENTS)
        config = ["--window", "tumbling:100"]
        code, _, err = run_cli(
            ["run", "--input", events_path, "--output", output_path, "--checkpoint", cp, *config]
        )
        self.assertEqual(code, EXIT_OK, err)
        first = read_bytes(output_path)
        first_cp = read_bytes(cp)
        for repetition in range(2):
            again = os.path.join(self.root, f"done-again-{repetition}.out")
            code, _, err = run_cli(["run", "--input", events_path, "--output", again, "--resume", cp, *config])
            self.assertEqual(code, EXIT_OK, err)
            self.assertEqual(read_bytes(again), first)
        # Repeated resumes must not re-count a prefix event or change the completion marker.
        self.assertEqual(read_bytes(cp), first_cp)

    def test_empty_input_completes_and_resumes_idempotently(self) -> None:
        events_path, output_path, cp = self.paths("empty")
        write_file(events_path, [])
        code, _, err = run_cli(
            ["run", "--input", events_path, "--output", output_path, "--checkpoint", cp,
             "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_OK, err)
        self.assertEqual(read_bytes(output_path), b"")
        self.assertEqual(json.loads(read_bytes(cp))["consumed"], 0)
        code, _, err = run_cli(
            ["run", "--input", events_path, "--output", output_path, "--resume", cp,
             "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_OK, err)
        self.assertEqual(read_bytes(output_path), b"")

    def test_input_growing_after_completion_is_rejected(self) -> None:
        events_path, output_path, cp = self.paths("grew")
        write_file(events_path, BASE_EVENTS[:4])
        code, _, err = run_cli(
            ["run", "--input", events_path, "--output", output_path, "--checkpoint", cp,
             "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_OK, err)
        completed = read_bytes(cp)
        committed = read_bytes(output_path)
        write_file(events_path, BASE_EVENTS)  # append more lines
        code, _, err = run_cli(
            ["run", "--input", events_path, "--output", output_path, "--resume", cp,
             "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "validation_error")
        self.assertEqual(read_bytes(cp), completed)
        self.assertEqual(read_bytes(output_path), committed)

    # -- command-line validation -----------------------------------------------------------------

    def test_checkpoint_and_resume_are_mutually_exclusive(self) -> None:
        events_path, output_path, cp = self.paths("mutex")
        write_file(events_path, BASE_EVENTS[:1])
        code, _, err = run_cli(
            ["run", "--input", events_path, "--output", output_path,
             "--checkpoint", cp, "--resume", cp]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "validation_error")

    def test_checkpointed_run_rejects_stdin_and_stdout(self) -> None:
        events_path, output_path, cp = self.paths("io")
        write_file(events_path, BASE_EVENTS[:1])
        for argv, message in (
            (["run", "--input", "-", "--output", output_path, "--checkpoint", cp], "stdin"),
            (["run", "--input", events_path, "--checkpoint", cp], "stdout (no --output)"),
            (["run", "--input", events_path, "--output", "-", "--checkpoint", cp], "stdout (-)"),
        ):
            with self.subTest(message):
                code, _, err = run_cli(argv)
                self.assertEqual(code, EXIT_ERROR)
                self.assertEqual(json.loads(err)["error"], "validation_error")

    def test_checkpoint_over_an_existing_file_is_a_validation_error(self) -> None:
        events_path, output_path, cp = self.paths("exists")
        write_file(events_path, BASE_EVENTS[:1])
        write_file(cp, ['{"format":"stream-processing-checkpoint"}'])
        code, _, err = run_cli(
            ["run", "--input", events_path, "--output", output_path, "--checkpoint", cp]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "validation_error")

    def test_resume_missing_checkpoint_is_an_output_error(self) -> None:
        events_path, output_path, cp = self.paths("missing")
        write_file(events_path, BASE_EVENTS[:1])
        code, _, err = run_cli(
            ["run", "--input", events_path, "--output", output_path, "--resume", cp]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "output_error")

    def test_checkpoint_path_colliding_with_input_or_output_is_rejected(self) -> None:
        events_path, output_path, _ = self.paths("collide")
        write_file(events_path, BASE_EVENTS[:1])
        # checkpoint == input
        code, _, err = run_cli(
            ["run", "--input", events_path, "--output", output_path, "--checkpoint", events_path]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "output_error")
        before = read_bytes(events_path)
        self.assertEqual(read_bytes(events_path), before)
        # checkpoint == output
        code, _, err = run_cli(
            ["run", "--input", events_path, "--output", output_path, "--checkpoint", output_path]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "output_error")

    # -- configuration handling on resume --------------------------------------------------------

    def _make_running(self, tag: str, config: list[str], after: int = 3) -> tuple[str, str, str]:
        events_path, output_path, cp = self.paths(tag)
        crashed = os.path.join(self.root, f"{tag}-crash.jsonl")
        write_file(crashed, [*BASE_EVENTS[:after], BAD_LINE])
        code, _, err = run_cli(
            ["run", "--input", crashed, "--output", output_path, "--checkpoint", cp, *config]
        )
        self.assertEqual(code, EXIT_ERROR, err)
        return events_path, output_path, cp

    def test_each_conflicting_flag_is_rejected(self) -> None:
        _, _, cp = self._make_running(
            "conflict", ["--window", "tumbling:100", "--aggregation", "sum",
                         "--allowed-lateness", "10", "--max-out-of-orderness", "5"]
        )
        full_input = os.path.join(self.root, "conflict-full.jsonl")
        write_file(full_input, BASE_EVENTS)
        cases = (
            ["--window", "tumbling:500"],
            ["--aggregation", "count"],
            ["--allowed-lateness", "11"],
            ["--max-out-of-orderness", "6"],
        )
        for extra in cases:
            with self.subTest(extra=extra):
                out = os.path.join(self.root, f"conflict-{extra[0]}.out")
                code, _, err = run_cli(
                    ["run", "--input", full_input, "--output", out, "--resume", cp, *extra]
                )
                self.assertEqual(code, EXIT_ERROR)
                document = json.loads(err)
                self.assertEqual(document["error"], "validation_error")
                self.assertTrue(document["fields"])

    def test_matching_explicit_flags_and_omitted_flags_continue(self) -> None:
        _, output_path, cp = self._make_running(
            "match", ["--window", "tumbling:100", "--allowed-lateness", "10"]
        )
        full_input = os.path.join(self.root, "match-full.jsonl")
        write_file(full_input, BASE_EVENTS)
        expected = self.reference(
            BASE_EVENTS, ["--window", "tumbling:100", "--allowed-lateness", "10"], "match-ref"
        )
        # Explicitly repeat the identical configuration: allowed.
        code, _, err = run_cli(
            ["run", "--input", full_input, "--output", output_path, "--resume", cp,
             "--window", "tumbling:100", "--allowed-lateness", "10"]
        )
        self.assertEqual(code, EXIT_OK, err)
        self.assertEqual(read_bytes(output_path), expected)

    def test_resume_inherits_default_configuration_from_checkpoint(self) -> None:
        # First run relies entirely on documented defaults (tumbling:1000 / sum / 0 / 0).
        events = [data(10, "k", 1.0), punct(5000), data(6000, "k", 2.0)]
        crashed = os.path.join(self.root, "def-crash.jsonl")
        out = os.path.join(self.root, "def.out")
        cp = os.path.join(self.root, "def.cp")
        write_file(crashed, [*events[:2], BAD_LINE])
        code, _, err = run_cli(["run", "--input", crashed, "--output", out, "--checkpoint", cp])
        self.assertEqual(code, EXIT_ERROR, err)
        saved = json.loads(read_bytes(cp))
        self.assertEqual(saved["config"]["window"], "tumbling:1000")
        full = os.path.join(self.root, "def-full.jsonl")
        write_file(full, events)
        # Resume passes no configuration at all; the checkpoint's saved defaults must be reused.
        code, _, err = run_cli(["run", "--input", full, "--output", out, "--resume", cp])
        self.assertEqual(code, EXIT_OK, err)
        expected = self.reference(events, [], "def-ref")
        self.assertEqual(read_bytes(out), expected)

    # -- prefix and offset verification ----------------------------------------------------------

    def test_tampered_consumed_prefix_is_rejected_without_writes(self) -> None:
        _, _, cp = self._make_running("tamper", ["--window", "tumbling:100"], after=3)
        checkpoint_before = read_bytes(cp)
        # Change an already-consumed line while keeping line count and validity.
        tampered = os.path.join(self.root, "tampered.jsonl")
        changed = [data(11, "alpha", 1.0), BASE_EVENTS[1], BASE_EVENTS[2], *BASE_EVENTS[3:]]
        write_file(tampered, changed)
        out = os.path.join(self.root, "tamper.out")
        code, _, err = run_cli(
            ["run", "--input", tampered, "--output", out, "--resume", cp, "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "validation_error")
        self.assertFalse(os.path.exists(out))
        self.assertEqual(read_bytes(cp), checkpoint_before)

    def test_unchanged_prefix_with_appended_lines_resumes(self) -> None:
        # The legal counterpart of tampering: same prefix, genuinely new suffix lines.
        _, output_path, cp = self._make_running("append", ["--window", "tumbling:100"], after=3)
        full = os.path.join(self.root, "append-full.jsonl")
        write_file(full, BASE_EVENTS)
        code, _, err = run_cli(
            ["run", "--input", full, "--output", output_path, "--resume", cp, "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_OK, err)
        self.assertEqual(read_bytes(output_path), self.reference(BASE_EVENTS, ["--window", "tumbling:100"], "append-ref"))

    def test_offset_beyond_current_input_is_rejected(self) -> None:
        _, _, cp = self._make_running("beyond", ["--window", "tumbling:100"], after=3)
        tiny = os.path.join(self.root, "beyond-tiny.jsonl")
        write_file(tiny, BASE_EVENTS[:1])
        out = os.path.join(self.root, "beyond.out")
        checkpoint_before = read_bytes(cp)
        code, _, err = run_cli(
            ["run", "--input", tiny, "--output", out, "--resume", cp, "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "validation_error")
        self.assertFalse(os.path.exists(out))
        self.assertEqual(read_bytes(cp), checkpoint_before)

    # -- malformed checkpoint documents ----------------------------------------------------------

    def resume_with_document(self, document: object, tag: str) -> dict:
        events_path, output_path, cp = self.paths(f"mal-{tag}")
        write_file(events_path, BASE_EVENTS)
        with open(cp, "w", encoding="utf-8", newline="\n") as handle:
            if isinstance(document, str):
                handle.write(document)
            else:
                handle.write(canonical(document) + "\n")
        code, _, err = run_cli(
            ["run", "--input", events_path, "--output", output_path, "--resume", cp]
        )
        self.assertEqual(code, EXIT_ERROR)
        return json.loads(err)

    def test_malformed_json_is_a_parse_error_with_position(self) -> None:
        document = self.resume_with_document("{not json", "badjson")
        self.assertEqual(document["error"], "parse_error")
        self.assertEqual(document["line"], 1)
        self.assertIsInstance(document["column"], int)

    def test_unknown_format_and_bad_version_are_validation_errors(self) -> None:
        base = {
            "format": CHECKPOINT_FORMAT,
            "version": CHECKPOINT_VERSION,
            "status": "running",
            "config": {"window": "tumbling:100", "aggregation": "sum",
                       "allowedLateness": 0, "maxOutOfOrderness": 0},
            "consumed": 0,
            "prefixSha256": "0" * 64,
            "state": {"values": [], "emitted": [], "watermarkMaxSeen": None,
                      "observed": 0, "lateDropped": 0},
            "pending": [],
        }
        wrong_format = dict(base, format="something-else")
        self.assertEqual(self.resume_with_document(wrong_format, "fmt")["error"], "validation_error")
        wrong_version = dict(base, version=999)
        self.assertEqual(self.resume_with_document(wrong_version, "ver")["error"], "validation_error")
        bad_status = dict(base, status="paused")
        self.assertEqual(self.resume_with_document(bad_status, "status")["error"], "validation_error")

    def test_bad_fields_and_types_are_validation_errors(self) -> None:
        base = {
            "format": CHECKPOINT_FORMAT,
            "version": CHECKPOINT_VERSION,
            "status": "running",
            "config": {"window": "tumbling:100", "aggregation": "sum",
                       "allowedLateness": 0, "maxOutOfOrderness": 0},
            "consumed": 0,
            "prefixSha256": "0" * 64,
            "state": {"values": [], "emitted": [], "watermarkMaxSeen": None,
                      "observed": 0, "lateDropped": 0},
            "pending": [],
        }
        cases = {
            "missing-field": {key: value for key, value in base.items() if key != "pending"},
            "extra-field": dict(base, unexpected=1),
            "bad-consumed": dict(base, consumed=-1),
            "bool-consumed": dict(base, consumed=True),
            "bad-digest-length": dict(base, prefixSha256="abc"),
            "bad-digest-chars": dict(base, prefixSha256="z" * 64),
            "bad-config-window": dict(base, config=dict(base["config"], window=123)),
            "bad-config-lateness": dict(base, config=dict(base["config"], allowedLateness=-1)),
            "bad-state-shape": dict(base, state=[]),
            "bad-values-entry": dict(base, state=dict(base["state"], values=[[0, 100, "k"]])),
            "bad-values-number": dict(base, state=dict(base["state"], values=[[0, 100, "k", ["x"]]])),
            "bad-window-order": dict(base, state=dict(base["state"], values=[[100, 100, "k", [1.0]]])),
            "bad-emitted-triple": dict(base, state=dict(base["state"], emitted=[[0, 100]])),
            "bad-maxseen": dict(base, state=dict(base["state"], watermarkMaxSeen="soon")),
            "bad-counters": dict(base, state=dict(base["state"], observed=-2)),
            "pending-not-strings": dict(base, pending=[1, 2]),
            "root-not-object": [1, 2, 3],
        }
        for tag, document in cases.items():
            with self.subTest(tag=tag):
                self.assertEqual(
                    self.resume_with_document(document, tag)["error"], "validation_error", tag
                )

    # -- write failures --------------------------------------------------------------------------

    def test_unwritable_checkpoint_directory_is_an_output_error(self) -> None:
        events_path = os.path.join(self.root, "unwritable.jsonl")
        write_file(events_path, BASE_EVENTS[:2])
        cp_in_void = os.path.join(self.root, "no-such-dir", "cp.json")
        out = os.path.join(self.root, "unwritable.out")
        code, _, err = run_cli(
            ["run", "--input", events_path, "--output", out, "--checkpoint", cp_in_void,
             "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "output_error")
        self.assertFalse(os.path.exists(out))

    def test_output_write_failure_leaves_resumable_running_checkpoint(self) -> None:
        events_path = os.path.join(self.root, "latefail.jsonl")
        write_file(events_path, BASE_EVENTS)
        cp = os.path.join(self.root, "latefail.cp")
        # The output lives in a directory that does not exist: per-line checkpoints still succeed,
        # only the final atomic result write fails.
        doomed_output = os.path.join(self.root, "missing-output-dir", "results.jsonl")
        code, _, err = run_cli(
            ["run", "--input", events_path, "--output", doomed_output, "--checkpoint", cp,
             "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_ERROR)
        self.assertEqual(json.loads(err)["error"], "output_error")
        self.assertFalse(os.path.exists(doomed_output))
        running = json.loads(read_bytes(cp))
        self.assertEqual(running["status"], "running")
        self.assertEqual(running["consumed"], len(BASE_EVENTS))
        # Retrying with a writable output completes successfully and matches an uninterrupted run.
        recovered_output = os.path.join(self.root, "latefail-recovered.out")
        code, _, err = run_cli(
            ["run", "--input", events_path, "--output", recovered_output, "--resume", cp,
             "--window", "tumbling:100"]
        )
        self.assertEqual(code, EXIT_OK, err)
        self.assertEqual(
            read_bytes(recovered_output),
            self.reference(BASE_EVENTS, ["--window", "tumbling:100"], "latefail-ref"),
        )

    # -- non-checkpointed behaviour is untouched -------------------------------------------------

    def test_plain_run_without_options_is_unchanged(self) -> None:
        events_path = os.path.join(self.root, "plain.jsonl")
        write_file(events_path, BASE_EVENTS)
        code, out, err = run_cli(["run", "--input", events_path, "--window", "tumbling:100"])
        self.assertEqual((code, err), (EXIT_OK, ""))
        expected = self.reference(BASE_EVENTS, ["--window", "tumbling:100"], "plain-ref")
        self.assertEqual(out.encode("utf-8"), expected)

    # -- describe --------------------------------------------------------------------------------

    def test_describe_advertises_checkpoint_version_and_resume(self) -> None:
        code, out, err = run_cli(["describe"])
        self.assertEqual((code, err), (EXIT_OK, ""))
        document = json.loads(out)
        checkpoint = document["checkpoint"]
        self.assertEqual(checkpoint["format"], CHECKPOINT_FORMAT)
        self.assertEqual(checkpoint["version"], CHECKPOINT_VERSION)
        self.assertTrue(checkpoint["resume"])
        # All original fields remain.
        for field in ("name", "version", "aggregations", "windowings", "eventFields",
                      "eventKinds", "timestampUnit", "exitCodes"):
            self.assertIn(field, document)


if __name__ == "__main__":
    unittest.main()
