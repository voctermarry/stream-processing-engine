"""Deterministic replay of keyed state and event-time timers across checkpoint recovery.

These tests never change windowing, late-data or emission rules. They constrain the existing
execution chain with repeatable evidence: the same finite event stream, driven by a controlled
event-time clock (explicit `punct` watermarks -- no real waiting, no threads, no timezone), must
produce exactly the same observable output when run continuously and when interrupted by a
successful checkpoint and resumed in a brand-new pipeline instance.

Comparison is position-wise over the output records in emission order. Nothing is sorted, deduped
or reduced to a final aggregate, so a duplicated write, a missing write or an order shift each show
up as the first differing position.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from dataclasses import dataclass
from typing import Callable, Sequence

from stream_processing import CHECKPOINT_VERSION, Event, Pipeline, session, sliding, tumbling
from stream_processing.cli import canonical
from stream_processing.errors import ValidationError

# Every scenario is executed this many times from scratch; a single divergence in any iteration
# fails the scenario.
REPEATS = 5


@dataclass(frozen=True)
class TEvent:
    """A test event with a stable identity, carried *beside* the engine's public Event model.

    The engine deliberately has no id field (unknown JSON fields are rejected), so the id drives
    test-side redelivery only; the engine itself keeps its baseline value semantics.
    """

    event_id: str
    timestamp: int
    key: str
    value: float
    kind: str = "data"

    def event(self) -> Event:
        return Event(timestamp=self.timestamp, key=self.key, value=self.value, kind=self.kind)


def punct(timestamp: int) -> TEvent:
    return TEvent(event_id=f"punct-{timestamp}", timestamp=timestamp, key="_clock", value=0.0, kind="punct")


@dataclass(frozen=True)
class Scenario:
    name: str
    build: Callable[[], Pipeline]
    prefix: tuple[TEvent, ...]   # ingested before the successful checkpoint
    suffix: tuple[TEvent, ...]   # ingested by the restored instance

    def all_events(self) -> list[TEvent]:
        return [*self.prefix, *self.suffix]


# -- Scenarios ---------------------------------------------------------------

# Tumbling 100ms, sum. The checkpoint is taken after the [0,100) timers of both keys have fired and
# their writes committed, while the [100,200) timers exist but have not fired (watermark is 100).
TUMBLING_SCENARIO = Scenario(
    name="tumbling-sum",
    build=lambda: Pipeline(windowing=tumbling(100), aggregation="sum"),
    prefix=(
        TEvent("e1", 10, "alpha", 1.0),
        TEvent("e2", 20, "beta", 10.0),
        TEvent("e3", 30, "alpha", 2.0),
        TEvent("e4", 40, "beta", 20.0),
        punct(100),   # same-timestamp multi-key timers: alpha and beta [0,100) fire together
        TEvent("e5", 110, "alpha", 4.0),
        TEvent("e6", 130, "beta", 40.0),
    ),
    suffix=(
        # A straggler that arrives at recovery time with timestamp 105, below the checkpoint
        # watermark 130: it must be late on the restored instance exactly as in the uninterrupted
        # run. An engine that reset its clock on restore would apply it to beta's pending window.
        TEvent("e9", 105, "beta", 3.0),
        punct(199),   # not yet at the trigger: [100,200) stays pending
        punct(200),   # restored timers cross their trigger here, exactly once
        # Redelivery of the same stable event id after its window committed: the baseline treats
        # it as late (watermark 200 > timestamp 30), so it is dropped, never re-applied.
        TEvent("e3", 30, "alpha", 2.0),
        TEvent("e7", 250, "gamma", 100.0),  # third key keeps independent state
        TEvent("e8", 260, "alpha", 8.0),
        punct(300),   # alpha and gamma [200,300) fire together
    ),
)

# Sliding windows (size 150, slide 50), count. Each event registers several overlapping timers;
# the checkpoint straddles a point where four overlapping timers committed and others are pending.
SLIDING_SCENARIO = Scenario(
    name="sliding-count",
    build=lambda: Pipeline(windowing=sliding(size=150, slide=50), aggregation="count"),
    prefix=(
        TEvent("s1", 60, "alpha", 1.0),
        TEvent("s2", 80, "beta", 2.0),
        punct(150),   # closes [-50,100) and [0,150) for both keys
        TEvent("s3", 170, "alpha", 3.0),  # joins the pending [50,200) timer and opens two more
    ),
    suffix=(
        punct(200),
        punct(250),
        punct(300),
    ),
)

# Session windows, max. The committed session [10,26) for alpha is a *merged* identity that is not
# itself a state key: checkpoint/restore must still remember it or the timer fires a second time.
# Events are fed in event-time order so nothing is late before the checkpoint.
SESSION_SCENARIO = Scenario(
    name="session-max",
    build=lambda: Pipeline(windowing=session(gap=20), aggregation="max"),
    prefix=(
        TEvent("n1", 10, "alpha", 1.0),
        TEvent("n3", 20, "beta", 7.0),
        TEvent("n2", 25, "alpha", 4.0),  # merges with n1 into [10,26)
        TEvent("n4", 100, "alpha", 9.0),  # separate session, still pending at the checkpoint
        punct(46),   # commits merged alpha session [10,26) and beta session [20,21)
    ),
    suffix=(
        TEvent("n5", 115, "alpha", 2.0),  # after recovery, merges with the pending [100,101)
        punct(136),
    ),
)

SCENARIOS = (TUMBLING_SCENARIO, SLIDING_SCENARIO, SESSION_SCENARIO)


# -- Execution paths ---------------------------------------------------------

def _drive(pipeline: Pipeline, events: Sequence[TEvent], outputs: list[dict]) -> None:
    for test_event in events:
        for result in pipeline.add(test_event.event()):
            outputs.append(result.to_document())


def run_baseline(scenario: Scenario) -> tuple[list[dict], Pipeline]:
    """Ingest every event from empty state, then flush."""
    pipeline = scenario.build()
    outputs: list[dict] = []
    _drive(pipeline, scenario.all_events(), outputs)
    outputs.extend(result.to_document() for result in pipeline.flush())
    return outputs, pipeline


def run_with_recovery(scenario: Scenario) -> tuple[list[dict], Pipeline, Pipeline]:
    """Ingest the prefix, take a successful checkpoint, stop, restore, ingest the suffix."""
    pipeline = scenario.build()
    outputs: list[dict] = []
    _drive(pipeline, scenario.prefix, outputs)
    checkpoint_text = pipeline.checkpoint_text()  # the durable success point
    checkpoint_document = json.loads(checkpoint_text)

    # Simulate stopping the instance: the old pipeline is abandoned and a fresh one is built only
    # from the checkpoint document (round-tripped through text and JSON, like a real restart).
    del pipeline
    resumed = Pipeline.restore_text(json.dumps(checkpoint_document))

    _drive(resumed, scenario.suffix, outputs)
    outputs.extend(result.to_document() for result in resumed.flush())
    return outputs, Pipeline.restore_text(checkpoint_text), resumed


# -- Strict comparison with first-difference diagnostics ---------------------

def mismatch_diagnostic(
    scenario: str,
    iteration: int,
    expected: list[dict],
    actual: list[dict],
) -> str:
    """Locate the first divergence. Lists are compared as given -- never sorted or deduplicated."""
    if len(expected) != len(actual):
        position = min(len(expected), len(actual))
        return (
            f"scenario={scenario} iteration={iteration} first mismatch at output position {position}: "
            f"category=record count expected={len(expected)} actual={len(actual)}"
        )
    for position, (wanted, got) in enumerate(zip(expected, actual)):
        if wanted == got:
            continue
        wanted_window = wanted.get("window") or {}
        got_window = got.get("window") or {}
        same_identity = (
            wanted.get("key") == got.get("key")
            and wanted_window.get("start") == got_window.get("start")
            and wanted_window.get("end") == got_window.get("end")
        )
        if same_identity:
            category = "aggregation result mismatch"
        elif position > 0 and actual[position] == actual[position - 1]:
            category = "duplicate write"
        elif wanted not in actual[position:]:
            category = "missing write"
        elif got not in expected[position:]:
            category = "unexpected/extra write"
        else:
            category = "order drift"
        return (
            f"scenario={scenario} iteration={iteration} first mismatch at output position {position}: "
            f"category={category} key expected={wanted.get('key')!r} actual={got.get('key')!r} "
            f"window expected=({wanted_window.get('start')},{wanted_window.get('end')}) "
            f"actual=({got_window.get('start')},{got_window.get('end')}) "
            f"expected={canonical(wanted)} actual={canonical(got)}"
        )
    return ""


class RecoveryReplayTests(unittest.TestCase):
    def assertStreamsEqual(
        self, scenario: str, iteration: int, expected: list[dict], actual: list[dict]
    ) -> None:
        diagnostic = mismatch_diagnostic(scenario, iteration, expected, actual)
        if diagnostic:
            self.fail(diagnostic)

    def test_baseline_is_deterministic_across_repeats(self) -> None:
        for scenario in SCENARIOS:
            reference, _ = run_baseline(scenario)
            for iteration in range(REPEATS):
                outputs, _ = run_baseline(scenario)
                self.assertStreamsEqual(scenario.name, iteration, reference, outputs)

    def test_recovery_is_deterministic_across_repeats(self) -> None:
        for scenario in SCENARIOS:
            reference, _, _ = run_with_recovery(scenario)
            for iteration in range(REPEATS):
                outputs, _, _ = run_with_recovery(scenario)
                self.assertStreamsEqual(scenario.name, iteration, reference, outputs)

    def test_recovery_replays_baseline_record_for_record(self) -> None:
        for scenario in SCENARIOS:
            for iteration in range(REPEATS):
                baseline, _ = run_baseline(scenario)
                recovered, _, _ = run_with_recovery(scenario)
                self.assertStreamsEqual(scenario.name, iteration, baseline, recovered)
                # Identity of a write is its position in the deterministic sequence; the count
                # guard plus the position-wise document comparison is what a duplicate or a missing
                # record cannot survive.
                self.assertEqual(
                    len(recovered),
                    len(baseline),
                    f"scenario={scenario.name} iteration={iteration}: record count differs",
                )

    def test_tumbling_scenario_has_the_hand_checked_output_contract(self) -> None:
        # Pins the semantics the equivalence relies on: exact records, keys, timer times,
        # aggregates and order -- on both execution paths.
        expected = [
            {"window": {"start": 0, "end": 100}, "key": "alpha", "aggregation": "sum", "value": 3.0, "count": 2},
            {"window": {"start": 0, "end": 100}, "key": "beta", "aggregation": "sum", "value": 30.0, "count": 2},
            {"window": {"start": 100, "end": 200}, "key": "alpha", "aggregation": "sum", "value": 4.0, "count": 1},
            {"window": {"start": 100, "end": 200}, "key": "beta", "aggregation": "sum", "value": 40.0, "count": 1},
            {"window": {"start": 200, "end": 300}, "key": "alpha", "aggregation": "sum", "value": 8.0, "count": 1},
            {"window": {"start": 200, "end": 300}, "key": "gamma", "aggregation": "sum", "value": 100.0, "count": 1},
        ]
        baseline, _ = run_baseline(TUMBLING_SCENARIO)
        recovered, _, _ = run_with_recovery(TUMBLING_SCENARIO)
        self.assertEqual(baseline, expected)
        self.assertEqual(recovered, expected)

    def test_output_documents_are_canonically_serializable(self) -> None:
        # The observable write is the CLI's canonical line; every record must round-trip through it.
        for scenario in SCENARIOS:
            baseline, _ = run_baseline(scenario)
            recovered, _, _ = run_with_recovery(scenario)
            baseline_lines = [canonical(document) for document in baseline]
            recovered_lines = [canonical(document) for document in recovered]
            self.assertEqual(baseline_lines, recovered_lines, f"scenario={scenario.name}")
            # Each observable write must be a single canonical JSON line.
            for line in baseline_lines:
                self.assertEqual(canonical(json.loads(line)), line)


class RecoveryBoundaryTests(unittest.TestCase):
    def _prefix_pipeline(self) -> tuple[Pipeline, list[dict]]:
        pipeline = TUMBLING_SCENARIO.build()
        committed: list[dict] = []
        _drive(pipeline, TUMBLING_SCENARIO.prefix, committed)
        return pipeline, committed

    def test_committed_timers_never_fire_again_after_recovery(self) -> None:
        for _ in range(REPEATS):
            pipeline, committed_before = self._prefix_pipeline()
            self.assertEqual([document["key"] for document in committed_before], ["alpha", "beta"])
            resumed = Pipeline.restore_text(pipeline.checkpoint_text())
            # Re-advance to the exact watermark that triggered them, then strictly past it:
            # the checkpointed [0,100) identities must stay silent.
            self.assertEqual(resumed.add(Event(timestamp=100, key="_clock", kind="punct")), [])
            self.assertEqual(resumed.add(Event(timestamp=150, key="_clock", kind="punct")), [])
            for document in committed_before:
                self.assertIn(
                    (document["window"]["start"], document["window"]["end"], document["key"]),
                    {(entry["start"], entry["end"], entry["key"]) for entry in resumed.checkpoint()["emitted"]},
                )

    def test_pending_timer_fires_exactly_once_when_watermark_first_crosses(self) -> None:
        for _ in range(REPEATS):
            pipeline, _ = self._prefix_pipeline()
            resumed = Pipeline.restore_text(pipeline.checkpoint_text())
            self.assertEqual(resumed.add(Event(timestamp=199, key="_clock", kind="punct")), [])
            crossing = resumed.add(Event(timestamp=200, key="_clock", kind="punct"))
            self.assertEqual(
                sorted((result.window.start, result.key) for result in crossing),
                [(100, "alpha"), (100, "beta")],
            )
            # Every later watermark advancement and the final flush must not produce those again.
            self.assertEqual(resumed.add(Event(timestamp=250, key="_clock", kind="punct")), [])
            self.assertEqual(resumed.flush(), [])

    def test_restored_keyed_state_does_not_leak_between_keys(self) -> None:
        pipeline, _ = self._prefix_pipeline()
        document = pipeline.checkpoint()
        state = {(entry["start"], entry["end"], entry["key"]): entry["values"] for entry in document["state"]}
        self.assertEqual(state[(100, 200, "alpha")], [4.0])
        self.assertEqual(state[(100, 200, "beta")], [40.0])
        self.assertNotIn((100, 200, "gamma"), state)
        # Closed windows remain stored with their committed identity, but their values are never
        # re-aggregated into another key (observable via the exact outputs).
        resumed = Pipeline.restore(pipeline.checkpoint())
        outputs = resumed.add(Event(timestamp=200, key="_clock", kind="punct"))
        by_key = {result.key: result.value for result in outputs}
        self.assertEqual(by_key, {"alpha": 4.0, "beta": 40.0})

    def test_event_time_clock_is_restored_not_reset(self) -> None:
        pipeline, _ = self._prefix_pipeline()
        self.assertEqual(pipeline.watermark.current, 130)
        resumed = Pipeline.restore_text(pipeline.checkpoint_text())
        self.assertEqual(resumed.watermark.current, 130)
        # An event behind the restored watermark is late on the new instance as well...
        late = resumed.add(Event(timestamp=30, key="alpha", value=2.0))
        self.assertEqual(late, [])
        self.assertEqual(resumed.watermark.late_dropped, 1)
        # ...whereas the uninterrupted baseline reaches the same counter at the same point.
        baseline = TUMBLING_SCENARIO.build()
        _drive(baseline, TUMBLING_SCENARIO.prefix, [])
        self.assertEqual(baseline.add(Event(timestamp=30, key="alpha", value=2.0)), [])
        self.assertEqual(baseline.watermark.late_dropped, 1)

    def test_redelivery_of_same_stable_id_after_commit_is_idempotent_in_both_paths(self) -> None:
        for _ in range(REPEATS):
            baseline, baseline_pipeline = run_baseline(TUMBLING_SCENARIO)
            recovered, checkpoint_pipeline, resumed = run_with_recovery(TUMBLING_SCENARIO)
            self.assertEqual(recovered, baseline)
            # Two late records are dropped on both paths: the recovery-time straggler e9 and the
            # redelivered e3; none exists yet at the checkpoint.
            self.assertEqual(baseline_pipeline.watermark.late_dropped, 2)
            self.assertEqual(resumed.watermark.late_dropped, 2)
            self.assertEqual(checkpoint_pipeline.watermark.late_dropped, 0)

    def test_redelivery_before_commit_keeps_baseline_value_semantics_after_recovery(self) -> None:
        # The baseline has no id-based dedup: the same stable event re-sent while the window is
        # still open contributes twice. Recovery must reproduce that semantics exactly -- it must
        # neither invent dedup nor lose the second value.
        events = [
            TEvent("k1", 10, "alpha", 1.0),
            TEvent("k1", 10, "alpha", 1.0),  # same stable id, window still open
            TEvent("k2", 10, "beta", 5.0),
        ]
        baseline = TUMBLING_SCENARIO.build()
        baseline_outputs: list[dict] = []
        _drive(baseline, [*events, punct(100)], baseline_outputs)
        baseline_outputs.extend(result.to_document() for result in baseline.flush())

        interrupted = TUMBLING_SCENARIO.build()
        recovered_outputs: list[dict] = []
        _drive(interrupted, events[:2], recovered_outputs)  # both copies before the checkpoint
        resumed = Pipeline.restore_text(interrupted.checkpoint_text())
        _drive(resumed, [events[2], punct(100)], recovered_outputs)
        recovered_outputs.extend(result.to_document() for result in resumed.flush())
        self.assertEqual(recovered_outputs, baseline_outputs)
        self.assertEqual(baseline_outputs[0]["value"], 2.0)

    def test_merged_session_identity_is_suppressed_after_recovery(self) -> None:
        pipeline = SESSION_SCENARIO.build()
        committed: list[dict] = []
        _drive(pipeline, SESSION_SCENARIO.prefix, committed)
        identities = {(entry["start"], entry["end"], entry["key"]) for entry in pipeline.checkpoint()["emitted"]}
        # The committed identities are the merged alpha window (no individual state key) and beta.
        self.assertEqual(identities, {(10, 26, "alpha"), (20, 21, "beta")})
        resumed = Pipeline.restore_text(pipeline.checkpoint_text())
        self.assertEqual(resumed.add(Event(timestamp=46, key="_clock", kind="punct")), [])
        suffix_outputs: list[dict] = []
        _drive(resumed, SESSION_SCENARIO.suffix, suffix_outputs)
        self.assertEqual(
            [(document["window"]["start"], document["key"]) for document in suffix_outputs],
            [(100, "alpha")],
        )
        self.assertEqual(suffix_outputs[0]["value"], 9.0)
        self.assertEqual(suffix_outputs[0]["count"], 2)


class CheckpointFormatTests(unittest.TestCase):
    def test_checkpoint_text_is_canonical_and_deterministic(self) -> None:
        first = TUMBLING_SCENARIO.build()
        second = TUMBLING_SCENARIO.build()
        outputs: list[dict] = []
        _drive(first, TUMBLING_SCENARIO.prefix, outputs)
        _drive(second, TUMBLING_SCENARIO.prefix, [])
        self.assertEqual(first.checkpoint_text(), second.checkpoint_text())
        self.assertEqual(first.checkpoint_text(), canonical(json.loads(first.checkpoint_text())))

    def test_restore_reproduces_an_identical_checkpoint(self) -> None:
        pipeline = TUMBLING_SCENARIO.build()
        _drive(pipeline, TUMBLING_SCENARIO.prefix, [])
        document = pipeline.checkpoint()
        self.assertEqual(Pipeline.restore(document).checkpoint(), document)

    def test_checkpoint_survives_a_file_roundtrip_like_a_real_restart(self) -> None:
        pipeline = TUMBLING_SCENARIO.build()
        _drive(pipeline, TUMBLING_SCENARIO.prefix, [])
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "checkpoint.json")
            with open(path, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(pipeline.checkpoint_text())
            with open(path, encoding="utf-8") as handle:
                resumed = Pipeline.restore_text(handle.read())
        expected, _ = run_baseline(TUMBLING_SCENARIO)
        recovered: list[dict] = []
        _drive(resumed, TUMBLING_SCENARIO.suffix, recovered)
        prefix_outputs: list[dict] = []
        fresh = TUMBLING_SCENARIO.build()
        _drive(fresh, TUMBLING_SCENARIO.prefix, prefix_outputs)
        self.assertEqual(prefix_outputs + recovered, expected)

    def test_version_and_required_sections(self) -> None:
        pipeline = TUMBLING_SCENARIO.build()
        _drive(pipeline, TUMBLING_SCENARIO.prefix, [])
        document = pipeline.checkpoint()
        self.assertEqual(document["version"], CHECKPOINT_VERSION)
        self.assertEqual(document["version"], 1)  # the compatibility range pinned by these tests
        for section in ("windowing", "aggregation", "maxOutOfOrderness", "allowedLateness",
                        "watermark", "state", "emitted"):
            self.assertIn(section, document)
        self.assertEqual(document["watermark"]["maxSeen"], 130)
        self.assertEqual(document["watermark"]["lateDropped"], 0)
        self.assertEqual(document["watermark"]["observed"], 6)

    def test_version_one_document_from_disk_is_still_loadable(self) -> None:
        # A frozen, hand-authored v1 document pins the compatibility promise: old successful
        # checkpoints keep restoring as the engine evolves.
        frozen = (
            '{"aggregation":"sum","allowedLateness":0,"emitted":[{"end":100,"key":"alpha","start":0}],'
            '"maxOutOfOrderness":0,"state":[{"end":100,"key":"alpha","start":0,"values":[3.0]},'
            '{"end":200,"key":"alpha","start":100,"values":[4.0]}],'
            '"version":1,"watermark":{"lateDropped":0,"maxSeen":110,"observed":2},'
            '"windowing":{"offset":0,"size":100,"type":"tumbling"}}'
        )
        pipeline = Pipeline.restore_text(frozen)
        # Committed timer suppressed, pending timer fires once as the watermark crosses.
        self.assertEqual(pipeline.add(Event(timestamp=100, key="_clock", kind="punct")), [])
        emitted = pipeline.add(Event(timestamp=200, key="_clock", kind="punct"))
        self.assertEqual([(result.window.start, result.key, result.value) for result in emitted], [(100, "alpha", 4.0)])

    def test_reject_non_object_document(self) -> None:
        with self.assertRaises(ValidationError):
            Pipeline.restore(["not", "an", "object"])  # type: ignore[arg-type]

    def test_reject_unsupported_version(self) -> None:
        document = self._valid_document()
        for bad_version in (0, 2, "1", None):
            corrupted = json.loads(json.dumps(document))
            corrupted["version"] = bad_version
            with self.assertRaises(ValidationError):
                Pipeline.restore(corrupted)

    def test_reject_unknown_fields_everywhere(self) -> None:
        corrupted = self._valid_document()
        corrupted["extra"] = 1
        with self.assertRaises(ValidationError):
            Pipeline.restore(corrupted)
        corrupted = self._valid_document()
        corrupted["state"][0]["values"] = list(corrupted["state"][0]["values"])
        corrupted["state"][0]["bogus"] = True
        with self.assertRaises(ValidationError):
            Pipeline.restore(corrupted)

    def test_reject_wrong_types(self) -> None:
        corrupted = self._valid_document()
        corrupted["aggregation"] = 7
        with self.assertRaises(ValidationError):
            Pipeline.restore(corrupted)
        corrupted = self._valid_document()
        corrupted["watermark"]["maxSeen"] = "110"
        with self.assertRaises(ValidationError):
            Pipeline.restore(corrupted)
        corrupted = self._valid_document()
        corrupted["state"][0]["values"] = [1.0, "no"]
        with self.assertRaises(ValidationError):
            Pipeline.restore(corrupted)

    def test_reject_duplicate_and_non_half_open_state_entries(self) -> None:
        corrupted = self._valid_document()
        corrupted["state"].append(dict(corrupted["state"][0]))
        with self.assertRaises(ValidationError):
            Pipeline.restore(corrupted)
        corrupted = self._valid_document()
        corrupted["state"][0]["end"] = corrupted["state"][0]["start"]
        with self.assertRaises(ValidationError):
            Pipeline.restore(corrupted)

    def test_reject_duplicate_emitted_entries(self) -> None:
        corrupted = self._valid_document()
        corrupted["emitted"].append(dict(corrupted["emitted"][0]))
        with self.assertRaises(ValidationError):
            Pipeline.restore(corrupted)

    def test_reject_malformed_checkpoint_text(self) -> None:
        with self.assertRaises(ValidationError):
            Pipeline.restore_text("{not json")

    def test_unknown_aggregation_in_checkpoint_is_a_validation_error(self) -> None:
        corrupted = self._valid_document()
        corrupted["aggregation"] = "median"
        with self.assertRaises(ValidationError):
            Pipeline.restore(corrupted)

    def test_unknown_windowing_type_is_a_validation_error(self) -> None:
        corrupted = self._valid_document()
        corrupted["windowing"] = {"type": "global"}
        with self.assertRaises(ValidationError):
            Pipeline.restore(corrupted)

    @staticmethod
    def _valid_document() -> dict:
        pipeline = TUMBLING_SCENARIO.build()
        _drive(pipeline, TUMBLING_SCENARIO.prefix, [])
        return json.loads(json.dumps(pipeline.checkpoint()))


if __name__ == "__main__":
    unittest.main()
