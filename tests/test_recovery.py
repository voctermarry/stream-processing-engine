"""Deterministic replay of keyed state and event-time timers across a checkpoint/restore boundary.

These tests add no engine features and alter no window/late-data/output rule. They constrain the
*existing* execution chain with repeatable evidence:

* the public execution entry points are :class:`stream_processing.Pipeline` (``add``/``flush``) and
  the CLI (``run``/``replay``); the harness never touches pipeline internals;
* the checkpoint is a test-side artifact: a versioned JSON document of the consumed prefix,
  watermark position, late-drop counters and the outputs already committed before the barrier;
* restore is a deterministic prefix replay through the same public ``Pipeline.add`` chain — no
  direct injection of internal state — followed by exactly the same punct/watermark schedule;
* already-committed outputs are reconciled with a strictly-ordered ledger (never a set: no sort,
  no dedup) so a duplicate, a missing record or an order drift surfaces at its exact position;
* time moves only via explicit ``kind="punct"`` watermarks on integer event-time milliseconds —
  no wall clock, no threads, no scheduler jitter, no time zone, no randomness;
* the stable event id lives only in the test data (an event document has a closed schema, so an
  ``id`` field on the wire would be a parse_error); a deliberately redelivered id is used to
  document the baseline's actual idempotency semantics (no id dedup: a timely re-feed counts
  twice, a late re-feed is counted as late_dropped — identically on both execution paths).
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
from dataclasses import dataclass
from typing import Any, Sequence

from stream_processing import Event, Pipeline, session, sliding, tumbling
from stream_processing.cli import EXIT_OK, canonical, main
from stream_processing.errors import OutputError, ParseError, StreamProcessingError, ValidationError
from stream_processing.events import parse_event_line
from stream_processing.windows import Session, Sliding, Tumbling

CHECKPOINT_FORMAT = "stream-processing-test-checkpoint"
CHECKPOINT_VERSION = 1


# -----------------------------------------------------------------------------------------------
# Test data
# -----------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TaggedEvent:
    """A test fixture event: stable id plus the public four-field event schema.

    `eid` never leaves the test harness — engine event documents carry timestamp/key/value/kind
    only, so feeding `eid` to the parser would (correctly) be a parse_error.
    """

    eid: str
    timestamp: int
    key: str
    value: float
    kind: str = "data"

    def event(self) -> Event:
        return Event(timestamp=self.timestamp, key=self.key, value=self.value, kind=self.kind)

    def line(self) -> str:
        return canonical(self.event().to_document())


def te(*, eid: str, ts: int, key: str, value: float, kind: str = "data") -> TaggedEvent:
    return TaggedEvent(eid=eid, timestamp=ts, key=key, value=value, kind=kind)


def punct(*, eid: str, ts: int) -> TaggedEvent:
    """An explicit watermark advancement; the key is inert punctuation metadata."""
    return TaggedEvent(eid=eid, timestamp=ts, key="clock", value=0.0, kind="punct")


# -----------------------------------------------------------------------------------------------
# Test-side checkpoint format
#
# The checkpoint is deliberately a plain, versioned JSON document. It captures only what a
# successful pre-failure checkpoint can observe from outside the engine:
#   - the exact processing configuration (window spec / aggregation / bounds),
#   - how many events were consumed,
#   - the watermark position reached (as an event-time millisecond position),
#   - late-drop / observed counters from the public WatermarkTracker,
#   - every output line committed *before* the barrier, in emission order.
# It carries no internal object state; restore re-derives keyed state by replaying the prefix.
# -----------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Checkpoint:
    config: dict[str, Any]
    offset: int
    watermark_target: int | None
    late_dropped: int
    observed: int
    committed: tuple[str, ...]

    def to_document(self) -> dict[str, Any]:
        return {
            "format": CHECKPOINT_FORMAT,
            "version": CHECKPOINT_VERSION,
            "config": self.config,
            "offset": self.offset,
            "watermarkTarget": self.watermark_target,
            "lateDropped": self.late_dropped,
            "observed": self.observed,
            "committed": list(self.committed),
        }

    def save(self, path: str) -> None:
        directory = os.path.dirname(os.path.abspath(path)) or "."
        handle = None
        temporary = None
        try:
            handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=directory, delete=False, newline="\n")
            temporary = handle.name
            handle.write(json.dumps(self.to_document(), sort_keys=True, separators=(",", ":")) + "\n")
            handle.close()
            handle = None
            os.replace(temporary, path)  # mirrors the CLI's atomic write guarantee
        except OSError as error:
            if handle is not None:
                handle.close()
            if temporary and os.path.exists(temporary):
                os.unlink(temporary)
            raise OutputError(f"cannot write checkpoint: {error.strerror or error}", value=path) from error


def load_checkpoint(path: str) -> Checkpoint:
    """Read and validate a checkpoint, reusing the baseline's stable error kinds on every failure."""
    try:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
    except OSError as error:
        raise OutputError(f"cannot read checkpoint: {error.strerror or error}", value=path) from error
    try:
        document = json.loads(text)
    except json.JSONDecodeError as error:
        raise ParseError(f"invalid checkpoint JSON: {error.msg}", line=1, column=error.colno) from error
    if not isinstance(document, dict):
        raise ParseError("checkpoint must be a JSON object", line=1, column=1)
    if document.get("format") != CHECKPOINT_FORMAT:
        raise ValidationError("unknown checkpoint format", value=document.get("format"))
    if document.get("version") != CHECKPOINT_VERSION:
        raise ValidationError(
            "unsupported checkpoint version",
            value=document.get("version"),
            supported=CHECKPOINT_VERSION,
        )
    required = ("config", "offset", "watermarkTarget", "lateDropped", "observed", "committed")
    missing = sorted(name for name in required if name not in document)
    if missing:
        raise ValidationError("checkpoint is missing field(s)", fields=missing)
    config = document["config"]
    if not isinstance(config, dict) or "window" not in config:
        raise ValidationError("checkpoint config must carry a window spec")
    offset = document["offset"]
    committed = document["committed"]
    if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
        raise ValidationError("checkpoint offset must be a non-negative integer", value=offset)
    if not isinstance(committed, list) or not all(isinstance(line, str) for line in committed):
        raise ValidationError("checkpoint committed outputs must be a list of strings")
    watermark_target = document["watermarkTarget"]
    if watermark_target is not None and (not isinstance(watermark_target, int) or isinstance(watermark_target, bool)):
        raise ValidationError("watermarkTarget must be an integer or null", value=watermark_target)
    return Checkpoint(
        config=config,
        offset=offset,
        watermark_target=watermark_target,
        late_dropped=int(document["lateDropped"]),
        observed=int(document["observed"]),
        committed=tuple(committed),
    )


# -----------------------------------------------------------------------------------------------
# Scenario definition
# -----------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Scenario:
    name: str
    window: str
    events: tuple[TaggedEvent, ...]
    checkpoint_after: int
    aggregation: str = "sum"
    allowed_lateness: int = 0
    max_out_of_orderness: int = 0

    def build_pipeline(self) -> Pipeline:
        return Pipeline(
            windowing=parse_spec(self.window),
            aggregation=self.aggregation,
            max_out_of_orderness=self.max_out_of_orderness,
            allowed_lateness=self.allowed_lateness,
        )

    def config(self) -> dict[str, Any]:
        return {
            "window": self.window,
            "aggregation": self.aggregation,
            "allowedLateness": self.allowed_lateness,
            "maxOutOfOrderness": self.max_out_of_orderness,
        }


def parse_spec(spec: str) -> Tumbling | Sliding | Session:
    """Same spec grammar as the CLI; scenarios still go through public window constructors."""
    parts = spec.split(":")
    kind = parts[0]
    try:
        numbers = tuple(int(part) for part in parts[1:])
    except ValueError as error:
        raise ValidationError(f"window numbers must be integers: {spec}", value=spec) from error
    if kind == "tumbling":
        return tumbling(*numbers)
    if kind == "sliding":
        return sliding(*numbers)
    if kind == "session":
        return session(*numbers)
    raise ValidationError("unknown window spec", value=spec)


# -----------------------------------------------------------------------------------------------
# Baseline and recovery execution
# -----------------------------------------------------------------------------------------------


def ingest(pipeline: Pipeline, events: Sequence[TaggedEvent]) -> list[str]:
    lines: list[str] = []
    for tagged in events:
        for result in pipeline.add(tagged.event()):
            lines.append(canonical(result.to_document()))
    return lines


def run_baseline(scenario: Scenario) -> list[str]:
    """Empty state, all events, explicit watermarks, flush at the end — the reference execution."""
    pipeline = scenario.build_pipeline()
    lines = ingest(pipeline, scenario.events)
    lines.extend(canonical(result.to_document()) for result in pipeline.flush())
    return lines


def take_checkpoint(scenario: Scenario) -> Checkpoint:
    """Run only the chosen prefix and snapshot everything a successful checkpoint would hold.

    The barrier lands strictly *after* prefix ingestion: some keyed state exists and timers are
    registered, while the scenario's punct schedule decides which timers have already fired.
    """
    if not 0 < scenario.checkpoint_after <= len(scenario.events):
        raise ValidationError(
            "checkpoint_after must split the stream",
            value=scenario.checkpoint_after,
            events=len(scenario.events),
        )
    prefix = scenario.events[: scenario.checkpoint_after]
    pipeline = scenario.build_pipeline()
    committed = ingest(pipeline, prefix)
    return Checkpoint(
        config=scenario.config(),
        offset=scenario.checkpoint_after,
        watermark_target=pipeline.watermark.max_seen,
        late_dropped=pipeline.watermark.late_dropped,
        observed=pipeline.watermark.observed,
        committed=tuple(committed),
    )


def restore_and_continue(scenario: Scenario, checkpoint: Checkpoint) -> list[str]:
    """A brand-new instance: rebuild state via public prefix replay, then ingest the remainder.

    Prefix replay outputs must match the committed ledger in order; only that prefix is replayed
    (never the suffix), so this is recovery semantics, not a second full run.
    """
    if checkpoint.config != scenario.config():
        raise ValidationError("checkpoint config does not match scenario", checkpoint=checkpoint.config)
    if checkpoint.offset > len(scenario.events):
        raise ValidationError("checkpoint offset is beyond the stream", offset=checkpoint.offset)

    pipeline = scenario.build_pipeline()
    redelivered = ingest(pipeline, scenario.events[: checkpoint.offset])

    # Reconcile against the ordered commit ledger — a ledger, not a set. Equal-length prefixes are
    # compared element by element; a longer redelivery is a duplicate-commit boundary failure.
    committed = list(checkpoint.committed)
    if len(redelivered) < len(committed):
        raise AssertionError(
            _mismatch_message(
                scenario,
                position=len(redelivered),
                expected=committed[len(redelivered)],
                actual=None,
                category="missing-output-after-restore",
            )
        )
    for index, (expected_line, actual_line) in enumerate(zip(committed, redelivered)):
        if expected_line != actual_line:
            raise AssertionError(
                _mismatch_message(
                    scenario,
                    position=index,
                    expected=expected_line,
                    actual=actual_line,
                    category="committed-output-drift-after-restore",
                )
            )
    if len(redelivered) > len(committed):
        raise AssertionError(
            _mismatch_message(
                scenario,
                position=len(committed),
                expected=None,
                actual=redelivered[len(committed)],
                category="duplicate-output-after-restore",
            )
        )
    if pipeline.watermark.max_seen != checkpoint.watermark_target:
        raise ValidationError(
            "restored watermark position differs from checkpoint",
            checkpoint=checkpoint.watermark_target,
            restored=pipeline.watermark.max_seen,
        )
    if pipeline.watermark.late_dropped != checkpoint.late_dropped:
        raise ValidationError(
            "restored late-drop counter differs from checkpoint",
            checkpoint=checkpoint.late_dropped,
            restored=pipeline.watermark.late_dropped,
        )
    if pipeline.watermark.observed != checkpoint.observed:
        raise ValidationError(
            "restored observed counter differs from checkpoint",
            checkpoint=checkpoint.observed,
            restored=pipeline.watermark.observed,
        )

    visible = ingest(pipeline, scenario.events[checkpoint.offset :])
    visible.extend(canonical(result.to_document()) for result in pipeline.flush())
    return visible


def run_recovery(scenario: Scenario) -> list[str]:
    """Full observable output of the fault-tolerant execution.

    The records whose writes were confirmed *inside* the successful checkpoint stay visible (they
    were delivered to the sink before the failure); the restarted instance then appends only what
    becomes due after restore. The ordered ledger reconciliation above proves the prefix matches
    the baseline's prefix, so concatenation reproduces the baseline position for position — the
    seam itself is part of the strict comparison, never glued with a sort or a set.
    """
    checkpoint = take_checkpoint(scenario)
    visible = restore_and_continue(scenario, checkpoint)
    return [*checkpoint.committed, *visible]


# -----------------------------------------------------------------------------------------------
# Strict, position-exact comparison
# -----------------------------------------------------------------------------------------------


def _diff_category(expected: Any, actual: Any) -> str:
    if expected is None:
        return "unexpected-extra-output"
    if actual is None:
        return "missing-output"
    if not isinstance(expected, dict) or not isinstance(actual, dict):
        return "record-mismatch"
    if expected.get("key") != actual.get("key"):
        return "business-key-mismatch"
    if expected.get("window") != actual.get("window"):
        return "window-or-timer-time-mismatch"
    if expected.get("aggregation") != actual.get("aggregation"):
        return "aggregation-mismatch"
    if expected.get("value") != actual.get("value") or expected.get("count") != actual.get("count"):
        return "aggregate-value-mismatch"
    return "record-mismatch"


def _mismatch_message(
    scenario: Scenario,
    *,
    position: int,
    expected: str | None,
    actual: str | None,
    category: str,
) -> str:
    expected_doc = json.loads(expected) if expected else None
    actual_doc = json.loads(actual) if actual else None
    side = actual_doc if actual_doc is not None else expected_doc
    key = side.get("key") if isinstance(side, dict) else None
    return (
        f"scenario {scenario.name!r}: first divergence at output position {position} "
        f"(0-based): category={category}, key={key!r}, "
        f"baseline={expected!r}, recovery={actual!r}"
    )


def assert_streams_equal(scenario: Scenario, baseline: Sequence[str], recovery: Sequence[str]) -> None:
    """Compare the *public output order* element by element.

    No sorting, no set/dedup comparison, no final-aggregate-only shortcut: record counts, business
    keys, window / timer times, aggregates and visible write identities are all part of every
    compared record.
    """
    count = max(len(baseline), len(recovery))
    for index in range(count):
        expected_line = baseline[index] if index < len(baseline) else None
        actual_line = recovery[index] if index < len(recovery) else None
        if expected_line == actual_line:
            continue
        expected_doc = json.loads(expected_line) if expected_line else None
        actual_doc = json.loads(actual_line) if actual_line else None
        category = _diff_category(expected_doc, actual_doc)
        raise AssertionError(
            _mismatch_message(
                scenario,
                position=index,
                expected=expected_line,
                actual=actual_line,
                category=category,
            )
        )


# -----------------------------------------------------------------------------------------------
# Fixture stream
#
# Two independent keys maintain state and register timers. The schedule deliberately covers:
#   * two keys whose timers fire at the SAME timestamp (wm 300 closes [0,100) for both; wm 500
#     closes [400,500) for both),
#   * timers already fired AND committed before the checkpoint (wm 300 pair),
#   * timers only due AFTER restore (the [400,500) pair at wm 500, the [700,800) pair at wm 800,
#     and the [900,1000) pair surfaced by the post-restore flush),
#   * key isolation (alpha and beta windows at identical boundaries never merge),
#   * a duplicate stable id straddling the barrier: e-05 is consumed before the checkpoint and
#     redelivered as the first post-restore event at the SAME timestamp 430 while the watermark is
#     still 430 — timely, so baseline semantics count it twice on BOTH paths (there is no id
#     dedup); a separate scenario covers the late-redelivery branch.
# -----------------------------------------------------------------------------------------------

REDELIVERED_IDS = frozenset({"e-05"})


def fixture_events() -> tuple[TaggedEvent, ...]:
    return (
        te(eid="e-01", ts=10, key="alpha", value=1.0),
        te(eid="e-02", ts=20, key="beta", value=10.0),
        punct(eid="w-300", ts=300),  # fires [0,100) for BOTH keys, committed before checkpoint
        te(eid="e-03", ts=410, key="alpha", value=2.0),
        te(eid="e-04", ts=420, key="beta", value=20.0),
        te(eid="e-05", ts=430, key="alpha", value=4.0),
        # ---- successful checkpoint here: [400,500) state exists, its timers not yet due -------
        te(eid="e-05", ts=430, key="alpha", value=4.0),  # redelivered, still timely: counted twice
        te(eid="e-06", ts=440, key="beta", value=40.0),
        punct(eid="w-500", ts=500),  # same-timestamp multi-key timers fire ONLY after restore
        te(eid="e-07", ts=710, key="alpha", value=8.0),
        te(eid="e-08", ts=720, key="beta", value=80.0),
        punct(eid="w-800", ts=800),  # post-restore timers for [700,800)
        te(eid="e-09", ts=910, key="alpha", value=16.0),
        te(eid="e-10", ts=920, key="beta", value=160.0),
    )  # no closing punct: the post-restore flush must surface [900,1000) deterministically


CHECKPOINT_AT = 6


def late_redelivery_events() -> tuple[TaggedEvent, ...]:
    return (
        te(eid="r-01", ts=10, key="alpha", value=1.0),
        te(eid="r-02", ts=20, key="beta", value=10.0),
        te(eid="r-03", ts=210, key="alpha", value=2.0),  # wm past 100 commits [0,100) for both
        punct(eid="rw-250", ts=250),  # [200,300) stays OPEN (watermark is 250)
        # ---- checkpoint here: [0,100) committed, alpha has open state in [200,300) ----
        te(eid="r-01", ts=240, key="alpha", value=5.0),  # below wm 250, window still open -> late
        punct(eid="rw-300", ts=300),  # closes [200,300): the dropped 5.0 must not be counted
        te(eid="r-04", ts=410, key="alpha", value=8.0),
        te(eid="r-05", ts=420, key="beta", value=80.0),
        punct(eid="rw-500", ts=500),
    )


def batch_order_events() -> tuple[TaggedEvent, ...]:
    # One watermark jump closes two DIFFERENT-start windows whose key name order opposes their
    # window-start order: beta owns [0,100), alpha owns [100,200). max_out_of_orderness keeps the
    # first window open until the explicit punctuation — beta@50 arrives AFTER alpha@110 but is
    # still on time because the watermark lags by 100 — so both fire in ONE batch. The contract
    # order is (window.start, key): beta's record precedes alpha's though alpha < beta.
    return (
        te(eid="b-01", ts=10, key="beta", value=1.0),
        te(eid="b-02", ts=110, key="alpha", value=2.0),
        te(eid="b-03", ts=50, key="beta", value=3.0),  # out of order, covered by moo=100
        punct(eid="bw-300", ts=300),  # wm = 200: closes [0,100) and [100,200) together
        # ---- checkpoint here: both timers fired in ONE batch, committed in start order ----
        te(eid="b-04", ts=410, key="alpha", value=4.0),
        te(eid="b-05", ts=420, key="beta", value=40.0),
        punct(eid="bw-600", ts=600),  # wm = 500: closes the [400,500) pair after restore
    )


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(name="tumbling-sum", window="tumbling:100", events=fixture_events(), checkpoint_after=CHECKPOINT_AT),
    Scenario(
        name="tumbling-count-lateness100",
        window="tumbling:100",
        aggregation="count",
        allowed_lateness=100,
        events=fixture_events(),
        checkpoint_after=CHECKPOINT_AT,
    ),
    Scenario(
        name="sliding-sum-300-100",
        window="sliding:300:100",
        events=fixture_events(),
        checkpoint_after=CHECKPOINT_AT,
    ),
    Scenario(
        name="session-sum-gap100",
        window="session:100",
        events=fixture_events(),
        checkpoint_after=CHECKPOINT_AT,
    ),
    Scenario(
        name="tumbling-sum-late-redelivery",
        window="tumbling:100",
        events=late_redelivery_events(),
        checkpoint_after=4,
    ),
    Scenario(
        name="tumbling-sum-batch-order",
        window="tumbling:100",
        max_out_of_orderness=100,
        events=batch_order_events(),
        checkpoint_after=4,
    ),
)

REPEATS = 5


class RecoveryDeterminismTests(unittest.TestCase):
    def test_fixture_ids_are_stable_and_unique_except_documented_redelivery(self) -> None:
        ids = [tagged.eid for tagged in fixture_events()]
        repeated = {eid for eid in ids if ids.count(eid) > 1}
        self.assertEqual(repeated, set(REDELIVERED_IDS))
        for tagged in fixture_events():
            self.assertIsInstance(tagged.timestamp, int)
            self.assertGreaterEqual(tagged.timestamp, 0)
            self.assertTrue(tagged.key)

    def test_checkpoint_boundary_splits_timers_as_designed(self) -> None:
        scenario = SCENARIOS[0]
        checkpoint = take_checkpoint(scenario)
        # Pre-barrier: [0,100) fired for both keys and was committed; [400,500) already holds
        # e-03/e-04/e-05 state but its timer has not fired. This is the premise of every boundary
        # assertion below.
        self.assertEqual(checkpoint.offset, CHECKPOINT_AT)
        committed_docs = [json.loads(line) for line in checkpoint.committed]
        self.assertEqual(
            [(document["window"]["start"], document["key"]) for document in committed_docs],
            [(0, "alpha"), (0, "beta")],
        )
        self.assertEqual([document["value"] for document in committed_docs], [1.0, 10.0])
        # Data events advanced event time to 430 even though the last punctuation was at 300.
        self.assertEqual(checkpoint.watermark_target, 430)
        self.assertEqual(checkpoint.late_dropped, 0)

    def test_baseline_and_recovery_are_position_identical_every_scenario_every_repeat(self) -> None:
        for scenario in SCENARIOS:
            reference = run_baseline(scenario)
            self.assertGreater(len(reference), 0, f"scenario {scenario.name} should emit output")
            for repetition in range(REPEATS):
                recovered = run_recovery(scenario)
                assert_streams_equal(scenario, reference, recovered)
                self.assertEqual(
                    len(recovered),
                    len(reference),
                    f"scenario {scenario.name} repetition {repetition}: record count differs",
                )

    def test_same_timestamp_timers_for_two_keys_fire_in_stable_order(self) -> None:
        scenario = SCENARIOS[0]
        docs = [json.loads(line) for line in run_baseline(scenario)]
        # wm 500 closes [400,500) for alpha and beta at the same instant:
        # ordered (window.start, key) -> alpha before beta, both present.
        same_instant = [document for document in docs if document["window"]["start"] == 400]
        self.assertEqual(
            [(document["key"], document["value"], document["count"]) for document in same_instant],
            [("alpha", 10.0, 3), ("beta", 60.0, 2)],
        )

    def test_committed_pre_barrier_timers_are_not_re_emitted_after_restore(self) -> None:
        scenario = SCENARIOS[0]
        checkpoint = take_checkpoint(scenario)
        visible = restore_and_continue(scenario, checkpoint)
        visible_docs = [json.loads(line) for line in visible]
        pre_barrier = [document for document in visible_docs if document["window"]["start"] == 0]
        self.assertEqual(pre_barrier, [], "timers fired before the checkpoint must not fire again")
        # And the committed records themselves remain exactly the baseline's first two records.
        reference = run_baseline(scenario)
        self.assertEqual(list(checkpoint.committed), reference[:2])

    def test_due_after_restore_timer_fires_exactly_once_on_first_crossing(self) -> None:
        scenario = SCENARIOS[0]
        # Feed the suffix up to (but not including) wm 500 manually: nothing may fire early.
        checkpoint = take_checkpoint(scenario)
        pipeline = scenario.build_pipeline()
        ingest(pipeline, scenario.events[: checkpoint.offset])  # restore prefix
        before_watermark = ingest(pipeline, scenario.events[checkpoint.offset : 8])  # dup + e-06
        self.assertEqual(before_watermark, [])
        crossing = ingest(pipeline, scenario.events[8:9])  # the wm-500 punctuation itself
        self.assertEqual(
            [(json.loads(line)["window"]["start"], json.loads(line)["key"]) for line in crossing],
            [(400, "alpha"), (400, "beta")],
        )
        # The full recovery path must show exactly one [400,500) record per key.
        recovered = run_recovery(scenario)
        for key, expected_value in (("alpha", 10.0), ("beta", 60.0)):
            matching = [
                line
                for line in recovered
                if json.loads(line)["key"] == key and json.loads(line)["window"]["start"] == 400
            ]
            self.assertEqual(len(matching), 1, f"{key}: post-restore timer must fire exactly once")
            self.assertEqual(json.loads(matching[0])["value"], expected_value)

    def test_post_restore_flush_emits_pending_timers_deterministically(self) -> None:
        scenario = SCENARIOS[0]
        recovered = run_recovery(scenario)
        tail = [(json.loads(line)["window"]["start"], json.loads(line)["key"]) for line in recovered[-2:]]
        self.assertEqual(tail, [(900, "alpha"), (900, "beta")])

    def test_state_is_partitioned_by_key(self) -> None:
        # Every (window, key) aggregate must come from that key's payloads alone; if state were
        # shared across keys, these independently computed sums would not line up.
        scenario = SCENARIOS[0]
        docs = [json.loads(line) for line in run_baseline(scenario)]
        expected = {
            (0, "alpha"): (1.0, 1),
            (0, "beta"): (10.0, 1),
            (400, "alpha"): (10.0, 3),
            (400, "beta"): (60.0, 2),
            (700, "alpha"): (8.0, 1),
            (700, "beta"): (80.0, 1),
            (900, "alpha"): (16.0, 1),
            (900, "beta"): (160.0, 1),
        }
        actual = {
            (document["window"]["start"], document["key"]): (document["value"], document["count"])
            for document in docs
        }
        self.assertEqual(actual, expected)

        def total(key: str) -> float:
            return sum(document["value"] for document in docs if document["key"] == key)

        # alpha 1 + (2+4+4) + 8 + 16 = 35 ; beta 10 + (20+40) + 80 + 160 = 310
        self.assertEqual(total("alpha"), 35.0)
        self.assertEqual(total("beta"), 310.0)

    def test_redelivered_stable_id_follows_baseline_idempotency_semantics(self) -> None:
        # Baseline has no event-id dedup: e-05 fed twice while [400,500) is open counts twice.
        scenario = SCENARIOS[0]
        first = next(document for document in (json.loads(line) for line in run_baseline(scenario))
                     if document["window"]["start"] == 400 and document["key"] == "alpha")
        self.assertEqual((first["value"], first["count"]), (10.0, 3))
        recovered = run_recovery(scenario)
        assert_streams_equal(scenario, run_baseline(scenario), recovered)

    def test_late_redelivery_is_dropped_identically_on_both_paths(self) -> None:
        scenario = SCENARIOS[4]
        reference = run_baseline(scenario)

        checkpoint = take_checkpoint(scenario)
        # The prefix committed [0,100) for both keys at wm 250 while [200,300) stayed open.
        self.assertEqual(
            [(json.loads(line)["window"]["start"], json.loads(line)["key"]) for line in checkpoint.committed],
            [(0, "alpha"), (0, "beta")],
        )
        self.assertEqual(checkpoint.late_dropped, 0)
        recovered = run_recovery(scenario)
        assert_streams_equal(scenario, reference, recovered)

        post_restore = restore_and_continue(scenario, checkpoint)
        counter_pipeline = scenario.build_pipeline()
        ingest(counter_pipeline, scenario.events[: checkpoint.offset])
        ingest(counter_pipeline, scenario.events[checkpoint.offset :])
        self.assertEqual(counter_pipeline.watermark.late_dropped, 1)
        # The redelivered 5.0 landed below the watermark in an otherwise-open window: it must be
        # counted as late and never applied — [200,300) stays 2.0/1.
        self.assertEqual(
            [
                (document["window"]["start"], document["value"], document["count"])
                for document in (json.loads(line) for line in post_restore)
                if document["key"] == "alpha"
            ],
            [(200, 2.0, 1), (400, 8.0, 1)],
        )

    def test_one_batch_closing_different_starts_keeps_window_start_order(self) -> None:
        # Contract order is (window.start, key): within the wm-200 batch beta's [0,100) precedes
        # alpha's [100,200) even though the key name would sort the other way.
        scenario = SCENARIOS[5]
        docs = [json.loads(line) for line in run_baseline(scenario)]
        self.assertEqual(
            [(document["window"]["start"], document["key"]) for document in docs],
            [(0, "beta"), (100, "alpha"), (400, "alpha"), (400, "beta")],
        )
        # The disordered b-03@50 is covered by max_out_of_orderness=100, so beta sums 1+3=4.
        self.assertEqual(
            [(document["value"], document["count"]) for document in docs[:2]],
            [(4.0, 2), (2.0, 1)],
        )
        # The checkpoint lands right after that batch, so the seam crosses the ordering contract.
        checkpoint = take_checkpoint(scenario)
        self.assertEqual(
            [json.loads(line)["key"] for line in checkpoint.committed],
            ["beta", "alpha"],
        )
        self.assertEqual(checkpoint.late_dropped, 0)
        assert_streams_equal(scenario, [json.dumps(document, sort_keys=True, separators=(",", ":")) for document in docs], run_recovery(scenario))

    def test_recovery_is_identical_across_repeated_executions(self) -> None:
        for scenario in SCENARIOS:
            seen = {json.dumps(run_recovery(scenario), separators=(",", ":")) for _ in range(REPEATS)}
            self.assertEqual(len(seen), 1, f"scenario {scenario.name}: recovery output was not stable")

    def test_checkpoint_roundtrip_through_json_file_preserves_contract(self) -> None:
        scenario = SCENARIOS[0]
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "checkpoint.json")
            take_checkpoint(scenario).save(path)
            with open(path, encoding="utf-8") as handle:
                document = json.load(handle)
            self.assertEqual(document["format"], CHECKPOINT_FORMAT)
            self.assertEqual(document["version"], CHECKPOINT_VERSION)
            loaded = load_checkpoint(path)
            self.assertEqual(loaded, take_checkpoint(scenario))
            recovered = [*loaded.committed, *restore_and_continue(scenario, loaded)]
            assert_streams_equal(scenario, run_baseline(scenario), recovered)

    def test_malformed_checkpoint_documents_use_existing_error_kinds(self) -> None:
        scenario = SCENARIOS[0]
        with tempfile.TemporaryDirectory() as directory:
            cases = (
                ("not-json.txt", "{not json", ParseError),
                ("bad-format.json", canonical({"format": "something-else", "version": 1}), ValidationError),
                ("bad-version.json", canonical({"format": CHECKPOINT_FORMAT, "version": 999}), ValidationError),
            )
            for name, payload, expected in cases:
                path = os.path.join(directory, name)
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write(payload + "\n")
                with self.assertRaises(StreamProcessingError) as caught:
                    load_checkpoint(path)
                self.assertIsInstance(caught.exception, expected)
                self.assertEqual(
                    caught.exception.kind,
                    "parse_error" if expected is ParseError else "validation_error",
                )
            with self.assertRaises(OutputError):
                load_checkpoint(os.path.join(directory, "missing.json"))
            # A structurally valid checkpoint for another configuration is rejected, never applied.
            other = Scenario(
                name="other",
                window="tumbling:500",
                events=scenario.events,
                checkpoint_after=CHECKPOINT_AT,
            )
            foreign = os.path.join(directory, "foreign.json")
            take_checkpoint(other).save(foreign)
            with self.assertRaises(ValidationError):
                restore_and_continue(scenario, load_checkpoint(foreign))

    def test_prefix_replay_reproduces_only_the_committed_prefix(self) -> None:
        # Guard against the test accidentally replaying the whole stream during restore.
        scenario = SCENARIOS[0]
        checkpoint = take_checkpoint(scenario)
        pipeline = scenario.build_pipeline()
        prefix_output = ingest(pipeline, scenario.events[: checkpoint.offset])
        self.assertEqual(prefix_output, list(checkpoint.committed))
        self.assertEqual(len(prefix_output), 2)

    # -- public CLI entry point end to end -----------------------------------------------------

    def test_cli_run_agrees_with_the_baseline_reference(self) -> None:
        scenario = SCENARIOS[0]
        with tempfile.TemporaryDirectory() as directory:
            input_path = os.path.join(directory, "events.jsonl")
            output_path = os.path.join(directory, "results.jsonl")
            with open(input_path, "w", encoding="utf-8") as handle:
                handle.write("".join(tagged.line() + "\n" for tagged in scenario.events))
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = main(
                    [
                        "run",
                        "--input",
                        input_path,
                        "--output",
                        output_path,
                        "--window",
                        scenario.window,
                        "--aggregation",
                        scenario.aggregation,
                    ]
                )
            self.assertEqual((code, err.getvalue()), (EXIT_OK, ""))
            with open(output_path, encoding="utf-8") as handle:
                cli_lines = handle.read().splitlines()
            self.assertEqual(cli_lines, run_baseline(scenario))

    def test_cli_replay_reports_identical_for_the_fixture(self) -> None:
        scenario = SCENARIOS[0]
        with tempfile.TemporaryDirectory() as directory:
            input_path = os.path.join(directory, "events.jsonl")
            with open(input_path, "w", encoding="utf-8") as handle:
                handle.write("".join(tagged.line() + "\n" for tagged in scenario.events))
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = main(
                    [
                        "replay",
                        "--input",
                        input_path,
                        "--window",
                        scenario.window,
                        "--aggregation",
                        scenario.aggregation,
                    ]
                )
            self.assertEqual((code, err.getvalue()), (EXIT_OK, ""))
            self.assertTrue(json.loads(out.getvalue())["identical"])

    # -- diagnostics: the first divergent position must name position, key and category --------

    def test_diagnostic_names_position_key_and_category(self) -> None:
        scenario = SCENARIOS[0]
        baseline = run_baseline(scenario)
        mutated = list(baseline)
        target = json.loads(mutated[2])
        target["value"] = target["value"] + 1.0
        mutated[2] = canonical(target)
        with self.assertRaises(AssertionError) as caught:
            assert_streams_equal(scenario, baseline, mutated)
        message = str(caught.exception)
        self.assertIn("position 2", message)
        self.assertIn("aggregate-value-mismatch", message)
        self.assertIn(f"key={target['key']!r}", message)

    def test_diagnostic_detects_duplicate_and_missing_without_masking(self) -> None:
        scenario = SCENARIOS[0]
        baseline = run_baseline(scenario)
        duplicated = baseline[:2] + baseline[1:]  # insert a duplicate write
        with self.assertRaises(AssertionError) as caught:
            assert_streams_equal(scenario, baseline, duplicated)
        self.assertIn("position 2", str(caught.exception))
        missing = baseline[:1] + baseline[2:]  # drop one committed write
        with self.assertRaises(AssertionError) as caught:
            assert_streams_equal(scenario, baseline, missing)
        self.assertIn("position 1", str(caught.exception))

    def test_diagnostic_detects_order_drift(self) -> None:
        scenario = SCENARIOS[0]
        baseline = run_baseline(scenario)
        drifted = list(baseline)
        drifted[2], drifted[3] = drifted[3], drifted[2]  # swap the same-instant key pair
        with self.assertRaises(AssertionError) as caught:
            assert_streams_equal(scenario, baseline, drifted)
        self.assertIn("position 2", str(caught.exception))
        self.assertIn("business-key-mismatch", str(caught.exception))

    def test_tagged_event_line_is_parseable_by_the_public_parser(self) -> None:
        tagged = te(eid="e-x", ts=123, key="k", value=3.5)
        self.assertEqual(parse_event_line(tagged.line()), tagged.event())
        # The stable id must not leak onto the wire: the closed event schema rejects it.
        with self.assertRaises(ParseError):
            parse_event_line(canonical({"eid": "leaked", "timestamp": 1, "key": "k", "value": 1.0}))


if __name__ == "__main__":
    unittest.main()
