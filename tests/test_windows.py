"""Window assignment, merging, and watermark behaviour."""

from __future__ import annotations

import unittest

from stream_processing import Event, WatermarkTracker, session, sliding, tumbling
from stream_processing.errors import ValidationError, WindowError
from stream_processing.pipeline import Pipeline
from stream_processing.windows import Window, merge_sessions


class TumblingTests(unittest.TestCase):
    def test_assign_is_half_open(self) -> None:
        assigner = tumbling(1000)
        self.assertEqual(assigner.assign(0)[0], Window(0, 1000))
        self.assertEqual(assigner.assign(999)[0], Window(0, 1000))
        self.assertEqual(assigner.assign(1000)[0], Window(1000, 2000))

    def test_offset_shifts_boundaries(self) -> None:
        assigner = tumbling(1000, 500)
        self.assertEqual(assigner.assign(500)[0], Window(500, 1500))
        self.assertEqual(assigner.assign(1499)[0], Window(500, 1500))
        self.assertEqual(assigner.assign(1500)[0], Window(1500, 2500))

    def test_boundaries_cover_range(self) -> None:
        boundaries = tumbling(100).boundaries(50, 250)
        self.assertEqual([window.start for window in boundaries], [0, 100, 200])

    def test_non_positive_size_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            tumbling(0)


class SlidingTests(unittest.TestCase):
    def test_overlapping_assignment(self) -> None:
        assigner = sliding(size=1000, slide=500)
        self.assertEqual([window.start for window in assigner.assign(750)], [0, 500])

    def test_timestamp_belongs_only_to_windows_that_contain_it(self) -> None:
        assigner = sliding(size=1000, slide=400)
        for window in assigner.assign(1000):
            self.assertTrue(window.contains(1000))


class SessionTests(unittest.TestCase):
    def test_merge_closes_small_gaps(self) -> None:
        merged = merge_sessions([Window(0, 10), Window(15, 20), Window(100, 110)], gap=10)
        self.assertEqual(merged, [Window(0, 20), Window(100, 110)])

    def test_gap_boundary_merges_timestamps_gap_apart(self) -> None:
        # Events at 0 and 10 are exactly `gap` apart: their [t, t+1) windows are 9 apart and merge.
        merged = merge_sessions([Window(0, 1), Window(10, 11)], gap=10)
        self.assertEqual(merged, [Window(0, 11)])

    def test_gap_boundary_splits_timestamps_gap_plus_one_apart(self) -> None:
        # Events at 0 and 11 are `gap + 1` apart: the windows are 10 apart and must stay split.
        merged = merge_sessions([Window(0, 1), Window(11, 12)], gap=10)
        self.assertEqual(merged, [Window(0, 1), Window(11, 12)])

    def test_gap_of_one_merges_only_adjacent_timestamps(self) -> None:
        merged = merge_sessions([Window(0, 1), Window(1, 2), Window(3, 4)], gap=1)
        self.assertEqual(merged, [Window(0, 2), Window(3, 4)])

    def test_invalid_gap_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            merge_sessions([Window(0, 1)], gap=0)


class SessionPipelineTests(unittest.TestCase):
    def test_gap_is_the_maximum_merge_distance(self) -> None:
        events = [Event(timestamp=t, key="a", value=1.0) for t in (0, 10, 21)]
        results = Pipeline(windowing=session(10), aggregation="sum").run(events)
        # 0 and 10 merge (diff == gap); 21 is gap + 1 away from 10 and stands alone.
        self.assertEqual(
            [(r.window.start, r.window.end, r.value, r.count) for r in results],
            [(0, 11, 2.0, 2), (21, 22, 1.0, 1)],
        )

    def test_gap_boundary_with_negative_and_cross_zero_timestamps(self) -> None:
        events = [Event(timestamp=t, key="a", value=1.0) for t in (-11, -1, 0, 10)]
        results = Pipeline(windowing=session(10), aggregation="count").run(events)
        # -11..-1 and -1..0 and 0..10 are each exactly gap apart: one transitive session.
        self.assertEqual([(r.window.start, r.window.end, r.count) for r in results], [(-11, 11, 4)])

    def test_same_timestamp_events_share_a_session(self) -> None:
        events = [Event(timestamp=5, key="a", value=v) for v in (1.0, 2.0, 3.0)]
        results = Pipeline(windowing=session(1), aggregation="sum").run(events)
        self.assertEqual([(r.window.start, r.window.end, r.value, r.count) for r in results], [(5, 6, 6.0, 3)])

    def test_out_of_order_bridge_merges_unemitted_sessions_once(self) -> None:
        pipeline = Pipeline(windowing=session(10), aggregation="sum", max_out_of_orderness=100)
        pipeline.add(Event(timestamp=0, key="a", value=1.0))
        pipeline.add(Event(timestamp=20, key="a", value=2.0))
        pipeline.add(Event(timestamp=10, key="a", value=4.0))  # timely bridge: joins both sides
        results = pipeline.flush()
        self.assertEqual([(r.window.start, r.window.end, r.value, r.count) for r in results], [(0, 21, 7.0, 3)])

    def test_session_emits_only_when_watermark_passes_last_plus_gap(self) -> None:
        pipeline = Pipeline(windowing=session(10), aggregation="sum")
        pipeline.add(Event(timestamp=0, key="a", value=1.0))
        # Watermark 10: an event at 10 (diff == gap) could still arrive, so nothing may emit.
        self.assertEqual(pipeline.add(Event(timestamp=10, key="clock", kind="punct")), [])
        # Watermark 11 == last + gap + 1: the boundary is provably passed, the session closes.
        emitted = pipeline.add(Event(timestamp=11, key="clock", kind="punct"))
        self.assertEqual([(r.window.start, r.window.end) for r in emitted], [(0, 1)])

    def test_session_emission_respects_allowed_lateness(self) -> None:
        pipeline = Pipeline(windowing=session(10), aggregation="sum", allowed_lateness=5)
        pipeline.add(Event(timestamp=0, key="a", value=1.0))
        self.assertEqual(pipeline.add(Event(timestamp=15, key="clock", kind="punct")), [])
        emitted = pipeline.add(Event(timestamp=16, key="clock", kind="punct"))
        self.assertEqual(len(emitted), 1)

    def test_late_event_never_reopens_an_emitted_session(self) -> None:
        pipeline = Pipeline(windowing=session(10), aggregation="sum")
        pipeline.add(Event(timestamp=0, key="a", value=1.0))
        emitted = pipeline.add(Event(timestamp=11, key="clock", kind="punct"))
        self.assertEqual(len(emitted), 1)
        # 5 is within the gap of the emitted session but below the watermark: dropped, not merged.
        self.assertEqual(pipeline.add(Event(timestamp=5, key="a", value=99.0)), [])
        self.assertEqual(pipeline.watermark.late_dropped, 1)
        self.assertEqual(pipeline.flush(), [])

    def test_keys_never_share_a_session(self) -> None:
        events = [Event(timestamp=0, key="a", value=1.0), Event(timestamp=5, key="b", value=2.0)]
        results = Pipeline(windowing=session(10), aggregation="sum").run(events)
        self.assertEqual(
            [(r.window.start, r.window.end, r.key) for r in results],
            [(0, 1, "a"), (5, 6, "b")],
        )


class WatermarkTests(unittest.TestCase):
    def test_watermark_lags_by_out_of_orderness(self) -> None:
        tracker = WatermarkTracker(max_out_of_orderness=100)
        self.assertIsNone(tracker.current)
        tracker.observe(1000)
        self.assertEqual(tracker.current, 900)

    def test_late_event_is_counted_and_not_applied(self) -> None:
        pipeline = Pipeline(windowing=tumbling(100), aggregation="sum", max_out_of_orderness=0)
        pipeline.add(Event(timestamp=250, key="a", value=1.0))
        pipeline.add(Event(timestamp=50, key="a", value=99.0))  # late: watermark is already 250
        self.assertEqual(pipeline.watermark.late_dropped, 1)
        results = pipeline.flush()
        self.assertEqual([result.value for result in results], [1.0])

    def test_punct_advances_time_without_a_value(self) -> None:
        pipeline = Pipeline(windowing=tumbling(100), aggregation="count")
        pipeline.add(Event(timestamp=10, key="a", value=1.0))
        emitted = pipeline.add(Event(timestamp=100, key="a", kind="punct"))
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0].value, 1.0)


class PipelineTests(unittest.TestCase):
    def test_results_are_ordered_and_stable(self) -> None:
        events = [
            Event(timestamp=100, key="b", value=2.0),
            Event(timestamp=50, key="a", value=1.0),
            Event(timestamp=150, key="a", value=4.0),
        ]
        # `max_out_of_orderness=200` is what makes event 50 usable: with 0 it would already be late
        # when it arrives, which is correct behaviour and a different test (see WatermarkTests).
        results = Pipeline(windowing=tumbling(100), aggregation="sum", max_out_of_orderness=200).run(events)
        self.assertEqual([(result.window.start, result.key) for result in results], [(0, "a"), (100, "a"), (100, "b")])

    def test_input_order_does_not_change_output_within_the_disorder_bound(self) -> None:
        events = [Event(timestamp=value, key="a", value=float(value)) for value in (10, 20, 30, 110)]
        forward = Pipeline(windowing=tumbling(100), aggregation="sum", max_out_of_orderness=200).run(events)
        backward = Pipeline(windowing=tumbling(100), aggregation="sum", max_out_of_orderness=200).run(list(reversed(events)))
        self.assertEqual([result.to_document() for result in forward], [result.to_document() for result in backward])

    def test_order_does_change_output_when_disorder_exceeds_the_bound(self) -> None:
        # The honest statement of the invariant: it holds only while the disorder is covered.
        events = [Event(timestamp=value, key="a", value=float(value)) for value in (10, 20, 30, 110)]
        forward = Pipeline(windowing=tumbling(100), aggregation="sum").run(events)
        backward = Pipeline(windowing=tumbling(100), aggregation="sum").run(list(reversed(events)))
        self.assertEqual(len(forward), 2)
        self.assertEqual(len(backward), 1)
        self.assertEqual(backward[0].value, 110.0)

    def test_unknown_aggregation_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            Pipeline(windowing=tumbling(10), aggregation="median")

    def test_window_requires_positive_length(self) -> None:
        with self.assertRaises(WindowError):
            Window(10, 10)


if __name__ == "__main__":
    unittest.main()
