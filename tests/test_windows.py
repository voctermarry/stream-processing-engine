"""Window assignment, merging, and watermark behaviour."""

from __future__ import annotations

import unittest

from stream_processing import Event, WatermarkTracker, session, sliding, tumbling
from stream_processing.errors import ValidationError, WindowError
from stream_processing.pipeline import AGGREGATORS, Pipeline
from stream_processing.windows import Window, merge_sessions


def data(timestamp: int, key: str, value: float = 1.0) -> Event:
    return Event(timestamp=timestamp, key=key, value=value, kind="data")


def punct(timestamp: int) -> Event:
    return Event(timestamp=timestamp, key="clock", value=0.0, kind="punct")


def run_session(events, gap, *, aggregation="sum", max_out_of_orderness=0, allowed_lateness=0):
    pipeline = Pipeline(
        windowing=session(gap),
        aggregation=aggregation,
        max_out_of_orderness=max_out_of_orderness,
        allowed_lateness=allowed_lateness,
    )
    emitted: list = []
    for event in events:
        emitted.extend(pipeline.add(event))
    emitted.extend(pipeline.flush())
    return emitted, pipeline


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

    def test_event_timestamp_gap_equal_to_gap_merges(self) -> None:
        # Events arrive as half-open point windows [t, t+1): t=0 and t=10 differ by exactly gap.
        merged = merge_sessions([Window(0, 1), Window(10, 11)], gap=10)
        self.assertEqual(merged, [Window(0, 11)])

    def test_event_timestamp_gap_gap_plus_one_stays_split(self) -> None:
        # The fixed boundary: a timestamp difference of gap+1 is one beyond the allowed maximum, so
        # the two point windows [t, t+1) must never merge.
        merged = merge_sessions([Window(0, 1), Window(11, 12)], gap=10)
        self.assertEqual(merged, [Window(0, 1), Window(11, 12)])

    def test_identical_timestamps_share_one_session(self) -> None:
        merged = merge_sessions([Window(5, 6), Window(5, 6)], gap=1)
        self.assertEqual(merged, [Window(5, 6)])

    def test_gap_one_boundary(self) -> None:
        merged_touching = merge_sessions([Window(0, 1), Window(1, 2)], gap=1)
        self.assertEqual(merged_touching, [Window(0, 2)])
        merged_apart = merge_sessions([Window(0, 1), Window(2, 3)], gap=1)
        self.assertEqual(merged_apart, [Window(0, 1), Window(2, 3)])

    def test_negative_and_cross_zero_timestamps_merge_by_timestamp_distance(self) -> None:
        merged = merge_sessions([Window(-3, -2), Window(-2, -1), Window(0, 1), Window(1, 2)], gap=2)
        self.assertEqual(merged, [Window(-3, 2)])
        split = merge_sessions([Window(-3, -2), Window(0, 1)], gap=2)
        self.assertEqual(split, [Window(-3, -2), Window(0, 1)])

    def test_invalid_gap_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            merge_sessions([Window(0, 1)], gap=0)


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


class SessionPipelineTests(unittest.TestCase):
    """End-to-end session semantics through the public Pipeline: boundary, emission, bridging."""

    def test_adjacent_events_merging_at_gap_and_splitting_at_gap_plus_one(self) -> None:
        merging, _ = run_session([data(0, "a", 1.0), data(10, "a", 2.0)], gap=10)
        self.assertEqual([(r.window.start, r.window.end, r.value, r.count) for r in merging], [(0, 11, 3.0, 2)])
        splitting, _ = run_session([data(0, "a", 1.0), data(11, "a", 2.0)], gap=10)
        self.assertEqual(
            [(r.window.start, r.window.end, r.value, r.count) for r in splitting],
            [(0, 1, 1.0, 1), (11, 12, 2.0, 1)],
        )

    def test_half_open_window_spans_earliest_to_latest_plus_one(self) -> None:
        results, _ = run_session([data(4, "a", 1.0), data(14, "a", 2.0)], gap=10)
        self.assertEqual([(r.window.start, r.window.end) for r in results], [(4, 15)])

    def test_session_closes_only_when_watermark_reaches_last_plus_gap_plus_one(self) -> None:
        # A still-on-time event could sit exactly on the merge boundary at lastTimestamp+gap, so the
        # session stays open through that watermark and fires at lastTimestamp+gap+1.
        pipeline = Pipeline(windowing=session(10), aggregation="sum")
        pipeline.add(data(0, "a", 1.0))
        self.assertEqual([(r.window.start, r.window.end) for r in pipeline.add(punct(10))], [])
        emitted = pipeline.add(punct(11))
        self.assertEqual([(r.window.start, r.window.end) for r in emitted], [(0, 1)])

    def test_allowed_lateness_delays_closing_by_the_same_amount(self) -> None:
        pipeline = Pipeline(windowing=session(10), aggregation="sum", allowed_lateness=5)
        pipeline.add(data(0, "a", 1.0))
        self.assertEqual(pipeline.add(punct(15)), [])
        self.assertEqual([(r.window.start, r.window.end) for r in pipeline.add(punct(16))], [(0, 1)])

    def test_a_data_event_and_a_punct_close_at_the_same_watermark(self) -> None:
        punct_pipeline = Pipeline(windowing=session(10), aggregation="sum")
        punct_pipeline.add(data(0, "a", 1.0))
        via_punct = punct_pipeline.add(punct(11))
        # A data event on a different key pushes the watermark to the same position 11.
        data_pipeline = Pipeline(windowing=session(10), aggregation="sum")
        data_pipeline.add(data(0, "a", 1.0))
        via_data = data_pipeline.add(data(11, "b", 7.0))
        self.assertEqual(
            [(r.window.start, r.window.end, r.key) for r in via_punct],
            [(r.window.start, r.window.end, r.key) for r in via_data if r.key == "a"],
        )

    def test_keys_are_always_isolated(self) -> None:
        # Both t=0 events land before either t=11 advances the watermark past them, so nothing is
        # late; same-boundary sessions for distinct keys must still never merge together.
        results, _ = run_session(
            [data(0, "a", 1.0), data(0, "b", 3.0), data(11, "a", 2.0), data(11, "b", 4.0)], gap=10
        )
        by_key = {}
        for result in results:
            by_key.setdefault(result.key, []).append((result.window.start, result.window.end, result.value))
        self.assertEqual(by_key["a"], [(0, 1, 1.0), (11, 12, 2.0)])
        self.assertEqual(by_key["b"], [(0, 1, 3.0), (11, 12, 4.0)])

    def test_a_timely_bridge_joins_two_open_sessions_counting_each_value_once(self) -> None:
        # 0 and 10 are gap-1 apart; the later-arriving event at 5 (still on time) links them into
        # one session. The merge must not double-count any value.
        results, pipeline = run_session(
            [data(0, "a", 1.0), data(10, "a", 100.0), data(5, "a", 10.0)],
            gap=10,
            max_out_of_orderness=10,
        )
        self.assertEqual([(r.window.start, r.window.end, r.value, r.count) for r in results], [(0, 11, 111.0, 3)])
        self.assertEqual(pipeline.watermark.late_dropped, 0)

    def test_bridging_is_independent_of_arrival_order(self) -> None:
        events = [data(0, "a", 1.0), data(10, "a", 100.0), data(5, "a", 10.0)]
        first, _ = run_session(events, gap=10, max_out_of_orderness=10)
        second, _ = run_session(list(reversed(events)), gap=10, max_out_of_orderness=10)
        signature = [(r.window.start, r.window.end, r.value, r.count) for r in first]
        self.assertEqual(signature, [(r.window.start, r.window.end, r.value, r.count) for r in second])

    def test_a_late_event_is_dropped_and_cannot_reopen_an_emitted_session(self) -> None:
        pipeline = Pipeline(windowing=session(10), aggregation="sum")
        pipeline.add(data(0, "a", 1.0))
        emitted = pipeline.add(punct(11))  # [0,1) is now committed
        self.assertEqual([(r.window.start, r.window.end) for r in emitted], [(0, 1)])
        after = pipeline.add(data(5, "a", 99.0))  # below watermark 11: late, must not reopen
        self.assertEqual(after, [])
        self.assertEqual(pipeline.watermark.late_dropped, 1)
        self.assertEqual([(r.window.start, r.window.end, r.value) for r in pipeline.flush()], [])

    def test_identical_timestamps_merge_and_count_both_values(self) -> None:
        results, _ = run_session([data(5, "a", 3.0), data(5, "a", 4.0)], gap=1)
        self.assertEqual([(r.window.start, r.window.end, r.value, r.count) for r in results], [(5, 6, 7.0, 2)])

    def test_negative_and_cross_zero_timestamps_follow_the_same_boundary(self) -> None:
        merged, _ = run_session(
            [data(-3, "a", 1.0), data(-2, "a", 1.0), data(0, "a", 1.0), data(1, "a", 1.0)], gap=2
        )
        self.assertEqual([(r.window.start, r.window.end, r.count) for r in merged], [(-3, 2, 4)])
        split, _ = run_session([data(-3, "a", 1.0), data(0, "a", 1.0)], gap=2)
        self.assertEqual([(r.window.start, r.window.end) for r in split], [(-3, -2), (0, 1)])

    def test_every_aggregation_aggregates_only_its_own_session(self) -> None:
        events = [data(0, "a", 2.0), data(1, "a", 4.0), data(20, "a", 8.0)]
        expected = {
            "count": ([(0, 2, 2.0, 2), (20, 21, 1.0, 1)]),
            "sum": ([(0, 2, 6.0, 2), (20, 21, 8.0, 1)]),
            "min": ([(0, 2, 2.0, 2), (20, 21, 8.0, 1)]),
            "max": ([(0, 2, 4.0, 2), (20, 21, 8.0, 1)]),
            "mean": ([(0, 2, 3.0, 2), (20, 21, 8.0, 1)]),
        }
        for aggregation, signature in expected.items():
            with self.subTest(aggregation=aggregation):
                results, _ = run_session(events, gap=10, aggregation=aggregation)
                self.assertEqual(
                    [(r.window.start, r.window.end, r.value, r.count) for r in results], signature
                )
        self.assertEqual(sorted(AGGREGATORS), ["count", "max", "mean", "min", "sum"])


if __name__ == "__main__":
    unittest.main()
