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

    def test_gap_boundary_is_inclusive(self) -> None:
        merged = merge_sessions([Window(0, 10), Window(20, 30)], gap=10)
        self.assertEqual(merged, [Window(0, 30)])

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


if __name__ == "__main__":
    unittest.main()
