"""Pruebas nuevas: una con TestPipeline y dos temporales con TestStream.

TestStream solo respeta el watermark si el pipeline corre con `streaming=True`.
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.windowed_value import PaneInfoTiming

BASE = 1_784_898_000  # 2026-07-24T13:00:00Z en segundos epoch


def streaming_options() -> PipelineOptions:
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    return options


class PaneSummary(beam.DoFn):
    """Resumir cada pane como (clave, total, timing, índice)."""

    def process(self, element, pane=beam.DoFn.PaneInfoParam):
        key, total = element
        yield key, total, PaneInfoTiming.to_string(pane.timing), pane.index


def test_pipeline_out_of_order_events_use_event_time_windows(solution):
    """TestPipeline: el orden de la lista no cambia la ventana y PENDING no suma."""
    events = [
        {
            "event_id": "p-2",
            "merchant_id": "m-a",
            "event_time": "2026-07-24T13:01:10Z",
            "arrival_time": "2026-07-24T13:01:11Z",
            "amount": 20,
            "status": "CONFIRMED",
        },
        {
            "event_id": "p-1",
            "merchant_id": "m-a",
            "event_time": "2026-07-24T13:00:50Z",
            "arrival_time": "2026-07-24T13:01:30Z",
            "amount": 10,
            "status": "CONFIRMED",
        },
        {
            "event_id": "p-3",
            "merchant_id": "m-a",
            "event_time": "2026-07-24T13:00:20Z",
            "arrival_time": "2026-07-24T13:00:21Z",
            "amount": 99,
            "status": "PENDING",
        },
    ]
    with BeamTestPipeline() as pipeline:
        output = solution.build_windowed_totals_pipeline(pipeline, events, window_seconds=60)
        assert_that(
            output,
            equal_to(
                [
                    {
                        "merchant_id": "m-a",
                        "window_start": "2026-07-24T13:00:00+00:00",
                        "window_end": "2026-07-24T13:01:00+00:00",
                        "total": 10,
                    },
                    {
                        "merchant_id": "m-a",
                        "window_start": "2026-07-24T13:01:00+00:00",
                        "window_end": "2026-07-24T13:02:00+00:00",
                        "total": 20,
                    },
                ]
            ),
        )


def test_late_event_within_lateness_emits_accumulating_late_pane(solution):
    """TestStream: late aceptado. Pane ON_TIME con 100 y luego LATE acumulado con 150."""
    stream = (
        BeamTestStream()
        .advance_watermark_to(Timestamp(0))
        .add_elements([TimestampedValue(("m-azul", 100), Timestamp(10))])
        .advance_watermark_to(Timestamp(70))  # cierra la ventana [0, 60)
        .add_elements([TimestampedValue(("m-azul", 50), Timestamp(20))])  # llega tarde
        .advance_watermark_to_infinity()
    )
    with BeamTestPipeline(options=streaming_options()) as pipeline:
        panes = (
            pipeline
            | stream
            | solution.build_trigger_policy(window_seconds=60, allowed_lateness_seconds=120)
            | beam.CombinePerKey(sum)
            | beam.ParDo(PaneSummary())
        )
        assert_that(
            panes,
            equal_to([("m-azul", 100, "ON_TIME", 0), ("m-azul", 150, "LATE", 1)]),
        )


def test_duplicate_after_window_end_is_still_deduplicated(solution):
    """TestStream: el timer vive hasta window.end + lateness.

    El reenvío de p-002 llega con el watermark en 13:01:41, pasado el cierre
    13:01:00. Si el timer limpiara el estado en window.end, se emitiría otra vez.
    """
    payment = {"event_id": "p-002"}
    stream = (
        BeamTestStream()
        .advance_watermark_to(Timestamp(BASE))
        .add_elements([TimestampedValue(("m-verde", payment), Timestamp(BASE + 18))])
        .advance_watermark_to(Timestamp(BASE + 101))
        .add_elements([TimestampedValue(("m-verde", payment), Timestamp(BASE + 18))])
        .advance_watermark_to_infinity()
    )
    with BeamTestPipeline(options=streaming_options()) as pipeline:
        output = (
            pipeline
            | stream.with_output_types(tuple[str, dict])
            | beam.WindowInto(beam.window.FixedWindows(60), allowed_lateness=120)
            | beam.ParDo(solution.DeduplicatePayments(allowed_lateness_seconds=120))
            | beam.Map(lambda item: (item[0], item[1]["event_id"]))
        )
        assert_that(output, equal_to([("m-verde", "p-002")]))
