import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook es un esqueleto. Las celdas de código contienen firmas,
    contratos y excepciones `NotImplementedError`; no incluyen la solución.

    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
        if not isinstance(raw_value, str) or not raw_value.endswith("Z"):
            raise ValueError(f"Timestamp invalido: {raw_value}")
        return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))


    return (parse_utc,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    Completá `parse_utc`.

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores inválidos con una excepción clara.

    Después, usá esa función cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""
        import math
        from datetime import UTC

        epoch_seconds = timestamp.timestamp()
        start_seconds = math.floor(epoch_seconds / size_seconds) * size_seconds
        start = datetime.fromtimestamp(start_seconds, tz=UTC)
        end = datetime.fromtimestamp(start_seconds + size_seconds, tz=UTC)
        return start, end

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar `(totals, audit)`.

        Cada fila de `totals` debe contener `merchant_id`, `window_start`,
        `window_end` y `total`; los límites de ventana se expresan como strings
        ISO-8601.

        Cada fila de `audit` debe contener `event_id`, `merchant_id`,
        `delay_seconds`, `duplicate`, `too_late`, `accepted`, `revision` y
        `reason`. `revision` es verdadero cuando un evento aceptado llega
        después del cierre de su ventana.
        """
        from collections import defaultdict

        totals = defaultdict(int)
        seen = set()
        audit = []

        for event in sorted(events, key=lambda item: parse_utc(item["arrival_time"])):
            event_dt = parse_utc(event["event_time"])
            arrival_dt = parse_utc(event["arrival_time"])
            delay_seconds = int((arrival_dt - event_dt).total_seconds())
            window_start, window_end = assign_fixed_window(event_dt, window_seconds)

            duplicate_key = (event["merchant_id"], event["event_id"])
            is_duplicate = duplicate_key in seen
            is_too_late = delay_seconds > allowed_lateness_seconds
            is_confirmed = event.get("status") == "CONFIRMED"

            accepted = (
                is_confirmed
                and not is_too_late
                and not (deduplicate and is_duplicate)
            )

            revision = bool(accepted and (arrival_dt >= window_end))

            if is_duplicate and deduplicate:
                reason = "duplicate"
            elif is_too_late:
                reason = "too_late"
            elif not is_confirmed:
                reason = str(event.get("status", "REJECTED")).lower()
            else:
                reason = "accepted"

            if accepted:
                key = (event["merchant_id"], window_start.isoformat(), window_end.isoformat())
                totals[key] += event["amount"]
                seen.add(duplicate_key)
            audit.append({
                "event_id": event["event_id"],
                "merchant_id": event["merchant_id"],
                "delay_seconds": delay_seconds,
                "duplicate": is_duplicate,
                "too_late": is_too_late,
                "accepted": accepted,
                "revision": revision,
                "reason": reason,
            })

        result_totals = [
            {"merchant_id": m, "window_start": ws, "window_end": we, "total": tot}
            for (m, ws, we), tot in sorted(totals.items())
        ]
        return result_totals, audit


    return (summarize_payments,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    Implementá `assign_fixed_window` y `summarize_payments`.

    Esta versión pura de Python funciona como oráculo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditoría conserva la razón de cada decisión;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para la configuración por defecto, documentá cuántos eventos entran,
    cuántos se aceptan y cuántos totales se producen.
    """)
    return


@app.cell
def _(mo, simulate_sink_retries, summarize_payments):
    import json

    _path = mo.notebook_dir() / "data" / "payments.jsonl"
    _events = [
        json.loads(_line)
        for _line in _path.read_text(encoding="utf-8").splitlines()
        if _line.strip()
    ]
    _totals, _audit = summarize_payments(_events)
    _materialized, _sink_audit = simulate_sink_retries(_totals, attempts=2, idempotent=True)
    resumen = {
        "total_eventos": len(_events),
        "aceptados": sum(r["accepted"] for r in _audit),
        "ventanas": len(_totals),
        "monto": sum(r["total"] for r in _totals),
        "intentos": len(_sink_audit),
        "materializados": len(_materialized),
    }
    return (resumen,)


@app.cell(hide_code=True)
def _(mo, resumen):
    mo.md(
        f"""
        ### Resumen sobre `data/payments.jsonl` (lateness 120 s)

        - Eventos leídos: {resumen['total_eventos']}
        - Eventos aceptados: {resumen['aceptados']}
        - Totales por ventana: {resumen['ventanas']}
        - Monto confirmado: Gs. {resumen['monto']:,}
        - Intentos de escritura: {resumen['intentos']}
        - Filas en el sink UPSERT: {resumen['materializados']}
        """
    )
    return


@app.cell
def _(Any, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        class FormatWindowedTotal(beam.DoFn):
            def process(self, element, window=beam.DoFn.WindowParam):
                from datetime import UTC, datetime
    
                merchant_id, total = element
                start_dt = datetime.fromtimestamp(float(window.start), tz=UTC)
                end_dt = datetime.fromtimestamp(float(window.end), tz=UTC)
                yield {
                    "merchant_id": merchant_id,
                    "window_start": start_dt.isoformat(),
                    "window_end": end_dt.isoformat(),
                    "total": total,
                }
        return (
        pipeline
        | "CreateEvents" >> beam.Create(events)
        | "AssignTimestamps" >> beam.Map(
            lambda p: beam.window.TimestampedValue(p, parse_utc(p["event_time"]).timestamp())
        )
        | "FilterConfirmed" >> beam.Filter(lambda p: p.get("status") == "CONFIRMED")
        | "ApplyFixedWindows" >> beam.WindowInto(beam.window.FixedWindows(window_seconds))
        | "KeyByMerchant" >> beam.Map(lambda p: (p["merchant_id"], p["amount"]))
        | "SumByMerchant" >> beam.CombinePerKey(sum)
        | "FormatTotals" >> beam.ParDo(FormatWindowedTotal())
    )



    return


@app.cell
def _(Any, SetStateSpec, StrUtf8Coder, TimeDomain, TimerSpec, beam, on_timer):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)
    
        def __init__(self, allowed_lateness_seconds: int = 120):
            super().__init__()
            self.allowed_lateness_seconds = allowed_lateness_seconds

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""

            from apache_beam.utils.timestamp import Duration

            merchant_id, payment = element
            event_id = payment["event_id"]
            if event_id in set(seen_ids.read()):
                return  # Duplicado detectado: no emitir nada
            seen_ids.add(event_id)
            expiry.set(window.end + Duration.of(self.allowed_lateness_seconds))
            yield merchant_id, payment

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()

    return


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming.

        Configurar un pane on-time por watermark, una estimación early por
        processing time, revisiones late y modo ACCUMULATING.
        """
   
        from apache_beam.transforms import trigger
        from apache_beam.utils.timestamp import Duration

        if not hasattr(Duration, "seconds"):
            Duration.seconds = property(lambda self: int(self.micros // 1_000_000))

        return beam.WindowInto(
            beam.window.FixedWindows(window_seconds),
            trigger=trigger.AfterWatermark(
                early=trigger.AfterProcessingTime(10),
                late=trigger.AfterCount(1),
            ),
            allowed_lateness=allowed_lateness_seconds,
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
        )

    return (build_trigger_policy,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    Completá:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los límites de ventana con `WindowParam`.

    Agregá pruebas con `TestPipeline` y al menos una prueba temporal con
    `TestStream` que evidencie un resultado late aceptado.

    ### Expiración

    Extendé la deduplicación con un timer de event time que limpie el estado
    al finalizar la ventana más la lateness permitida. Explicá por qué un
    estado sin expiración crece indefinidamente.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Expiración del estado

    La deduplicación guarda los `event_id` ya vistos en un `SetState` por comercio
    y ventana. En un flujo no acotado siempre llegan IDs nuevos: si nadie borra ese
    conjunto, crece para siempre, consume memoria del backend de estado y cada
    checkpoint se vuelve más pesado.

    Por eso el timer de event time vence en `window.end + allowed_lateness`:

    - **antes** de ese punto no se puede borrar, porque todavía puede llegar un
      duplicado tardío que hay que descartar;
    - **después**, la ventana ya no acepta eventos y guardar sus IDs no sirve.

    Beam también borra el estado de una ventana cuando el watermark supera
    `window.end + allowed_lateness`. El timer explícito hace visible esa
    política y es imprescindible en ventanas que nunca cierran, como la global.
    """)
    return


@app.cell
def _(beam, build_trigger_policy, mo):
    from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
    from apache_beam.testing.test_pipeline import TestPipeline
    from apache_beam.testing.test_stream import TestStream
    from apache_beam.testing.util import assert_that, equal_to
    from apache_beam.utils.timestamp import Timestamp
    from apache_beam.utils.windowed_value import PaneInfoTiming


    class _PaneSummary(beam.DoFn):
        def process(self, element, pane=beam.DoFn.PaneInfoParam):
            _key, _total = element
            yield _key, _total, PaneInfoTiming.to_string(pane.timing), pane.index


    # Un evento a tiempo, el watermark cierra [0, 60) y después llega uno tardío.
    _stream = (
        TestStream()
        .advance_watermark_to(Timestamp(0))
        .add_elements([beam.window.TimestampedValue(("m-azul", 100), Timestamp(10))])
        .advance_watermark_to(Timestamp(70))
        .add_elements([beam.window.TimestampedValue(("m-azul", 50), Timestamp(20))])
        .advance_watermark_to_infinity()
    )
    _options = PipelineOptions()
    _options.view_as(StandardOptions).streaming = True  # sin esto TestStream ignora el watermark
    _expected = [("m-azul", 100, "ON_TIME", 0), ("m-azul", 150, "LATE", 1)]

    with TestPipeline(options=_options) as _p:
        _panes = (
            _p
            | _stream
            | build_trigger_policy(window_seconds=60, allowed_lateness_seconds=120)
            | beam.CombinePerKey(sum)
            | beam.ParDo(_PaneSummary())
        )
        assert_that(_panes, equal_to(_expected))

    mo.md(
        """
        **TestStream OK.** Panes emitidos para `m-azul`, ventana [0, 60):

        | Pane | Timing | Total acumulado |
        |---|---|---:|
        | 0 | ON_TIME | 100 |
        | 1 | LATE | 150 |

        El evento de 50 llegó con el watermark en 70 (después del cierre en 60),
        pero dentro de los 120 s de lateness: se aceptó y generó una revisión
        acumulativa.
        """
    )
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        return f"{result['merchant_id']}|{result['window_start']}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente, múltiples intentos del mismo resultado deben dejar
        una sola fila materializada. En modo append, cada intento agrega una.
        """
    
        append_sink = []
        upsert_sink = {}
        audit = []

        for result in results:
            key = make_idempotency_key(result)
            for attempt_number in range(1, attempts + 1):
                row = {
                    "idempotency_key": key,
                    "attempt": attempt_number,
                    **result,
                }
                operation = "UPSERT" if idempotent else "POST"
                if idempotent:
                    upsert_sink[key] = row
                else:
                    append_sink.append(row)
                audit.append({
                    "idempotency_key": key,
                    "attempt": attempt_number,
                    "operation": operation,
                })

        materialized = list(upsert_sink.values()) if idempotent else append_sink
        return materialized, audit

    return (simulate_sink_retries,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    Completá `make_idempotency_key` y `simulate_sink_retries`.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditoría. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lógica.

    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    Al comienzo deben fallar con `NotImplementedError`. Implementá las
    funciones hasta que estas garantías queden verdes:

    - [x] un duplicado no modifica el total;
    - [x] claves distintas no comparten estado;
    - [x] un evento fuera de orden cae en su ventana de evento;
    - [x] un evento con atraso aceptado produce una revisión;
    - [x] un evento demasiado tardío queda auditado;
    - [x] dos escrituras del mismo resultado dejan una sola entidad;
    - [x] el timer limpia el estado cuando corresponde.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Entrega

    Publicá un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación breve de ventanas, triggers, estado, timer e
       idempotencia; (Respuestas en el README)
    5. evidencia de ejecución y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |

    Se evalúa corrección conceptual y evidencia, no complejidad innecesaria.
    """)
    return


if __name__ == "__main__":
    app.run()
