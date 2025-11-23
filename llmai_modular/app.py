import json
from datetime import datetime, timedelta
from fastapi import BackgroundTasks, FastAPI

from .clustering import cluster_events, parse_post_for_event
from .context import is_holiday, weather_summary
from .config import MIN_REPORTS_PER_WINDOW, WINDOW_MINUTES
from .incident_builder import alerts_by_line_station, incident_to_historial_dict, parsed_to_incident, summarize_by_interval, event_to_incident
from .models import LoteMineria
from .processing import (
    incident_from_summary,
    detect_line_with_ollama,
    is_relevant_incident,
    post_incidents_backend,
    post_to_incidents_from_file,
    procesar_lote,
    run_continuous,
    summarize_bucket_with_ollama,
)
from .simulation import ESCENARIOS_DEMO
from .state import CONT_LOCK, CONT_STATE, HISTORIAL, HISTORIAL_LOCK, REALTIME_BUFFER, REALTIME_LOCK
from .utils import save_json, bucket_start, bucket_end

app = FastAPI(title="MetroCDMX Ollama Demo (modular)")


def _load_posts(file: str):
    with open(file, "r", encoding="utf-8") as f:
        return json.load(f)


def _sanitize_log(text: str) -> str:
    """Limpia caracteres no ascii para evitar errores de encoding en consola Windows."""
    if not isinstance(text, str):
        return ""
    return text.encode("ascii", "ignore").decode("ascii")


async def _process_ready_buckets(ready):
    """Procesa buckets listos: llama a Ollama, genera incidentes y envía al backend."""
    all_incidents = []
    ollama_summaries = []
    for start, line, station, bucket_posts in ready:
        print(f"[BUCKET] start={start.isoformat()} line={line} station={station} posts={len(bucket_posts)}")
        context = {"holiday": is_holiday(start), "weather": weather_summary(bucket_posts, start, bucket_end(start))}

        bucket_texts = [p.get("text", "") for p in bucket_posts if p.get("text")]
        summary = summarize_bucket_with_ollama(bucket_texts, line_hint=line)
        if summary:
            print(f"[OLLAMA] line={line} semaforo={summary.get('semaforo')} resumen={summary.get('resumen')}")
            ollama_summaries.append({"interval_start": start.isoformat(), "line": line, "station": station, **summary})
        else:
            print(f"[OLLAMA] line={line} sin resumen (None)")

        incident_payload = incident_from_summary(line, station, summary, start)
        all_incidents.append(incident_payload)

        safe_start = start.isoformat().replace(":", "-")
        safe_station = (station or "DESCONOCIDA").replace(" ", "_")
        save_json(
            {
                "interval_start": start.isoformat(),
                "interval_end": bucket_end(start).isoformat(),
                "line": line,
                "station": station,
                "posts": bucket_posts,
                "context": context,
                "ollama_summary": summary,
                "incident_payload": incident_payload,
            },
            f"bucket_rt_{safe_start}_{line.replace(' ', '_')}_{safe_station}.json",
        )

        end_dt = bucket_end(start)
        remaining = max(0, (end_dt - datetime.utcnow()).total_seconds())
        print(f"[VENTANA] {start.isoformat()} - {end_dt.isoformat()} linea={line} restante: {remaining:.1f}s")

    async with HISTORIAL_LOCK:
        for inc in all_incidents:
            HISTORIAL.append(incident_to_historial_dict(inc))

    backend_results = post_incidents_backend(all_incidents) if all_incidents else {"sent": False, "results": []}
    if all_incidents:
        print(f"[BACKEND] enviados={len(all_incidents)} resultados={backend_results}")
    return all_incidents, ollama_summaries, backend_results


@app.post("/ingestar-datos/")
async def recibir_datos(lote: LoteMineria, background_tasks: BackgroundTasks):
    background_tasks.add_task(procesar_lote, lote)
    return {"status": "ok", "mensaje": "Procesando en segundo plano..."}


@app.get("/historial/")
async def ver_historial():
    try:
        async with HISTORIAL_LOCK:
            return {"historial": list(reversed(HISTORIAL[-50:]))}
    except Exception as e:
        return {"error": str(e)}


@app.post("/test-json/")
async def probar_con_json(file: str = "export_data.json", limit: int = 50):
    try:
        posts = _load_posts(file)
    except Exception as e:
        return {"error": f"No pude leer {file}: {e}"}

    incidents, usable_posts = post_to_incidents_from_file(posts)
    subset = incidents[: limit if limit > 0 else len(incidents)]
    return {
        "file": file,
        "total_posts": len(posts),
        "usable_posts": len(usable_posts),
        "count": len(incidents),
        "returned": len(subset),
        "incidents": [inc.dict() for inc in subset],
    }


@app.post("/test-json-intervals/")
async def probar_intervalos(file: str = "export_data.json", limit_intervals: int = 50):
    try:
        posts = _load_posts(file)
    except Exception as e:
        return {"error": f"No pude leer {file}: {e}"}

    incidents, usable_posts = post_to_incidents_from_file(posts)
    summaries = summarize_by_interval(incidents)
    subset = summaries[: limit_intervals if limit_intervals > 0 else len(summaries)]

    return {
        "file": file,
        "total_posts": len(posts),
        "usable_posts": len(usable_posts),
        "intervals_total": len(summaries),
        "intervals_returned": len(subset),
        "intervals": subset,
    }


@app.post("/test-json-alertas/")
async def probar_alertas(file: str = "export_data.json", min_count: int = 5, limit_alerts: int = 100):
    try:
        posts = _load_posts(file)
    except Exception as e:
        return {"error": f"No pude leer {file}: {e}"}

    incidents, usable_posts = post_to_incidents_from_file(posts)
    alerts = alerts_by_line_station(incidents, min_count=min_count)
    subset = alerts[: limit_alerts if limit_alerts > 0 else len(alerts)]

    return {
        "file": file,
        "total_posts": len(posts),
        "usable_posts": len(usable_posts),
        "alerts_total": len(alerts),
        "alerts_returned": len(subset),
        "min_count": min_count,
        "alerts": subset,
    }


@app.post("/test-json-eventos/")
async def probar_eventos(
    file: str = "export_data.json",
    min_reports: int = 2,
    min_authors: int = 2,
    limit_events: int = 100,
    similarity: float = 0.72,
):
    try:
        posts = _load_posts(file)
    except Exception as e:
        return {"error": f"No pude leer {file}: {e}"}

    usable_posts = [p for p in posts if (p.get("texto") or p.get("text") or p.get("mensaje"))]
    parsed = [parse_post_for_event(p) for p in usable_posts]
    events = cluster_events(parsed, min_reports=min_reports, min_authors=min_authors, similarity=similarity)
    subset = events[: limit_events if limit_events > 0 else len(events)]

    return {
        "file": file,
        "total_posts": len(posts),
        "usable_posts": len(usable_posts),
        "events_total": len(events),
        "events_returned": len(subset),
        "min_reports": min_reports,
        "min_authors": min_authors,
        "similarity": similarity,
        "events": subset,
    }


@app.post("/continuous-start/")
async def continuous_start(
    background_tasks: BackgroundTasks,
    file: str = "export_data.json",
    min_reports: int = 2,
    min_authors: int = 2,
    similarity: float = 0.72,
    delay_seconds: float = 0.0,
):
    async with CONT_LOCK:
        if CONT_STATE["running"]:
            return {"status": "already_running", "file": CONT_STATE["file"], "started_at": CONT_STATE["started_at"]}
        CONT_STATE.update(
            {
                "running": True,
                "file": file,
                "started_at": datetime.utcnow().isoformat(),
                "ended_at": None,
                "buckets": [],
                "error": None,
            }
        )

    background_tasks.add_task(run_continuous, file, min_reports, min_authors, similarity, delay_seconds)
    return {
        "status": "started",
        "file": file,
        "min_reports": min_reports,
        "min_authors": min_authors,
        "similarity": similarity,
        "delay_seconds": delay_seconds,
    }


@app.get("/continuous-status/")
async def continuous_status():
    async with CONT_LOCK:
        data = CONT_STATE.copy()
        data["buckets_count"] = len(CONT_STATE.get("buckets", []))
        return data


def _normalize_reportes(payload) -> list:
    """
    Normaliza las distintas formas de entrada a una lista de reportes:
    - {"reportes": [...]}
    - {"data": [...]}
    - lista de objetos
    - un solo objeto
    - un string suelto (se envuelve como {"texto": <string>})
    """
    if isinstance(payload, dict):
        if isinstance(payload.get("reportes"), list):
            return payload.get("reportes")
        if isinstance(payload.get("data"), list):
            return payload.get("data")
        return [payload]
    if isinstance(payload, list):
        return payload
    if isinstance(payload, str):
        return [{"texto": payload}]
    return []


@app.post("/ingestar-realtime/")
async def ingestar_realtime(payload: dict):
    reportes = _normalize_reportes(payload)
    if not isinstance(reportes, list):
        return {"error": "Payload debe ser un objeto JSON con reportes"}
    if not reportes:
        return {"error": "No se recibieron reportes"}

    now_save = datetime.utcnow()
    safe_now = now_save.isoformat().replace(":", "-")
    save_json({"received_at": now_save.isoformat(), "raw_payload": payload}, f"incoming_rt_{safe_now}.json")
    print(f"[INGEST] reportes_recibidos={len(reportes)}")
    started_at = datetime.utcnow()
    parsed = [parse_post_for_event(p) for p in reportes]

    # Filtra por relevancia usando Ollama y luego completa linea si falta
    relevant = []
    skipped = 0
    for p in parsed:
        texto = p.get("text", "")
        if not is_relevant_incident(texto):
            skipped += 1
            continue
        if not p.get("line"):
            linea_ia = detect_line_with_ollama(texto)
            if linea_ia and linea_ia.upper().startswith("LINEA"):
                p["line"] = linea_ia
        relevant.append(p)

    if skipped:
        print(f"[INGEST] filtrados_no_relevantes={skipped}")

    parsed = relevant
    if not parsed:
        return {"status": "ok", "mensaje": "No hay reportes relevantes"}
    det_log = [
        _sanitize_log(f"{p.get('line') or 'DESCONOCIDA'}|{p.get('station') or ''}|{(p.get('text') or '')[:60]}")
        for p in parsed[:5]
    ]
    print(f"[INGEST] detecciones={det_log}")
    # Si llegan timestamps muy antiguos, ajustamos al now para que el contador de ventana tenga sentido
    now_ts = datetime.utcnow()
    for p in parsed:
        ts = p.get("timestamp")
        if isinstance(ts, datetime):
            if ts < now_ts - timedelta(minutes=WINDOW_MINUTES):
                p["timestamp"] = now_ts

    # Guardamos en buffer por bucket (ventana/linea)
    now = datetime.utcnow()
    async with REALTIME_LOCK:
        for p in parsed:
            start = bucket_start(p["timestamp"])
            line = p.get("line") or "DESCONOCIDA"
            station = p.get("station") or "DESCONOCIDA"
            key = (start, line, station)
            REALTIME_BUFFER.setdefault(key, []).append(p)

        # Seleccionamos buckets cuya ventana ya cerró
        ready = []
        for key, posts in list(REALTIME_BUFFER.items()):
            start, line, station = key
            if bucket_end(start) <= now:
                ready.append((start, line, station, posts))
                REALTIME_BUFFER.pop(key, None)

    # Log de estado de buffer por bucket (intervalo, linea, estacion, total)
    buffer_status = [
        f"{k[0].isoformat()}|{k[1]}|{k[2]} -> {len(v)}"
        for k, v in REALTIME_BUFFER.items()
    ]
    print(f"[INGEST] buckets_listos={len(ready)} en buffer={len(REALTIME_BUFFER)} detalle={buffer_status}")

    all_incidents, ollama_summaries, backend_results = await _process_ready_buckets(ready)

    ended_at = datetime.utcnow()
    elapsed_ms = int((ended_at - started_at).total_seconds() * 1000)

    return {
        "count": len(all_incidents),
        "buckets": len(ready),
        "incidents": all_incidents,
        "ollama_summaries": ollama_summaries,
        "backend_results": backend_results,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "elapsed_ms": elapsed_ms,
    }


@app.post("/simulacion-start/")
async def correr_simulacion(background_tasks: BackgroundTasks):
    print("INICIANDO SIMULACION DE 4 ESCENARIOS...")

    for escenario in ESCENARIOS_DEMO:
        lote_obj = LoteMineria(**escenario)
        background_tasks.add_task(procesar_lote, lote_obj)

    return {"mensaje": "Simulacion iniciada. Revisa la terminal para ver el progreso de la IA."}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
