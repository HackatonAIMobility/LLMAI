from datetime import datetime
from typing import List

from .models import Incident
from .utils import generate_incident_id, infer_priority, map_priority_enum, bucket_start, bucket_end


def incident_to_historial_dict(inc: dict) -> dict:
    return {
        "fecha": inc.get("Timestamp", datetime.utcnow().isoformat()),
        "linea": inc.get("Line") or inc.get("StartStation", ""),
        "semaforo": inc.get("Priority", "MESSAGE"),
        "resumen": inc.get("ShortDescription", ""),
    }


def post_to_incident(post: dict) -> Incident:
    text = post.get("texto") or post.get("text") or post.get("mensaje") or ""
    if not isinstance(text, str):
        text = ""
    title = text[:60].strip()
    short_desc = text[:120].strip()
    source = post.get("fuente") or post.get("source") or "desconocida"
    author = post.get("autor") or post.get("author") or "n/a"
    metadata = post.get("metadata") or {}
    meta_priority = metadata.get("prioridad") if isinstance(metadata, dict) else None
    meta_type = metadata.get("tipo") if isinstance(metadata, dict) else None
    meta_parts = []
    if meta_priority:
        meta_parts.append(f"prioridad declarada: {meta_priority}")
    if meta_type:
        meta_parts.append(f"tipo: {meta_type}")
    meta_suffix = f" [{' | '.join(meta_parts)}]" if meta_parts else ""
    description = f"{text} (fuente: {source}, autor: {author}){meta_suffix}"
    timestamp_raw = post.get("timestamp")
    try:
        ts = datetime.fromisoformat(timestamp_raw) if timestamp_raw else datetime.utcnow()
    except Exception:
        ts = datetime.utcnow()

    linea_meta = ""
    estacion_meta = ""
    if isinstance(metadata, dict):
        linea_meta = metadata.get("linea_detectada") or ""
        estacion_meta = metadata.get("estacion") or ""

    raw_id = post.get("_id") or post.get("id") or post.get("url") or post.get("uid")
    if not raw_id:
        raw_id = generate_incident_id(ts)
    else:
        raw_id = generate_incident_id(ts)

    start_station = estacion_meta or linea_meta or "DESCONOCIDA"
    line_val = linea_meta or "DESCONOCIDA"
    return Incident(
        Id=raw_id,
        Title=title,
        ShortDescription=short_desc,
        Description=description,
        Timestamp=ts,
        StartStation=start_station,
        EndStation=estacion_meta,
        Line=line_val,
        Priority=infer_priority(text, metadata),
    )


def summarize_by_interval(incidents: List[Incident]):
    buckets = {}
    for inc in incidents:
        start = bucket_start(inc.Timestamp)
        buckets.setdefault(start, []).append(inc)

    summaries = []
    for start, group in sorted(buckets.items()):
        counts = {"CRITICAL": 0, "WARNING": 0, "MESSAGE": 0}
        for g in group:
            counts[g.Priority] = counts.get(g.Priority, 0) + 1
        summaries.append(
            {
                "interval_start": start.isoformat(),
                "interval_end": bucket_end(start).isoformat(),
                "total": len(group),
                "critical": counts.get("CRITICAL", 0),
                "warning": counts.get("WARNING", 0),
                "message": counts.get("MESSAGE", 0),
                "examples": [g.Title for g in group[:3]],
            }
        )
    return summaries


def alerts_by_line_station(incidents: List[Incident], min_count: int = 5):
    buckets = {}
    for inc in incidents:
        start = bucket_start(inc.Timestamp)
        key = (start, inc.Line or inc.StartStation or "desconocida", inc.EndStation or "")
        buckets.setdefault(key, []).append(inc)

    alerts = []
    for (start, line, station), group in sorted(buckets.items()):
        if len(group) < min_count:
            continue
        counts = {"CRITICAL": 0, "WARNING": 0, "MESSAGE": 0}
        for g in group:
            counts[g.Priority] = counts.get(g.Priority, 0) + 1
        alerts.append(
            {
                "interval_start": start.isoformat(),
                "interval_end": bucket_end(start).isoformat(),
                "line": line,
                "station": station,
                "total": len(group),
                "critical": counts.get("CRITICAL", 0),
                "warning": counts.get("WARNING", 0),
                "message": counts.get("MESSAGE", 0),
                "examples": [g.Title for g in group[:3]],
            }
        )
    return alerts


def event_to_incident(event: dict, context: dict | None) -> dict:
    examples = event.get("examples") or []
    line = event.get("line") or ""
    station = event.get("station") or ""
    if examples:
        title = examples[0][:60]
    elif line or station:
        title = f"Reporte {line} {station}".strip()
    else:
        title = "Reporte Metro"
    short_desc = examples[0][:120] if examples else title

    extra_ctx = []
    if context:
        if context.get("holiday"):
            extra_ctx.append("dia festivo")
        weather = context.get("weather")
        if weather and weather.get("severity") in ("rain", "heat"):
            extra_ctx.append(f"clima:{weather.get('severity')}")

    desc_parts = [
        f"Linea: {line}" if line else "",
        f"Estacion: {station}" if station else "",
        f"Reportes: {event.get('total_reports', 0)}",
        f"Autores unicos: {event.get('unique_authors', 0)}",
        f"Prioridad cluster: {event.get('priority')}",
    ]
    if examples:
        desc_parts.append(f"Ejemplos: {' | '.join(examples[:3])}")
    if extra_ctx:
        desc_parts.append(f"Contexto: {', '.join(extra_ctx)}")
    description = " ".join([p for p in desc_parts if p])

    ts_raw = event.get("interval_start")
    try:
        ts = datetime.fromisoformat(ts_raw) if ts_raw else datetime.utcnow()
    except Exception:
        ts = datetime.utcnow()

    start_station = station or line or "DESCONOCIDA"
    line_val = line or "DESCONOCIDA"
    return {
        "Id": generate_incident_id(ts),
        "Title": title,
        "ShortDescription": short_desc,
        "Description": description,
        "Timestamp": ts.isoformat(),
        "StartStation": start_station,
        "EndStation": "",
        "Line": line_val,
        "Priority": map_priority_enum(event.get("priority", "MESSAGE")),
    }


def parsed_to_incident(parsed: dict, context: dict | None) -> dict:
    text = parsed.get("text", "") or ""
    line = parsed.get("line") or ""
    station = parsed.get("station") or ""
    source = parsed.get("source", "")
    meta_priority = parsed.get("metadata_priority")
    meta_type = parsed.get("metadata_type")
    title_fallback = f"Reporte {line} {station}".strip() or f"Reporte {source}".strip() or "Reporte Metro"
    title = text[:60] if text else title_fallback
    short_desc = text[:120] if text else title

    extra_ctx = []
    if context:
        if context.get("holiday"):
            extra_ctx.append("dia festivo")
        weather = context.get("weather")
        if weather and weather.get("severity") in ("rain", "heat"):
            extra_ctx.append(f"clima:{weather.get('severity')}")

    desc_parts = [
        f"Linea: {line}" if line else "",
        f"Estacion: {station}" if station else "",
        f"Fuente: {parsed.get('source', '')}",
        f"Autor: {parsed.get('author', '')}",
    ]
    if meta_priority:
        desc_parts.append(f"Prioridad declarada: {meta_priority}")
    if meta_type:
        desc_parts.append(f"Tipo: {meta_type}")
    if text:
        desc_parts.append(f"Texto: {text}")
    if extra_ctx:
        desc_parts.append(f"Contexto: {', '.join(extra_ctx)}")
    description = " ".join([x for x in desc_parts if x])

    ts = parsed.get("timestamp") or datetime.utcnow()
    start_station = station or line or "DESCONOCIDA"
    line_val = line or "DESCONOCIDA"
    return {
        "Id": generate_incident_id(ts if isinstance(ts, datetime) else None),
        "Title": title,
        "ShortDescription": short_desc,
        "Description": description,
        "Timestamp": ts.isoformat() if isinstance(ts, datetime) else ts,
        "StartStation": start_station,
        "EndStation": "",
        "Line": line_val,
        "Priority": map_priority_enum(parsed.get("priority", "MESSAGE")),
    }
