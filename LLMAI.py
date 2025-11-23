import asyncio
import json
import os
import re
import uuid
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import List, Literal, Tuple

import requests
from fastapi import BackgroundTasks, FastAPI
from pydantic import BaseModel
from threading import Lock

# API HTTP que expone los endpoints
app = FastAPI(title="MetroCDMX Ollama Demo")

# --- CONFIGURACION ---
# Se pueden sobrescribir con variables de entorno en Windows:
#   set OLLAMA_URL=http://otrohost:11434/api/generate
#   set OLLAMA_MODEL=llama3.2:3b
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
MODELO = os.getenv("OLLAMA_MODEL", "llama3.2:3b")
JSON_DIR = "jsons"

# --- ALMACENAMIENTO EN MEMORIA ---
# En Windows no tenemos SurrealDB en este entorno, asi que guardamos
# los registros en una lista en memoria (se pierde al cerrar el proceso).
HISTORIAL: List[dict] = []
HISTORIAL_LOCK = asyncio.Lock()
INCIDENT_COUNTER = 0
INCIDENT_LOCK = Lock()

# Estado para ejecuciones continuas (agrupadas por intervalos)
CONT_STATE = {
    "running": False,
    "file": None,
    "started_at": None,
    "ended_at": None,
    "buckets": [],  # lista de summaries por intervalo
    "error": None,
}
CONT_LOCK = asyncio.Lock()

# Lista básica de días festivos (se puede extender vía código si hace falta)
HOLIDAYS = {
    "01-01",  # Año Nuevo
    "02-05",  # Constitución
    "03-21",  # Benito Juárez
    "05-01",  # Día del Trabajo
    "09-16",  # Independencia
    "11-02",  # Día de Muertos (no oficial pero relevante)
    "11-20",  # Revolución
    "12-12",  # Guadalupe (no oficial)
    "12-25",  # Navidad
}


# --- MODELOS DE DATOS ---
class Comentario(BaseModel):
    texto: str
    timestamp: str


class LoteMineria(BaseModel):
    linea: str
    inicio: str
    fin: str
    datos: List[Comentario]

class ReporteEntrante(BaseModel):
    contenido: dict


class Incident(BaseModel):
    Id: str
    Title: str
    ShortDescription: str
    Description: str
    Timestamp: datetime
    StartStation: str
    EndStation: str
    Priority: Literal["MESSAGE", "WARNING", "CRITICAL"]
    Line: str = ""


# --- DATOS DE SIMULACION (HARDCODED) ---
ESCENARIOS_DEMO = [
    # ESCENARIO 1: Calma (10:00)
    {
        "linea": "Linea 3",
        "inicio": "10:00",
        "fin": "10:10",
        "datos": [
            {"texto": "La linea 3 va fluida hoy, milagro", "timestamp": "10:01"},
            {"texto": "llegue rapido a universidad", "timestamp": "10:05"},
            {"texto": "hay lugar sentado, aprovechen", "timestamp": "10:08"},
        ],
    },
    # ESCENARIO 2: Inicio de Lluvia (10:10)
    {
        "linea": "Linea 3",
        "inicio": "10:10",
        "fin": "10:20",
        "datos": [
            {
                "texto": "empezo a llover en potrero y ya le bajaron la velocidad",
                "timestamp": "10:12",
            },
            {"texto": "que onda con la lluvia, vamos a vuelta de rueda", "timestamp": "10:15"},
            {"texto": "cierren las ventanas que se mete el agua", "timestamp": "10:18"},
        ],
    },
    # ESCENARIO 3: Caos Total (10:20)
    {
        "linea": "Linea 3",
        "inicio": "10:20",
        "fin": "10:30",
        "datos": [
            {"texto": "NO MAMEN se inundo la via en indios verdes", "timestamp": "10:21"},
            {"texto": "nos bajaron del vagon, esta horrible", "timestamp": "10:22"},
            {"texto": "servicio detenido linea 3, busquen alternas", "timestamp": "10:25"},
            {"texto": "el agua llega al anden wtf", "timestamp": "10:28"},
        ],
    },
    # ESCENARIO 4: Recuperacion (10:30)
    {
        "linea": "Linea 3",
        "inicio": "10:30",
        "fin": "10:40",
        "datos": [
            {"texto": "ya avanza el tren, pero va llenisimo", "timestamp": "10:32"},
            {"texto": "al fin nos movemos, gracias a dios", "timestamp": "10:35"},
            {"texto": "sigue lento pero ya hay servicio", "timestamp": "10:38"},
        ],
    },
]


# --- LOGICA PRINCIPAL ---
async def procesar_lote(lote: LoteMineria):
    print(f"[IA] Analizando bloque de {lote.inicio}...")

    # 1. Sintesis con Ollama
    textos = "\n".join([f"- {d.texto}" for d in lote.datos])
    prompt = f"""
Analiza estos comentarios sobre la {lote.linea} del Metro CDMX.
COMENTARIOS:
{textos}

Instrucciones:
- Define un semaforo (ROJO/AMARILLO/VERDE) y un resumen corto.
- Responde solo en JSON plano, sin texto adicional.
Formato esperado:
{{
    "semaforo": "ROJO|AMARILLO|VERDE",
    "resumen": "texto corto",
    "incidente_confirmado": true|false
}}
"""

    try:
        # Llamada a la IA
        res_ia = requests.post(
            OLLAMA_URL,
            json={"model": MODELO, "prompt": prompt, "stream": False, "format": "json"},
            timeout=120,
        )
        res_ia.raise_for_status()

        # Validamos que Ollama no nos de basura
        resp_json = res_ia.json().get("response", "{}")
        analisis = json.loads(resp_json)

        # 2. Guardado en memoria
        registro = {
            "fecha": datetime.now().isoformat(),  # Fecha real de procesamiento
            "fecha_simulada": lote.inicio,  # Para saber que fue del simulacro
            "linea": lote.linea,
            "semaforo": analisis["semaforo"],
            "resumen": analisis["resumen"],
            "raw_data_count": len(lote.datos),
        }

        async with HISTORIAL_LOCK:
            HISTORIAL.append(registro)

        print(f"[LISTO] {lote.inicio}: {analisis['semaforo']} -> {analisis['resumen']}")

    except Exception as e:
        print(f"[ERROR] Procesando {lote.inicio}: {e}")


# --- ENDPOINTS ---
@app.post("/ingestar-datos/")
async def recibir_datos(lote: LoteMineria, background_tasks: BackgroundTasks):
    background_tasks.add_task(procesar_lote, lote)
    return {"status": "ok", "mensaje": "Procesando en segundo plano..."}


@app.get("/historial/")
async def ver_historial():
    try:
        async with HISTORIAL_LOCK:
            # Ultimos 50, en orden de mas reciente a mas antiguo
            return {"historial": list(reversed(HISTORIAL[-50:]))}
    except Exception as e:
        return {"error": str(e)}


def _infer_priority(texto: str) -> Literal["MESSAGE", "WARNING", "CRITICAL"]:
    lowered = texto.lower()
    crit_keywords = ["incend", "huele a quemado", "quemado", "inundo", "detenido", "no pasa nada", "peleand"]
    warn_keywords = ["lenta", "precaucion", "precauciones", "esperando", "no sirvan", "espera", "retras"]

    if any(k in lowered for k in crit_keywords):
        return "CRITICAL"
    if any(k in lowered for k in warn_keywords):
        return "WARNING"
    return "MESSAGE"


def _parse_ts(ts_raw):
    try:
        return datetime.fromisoformat(ts_raw) if ts_raw else None
    except Exception:
        return None


def _generate_incident_id(ts: datetime | None) -> str:
    """
    Construye un ID determinista basado en timestamp y un contador secuencial.
    """
    global INCIDENT_COUNTER
    if ts is None:
        ts = datetime.utcnow()
    with INCIDENT_LOCK:
        INCIDENT_COUNTER += 1
        seq = INCIDENT_COUNTER
    return f"{ts.strftime('%Y%m%d%H%M%S')}_{seq}"


def _ensure_json_dir():
    os.makedirs(JSON_DIR, exist_ok=True)


def _save_json_notification(payload: dict, filename: str):
    _ensure_json_dir()
    path = os.path.join(JSON_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _incident_to_historial_dict(inc: dict) -> dict:
    return {
        "fecha": inc.get("Timestamp", datetime.utcnow().isoformat()),
        "linea": inc.get("StartStation", ""),
        "semaforo": inc.get("Priority", "MESSAGE"),
        "resumen": inc.get("ShortDescription", ""),
    }


def _post_to_incident(post: dict) -> Incident:
    text = post.get("texto") or post.get("text") or post.get("mensaje") or ""
    if not isinstance(text, str):
        text = ""
    title = text[:60].strip()
    short_desc = text[:120].strip()
    # Construimos una descripcion que incluya la fuente y el autor para trazabilidad.
    source = post.get("fuente") or post.get("source") or "desconocida"
    author = post.get("autor") or post.get("author") or "n/a"
    description = f"{text} (fuente: {source}, autor: {author})"
    timestamp_raw = post.get("timestamp")
    try:
        ts = datetime.fromisoformat(timestamp_raw) if timestamp_raw else datetime.utcnow()
    except Exception:
        ts = datetime.utcnow()

    linea_meta = ""
    estacion_meta = ""
    metadata = post.get("metadata") or {}
    if isinstance(metadata, dict):
        linea_meta = metadata.get("linea_detectada") or ""
        estacion_meta = metadata.get("estacion") or ""

    raw_id = post.get("_id") or post.get("id") or post.get("url") or post.get("uid")
    if raw_id is None or raw_id == "":
        raw_id = _generate_incident_id(ts)
    else:
        # Aseguramos string, pero preferimos un id propio basado en timestamp+contador
        raw_id = _generate_incident_id(ts)

    return Incident(
        Id=raw_id,
        Title=title,
        ShortDescription=short_desc,
        Description=description,
        Timestamp=ts,
        StartStation=linea_meta or estacion_meta,
        EndStation=estacion_meta,
        Line=linea_meta,
        Priority=_infer_priority(text),
    )


def _bucket_start_10m(dt: datetime) -> datetime:
    """Devuelve el inicio del intervalo de 10 minutos para el timestamp dado."""
    return dt.replace(minute=(dt.minute // 10) * 10, second=0, microsecond=0)


def _summarize_by_interval(incidents: List[Incident]):
    """Agrupa incidentes en ventanas de 10 minutos y genera resumen."""
    buckets = {}
    for inc in incidents:
        start = _bucket_start_10m(inc.Timestamp)
        buckets.setdefault(start, []).append(inc)

    summaries = []
    for start, group in sorted(buckets.items()):
        counts = {"CRITICAL": 0, "WARNING": 0, "MESSAGE": 0}
        for g in group:
            counts[g.Priority] = counts.get(g.Priority, 0) + 1
        summaries.append(
            {
                "interval_start": start.isoformat(),
                "interval_end": (start + timedelta(minutes=10)).isoformat(),
                "total": len(group),
                "critical": counts.get("CRITICAL", 0),
                "warning": counts.get("WARNING", 0),
                "message": counts.get("MESSAGE", 0),
                "examples": [g.Title for g in group[:3]],
            }
        )
    return summaries


def _alerts_by_line_station(incidents: List[Incident], min_count: int = 5):
    """
    Agrupa incidentes por intervalo de 10 min y por (linea, estacion) y devuelve solo
    los grupos que superan min_count (para disparar alerta).
    """
    buckets = {}
    for inc in incidents:
        start = _bucket_start_10m(inc.Timestamp)
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
                "interval_end": (start + timedelta(minutes=10)).isoformat(),
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


# ----------------- PROTOTIPO: EVENTOS POR SIMILITUD -----------------
def _is_holiday(dt: datetime) -> bool:
    return dt.strftime("%m-%d") in HOLIDAYS


def _weather_summary(posts_raw: List[dict], start: datetime, end: datetime):
    conds = []
    temps = []
    for p in posts_raw:
        ts = _parse_ts(p.get("timestamp"))
        if not ts:
            continue
        if not (start <= ts < end):
            continue
        cond = p.get("condicion") or p.get("weather") or ""
        if cond:
            conds.append(cond)
        temp = p.get("temperatura")
        if isinstance(temp, (int, float)):
            temps.append(temp)

    if not conds and not temps:
        return None

    joined = " ".join(conds).lower()
    severity = "ok"
    if any(k in joined for k in ["tormenta", "lluvia", "inund", "granizo"]):
        severity = "rain"
    if any(k in joined for k in ["calor extremo", "ola de calor"]):
        severity = "heat"
    avg_temp = sum(temps) / len(temps) if temps else None

    return {
        "conditions": list(set(conds)),
        "avg_temp": avg_temp,
        "severity": severity,
    }


_LINE_RE = re.compile(r"(?:linea|l[íi]nea|l)\s*([0-9]{1,2}|[a-zA-Z])", re.IGNORECASE)
_STATION_RE = re.compile(r"(?:estacion|estaci[óo]n)\s+([a-zA-ZÁÉÍÓÚÑñ0-9]+)", re.IGNORECASE)


def _normalize_line_station(text: str, metadata: dict) -> Tuple[str, str]:
    line = ""
    station = ""
    if isinstance(metadata, dict):
        line = metadata.get("linea_detectada") or metadata.get("linea") or line
        station = metadata.get("estacion") or station

    if not line:
        m = _LINE_RE.search(text)
        if m:
            val = m.group(1).upper()
            line = f"LINEA {val}"

    if not station:
        m = _STATION_RE.search(text)
        if m:
            station = m.group(1)

    return line, station


def _classify_priority(text: str) -> Literal["CRITICAL", "WARNING", "MESSAGE"]:
    # Reutiliza inferencia básica pero con más keywords críticas.
    critical_terms = ["humo", "fuego", "incend", "evacuac", "choque", "desalojo", "sin avance", "detenido", "peleand", "bomba", "explosion"]
    warning_terms = ["lento", "retraso", "esperando", "lluvia", "marcha lenta", "sin servicio"]
    t = text.lower()
    if any(k in t for k in critical_terms):
        return "CRITICAL"
    if any(k in t for k in warning_terms):
        return "WARNING"
    return "MESSAGE"


def _parse_post_for_event(post: dict):
    text = post.get("texto") or post.get("text") or post.get("mensaje") or ""
    if not isinstance(text, str):
        text = ""
    metadata = post.get("metadata") or {}
    line, station = _normalize_line_station(text, metadata)
    source = post.get("fuente") or post.get("source") or "desconocida"
    author = post.get("autor") or post.get("author") or "anon"
    timestamp_raw = post.get("timestamp")
    try:
        ts = datetime.fromisoformat(timestamp_raw) if timestamp_raw else datetime.utcnow()
    except Exception:
        ts = datetime.utcnow()
    priority = _classify_priority(text)
    return {
        "text": text,
        "line": line or "",
        "station": station or "",
        "source": source,
        "author": author,
        "timestamp": ts,
        "priority": priority,
    }


def _cluster_events(posts: List[dict], min_reports: int = 2, min_authors: int = 2, similarity: float = 0.72):
    """
    Agrupa por intervalo de 10 min y (linea, estacion), luego hace clustering por similitud
    de texto dentro de cada grupo. Solo devuelve clusters que cumplan min_reports y min_authors.
    """
    buckets = {}
    for p in posts:
        start = _bucket_start_10m(p["timestamp"])
        key = (start, p.get("line") or "desconocida", p.get("station") or "")
        buckets.setdefault(key, []).append(p)

    events = []
    for (start, line, station), group in sorted(buckets.items()):
        clusters = []
        for item in group:
            placed = False
            for cl in clusters:
                sim = SequenceMatcher(None, item["text"], cl["repr"]).ratio()
                if sim >= similarity:
                    cl["items"].append(item)
                    cl["authors"].add(item["author"])
                    cl["repr"] = cl["repr"] if len(cl["repr"]) >= len(item["text"]) else item["text"]
                    cl["priority_counts"][item["priority"]] = cl["priority_counts"].get(item["priority"], 0) + 1
                    placed = True
                    break
            if not placed:
                clusters.append(
                    {
                        "repr": item["text"],
                        "items": [item],
                        "authors": {item["author"]},
                        "priority_counts": {item["priority"]: 1},
                    }
                )

        for cl in clusters:
            if len(cl["items"]) < min_reports or len(cl["authors"]) < min_authors:
                continue
            counts = cl["priority_counts"]
            highest = "MESSAGE"
            if counts.get("CRITICAL", 0) > 0:
                highest = "CRITICAL"
            elif counts.get("WARNING", 0) > 0:
                highest = "WARNING"

            events.append(
                {
                    "interval_start": start.isoformat(),
                    "interval_end": (start + timedelta(minutes=10)).isoformat(),
                    "line": line,
                    "station": station,
                    "total_reports": len(cl["items"]),
                    "unique_authors": len(cl["authors"]),
                    "priority": highest,
                    "priority_counts": counts,
                    "examples": [it["text"] for it in cl["items"][:3]],
                }
        )
    return events


def _map_priority_enum(priority: str) -> str:
    if priority == "CRITICAL":
        return "HIGH"
    if priority == "WARNING":
        return "MEDIUM"
    return "MESSAGE"


def _event_to_incident(event: dict, context: dict | None) -> dict:
    examples = event.get("examples") or []
    line = event.get("line") or ""
    station = event.get("station") or ""
    title = ""
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
            extra_ctx.append("día festivo")
        weather = context.get("weather")
        if weather and weather.get("severity") in ("rain", "heat"):
            extra_ctx.append(f"clima:{weather.get('severity')}")

    desc_parts = [
        f"Línea: {line}" if line else "",
        f"Estación: {station}" if station else "",
        f"Reportes: {event.get('total_reports', 0)}",
        f"Autores únicos: {event.get('unique_authors', 0)}",
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

    return {
        "Id": _generate_incident_id(ts),
        "Title": title,
        "ShortDescription": short_desc,
        "Description": description,
        "Timestamp": ts.isoformat(),
        "StartStation": station or line,
        "EndStation": "",
        "Priority": _map_priority_enum(event.get("priority", "MESSAGE")),
    }


def _parsed_to_incident(p: dict, context: dict | None) -> dict:
    text = p.get("text", "") or ""
    line = p.get("line") or ""
    station = p.get("station") or ""
    source = p.get("source", "")
    title_fallback = f"Reporte {line} {station}".strip() or f"Reporte {source}".strip() or "Reporte Metro"
    title = text[:60] if text else title_fallback
    short_desc = text[:120] if text else title

    extra_ctx = []
    if context:
        if context.get("holiday"):
            extra_ctx.append("día festivo")
        weather = context.get("weather")
        if weather and weather.get("severity") in ("rain", "heat"):
            extra_ctx.append(f"clima:{weather.get('severity')}")

    desc_parts = [
        f"Línea: {line}" if line else "",
        f"Estación: {station}" if station else "",
        f"Fuente: {p.get('source', '')}",
        f"Autor: {p.get('author', '')}",
    ]
    if text:
        desc_parts.append(f"Texto: {text}")
    if extra_ctx:
        desc_parts.append(f"Contexto: {', '.join(extra_ctx)}")
    description = " ".join([x for x in desc_parts if x])

    ts = p.get("timestamp") or datetime.utcnow()
    return {
        "Id": _generate_incident_id(ts if isinstance(ts, datetime) else None),
        "Title": title,
        "ShortDescription": short_desc,
        "Description": description,
        "Timestamp": ts.isoformat(),
        "StartStation": station or line,
        "EndStation": "",
        "Priority": _map_priority_enum(p.get("priority", "MESSAGE")),
    }


async def _run_continuous(file: str, min_reports: int, min_authors: int, similarity: float, delay_seconds: float):
    """
    Lee el archivo, lo procesa por intervalos de 10 minutos y va almacenando resúmenes
    en CONT_STATE["buckets"]. Opcionalmente espera delay_seconds entre intervalos para simular tiempo real.
    """
    async with CONT_LOCK:
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

    try:
        with open(file, "r", encoding="utf-8") as f:
            posts = json.load(f)
    except Exception as e:
        async with CONT_LOCK:
            CONT_STATE["running"] = False
            CONT_STATE["error"] = f"No pude leer {file}: {e}"
        return

    usable_posts = [p for p in posts if (p.get("texto") or p.get("text") or p.get("mensaje"))]
    parsed = [_parse_post_for_event(p) for p in usable_posts]
    # Ordenar por tiempo
    parsed.sort(key=lambda x: x["timestamp"])

    # Agrupar por bucket (inicio de 10 min)
    buckets = {}
    for p in parsed:
        start = _bucket_start_10m(p["timestamp"])
        buckets.setdefault(start, []).append(p)

    # Procesar cada bucket en orden cronológico
    for start in sorted(buckets.keys()):
        bucket_posts = buckets[start]
        events = _cluster_events(bucket_posts, min_reports=min_reports, min_authors=min_authors, similarity=similarity)
        weather_ctx = _weather_summary(posts, start, start + timedelta(minutes=10))
        context = {
            "holiday": _is_holiday(start),
            "weather": weather_ctx,
        }
        incidents = [_event_to_incident(ev, context) for ev in events]
        if not incidents and bucket_posts:
            # Si no hay clusters que cumplan el mínimo, devolvemos incidentes directos de los posts del intervalo
            incidents = [_parsed_to_incident(p, context) for p in bucket_posts]
        summary = {
            "interval_start": start.isoformat(),
            "interval_end": (start + timedelta(minutes=10)).isoformat(),
            "total_posts": len(bucket_posts),
            "events_total": len(events),
        "events": events,
        "incidents": incidents,
        "context": context,
    }
        async with CONT_LOCK:
            CONT_STATE["buckets"].append(summary)
        # Guardamos cada intervalo como JSON independiente
        safe_start = start.isoformat().replace(":", "-")
        # Guardamos solo la lista de incidentes en el json externo
        _save_json_notification({"interval_start": summary["interval_start"], "interval_end": summary["interval_end"], "incidents": incidents}, f"bucket_{safe_start}.json")
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)

    async with CONT_LOCK:
        CONT_STATE["running"] = False
        CONT_STATE["ended_at"] = datetime.utcnow().isoformat()


# ----------------- CONTEXTO (FESTIVOS / CLIMA) -----------------
def _load_context(context_file: str):
    if not context_file:
        return {"festivos": [], "clima": []}
    try:
        with open(context_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        festivos = data.get("festivos", [])
        clima = data.get("clima", [])
        return {"festivos": festivos, "clima": clima}
    except Exception:
        return {"festivos": [], "clima": []}


def _annotate_with_context(events: List[dict], context: dict):
    festivos = set()
    for d in context.get("festivos", []):
        try:
            festivos.add(datetime.fromisoformat(d).date())
        except Exception:
            # Acepta formatos YYYY-MM-DD también
            try:
                festivos.add(datetime.strptime(d, "%Y-%m-%d").date())
            except Exception:
                pass

    clima_entries = []
    for c in context.get("clima", []):
        ts = c.get("timestamp")
        try:
            dt = datetime.fromisoformat(ts) if ts else None
        except Exception:
            dt = None
        clima_entries.append({"dt": dt, "condicion": c.get("condicion"), "temperatura": c.get("temperatura")})

    annotated = []
    for ev in events:
        start = datetime.fromisoformat(ev["interval_start"])
        end = datetime.fromisoformat(ev["interval_end"])
        is_festivo = start.date() in festivos
        clima_match = [c for c in clima_entries if c["dt"] and start <= c["dt"] < end]
        # Tomamos la primera coincidencia de clima si existe
        weather = clima_match[0] if clima_match else None
        ev_copy = dict(ev)
        ev_copy["festivo"] = is_festivo
        if weather:
            ev_copy["weather"] = {"timestamp": weather["dt"].isoformat(), "condicion": weather["condicion"], "temperatura": weather["temperatura"]}
        else:
            ev_copy["weather"] = None
        annotated.append(ev_copy)
    return annotated


# --- ENDPOINT PARA PROBAR CON EL JSON export_data.json ---
@app.post("/test-json/")
async def probar_con_json(file: str = "export_data.json", limit: int = 50):
    """
    Lee un archivo JSON local (por defecto export_data.json) y regresa incidentes
    con la forma esperada del modelo Incident.

    Parámetros opcionales (query string):
      - file: nombre del archivo en el mismo folder, p. ej. datos_metro_cdmx.json
      - limit: cuántos incidentes regresar (para evitar payload enorme)
    """
    try:
        with open(file, "r", encoding="utf-8") as f:
            posts = json.load(f)
    except Exception as e:
        return {"error": f"No pude leer {file}: {e}"}

    # Filtramos entradas que tengan texto util
    usable_posts = [p for p in posts if (p.get("texto") or p.get("text") or p.get("mensaje"))]
    incidents = [_post_to_incident(p) for p in usable_posts]
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
    """
    Lee un archivo JSON, genera incidentes y agrupa por intervalos de 10 minutos.

    Parámetros:
      - file: nombre del archivo (por defecto export_data.json, puedes pasar datos_metro_cdmx.json)
      - limit_intervals: cuántos intervalos devolver (orden cronológico). 0 = todos.
    """
    try:
        with open(file, "r", encoding="utf-8") as f:
            posts = json.load(f)
    except Exception as e:
        return {"error": f"No pude leer {file}: {e}"}

    usable_posts = [p for p in posts if (p.get("texto") or p.get("text") or p.get("mensaje"))]
    incidents = [_post_to_incident(p) for p in usable_posts]
    summaries = _summarize_by_interval(incidents)
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
    """
    Lee un archivo JSON, genera incidentes y agrupa por intervalos de 10 minutos y (linea, estacion).
    Solo devuelve grupos que tengan min_count o mas reportes en ese intervalo (umbral para alerta real).

    Parámetros:
      - file: archivo JSON a leer (p. ej. datos_metro_cdmx.json)
      - min_count: umbral mínimo para considerar alerta (por defecto 5)
      - limit_alerts: cuántas alertas devolver (0 = todas)
    """
    try:
        with open(file, "r", encoding="utf-8") as f:
            posts = json.load(f)
    except Exception as e:
        return {"error": f"No pude leer {file}: {e}"}

    usable_posts = [p for p in posts if (p.get("texto") or p.get("text") or p.get("mensaje"))]
    incidents = [_post_to_incident(p) for p in usable_posts]
    alerts = _alerts_by_line_station(incidents, min_count=min_count)
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
    """
    Prototipo: agrupa mensajes por intervalo de 10 minutos y por linea/estacion, hace clustering
    por similitud de texto y devuelve solo los clusters que cumplen los mínimos.

    Parámetros:
      - file: archivo JSON (p. ej. datos_metro_cdmx.json)
      - min_reports: reportes mínimos en el cluster (por defecto 2)
      - min_authors: autores distintos mínimos (por defecto 2) para evitar un solo usuario spameando
      - limit_events: cuántos eventos devolver (0 = todos)
      - similarity: umbral de similitud (0-1) para agrupar textos parecidos
    """
    try:
        with open(file, "r", encoding="utf-8") as f:
            posts = json.load(f)
    except Exception as e:
        return {"error": f"No pude leer {file}: {e}"}

    usable_posts = [p for p in posts if (p.get("texto") or p.get("text") or p.get("mensaje"))]
    parsed = [_parse_post_for_event(p) for p in usable_posts]
    events = _cluster_events(parsed, min_reports=min_reports, min_authors=min_authors, similarity=similarity)
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
    """
    Inicia un procesamiento continuo del archivo: agrupa por intervalos de 10 minutos,
    clusteriza por similitud y va guardando resúmenes en memoria hasta terminar.
    - delay_seconds: si >0, espera ese tiempo entre intervalos (simula real time).
    """
    async with CONT_LOCK:
        if CONT_STATE["running"]:
            return {"status": "already_running", "file": CONT_STATE["file"], "started_at": CONT_STATE["started_at"]}
        # Reset state
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

    background_tasks.add_task(_run_continuous, file, min_reports, min_authors, similarity, delay_seconds)
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
    """
    Devuelve el estado y los resúmenes por intervalo acumulados hasta el momento.
    """
    async with CONT_LOCK:
        data = CONT_STATE.copy()
        data["buckets_count"] = len(CONT_STATE.get("buckets", []))
        return data


@app.post("/ingestar-realtime/")
async def ingestar_realtime(payload: dict):
    """
    Recibe reportes en tiempo real (lista de objetos estilo post) y devuelve incidentes
    en el formato esperado. También guarda un archivo JSON en la carpeta jsons.

    Body esperado: {"reportes": [ {...}, {...} ]}
    """
    reportes = payload.get("reportes") or []
    if not isinstance(reportes, list):
        return {"error": "El body debe contener 'reportes' como lista"}

    parsed = [_parse_post_for_event(p) for p in reportes]
    now = datetime.utcnow()
    context = {"holiday": _is_holiday(now), "weather": None}
    incidents = [_parsed_to_incident(p, context) for p in parsed]

    # Guardamos el lote en un archivo
    safe_now = now.isoformat().replace(":", "-")
    _save_json_notification(
        {"received_at": now.isoformat(), "incidents": incidents},
        f"realtime_{safe_now}.json",
    )

    # Añadimos al historial en memoria (solo un resumen corto)
    async with HISTORIAL_LOCK:
        for inc in incidents:
            HISTORIAL.append(_incident_to_historial_dict(inc))

    return {"count": len(incidents), "incidents": incidents}


# --- NUEVO ENDPOINT DE SIMULACION ---
@app.post("/simulacion-start/")
async def correr_simulacion(background_tasks: BackgroundTasks):
    """
    Inyecta los 4 escenarios de prueba secuencialmente.
    """
    print("INICIANDO SIMULACION DE 4 ESCENARIOS...")

    for escenario in ESCENARIOS_DEMO:
        # Convertimos el diccionario a Objeto Pydantic
        lote_obj = LoteMineria(**escenario)
        # Lo mandamos a procesar (Ojo: se iran casi al mismo tiempo a la cola)
        background_tasks.add_task(procesar_lote, lote_obj)

    return {"mensaje": "Simulacion iniciada. Revisa la terminal para ver el progreso de la IA."}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
