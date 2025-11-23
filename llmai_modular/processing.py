import asyncio
import json
from datetime import datetime
from typing import List

import requests

from .clustering import cluster_events, parse_post_for_event
from .config import BACKEND_TOKEN, BACKEND_URL, MODELO, OLLAMA_URL
from .context import is_holiday, weather_summary
from .incident_builder import (
    alerts_by_line_station,
    event_to_incident,
    incident_to_historial_dict,
    parsed_to_incident,
    post_to_incident,
    summarize_by_interval,
)
from .models import Incident, LoteMineria
from .state import CONT_LOCK, CONT_STATE, HISTORIAL, HISTORIAL_LOCK
from .utils import bucket_start, bucket_end, save_json, generate_incident_id


async def procesar_lote(lote: LoteMineria):
    """
    Ejecuta el flujo original contra Ollama y guarda en el historial en memoria.
    """
    print(f"[IA] Analizando bloque de {lote.inicio}...")

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
        res_ia = requests.post(
            OLLAMA_URL,
            json={"model": MODELO, "prompt": prompt, "stream": False, "format": "json"},
            timeout=120,
        )
        res_ia.raise_for_status()

        resp_json = res_ia.json().get("response", "{}")
        analisis = json.loads(resp_json)

        registro = {
            "fecha": datetime.now().isoformat(),
            "fecha_simulada": lote.inicio,
            "linea": lote.linea,
            "semaforo": analisis.get("semaforo"),
            "resumen": analisis.get("resumen"),
            "raw_data_count": len(lote.datos),
        }

        async with HISTORIAL_LOCK:
            HISTORIAL.append(registro)

        print(f"[LISTO] {lote.inicio}: {analisis.get('semaforo')} -> {analisis.get('resumen')}")

    except Exception as e:
        print(f"[ERROR] Procesando {lote.inicio}: {e}")


def post_incidents_backend(
    incidents: list, line: str = '', station: str = '', interval_start: str = '', interval_end: str = ''
) -> dict:
    """
    Envia todos los incidentes recibidos al backend (uno por request).
    Regresa lista de resultados para reportarlo al cliente.
    """
    if not BACKEND_URL:
        return {"sent": False, "error": "BACKEND_URL not configured", "results": []}
    if not incidents:
        return {"sent": False, "error": "no incidents to send", "results": []}
    headers = {"Content-Type": "application/json"}
    if BACKEND_TOKEN:
        headers["Authorization"] = f"Bearer {BACKEND_TOKEN}"

    def _map_priority(val):
        if isinstance(val, str):
            upper = val.upper()
            if upper == 'CRITICAL':
                return 3
            if upper == 'WARNING':
                return 2
            if upper == 'MESSAGE':
                return 1
        return val

    results = []
    for inc in incidents:
        payload = inc
        if isinstance(payload, dict) and 'Priority' in payload:
            payload = dict(payload)
            payload['Priority'] = _map_priority(payload.get('Priority'))
        try:
            resp = requests.post(BACKEND_URL, json=payload, headers=headers, timeout=10)
            results.append({'sent': resp.ok, 'status_code': resp.status_code, 'text': resp.text})
        except Exception as e:
            warn_msg = f"[WARN] No se pudo enviar al backend: {e}"
            print(warn_msg)
            results.append({'sent': False, 'error': str(e)})

    return {
        'sent': all(r.get('sent') for r in results) if results else False,
        'results': results,
    }



def post_to_incidents_from_file(posts: List[dict]):
    usable_posts = [p for p in posts if (p.get("texto") or p.get("text") or p.get("mensaje"))]
    return [post_to_incident(p) for p in usable_posts], usable_posts


def detect_line_with_ollama(text: str) -> str:
    """
    Pide a Ollama que detecte la linea del Metro CDMX en un texto.
    Devuelve algo como "LINEA 1", "LINEA 12", "LINEA A", "LINEA B" o "DESCONOCIDA".
    """
    if not text:
        return "DESCONOCIDA"
    prompt = f"""
Detecta la linea del Metro CDMX a la que se refiere este texto.
Texto: \"{text}\"
Responde solo JSON: {{"linea": "LINEA X|LINEA A|LINEA B|DESCONOCIDA"}}
"""
    try:
        res = requests.post(
            OLLAMA_URL,
            json={"model": MODELO, "prompt": prompt, "stream": False, "format": "json"},
            timeout=30,
        )
        res.raise_for_status()
        resp_json = res.json().get("response", "{}")
        data = json.loads(resp_json)
        linea = data.get("linea")
        if isinstance(linea, str) and linea:
            return linea.strip().upper()
    except Exception as e:
        print(f"[WARN] Ollama detect_line fallo: {e}")
    return "DESCONOCIDA"


def is_relevant_incident(text: str) -> bool:
    """
    Usa Ollama para decidir si el texto es relevante a incidencias/operacion del Metro CDMX.
    """
    if not text:
        return False
    prompt = f"""
Determina si este texto es un reporte/incidencia sobre el Metro CDMX (servicio, retrasos, fallas, seguridad).
Texto: \"{text}\"
Responde solo JSON: {{"relevante": true|false}}
"""
    try:
        res = requests.post(
            OLLAMA_URL,
            json={"model": MODELO, "prompt": prompt, "stream": False, "format": "json"},
            timeout=30,
        )
        res.raise_for_status()
        resp_json = res.json().get("response", "{}")
        data = json.loads(resp_json)
        return bool(data.get("relevante"))
    except Exception as e:
        print(f"[WARN] Ollama relevance fallo: {e}")
        return False


def summarize_bucket_with_ollama(texts: List[str], line_hint: str = "") -> dict | None:
    """
    Llama a Ollama para sintetizar una lista de textos del bucket.
    Devuelve {"semaforo":..., "resumen":...} o None si falla.
    """
    if not texts:
        return None
    joined = "\n".join([f"- {t}" for t in texts])
    prompt = f"""
Analiza estos mensajes del Metro CDMX{f' sobre {line_hint}' if line_hint else ''}:
{joined}

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
        res_ia = requests.post(
            OLLAMA_URL,
            json={"model": MODELO, "prompt": prompt, "stream": False, "format": "json"},
            timeout=60,
        )
        res_ia.raise_for_status()
        resp_json = res_ia.json().get("response", "{}")
        data = json.loads(resp_json)
        return {"semaforo": data.get("semaforo"), "resumen": data.get("resumen")}
    except Exception as e:
        print(f"[WARN] Ollama fallo en bucket: {e}")
        return None


def incident_from_summary(line: str, station: str, summary: dict | None, start: datetime) -> dict:
    """
    Construye un incidente para el backend a partir del resumen de Ollama.
    Priority se mapea a numero (1=MESSAGE,2=WARNING,3=CRITICAL).
    """
    def map_semaforo(val: str | None):
        if not isinstance(val, str):
            return 1
        v = val.upper()
        if v == "ROJO":
            return 3
        if v == "AMARILLO":
            return 2
        return 1

    pri = map_semaforo(summary.get("semaforo") if summary else None)
    title = f"Resumen {line or station or 'Metro'}"
    desc = summary.get("resumen") if summary else "Sin resumen"
    ts = start if isinstance(start, datetime) else datetime.utcnow()
    start_station = station or line or "DESCONOCIDA"
    line_val = line or "DESCONOCIDA"
    return {
        "Id": generate_incident_id(ts),
        "Title": title[:60],
        "ShortDescription": desc[:120] if isinstance(desc, str) else title,
        "Description": desc if isinstance(desc, str) else title,
        "Timestamp": ts.isoformat(),
        "StartStation": start_station,
        "EndStation": "",
        "Line": line_val,
        "Priority": pri,
    }


async def run_continuous(file: str, min_reports: int, min_authors: int, similarity: float, delay_seconds: float):
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
    parsed = [parse_post_for_event(p) for p in usable_posts]
    parsed.sort(key=lambda x: x["timestamp"])

    buckets = {}
    for p in parsed:
        start = bucket_start(p["timestamp"])
        buckets.setdefault(start, []).append(p)

    for start in sorted(buckets.keys()):
        bucket_posts = buckets[start]
        events = cluster_events(bucket_posts, min_reports=min_reports, min_authors=min_authors, similarity=similarity)
        weather_ctx = weather_summary(posts, start, bucket_end(start))
        context = {
            "holiday": is_holiday(start),
            "weather": weather_ctx,
        }
        incidents = [event_to_incident(ev, context) for ev in events]
        if not incidents and bucket_posts:
            incidents = [parsed_to_incident(p, context) for p in bucket_posts]

        summary = {
            "interval_start": start.isoformat(),
            "interval_end": bucket_end(start).isoformat(),
            "total_posts": len(bucket_posts),
            "events_total": len(events),
            "events": events,
            "incidents": incidents,
            "context": context,
        }
        async with CONT_LOCK:
            CONT_STATE["buckets"].append(summary)

        safe_start = start.isoformat().replace(":", "-")
        save_json(
            {"interval_start": summary["interval_start"], "interval_end": summary["interval_end"], "incidents": incidents},
            f"bucket_{safe_start}.json",
        )
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)

    async with CONT_LOCK:
        CONT_STATE["running"] = False
        CONT_STATE["ended_at"] = datetime.utcnow().isoformat()


def incidents_from_posts(posts: List[dict], min_count_alert: int = 5):
    incidents, usable_posts = post_to_incidents_from_file(posts)
    return incidents, usable_posts


def summarize_incidents(incidents: List[Incident]):
    return summarize_by_interval(incidents)


def alerts_from_incidents(incidents: List[Incident], min_count: int):
    return alerts_by_line_station(incidents, min_count=min_count)
