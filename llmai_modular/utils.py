import json
import os
import re
import unicodedata
from functools import lru_cache
from datetime import datetime, timedelta
from typing import Literal

from .config import JSON_DIR, WINDOW_MINUTES
from .state import next_incident_sequence


def ensure_json_dir() -> None:
    os.makedirs(JSON_DIR, exist_ok=True)


def save_json(payload: dict, filename: str) -> str:
    ensure_json_dir()
    path = os.path.join(JSON_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        # default=str para serializar datetimes u otros objetos no JSON.
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    return path


def parse_ts(ts_raw):
    try:
        return datetime.fromisoformat(ts_raw) if ts_raw else None
    except Exception:
        return None


def bucket_start(dt: datetime) -> datetime:
    """Regresa el inicio del intervalo configurado (WINDOW_MINUTES) para el timestamp dado."""
    minute_bucket = (dt.minute // WINDOW_MINUTES) * WINDOW_MINUTES
    return dt.replace(minute=minute_bucket, second=0, microsecond=0)


def generate_incident_id(ts: datetime | None) -> str:
    if ts is None:
        ts = datetime.utcnow()
    seq = next_incident_sequence()
    return f"{ts.strftime('%Y%m%d%H%M%S')}_{seq}"


def normalize_basic(text: str) -> str:
    """Quita acentos y pone mayusculas para matching robusto."""
    if not isinstance(text, str):
        return ""
    nfkd = unicodedata.normalize("NFKD", text)
    without_accents = "".join([c for c in nfkd if not unicodedata.combining(c)])
    return without_accents.upper()


@lru_cache(maxsize=1)
def load_metrolines_data(path: str | None = None) -> dict:
    """
    Carga metrolines desde JSON (fallBack: metrolines.json / metro_lines.json).
    Regresa {"lines": {linea: [stations]}, "station_index": [(norm_station, station, line)]}
    """
    candidates = [path, os.getenv("METRO_LINES_FILE"), "metrolines.json", "metro_lines.json"]
    lines_map = {}
    station_index = []
    loaded = False
    for cand in candidates:
        if not cand:
            continue
        if not os.path.exists(cand):
            continue
        try:
            with open(cand, "r", encoding="utf-8") as f:
                data = json.load(f)
            lines_raw = data.get("lines") if isinstance(data, dict) else None
            if not isinstance(lines_raw, list):
                continue
            for entry in lines_raw:
                raw_name = entry.get("name") or ""
                norm_name = normalize_basic(raw_name)
                match = re.search(r"LINEA\s*([0-9]{1,2}|[AB])", norm_name)
                line_label = f"LINEA {match.group(1)}" if match else raw_name
                stations = entry.get("stations") or []
                lines_map[line_label] = stations
                for st in stations:
                    norm_station = normalize_basic(st)
                    if not norm_station:
                        continue
                    station_index.append((norm_station, st, line_label))
            loaded = True
            break
        except Exception:
            continue

    if not loaded:
        return {"lines": {}, "station_index": []}
    return {"lines": lines_map, "station_index": station_index}


def _priority_from_metadata(metadata: dict | None) -> Literal["MESSAGE", "WARNING", "CRITICAL"] | None:
    if not isinstance(metadata, dict):
        return None
    raw = metadata.get("prioridad") or metadata.get("priority")
    if not isinstance(raw, str):
        return None
    val = raw.strip().lower()
    if val in {"alta", "high", "critica", "crítica", "urgente"}:
        return "CRITICAL"
    if val in {"media", "medium", "warning"}:
        return "WARNING"
    if val in {"baja", "low"}:
        return "MESSAGE"
    return None


def infer_priority(texto: str, metadata: dict | None = None) -> Literal["MESSAGE", "WARNING", "CRITICAL"]:
    meta = _priority_from_metadata(metadata)
    if meta:
        return meta

    lowered = texto.lower()
    crit_keywords = ["incend", "huele a quemado", "quemado", "inundo", "detenido", "no pasa nada", "peleand"]
    warn_keywords = ["lenta", "precaucion", "precauciones", "esperando", "no sirvan", "espera", "retras"]

    if any(k in lowered for k in crit_keywords):
        return "CRITICAL"
    if any(k in lowered for k in warn_keywords):
        return "WARNING"
    return "MESSAGE"


def map_priority_enum(priority: str) -> str:
    if priority == "CRITICAL":
        return "HIGH"
    if priority == "WARNING":
        return "MEDIUM"
    return "MESSAGE"


def bucket_end(start: datetime) -> datetime:
    return start + timedelta(minutes=WINDOW_MINUTES)
