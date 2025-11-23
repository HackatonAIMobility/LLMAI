import re
import unicodedata
from datetime import datetime
from difflib import SequenceMatcher
from typing import List, Tuple

from .utils import bucket_start, bucket_end, load_metrolines_data, normalize_basic

# Alias comunes a formato canonico "LINEA X"
_LINE_ALIASES = {
    "1": "LINEA 1",
    "01": "LINEA 1",
    "L1": "LINEA 1",
    "LINEA1": "LINEA 1",
    "LINEA 1": "LINEA 1",
    "UNO": "LINEA 1",
    "2": "LINEA 2",
    "02": "LINEA 2",
    "L2": "LINEA 2",
    "LINEA2": "LINEA 2",
    "LINEA 2": "LINEA 2",
    "DOS": "LINEA 2",
    "3": "LINEA 3",
    "03": "LINEA 3",
    "L3": "LINEA 3",
    "LINEA3": "LINEA 3",
    "LINEA 3": "LINEA 3",
    "TRES": "LINEA 3",
    "4": "LINEA 4",
    "04": "LINEA 4",
    "L4": "LINEA 4",
    "LINEA4": "LINEA 4",
    "LINEA 4": "LINEA 4",
    "5": "LINEA 5",
    "05": "LINEA 5",
    "L5": "LINEA 5",
    "LINEA5": "LINEA 5",
    "LINEA 5": "LINEA 5",
    "6": "LINEA 6",
    "06": "LINEA 6",
    "L6": "LINEA 6",
    "LINEA6": "LINEA 6",
    "LINEA 6": "LINEA 6",
    "7": "LINEA 7",
    "07": "LINEA 7",
    "L7": "LINEA 7",
    "LINEA7": "LINEA 7",
    "LINEA 7": "LINEA 7",
    "8": "LINEA 8",
    "08": "LINEA 8",
    "L8": "LINEA 8",
    "LINEA8": "LINEA 8",
    "LINEA 8": "LINEA 8",
    "9": "LINEA 9",
    "09": "LINEA 9",
    "L9": "LINEA 9",
    "LINEA9": "LINEA 9",
    "LINEA 9": "LINEA 9",
    "A": "LINEA A",
    "LA": "LINEA A",
    "LINEAA": "LINEA A",
    "LINEA A": "LINEA A",
    "B": "LINEA B",
    "LB": "LINEA B",
    "LINEAB": "LINEA B",
    "LINEA B": "LINEA B",
    "12": "LINEA 12",
    "L12": "LINEA 12",
    "LINEA12": "LINEA 12",
    "LINEA 12": "LINEA 12",
}


def _normalize(text: str) -> str:
    """Mayusculas sin acentos para mejorar matching."""
    return normalize_basic(text)


def _canonical_line(raw: str) -> str:
    """Normaliza cualquier variante a 'LINEA X'."""
    norm = _normalize(raw).replace(" ", "")
    if norm in _LINE_ALIASES:
        return _LINE_ALIASES[norm]
    match = re.search(r"L[A-Z-]*?INEA?[-\s]*([0-9]{1,2}|[AB])", _normalize(raw))
    if match:
        token = match.group(1)
        return _LINE_ALIASES.get(token, f"LINEA {token}")
    return ""


def _find_line_in_text(text: str) -> str:
    norm = _normalize(text)
    # Hashtags tipo #L1 o #LINEA1
    match = re.search(r"#L(?:INEA)?\s*([0-9]{1,2}|[AB])\b", norm)
    if match:
        token = match.group(1)
        return _LINE_ALIASES.get(token, f"LINEA {token}")
    match = re.search(r"L[A-Z-]*?INEA?\s*([0-9]{1,2}|[AB])", norm)
    if match:
        token = match.group(1)
        return _LINE_ALIASES.get(token, f"LINEA {token}")
    match = re.search(r"\bL([0-9]{1,2}|[AB])\b", norm)
    if match:
        token = match.group(1)
        return _LINE_ALIASES.get(token, f"LINEA {token}")
    return ""


def _find_station_in_text(text: str) -> str:
    norm = _normalize(text)
    match = re.search(r"ESTAC[A-Z]*ION\s+([A-Z0-9\s_-]{2,80})", norm)
    if match:
        raw_station = match.group(1)
        # Cortar en separadores comunes y limpiar espacios
        cut = re.split(r"[#:\.,]", raw_station)[0]
        return " ".join(cut.split()).strip(" -")
    return ""


def normalize_line_station(text: str, metadata: dict) -> Tuple[str, str]:
    """
    Detecta linea/estacion usando metadata primero y el texto como respaldo.
    """
    metro_data = load_metrolines_data()
    station_index = metro_data.get("station_index", [])
    line = ""
    station = ""
    if isinstance(metadata, dict):
        line = _canonical_line(metadata.get("linea_detectada") or metadata.get("linea") or "")
        station = _normalize(metadata.get("estacion") or "").strip()

    if not station and station_index:
        norm_text = _normalize(text)
        for norm_station, canonical_station, line_label in station_index:
            if norm_station and norm_station in norm_text:
                station = canonical_station
                if not line:
                    line = _canonical_line(line_label) or line_label
                break

    if not line:
        line = _find_line_in_text(text)

    if not station:
        station = _find_station_in_text(text)

    return line, station


def classify_priority(text: str, metadata: dict | None = None) -> str:
    if isinstance(metadata, dict):
        meta = metadata.get("prioridad") or metadata.get("priority")
        if isinstance(meta, str):
            m = meta.strip().lower()
            if m in {"alta", "critica", "crA-tica", "urgente", "high"}:
                return "CRITICAL"
            if m in {"media", "medium", "warning"}:
                return "WARNING"
            if m in {"baja", "low"}:
                return "MESSAGE"
    critical_terms = [
        "humo",
        "fuego",
        "incend",
        "evacuac",
        "choque",
        "desalojo",
        "sin avance",
        "detenido",
        "peleand",
        "bomba",
        "explosion",
    ]
    warning_terms = ["lento", "retraso", "esperando", "lluvia", "marcha lenta", "sin servicio"]
    t = text.lower()
    if any(k in t for k in critical_terms):
        return "CRITICAL"
    if any(k in t for k in warning_terms):
        return "WARNING"
    return "MESSAGE"


def parse_post_for_event(post: dict):
    text = post.get("texto") or post.get("text") or post.get("mensaje") or ""
    if not isinstance(text, str):
        text = ""
    metadata = post.get("metadata") or {}
    line, station = normalize_line_station(text, metadata)
    source = post.get("fuente") or post.get("source") or "desconocida"
    author = post.get("autor") or post.get("author") or "anon"
    timestamp_raw = post.get("timestamp")
    try:
        ts = datetime.fromisoformat(timestamp_raw) if timestamp_raw else datetime.utcnow()
    except Exception:
        ts = datetime.utcnow()
    priority = classify_priority(text, metadata)
    meta_priority = metadata.get("prioridad") if isinstance(metadata, dict) else None
    meta_type = metadata.get("tipo") if isinstance(metadata, dict) else None
    return {
        "text": text,
        "line": line or "",
        "station": station or "",
        "source": source,
        "author": author,
        "timestamp": ts,
        "priority": priority,
        "metadata_priority": meta_priority,
        "metadata_type": meta_type,
    }


def cluster_events(posts: List[dict], min_reports: int = 2, min_authors: int = 2, similarity: float = 0.72):
    """
    Agrupa por intervalo de WINDOW_MINUTES y (linea, estacion), luego hace clustering
    por similitud de texto dentro de cada grupo. Solo devuelve clusters que cumplen
    los minimos.
    """
    buckets = {}
    for p in posts:
        start = bucket_start(p["timestamp"])
        key = (start, p.get("line") or "DESCONOCIDA", p.get("station") or "")
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
                    "interval_end": bucket_end(start).isoformat(),
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
