import json
from datetime import datetime
from typing import List

from .config import HOLIDAYS
from .utils import parse_ts


def is_holiday(dt: datetime) -> bool:
    return dt.strftime("%m-%d") in HOLIDAYS


def weather_summary(posts_raw: List[dict], start: datetime, end: datetime):
    conds = []
    temps = []
    for p in posts_raw:
        ts = parse_ts(p.get("timestamp"))
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


def load_context(context_file: str):
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


def annotate_with_context(events: List[dict], context: dict):
    festivos = set()
    for d in context.get("festivos", []):
        try:
            festivos.add(datetime.fromisoformat(d).date())
        except Exception:
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
        weather = clima_match[0] if clima_match else None
        ev_copy = dict(ev)
        ev_copy["festivo"] = is_festivo
        if weather:
            ev_copy["weather"] = {
                "timestamp": weather["dt"].isoformat(),
                "condicion": weather["condicion"],
                "temperatura": weather["temperatura"],
            }
        else:
            ev_copy["weather"] = None
        annotated.append(ev_copy)
    return annotated

