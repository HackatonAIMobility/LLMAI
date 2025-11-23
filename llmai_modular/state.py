import asyncio
from threading import Lock
from typing import Dict, List

# Almacenamiento en memoria (similar a LLMAI.py)
HISTORIAL: List[dict] = []
HISTORIAL_LOCK = asyncio.Lock()

INCIDENT_COUNTER = 0
INCIDENT_LOCK = Lock()

# Buffer de tiempo real (reserva para usos futuros)
REALTIME_BUFFER: Dict = {}
REALTIME_LOCK = asyncio.Lock()

# Estado para ejecuciones continuas
CONT_STATE = {
    "running": False,
    "file": None,
    "started_at": None,
    "ended_at": None,
    "buckets": [],
    "error": None,
}
CONT_LOCK = asyncio.Lock()


def next_incident_sequence() -> int:
    """Genera un consecutivo seguro para IDs de incidentes."""
    global INCIDENT_COUNTER
    with INCIDENT_LOCK:
        INCIDENT_COUNTER += 1
        return INCIDENT_COUNTER

