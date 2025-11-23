from datetime import datetime
from typing import List, Literal

from pydantic import BaseModel


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

