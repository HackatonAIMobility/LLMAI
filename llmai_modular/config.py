import os

# Configuracion de endpoints y rutas
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
MODELO = os.getenv("OLLAMA_MODEL", "llama3.2:3b")
JSON_DIR = "jsons"

# Backend opcional para reportes
BACKEND_URL = os.getenv("BACKEND_URL", "http://10.110.168.15:8000/incidents")
BACKEND_TOKEN = os.getenv("BACKEND_TOKEN", "")

# Umbrales
MIN_REPORTS_PER_WINDOW = 2  # minimos por ventana de tiempo
# Ventana de agrupacion en minutos para buckets (ajustable)
WINDOW_MINUTES = 2

# Dias festivos basicos (MM-DD)
HOLIDAYS = {
    "01-01",
    "02-05",
    "03-21",
    "05-01",
    "09-16",
    "11-02",
    "11-20",
    "12-12",
    "12-25",
}
