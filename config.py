"""
CONFIGURACIÓN CENTRALIZADA - TFM Data Engineering
TODAS LAS CONSTANTES EN UN SOLO LUGAR
"""

# ============================================================================
# ZONAS DE MONTAÑA
# ============================================================================

MOUNTAIN_AREAS = {
    "sierra_nevada": {
        "bbox": [-3.8, 36.8, -3.1, 37.3],
        "name": "Sierra Nevada",
        "target_routes": 1000,
        "description": "Macizo andaluz con las cumbres más altas de la península",
        "base_elevation": 1500,
        "max_ascent": 1200,
        "center_lat": 37.05,
        "center_lon": -3.45
    },
    "pirineo_central": {
        "bbox": [-1.5, 42.3, 0.5, 43.0],
        "name": "Pirineo Central",
        "target_routes": 1000,
        "description": "Macizo pirenaico con rutas espectaculares",
        "base_elevation": 1000,
        "max_ascent": 800,
        "center_lat": 42.65,
        "center_lon": -0.5
    },
    "sierra_guadarrama": {
        "bbox": [-4.5, 40.4, -3.3, 41.1],
        "name": "Sierra de Guadarrama",
        "target_routes": 1000,
        "description": "Sierra central con rutas accesibles desde Madrid",
        "base_elevation": 1200,
        "max_ascent": 500,
        "center_lat": 40.75,
        "center_lon": -3.9
    }
}

# ============================================================================
# CONFIGURACIÓN DE APIs
# ============================================================================

OVERPASS_URL = "http://overpass-api.de/api/interpreter"
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_TOPO_URL = "https://api.opentopodata.org/v1/srtm30m"

# ============================================================================
# CONFIGURACIÓN DE MONGODB
# ============================================================================

DEFAULT_MONGO_URI = "mongodb://localhost:27017/"
DEFAULT_DATABASE = "hiking_trails_tfm"

# ============================================================================
# CONFIGURACIÓN DEL PIPELINE
# ============================================================================

REQUEST_DELAY = 5  # segundos
DEFAULT_TIMEOUT = 180
REPRODUCIBILITY_SEED = 42  # ¡CRÍTICO! Para datos reproducibles

# ============================================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================================

LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'handlers': ['file', 'console']
}