"""
FUNCIONES REUTILIZABLES
"""

import hashlib
import numpy as np
from datetime import datetime
import random

def generate_data_hash(element):
    """
    Generar hash único para idempotencia.
    """
    if 'type' in element and 'id' in element:
        osm_id = f"{element.get('type')}_{element.get('id')}"
        timestamp = element.get('timestamp', '')
        tags_str = str(sorted(element.get('tags', {}).items()))
        hash_input = f"{osm_id}_{timestamp}_{tags_str}"
    else:
        name = element.get('name', '')
        mountain_range = element.get('mountain_range', '')
        distance = element.get('distance_km', '')
        hash_input = f"synthetic_{name}_{mountain_range}_{distance}_{datetime.now().timestamp()}"
    
    return hashlib.md5(hash_input.encode()).hexdigest()

def set_global_seed(seed: int = 42) -> None:
    """
    Establecer semillas para reproducibilidad.
    """
    np.random.seed(seed)
    random.seed(seed)
    print(f"Semillas establecidas para reproducibilidad: {seed}")

def calculate_realistic_distance(element_type):
    """
    Calcular distancia realista basada en tipo de elemento.
    Versión reproducible (usa semilla global).
    """
    if element_type == 'relation':
        return float(np.random.randint(10, 25))
    elif element_type == 'way':
        return float(np.random.randint(3, 15))
    else:
        return float(np.random.randint(2, 8))

def estimate_duration_from_distance(distance_km):
    """
    Estimar duración en horas/minutos.
    Versión reproducible.
    """
    try:
        distance = float(distance_km)
        base_hours = distance / 3.5  # 3.5 km/h velocidad media
        total_hours = base_hours * np.random.uniform(0.8, 1.3)
        
        hours = int(total_hours)
        minutes = int((total_hours - hours) * 60)
        
        if hours > 0:
            return f"{hours}h {minutes}min"
        else:
            return f"{minutes}min"
    except:
        return "3-4 horas"

def create_coordinates_profile(base_elevation: int, max_ascent: int):
    """
    Crear perfil de coordenadas realista.
    """
    import numpy as np
    num_points = np.random.randint(20, 50)
    coordinates = []
    
    for i in range(num_points):
        progress = i / (num_points - 1)
        distance = progress * 20
        
        elevation = base_elevation + max_ascent * np.sin(progress * np.pi)
        elevation += 200 * np.sin(progress * np.pi * 3)
        
        coordinates.append({
            'distance_km': round(distance, 2),
            'elevation_m': round(elevation, 1),
            'slope_percent': round(np.random.uniform(-15, 15), 1)
        })
    
    return coordinates

def get_random_difficulty():
    """
    Obtener dificultad aleatoria reproducible.
    """
    return np.random.choice(['Fácil', 'Moderada', 'Difícil'], p=[0.4, 0.4, 0.2])
