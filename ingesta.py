"""
TFM DATA ENGINEERING - MÓDULO DE INGESTIÓN COMPLETO
AUTOR: [Tu nombre]
FECHA: [Fecha]
DESCRIPCIÓN: Sistema de ingestión de rutas de senderismo desde OpenStreetMap a MongoDB
             con trazabilidad, idempotencia y validación.
"""

# ============================================================================
# IMPORTS MEJORADOS CON REPRODUCIBILIDAD
# ============================================================================

# 1. SEMILLAS PARA REPRODUCIBILIDAD (AÑADIR AL INICIO)
import numpy as np
import random

# 2. IMPORTAR CONFIGURACIÓN DESDE ARCHIVOS SEPARADOS
from config import MOUNTAIN_AREAS, OVERPASS_URL, REPRODUCIBILITY_SEED
from utils import set_global_seed, generate_data_hash, calculate_realistic_distance, estimate_duration_from_distance

# 3. ESTABLECER SEMILLA GLOBAL (¡CRÍTICO!)
set_global_seed(REPRODUCIBILITY_SEED)

# 4. RESTO DE IMPORTS (IGUAL QUE TENÍAS)
import requests
import json
import time
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
from pymongo import MongoClient, errors
import logging
import sys
import argparse
import traceback

# ============================================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CLASE PRINCIPAL DE INGESTIÓN (MODIFICADA CON SEMILLAS)
# ============================================================================

class HikingTrailsIngestor:
    """
    Sistema completo de ingestión de rutas de senderismo.
    Combina extracción de datos reales OSM con generación de datos sintéticos
    cuando sea necesario, manteniendo trazabilidad completa.
    """
    
    # Configuraciones (ahora usando constantes importadas)
    OVERPASS_URL = OVERPASS_URL  # <-- Desde config.py
    DEFAULT_TIMEOUT = 180
    REQUEST_DELAY = 5  # segundos entre peticiones
    
    def __init__(self, 
                 mongo_uri: str = "mongodb://localhost:27017/",
                 database: str = "hiking_trails_tfm",
                 use_real_data: bool = True,
                 use_synthetic_backup: bool = True):
        """
        Inicializar el sistema de ingestión.
        """
        # Asegurar semilla también en constructor
        set_global_seed(REPRODUCIBILITY_SEED)
        
        self.use_real_data = use_real_data
        self.use_synthetic_backup = use_synthetic_backup
        
        # Conectar a MongoDB
        try:
            self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client[database]
            
            # Colecciones
            self.routes_collection = self.db['routes']
            self.ingestion_log = self.db['ingestion_log']
            self.raw_data_collection = self.db['raw_osm_data']
            
            # Crear índices
            self._create_indexes()
            
            logger.info(f"MongoDB conectado: {database}")
            
        except errors.ConnectionFailure as e:
            logger.error(f"Error de conexión MongoDB: {e}")
            raise
    
    def _create_indexes(self):
        """Crear índices para optimizar consultas."""
        indexes = [
            [("mountain_range", 1)],
            [("difficulty", 1)],
            [("distance_km", 1)],
            [("name", "text")],
            [("timestamp", -1)],
            [("_ingestion_metadata.data_hash", 1)],  # Para evitar duplicados
            [("_ingestion_metadata.source", 1)],
            [("_ingestion_metadata.region", 1)]
        ]
        
        for index in indexes:
            try:
                self.routes_collection.create_index(index)
            except Exception as e:
                logger.warning(f"No se pudo crear índice {index}: {e}")
    
    def _generate_data_hash(self, element: Dict) -> str:
        """
        Generar hash único para cada elemento.
        Usa función importada desde utils.py
        """
        return generate_data_hash(element)
    
    def _enrich_with_metadata(self, element: Dict, region_key: str, 
                             source: str = "osm") -> Dict:
        """
        Enriquecer elemento con metadatos de ingesta.
        """
        enriched = element.copy()
        
        # Metadatos de ingesta (separados de los datos originales)
        enriched['_ingestion_metadata'] = {
            'source': source,
            'region': region_key,
            'ingestion_timestamp': datetime.utcnow().isoformat(),
            'data_hash': self._generate_data_hash(element),
            'ingestion_version': '1.0',
            'batch_id': f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'raw_data': True if source == 'osm' else False
        }
        
        # Añadir información de la región
        if region_key in MOUNTAIN_AREAS:
            region_info = MOUNTAIN_AREAS[region_key]
            enriched['mountain_range'] = region_info['name']
            enriched['region_description'] = region_info['description']
        
        return enriched
    
    def _query_overpass_api(self, bbox: List[float]) -> Optional[Dict]:
        """
        Consultar API de Overpass para obtener datos reales.
        """
        query = f"""
        [out:json][timeout:{self.DEFAULT_TIMEOUT}];
        (
          // Relaciones de senderismo (rutas organizadas)
          relation["route"="hiking"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
          relation["route"="foot"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
          
          // Vías de senderos
          way["highway"="path"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
          way["highway"="footway"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
          way["highway"="track"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
          
          // Puntos de interés montañeros
          node["natural"="peak"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
          node["tourism"="alpine_hut"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
          node["amenity"="shelter"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
          
          // Rutas con nombres específicos
          way["name"~"GR|PR|SL|sendero|ruta|camino",i]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
          relation["name"~"GR|PR|SL|sendero|ruta|camino",i]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
        );
        out body;
        >;
        out skel qt;
        """
        
        try:
            logger.info(f"Consultando Overpass API para bbox: {bbox}")
            response = requests.post(self.OVERPASS_URL, data=query, timeout=120)
            response.raise_for_status()
            
            data = response.json()
            elements_count = len(data.get('elements', []))
            logger.info(f"Datos obtenidos: {elements_count} elementos")
            
            return data
            
        except requests.exceptions.Timeout:
            logger.error(f"Timeout en consulta API para bbox {bbox}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error API: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error JSON: {e}")
            return None
    
    def _get_weather_for_coordinates(self, lat: float, lon: float) -> Optional[Dict]:
        """Obtiene datos climáticos para coordenadas específicas usando Open-Meteo."""
        from config import OPEN_METEO_URL
        
        url = OPEN_METEO_URL
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
            "end_date": (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
            "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "windspeed_10m_max", "weathercode"],
            "timezone": "Europe/Madrid"
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if "daily" in data:
                daily = data["daily"]
                code = daily["weathercode"][0]
                
                weather_description = {
                    0: "Despejado ", 1: "Principalmente despejado ",
                    2: "Parcialmente nublado ", 3: "Nublado ",
                    45: "Niebla ", 48: "Niebla con escarcha ",
                    51: "Lluvia ligera ", 61: "Lluvia moderada ",
                    63: "Lluvia intensa ", 71: "Nieve ligera ",
                    73: "Nieve moderada ", 75: "Nieve intensa ",
                    95: "Tormenta ", 96: "Tormenta con granizo ",
                    99: "Tormenta con granizo fuerte "
                }
                
                return {
                    "date": daily["time"][0],
                    "temp_max": daily["temperature_2m_max"][0],
                    "temp_min": daily["temperature_2m_min"][0],
                    "precipitation": daily["precipitation_sum"][0],
                    "wind_max": daily["windspeed_10m_max"][0],
                    "weather_code": code,
                    "weather_text": weather_description.get(code, "Desconocido"),
                    "storm": code in [95, 96, 99]
                }
        except Exception as e:
            logger.error(f"Error al obtener clima: {e}")
        
        return None

    def _get_elevation_for_coordinates(self, lat: float, lon: float) -> Optional[float]:
        """Obtiene la elevación para coordenadas específicas usando OpenTopoData."""
        from config import OPEN_TOPO_URL
        
        url = OPEN_TOPO_URL
        params = {
            "locations": f"{lat},{lon}",
            "interpolation": "cubic"
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") == "OK" and data.get("results"):
                return data["results"][0]["elevation"]
        except Exception as e:
            logger.error(f"Error al obtener elevación: {e}")
        
        return None
    
    def _process_real_osm_data(self, osm_data: Dict, region_key: str, 
                               region_weather: Optional[Dict], 
                               region_elevation: Optional[float]) -> List[Dict]:
        """
        Procesar datos reales de OSM para extraer rutas.
        """
        routes = []
        region_info = MOUNTAIN_AREAS[region_key]
        
        for element in osm_data.get('elements', []):
            try:
                tags = element.get('tags', {})
                
                # Filtrar elementos relevantes
                if not self._is_hiking_related(element, tags):
                    continue
                
                # Crear estructura de ruta
                route_data = {
                    'osm_id': f"{element['type']}_{element['id']}",
                    'name': tags.get('name', self._generate_route_name(element, region_info)),
                    'ref': tags.get('ref', ''),
                    'distance_km': tags.get('distance', self._estimate_distance(element)),
                    'difficulty': self._infer_difficulty(element, tags),
                    'ascent_m': tags.get('ascent', str(np.random.randint(200, 1200))),
                    'descent_m': tags.get('descent', str(np.random.randint(150, 1000))),
                    'duration': self._estimate_duration(tags.get('distance', '10')),
                    'mountain_range': region_info['name'],
                    'element_type': element['type'],
                    'highway_type': tags.get('highway', ''),
                    'natural_type': tags.get('natural', ''),
                    'tourism_type': tags.get('tourism', ''),
                    'coordinates': self._generate_coordinates_profile(region_info),
                    'description': self._generate_description(element, tags, region_info),
                    'source': 'openstreetmap',
                    'timestamp': datetime.utcnow().isoformat(),
                    'raw_tags': tags  # Guardar tags originales para trazabilidad
                }
                
                # Añadir datos climáticos y de elevación si están disponibles
                if region_weather:
                    route_data['weather'] = region_weather
                if region_elevation:
                    route_data['region_elevation'] = region_elevation
                
                routes.append(route_data)
                
            except Exception as e:
                logger.warning(f"Error procesando elemento {element.get('id')}: {e}")
                continue
        
        return routes
    
    def _generate_synthetic_routes(self, region_key: str, count: int,
                                   region_weather: Optional[Dict], 
                                   region_elevation: Optional[float]) -> List[Dict]:
        """
        Generar rutas sintéticas para complementar datos reales.
        """
        routes = []
        region_info = MOUNTAIN_AREAS[region_key]
        
        for i in range(count):
            try:
                # Generar datos realistas pero ficticios
                route_num = i + 1
                distance = np.random.randint(3, 25)
                
                route_data = {
                    'synthetic_id': f"syn_{region_key}_{datetime.now().strftime('%Y%m%d')}_{route_num:04d}",
                    'name': f"{['Sendero', 'Ruta', 'Camino', 'Ascenso'][i % 4]} {region_info['name']} {route_num}",
                    'ref': np.random.choice(['GR', 'PR', 'SL', ''], p=[0.1, 0.3, 0.3, 0.3]),
                    'distance_km': str(distance),
                    'difficulty': np.random.choice(['Fácil', 'Moderada', 'Difícil'], 
                                                   p=[0.4, 0.4, 0.2]),
                    'ascent_m': str(np.random.randint(200, 1500)),
                    'descent_m': str(np.random.randint(150, 1200)),
                    'duration': self._estimate_duration(str(distance)),
                    'mountain_range': region_info['name'],
                    'element_type': 'synthetic',
                    'highway_type': np.random.choice(['path', 'footway', 'track', '']),
                    'natural_type': np.random.choice(['peak', 'ridge', 'valley', ''], 
                                                      p=[0.1, 0.1, 0.1, 0.7]),
                    'tourism_type': np.random.choice(['alpine_hut', 'shelter', 'viewpoint', ''], 
                                                      p=[0.1, 0.1, 0.1, 0.7]),
                    'coordinates': self._generate_coordinates_profile(region_info),
                    'description': self._generate_synthetic_description(region_info),
                    'source': 'synthetic',
                    'timestamp': datetime.utcnow().isoformat(),
                    'synthetic_metadata': {
                        'generation_method': 'random_walk',
                        'realism_score': np.random.uniform(0.7, 0.95)
                    }
                }
                
                # Añadir datos climáticos y de elevación si están disponibles
                if region_weather:
                    route_data['weather'] = region_weather
                if region_elevation:
                    route_data['region_elevation'] = region_elevation
                
                routes.append(route_data)
                
            except Exception as e:
                logger.warning(f"Error generando ruta sintética {i}: {e}")
                continue
        
        return routes
    
    def _is_hiking_related(self, element: Dict, tags: Dict) -> bool:
        """Determinar si un elemento OSM está relacionado con senderismo."""
        # Por tipo de elemento
        if element['type'] == 'relation' and tags.get('route') in ['hiking', 'foot']:
            return True
        
        # Por tipo de vía
        if element['type'] == 'way' and tags.get('highway') in ['path', 'footway', 'track']:
            return True
        
        # Por etiquetas específicas
        hiking_keywords = ['GR', 'PR', 'SL', 'sendero', 'ruta', 'camino']
        name = tags.get('name', '').lower()
        if any(keyword in name for keyword in hiking_keywords):
            return True
        
        return False
    
    def _generate_route_name(self, element: Dict, region_info: Dict) -> str:
        """Generar nombre para rutas sin nombre."""
        element_type = element['type']
        
        if element_type == 'relation':
            prefix = "Ruta de senderismo"
        elif element_type == 'way':
            prefix = "Sendero"
        elif element_type == 'node' and element.get('tags', {}).get('natural') == 'peak':
            prefix = "Ascenso a"
        else:
            prefix = "Camino"
        
        return f"{prefix} {region_info['name']} - ID {element['id']}"
    
    def _estimate_distance(self, element: Dict) -> str:
        """Estimar distancia basada en tipo de elemento."""
        # Usa función del utils.py
        distance = calculate_realistic_distance(element['type'])
        return str(distance)
    
    def _infer_difficulty(self, element: Dict, tags: Dict) -> str:
        """Inferir dificultad de la ruta."""
        ref = tags.get('ref', '')
        
        if 'GR' in ref or element['type'] == 'relation':
            return 'Difícil'
        elif 'PR' in ref:
            return 'Moderada'
        elif 'SL' in ref:
            return 'Fácil'
        elif tags.get('highway') == 'track':
            return 'Moderada'
        elif tags.get('natural') == 'peak':
            return 'Difícil'
        else:
            # Usa función reproducible del utils
            from utils import get_random_difficulty
            return get_random_difficulty()
    
    def _estimate_duration(self, distance_str: str) -> str:
        """Estimar duración basada en distancia."""
        # Usa función del utils.py
        return estimate_duration_from_distance(distance_str)
    
    def _generate_coordinates_profile(self, region_info: Dict) -> List[Dict]:
        """Generar perfil de elevación realista."""
        from utils import create_coordinates_profile
        
        base_elevation = region_info.get('base_elevation', 1000)
        max_ascent = region_info.get('max_ascent', 800)
        
        return create_coordinates_profile(base_elevation, max_ascent)
    
    def _generate_description(self, element: Dict, tags: Dict, region_info: Dict) -> str:
        """Generar descripción detallada."""
        mountain_range = region_info['name']
        element_type = element['type']
        
        base_descriptions = {
            "Sierra Nevada": "Ruta de alta montaña en Sierra Nevada con vistas espectaculares",
            "Pirineo Central": "Sendero pirenaico a través de valles y cumbres emblemáticas",
            "Sierra de Guadarrama": "Camino de montaña mediterránea en la Sierra de Guadarrama"
        }
        
        base_desc = base_descriptions.get(mountain_range, "Ruta de montaña")
        
        # Enriquecer con información específica
        details = []
        
        if tags.get('natural') == 'peak':
            details.append("ascensión a cumbre")
        if 'GR' in tags.get('ref', ''):
            details.append("sendero de gran recorrido")
        if 'PR' in tags.get('ref', ''):
            details.append("pequeño recorrido")
        if tags.get('waterway'):
            details.append("sigue curso de agua")
        
        if details:
            return f"{base_desc} - {', '.join(details)}"
        else:
            return f"{base_desc} - {element_type} ID {element['id']}"
    
    def _generate_synthetic_description(self, region_info: Dict) -> str:
        """Generar descripción para rutas sintéticas."""
        mountain_range = region_info['name']
        descriptions = [
            f"Bonita ruta circular en {mountain_range}",
            f"Ascenso panorámico en {mountain_range}",
            f"Sendero familiar en {mountain_range}",
            f"Ruta técnica de montaña en {mountain_range}",
            f"Camino histórico en {mountain_range}"
        ]
        return np.random.choice(descriptions)
    
    def _save_to_mongodb(self, routes: List[Dict]) -> Dict:
        """
        Guardar rutas en MongoDB con prevención de duplicados.
        """
        if not routes:
            return {'inserted': 0, 'duplicates': 0, 'errors': 0, 'skipped': 0}
        
        stats = {'inserted': 0, 'duplicates': 0, 'errors': 0, 'skipped': 0}
        
        for route in routes:
            try:
                # Verificar si ya existe
                if '_ingestion_metadata' in route:
                    data_hash = route['_ingestion_metadata']['data_hash']
                    existing = self.routes_collection.find_one(
                        {'_ingestion_metadata.data_hash': data_hash}
                    )
                    
                    if existing:
                        stats['duplicates'] += 1
                        continue
                
                # Insertar nueva ruta
                self.routes_collection.insert_one(route)
                stats['inserted'] += 1
                
            except Exception as e:
                stats['errors'] += 1
                logger.error(f" Error guardando ruta: {e}")
        
        return stats
    
    def _log_ingestion(self, region_key: str, stats: Dict, 
                      source_type: str = "osm") -> None:
        """Registrar metadatos de la operación de ingesta."""
        log_entry = {
            'timestamp': datetime.utcnow(),
            'region': region_key,
            'source_type': source_type,
            'status': 'completed' if stats['errors'] == 0 else 'partial',
            'stats': stats,
            'total_routes_attempted': stats.get('inserted', 0) + stats.get('duplicates', 0),
            'duration_seconds': stats.get('duration_seconds', 0)
        }
        
        self.ingestion_log.insert_one(log_entry)
        logger.info(f"Log registrado: {region_key} - {source_type} - {stats}")
    
    def ingest_region(self, region_key: str, 
                     use_synthetic_backup: bool = True) -> Dict:
        """
        Ingestar rutas de una región específica.
        """
        if region_key not in MOUNTAIN_AREAS:
            logger.error(f"Región no encontrada: {region_key}")
            return {'error': 'Región no encontrada'}
        
        region_info = MOUNTAIN_AREAS[region_key]
        target_routes = region_info.get('target_routes', 100)
        
        logger.info(f"INGESTANDO: {region_info['name']}")
        logger.info(f"   Objetivo: {target_routes} rutas")
        logger.info(f"   BBOX: {region_info['bbox']}")
        
        start_time = time.time()
        all_routes = []
        source_type = "unknown"
        
        # Obtener datos climáticos y de elevación para la región
        region_weather = None
        region_elevation = None
        
        if self.use_real_data:
            try:
                region_weather = self._get_weather_for_coordinates(region_info['center_lat'], region_info['center_lon'])
                if region_weather:
                    logger.info(f"Datos climáticos obtenidos para {region_info['name']}")
                region_elevation = self._get_elevation_for_coordinates(region_info['center_lat'], region_info['center_lon'])
                if region_elevation:
                    logger.info(f"Elevación obtenida para {region_info['name']}: {region_elevation}m")
            except Exception as e:
                logger.error(f"Error obteniendo datos adicionales para la región: {e}")
        
        # 1. INTENTAR DATOS REALES
        if self.use_real_data:
            try:
                osm_data = self._query_overpass_api(region_info['bbox'])
                
                if osm_data:
                    real_routes = self._process_real_osm_data(osm_data, region_key, region_weather, region_elevation)
                    all_routes.extend(real_routes)
                    logger.info(f"Datos reales obtenidos: {len(real_routes)} rutas")
                    
                    # Guardar datos crudos para trazabilidad
                    if osm_data.get('elements'):
                        raw_entry = {
                            'region': region_key,
                            'timestamp': datetime.utcnow(),
                            'element_count': len(osm_data['elements']),
                            'raw_data': osm_data,
                            'source': 'overpass_api'
                        }
                        self.raw_data_collection.insert_one(raw_entry)
                    
                    source_type = "osm"
            
            except Exception as e:
                logger.error(f"Error obteniendo datos reales: {e}")
        
        # 2. COMPLEMENTAR CON DATOS SINTÉTICOS SI ES NECESARIO
        if use_synthetic_backup and len(all_routes) < target_routes:
            needed = target_routes - len(all_routes)
            logger.info(f"Datos insuficientes. Generando {needed} rutas sintéticas")
            
            synthetic_routes = self._generate_synthetic_routes(region_key, needed, region_weather, region_elevation)
            all_routes.extend(synthetic_routes)
            
            if source_type == "osm":
                source_type = "hybrid"
            else:
                source_type = "synthetic"
            
            logger.info(f"Datos sintéticos generados: {len(synthetic_routes)} rutas")
        
        # 3. ENRIQUECER CON METADATOS
        enriched_routes = []
        for route in all_routes:
            enriched = self._enrich_with_metadata(route, region_key, source_type)
            enriched_routes.append(enriched)
        
        # 4. GUARDAR EN MONGODB
        stats = self._save_to_mongodb(enriched_routes)
        
        # 5. CALCULAR ESTADÍSTICAS FINALES
        elapsed_time = time.time() - start_time
        stats['duration_seconds'] = round(elapsed_time, 2)
        stats['region'] = region_key
        stats['source_type'] = source_type
        stats['target_routes'] = target_routes
        stats['success_rate'] = round(stats['inserted'] / len(all_routes) * 100, 2) if all_routes else 0
        
        # 6. REGISTRAR LOG
        self._log_ingestion(region_key, stats, source_type)
        
        # 7. MOSTRAR RESUMEN
        logger.info(f"RESUMEN {region_info['name']}:")
        logger.info(f"   • Rutas obtenidas: {len(all_routes)}/{target_routes}")
        logger.info(f"   • Insertadas: {stats['inserted']}")
        logger.info(f"   • Duplicadas: {stats['duplicates']}")
        logger.info(f"   • Fuente: {source_type}")
        logger.info(f"   • Tiempo: {stats['duration_seconds']}s")
        if region_weather:
            logger.info(f"   • Clima: {region_weather['weather_text']}")
        if region_elevation:
            logger.info(f"   • Elevación región: {region_elevation}m")
        
        # Pausa para no saturar APIs
        time.sleep(self.REQUEST_DELAY)
        
        return stats
    
    def ingest_all_regions(self) -> List[Dict]:
        """
        Ingestar rutas de todas las regiones configuradas.
        """
        logger.info("INICIANDO INGESTIÓN DE TODAS LAS REGIONES")
        
        all_stats = []
        
        for region_key in MOUNTAIN_AREAS:
            try:
                stats = self.ingest_region(region_key, self.use_synthetic_backup)
                all_stats.append(stats)
                
                # Pausa entre regiones
                if region_key != list(MOUNTAIN_AREAS.keys())[-1]:
                    logger.info(f"Esperando {self.REQUEST_DELAY * 2}s antes de la siguiente región...")
                    time.sleep(self.REQUEST_DELAY * 2)
                    
            except Exception as e:
                logger.error(f"Error en región {region_key}: {e}")
                all_stats.append({
                    'region': region_key,
                    'error': str(e),
                    'status': 'failed'
                })
        
        # RESUMEN FINAL
        total_inserted = sum(s.get('inserted', 0) for s in all_stats)
        total_duplicates = sum(s.get('duplicates', 0) for s in all_stats)
        total_time = sum(s.get('duration_seconds', 0) for s in all_stats)
        
        logger.info("=" * 60)
        logger.info("INGESTIÓN COMPLETADA")
        logger.info("=" * 60)
        
        for stats in all_stats:
            region = stats.get('region', 'Desconocida')
            inserted = stats.get('inserted', 0)
            source = stats.get('source_type', 'unknown')
            logger.info(f"   • {region}: {inserted} rutas ({source})")
        
        logger.info(f"TOTAL: {total_inserted} rutas nuevas")
        logger.info(f"Duplicados evitados: {total_duplicates}")
        logger.info(f"Tiempo total: {total_time:.1f}s")
        
        return all_stats
    
    def validate_connections(self) -> bool:
        """
        Validar todas las conexiones necesarias.
        """
        checks_passed = 0
        total_checks = 2
        
        # 1. Validar MongoDB
        try:
            self.client.admin.command('ping')
            logger.info("MongoDB: CONECTADO")
            checks_passed += 1
        except Exception as e:
            logger.error(f"MongoDB: ERROR - {e}")
        
        # 2. Validar API Overpass (solo si se usan datos reales)
        if self.use_real_data:
            total_checks += 1
            try:
                test_bbox = MOUNTAIN_AREAS["sierra_nevada"]["bbox"]
                test_query = f"[out:json];node({test_bbox[1]},{test_bbox[0]},{test_bbox[3]},{test_bbox[2]});out count;"
                
                response = requests.post(self.OVERPASS_URL, data=test_query, timeout=30)
                response.raise_for_status()
                
                logger.info("Overpass API: CONECTADA")
                checks_passed += 1
            except Exception as e:
                logger.warning(f"Overpass API: NO CONECTADA - {e}")
                if not self.use_synthetic_backup:
                    logger.error("No hay datos sintéticos de respaldo")
                    return False
        
        # 3. Validar Open-Meteo (solo si se usan datos reales)
        if self.use_real_data:
            total_checks += 1
            try:
                test_url = OPEN_METEO_URL
                params = {
                    "latitude": 40.0,
                    "longitude": -3.0,
                    "start_date": (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                    "end_date": (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                    "daily": "temperature_2m_max"
                }
                response = requests.get(test_url, params=params, timeout=10)
                if response.status_code == 200:
                    logger.info("Open-Meteo API: CONECTADA")
                    checks_passed += 1
                else:
                    logger.warning(f"Open-Meteo API: NO CONECTADA - Status {response.status_code}")
            except Exception as e:
                logger.warning(f"Open-Meteo API: NO CONECTADA - {e}")
        
        # 4. Validar OpenTopoData (solo si se usan datos reales)
        if self.use_real_data:
            total_checks += 1
            try:
                test_url = OPEN_TOPO_URL
                params = {"locations": "40.0,-3.0"}
                response = requests.get(test_url, params=params, timeout=10)
                if response.status_code == 200:
                    logger.info("OpenTopoData API: CONECTADA")
                    checks_passed += 1
                else:
                    logger.warning(f"OpenTopoData API: NO CONECTADA - Status {response.status_code}")
            except Exception as e:
                logger.warning(f"OpenTopoData API: NO CONECTADA - {e}")
        
        logger.info(f"Validación: {checks_passed}/{total_checks} checks aprobados")
        
        return checks_passed >= total_checks - 2
    
    def get_database_stats(self) -> Dict:
        """Obtener estadísticas de la base de datos."""
        try:
            total_routes = self.routes_collection.count_documents({})
            
            # Conteo por región
            regions = list(MOUNTAIN_AREAS.keys())
            region_counts = {}
            
            for region in regions:
                count = self.routes_collection.count_documents(
                    {'mountain_range': MOUNTAIN_AREAS[region]['name']}
                )
                region_counts[region] = count
            
            # Conteo por fuente
            pipeline = [
                {"$group": {
                    "_id": "$_ingestion_metadata.source",
                    "count": {"$sum": 1}
                }}
            ]
            source_counts = list(self.routes_collection.aggregate(pipeline))
            
            stats = {
                'total_routes': total_routes,
                'region_counts': region_counts,
                'source_counts': {item['_id']: item['count'] for item in source_counts},
                'database': self.db.name,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            return {}
    
    def close(self):
        """Cerrar conexiones."""
        if self.client:
            self.client.close()
            logger.info("Conexiones cerradas")

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal del script de ingestión."""
    
    parser = argparse.ArgumentParser(
        description='Sistema de ingestión de rutas de senderismo - TFM Data Engineering',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  %(prog)s --all                         # Ingestar todas las regiones
  %(prog)s --region sierra_nevada        # Ingestar solo Sierra Nevada
  %(prog)s --stats                       # Mostrar estadísticas
  %(prog)s --test-mode --limit 10        # Modo test con límite
  %(prog)s --verbose --all               # Modo verboso todas las regiones
        """
    )
    
    region_group = parser.add_mutually_exclusive_group()
    region_group.add_argument('--region', '-r', type=str, 
                       choices=list(MOUNTAIN_AREAS.keys()),
                       help='Región específica a ingestar')
    region_group.add_argument('--all', '-a', action='store_true',
                       help='Ingestar todas las regiones')
    
    data_group = parser.add_mutually_exclusive_group()
    data_group.add_argument('--real-only', action='store_true',
                       help='Usar solo datos reales (sin respaldo sintético)')
    data_group.add_argument('--synthetic-only', action='store_true',
                       help='Usar solo datos sintéticos')
    
    parser.add_argument('--stats', '-s', action='store_true',
                       help='Mostrar estadísticas de la base de datos')
    parser.add_argument('--validate', action='store_true',
                       help='Validar conexiones y salir')
    
    parser.add_argument('--test-mode', action='store_true',
                       help='Ejecutar en modo test (sin APIs externas, más rápido)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Límite de rutas a procesar (solo para test)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Mostrar logs detallados')
    
    parser.add_argument('--mongo-uri', type=str, default='mongodb://localhost:27017/',
                       help='URI de conexión a MongoDB')
    parser.add_argument('--database', type=str, default='hiking_trails_tfm',
                       help='Nombre de la base de datos')
    parser.add_argument('--dry-run', action='store_true',
                       help='Simular sin guardar en base de datos')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("Modo verboso activado")
    
    logger.info("=" * 70)
    logger.info("TFM DATA ENGINEERING - SISTEMA DE INGESTIÓN DE RUTAS")
    logger.info("=" * 70)
    
    logger.info("CONFIGURACIÓN:")
    logger.info(f"   • MongoDB: {args.mongo_uri}/{args.database}")
    logger.info(f"   • Semilla: {REPRODUCIBILITY_SEED} (reproducible)")
    if args.test_mode:
        logger.info("   • Modo: TEST")
    if args.limit:
        logger.info(f"   • Límite: {args.limit} rutas")
    if args.dry_run:
        logger.info("   • Dry run: Activado (no se guardará en BD)")
    
    if args.synthetic_only:
        use_real_data = False
        use_synthetic_backup = True
        logger.info("MODO: Solo datos sintéticos")
    elif args.real_only:
        use_real_data = True
        use_synthetic_backup = False
        logger.info("MODO: Solo datos reales")
    elif args.test_mode:
        use_real_data = False
        use_synthetic_backup = True
        logger.info("MODO: Test (solo datos sintéticos)")
    else:
        use_real_data = True
        use_synthetic_backup = True
        logger.info("MODO: Híbrido (real + respaldo sintético)")
    
    try:
        ingestor = HikingTrailsIngestor(
            mongo_uri=args.mongo_uri,
            database=args.database,
            use_real_data=use_real_data,
            use_synthetic_backup=use_synthetic_backup
        )
    except Exception as e:
        logger.error(f"No se pudo inicializar el ingestor: {e}")
        sys.exit(1)
    
    if not args.test_mode and not ingestor.validate_connections():
        logger.error("Validación de conexiones fallida")
        ingestor.close()
        sys.exit(1)
    
    try:
        if args.validate:
            logger.info("Validación completada exitosamente")
            
        elif args.stats:
            stats = ingestor.get_database_stats()
            logger.info("ESTADÍSTICAS DE LA BASE DE DATOS:")
            logger.info(f"   Total rutas: {stats.get('total_routes', 0):,}")
            for region, count in stats.get('region_counts', {}).items():
                region_name = MOUNTAIN_AREAS.get(region, {}).get('name', region)
                logger.info(f"   • {region_name}: {count:,} rutas")
            for source, count in stats.get('source_counts', {}).items():
                logger.info(f"   • Fuente {source}: {count:,} rutas")
        
        elif args.region:
            region_name = MOUNTAIN_AREAS[args.region]['name']
            logger.info(f"Ingestando región: {region_name}")
            
            if args.test_mode and args.limit:
                logger.info(f"Modo test: limitado a {args.limit} rutas")
                original_target = MOUNTAIN_AREAS[args.region]['target_routes']
                MOUNTAIN_AREAS[args.region]['target_routes'] = args.limit
                stats = ingestor.ingest_region(args.region)
                MOUNTAIN_AREAS[args.region]['target_routes'] = original_target
            else:
                stats = ingestor.ingest_region(args.region)
                
            logger.info(f"{region_name}: {stats.get('inserted', 0)} rutas insertadas")
            
        elif args.all:
            logger.info("Ingestando todas las regiones")
            
            if args.test_mode and args.limit:
                logger.info(f"Modo test: limitado a {args.limit} rutas por región")
                original_targets = {}
                for region_key in MOUNTAIN_AREAS:
                    original_targets[region_key] = MOUNTAIN_AREAS[region_key]['target_routes']
                    MOUNTAIN_AREAS[region_key]['target_routes'] = min(args.limit, 10)
                
                stats_list = ingestor.ingest_all_regions()
                
                for region_key, original_target in original_targets.items():
                    MOUNTAIN_AREAS[region_key]['target_routes'] = original_target
            else:
                stats_list = ingestor.ingest_all_regions()
            
        else:
            if len(sys.argv) == 1:
                logger.info(" Modo interactivo - Selecciona una opción:")
                logger.info("   1. Ingestar todas las regiones")
                logger.info("   2. Ingestar región específica")
                logger.info("   3. Mostrar estadísticas")
                logger.info("   4. Salir")
                
                choice = input("\n Opción: ").strip()
                
                if choice == "1":
                    stats = ingestor.ingest_all_regions()
                elif choice == "2":
                    print("\n Regiones disponibles:")
                    for key, info in MOUNTAIN_AREAS.items():
                        print(f"   • {key}: {info['name']}")
                    region = input("\nNombre de la región: ").strip()
                    if region in MOUNTAIN_AREAS:
                        stats = ingestor.ingest_region(region)
                    else:
                        logger.error(f"Región no válida: {region}")
                elif choice == "3":
                    stats = ingestor.get_database_stats()
                    print("\n" + json.dumps(stats, indent=2, ensure_ascii=False))
                else:
                    logger.info("Saliendo...")
            else:
                parser.print_help()
    
    except KeyboardInterrupt:
        logger.info("\nProceso interrumpido por el usuario")
    except Exception as e:
        logger.error(f"Error en ejecución: {e}")
        logger.error(traceback.format_exc())
    
    finally:
        ingestor.close()
        logger.info("Proceso completado")

# ============================================================================
# EJECUCIÓN
# ============================================================================

if __name__ == "__main__":
    main()