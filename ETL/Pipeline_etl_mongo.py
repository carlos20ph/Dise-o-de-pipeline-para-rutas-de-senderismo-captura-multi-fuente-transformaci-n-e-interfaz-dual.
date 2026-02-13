"""
PIPELINE ETL UNIFICADO CON TRANSFORMACIONES AVANZADAS
Pipeline completo desde MongoDB con limpieza y transformaciones avanzadas integradas
"""

import os
import json
import hashlib
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import warnings
import argparse
import logging

import numpy as np
import pandas as pd
from pymongo import MongoClient

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONEXIÓN A MONGODB
# ============================================================================

class MongoDBConnector:
    """Conector a MongoDB para lectura/escritura de datos"""
    
    def __init__(self, mongo_uri="mongodb://localhost:27017/", database="hiking_trails_tfm"):
        """Inicializar conexión a MongoDB"""
        try:
            self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client[database]
            logger.info(f"Conectado a MongoDB: {database}")
        except Exception as e:
            logger.error(f"Error conectando a MongoDB: {e}")
            raise
    
    def get_all_routes(self, limit: Optional[int] = None, 
                      source_filter: Optional[str] = None,
                      batch_size: int = 1000) -> List[Dict]:
        """Obtener rutas con paginación para mejor performance"""
        collection = self.db['routes']
        
        query = {}
        if source_filter:
            query['_ingestion_metadata.source'] = source_filter
        
        if limit:
            cursor = collection.find(query).limit(limit)
        else:
            cursor = collection.find(query).batch_size(batch_size)
        
        routes = list(cursor)
        
        # Convertir ObjectId a string para serialización
        for route in routes:
            if '_id' in route:
                route['_id'] = str(route['_id'])
        
        logger.info(f"Rutas obtenidas: {len(routes)}")
        return routes
    
    def get_routes_by_date_range(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Obtener rutas ingeridas en un rango de fechas (para ingestión incremental)"""
        collection = self.db['routes']
        
        query = {
            '_ingestion_metadata.ingestion_timestamp': {
                '$gte': start_date.isoformat(),
                '$lte': end_date.isoformat()
            }
        }
        
        routes = list(collection.find(query))
        for route in routes:
            if '_id' in route:
                route['_id'] = str(route['_id'])
        
        logger.info(f"Rutas en rango {start_date.date()} - {end_date.date()}: {len(routes)}")
        return routes
    
    def save_transformed_data(self, df: pd.DataFrame, 
                            collection_name: str = 'transformed_routes',
                            replace: bool = True) -> Tuple[int, int]:
        """
        Guardar datos transformados en MongoDB con control de versiones
        
        Returns:
            (inserted_count, updated_count)
        """
        try:
            collection = self.db[collection_name]
            
            if replace:
                # Eliminar colección existente
                collection.delete_many({})
                logger.info(f"Coleccion '{collection_name}' limpiada")
            
            # Añadir metadata de transformación
            records = df.to_dict('records')
            for record in records:
                record['_transformation_metadata'] = {
                    'transformed_at': datetime.utcnow().isoformat(),
                    'etl_version': '3.0',
                    'data_hash': hashlib.md5(str(record).encode()).hexdigest()[:16]
                }
            
            # Insertar en lotes para mejor performance
            batch_size = 100
            inserted_count = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                result = collection.insert_many(batch)
                inserted_count += len(result.inserted_ids)
            
            logger.info(f"{inserted_count} documentos guardados en '{collection_name}'")
            return inserted_count, 0
            
        except Exception as e:
            logger.error(f"Error guardando en MongoDB: {e}")
            return 0, 0
    
    def get_database_stats(self) -> Dict:
        """Obtener estadísticas completas de la base de datos"""
        try:
            stats = {
                'routes': self.db['routes'].count_documents({}),
                'transformed_routes': self.db['transformed_routes'].count_documents({}),
                'ingestion_log': self.db['ingestion_log'].count_documents({}),
                'collections': list(self.db.list_collection_names()),
                'database_size': self.db.command('dbStats')['dataSize'],
                'last_ingestion': None,
                'last_transformation': None
            }
            
            # Última ingesta
            last_ingestion = list(self.db['ingestion_log'].find().sort('timestamp', -1).limit(1))
            if last_ingestion:
                stats['last_ingestion'] = last_ingestion[0]['timestamp']
            
            # Última transformación
            last_transform = list(self.db['transformed_routes'].find().sort('_transformation_metadata.transformed_at', -1).limit(1))
            if last_transform and '_transformation_metadata' in last_transform[0]:
                stats['last_transformation'] = last_transform[0]['_transformation_metadata']['transformed_at']
            
            return stats
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            return {}
    
    def close(self):
        """Cerrar conexión a MongoDB"""
        if self.client:
            self.client.close()
            logger.info("Conexión MongoDB cerrada")

# ============================================================================
# TRANSFORMACIONES BÁSICAS (LIMPIEZA)
# ============================================================================

class DataCleaner:
    """Clase para limpieza básica de datos"""
    
    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Limpieza básica del DataFrame"""
        if df.empty:
            return df
        
        df_clean = df.copy()
        
        # 1. Eliminar columnas de metadatos internos
        columns_to_drop = ['_id', '__v', 'raw_tags', 'synthetic_metadata']
        existing_cols = [col for col in columns_to_drop if col in df_clean.columns]
        if existing_cols:
            df_clean = df_clean.drop(columns=existing_cols)
        
        # 2. Normalizar tipos de datos
        numeric_columns = ['distance_km', 'ascent_m', 'descent_m']
        for col in numeric_columns:
            if col in df_clean.columns:
                # Primero convertir cualquier string a número
                if df_clean[col].dtype == object:
                    df_clean[col] = df_clean[col].astype(str).str.extract(r'(\d+\.?\d*)')[0]
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        
        # 3. Rellenar valores nulos
        for col in numeric_columns:
            if col in df_clean.columns:
                median_val = df_clean[col].median() if not df_clean[col].isna().all() else 0
                df_clean[col] = df_clean[col].fillna(median_val)
        
        # 4. Normalizar strings
        if 'difficulty' in df_clean.columns:
            valid_difficulties = ['Fácil', 'Moderada', 'Difícil']
            df_clean['difficulty'] = df_clean['difficulty'].apply(
                lambda x: x if x in valid_difficulties else 'Moderada'
            )
        
        if 'mountain_range' in df_clean.columns:
            df_clean['mountain_range'] = df_clean['mountain_range'].fillna('Desconocido')
        
        logger.info(f"Datos limpiados: {len(df_clean)} registros")
        return df_clean
    
    @staticmethod
    def extract_elevation_data(df: pd.DataFrame) -> pd.DataFrame:
        """Extraer información de elevación de las coordenadas"""
        if df.empty or 'coordinates' not in df.columns:
            return df
        
        df_with_elevation = df.copy()
        
        try:
            elevation_metrics = []
            
            for idx, coords in enumerate(df_with_elevation['coordinates']):
                if isinstance(coords, list) and len(coords) > 0:
                    elevations = [point.get('elevation_m', 0) for point in coords]
                    slopes = [point.get('slope_percent', 0) for point in coords]
                    
                    if elevations:
                        max_elev = max(elevations)
                        min_elev = min(elevations)
                        avg_slope = np.mean(slopes) if slopes else 0
                        total_climb = sum(max(0, elevations[i] - elevations[i-1]) 
                                         for i in range(1, len(elevations)))
                    else:
                        max_elev = min_elev = avg_slope = total_climb = 0
                    
                    elevation_metrics.append({
                        'max_elevation': max_elev,
                        'min_elevation': min_elev,
                        'elevation_gain': total_climb,
                        'avg_slope': avg_slope,
                        'elevation_range': max_elev - min_elev
                    })
                else:
                    elevation_metrics.append({
                        'max_elevation': 0,
                        'min_elevation': 0,
                        'elevation_gain': 0,
                        'avg_slope': 0,
                        'elevation_range': 0
                    })
            
            # Añadir métricas al DataFrame
            elevation_df = pd.DataFrame(elevation_metrics)
            df_with_elevation = pd.concat([df_with_elevation, elevation_df], axis=1)
            logger.info("Datos de elevación extraídos")
            
        except Exception as e:
            logger.warning(f"Error extrayendo elevación: {e}")
        
        return df_with_elevation

# ============================================================================
# TRANSFORMACIONES AVANZADAS (FEATURE ENGINEERING)
# ============================================================================

class FeatureEngineer:
    """Clase para crear características avanzadas y métricas derivadas"""
    
    @staticmethod
    def safe_divide(numerator, denominator):
        """División segura que evita divisiones por cero"""
        try:
            if isinstance(denominator, (int, float, np.number)) and denominator != 0:
                return numerator / denominator
            return 0.0
        except (TypeError, ValueError):
            return 0.0
    
    @staticmethod
    def calculate_basic_metrics(df: pd.DataFrame) -> pd.DataFrame:
        """Calcular métricas básicas derivadas"""
        if df.empty:
            return df
        
        df_metrics = df.copy()
        
        # 1. Intensidad (desnivel/km) - CON DIVISIÓN SEGURA
        if 'ascent_m' in df_metrics.columns and 'distance_km' in df_metrics.columns:
            df_metrics['intensity'] = df_metrics.apply(
                lambda row: FeatureEngineer.safe_divide(row['ascent_m'], row['distance_km']), 
                axis=1
            )
        
        # 2. Clasificar por intensidad
        if 'intensity' in df_metrics.columns:
            conditions = [
                df_metrics['intensity'] > 100,
                df_metrics['intensity'] > 60,
                df_metrics['intensity'] > 30,
                df_metrics['intensity'] > 0
            ]
            choices = ['Muy Alta', 'Alta', 'Media', 'Baja']
            df_metrics['intensity_level'] = np.select(conditions, choices, default='Nula')
        
        # 3. Duración estimada (modelo más preciso)
        if 'distance_km' in df_metrics.columns and 'ascent_m' in df_metrics.columns:
            # Modelo: 4 km/h en plano, más penalización por desnivel
            base_hours = df_metrics['distance_km'] / 4.0
            ascent_penalty = df_metrics['ascent_m'] / 400  # 400m de desnivel = +1h
            df_metrics['estimated_hours'] = base_hours + ascent_penalty
        
        # 4. Ratio ascenso/descenso - CON DIVISIÓN SEGURA
        if 'ascent_m' in df_metrics.columns and 'descent_m' in df_metrics.columns:
            df_metrics['ascent_descent_ratio'] = df_metrics.apply(
                lambda row: FeatureEngineer.safe_divide(row['ascent_m'], row['descent_m']),
                axis=1
            )
        
        # 5. Dificultad numérica (para ordenación)
        if 'difficulty' in df_metrics.columns:
            difficulty_map = {'Fácil': 1, 'Moderada': 2, 'Difícil': 3}
            df_metrics['difficulty_numeric'] = df_metrics['difficulty'].map(difficulty_map).fillna(2)
        
        # 6. Categorías por longitud
        if 'distance_km' in df_metrics.columns:
            def categorize_length(distance):
                try:
                    dist = float(distance)
                    if dist <= 5:
                        return 'Corta (<5km)'
                    elif dist <= 15:
                        return 'Media (5-15km)'
                    elif dist <= 25:
                        return 'Larga (15-25km)'
                    else:
                        return 'Muy Larga (>25km)'
                except (ValueError, TypeError):
                    return 'Desconocida'
            
            df_metrics['length_category'] = df_metrics['distance_km'].apply(categorize_length)
        
        # 7. Categorías por desnivel
        if 'ascent_m' in df_metrics.columns:
            def categorize_ascent(ascent):
                try:
                    asc = float(ascent)
                    if asc <= 300:
                        return 'Suave (<300m)'
                    elif asc <= 800:
                        return 'Moderado (300-800m)'
                    elif asc <= 1500:
                        return 'Exigente (800-1500m)'
                    else:
                        return 'Extremo (>1500m)'
                except (ValueError, TypeError):
                    return 'Desconocido'
            
            df_metrics['ascent_category'] = df_metrics['ascent_m'].apply(categorize_ascent)
        
        logger.info(f"Métricas básicas calculadas: {len(df_metrics)} registros")
        return df_metrics
    
    @staticmethod
    def calculate_seasonality(row) -> str:
        """Calcula la mejor época para hacer cada ruta basada en región y elevación"""
        mountain_range = str(row.get('mountain_range', ''))
        max_elevation = row.get('max_elevation', 0)
        
        if 'Sierra Nevada' in mountain_range:
            if isinstance(max_elevation, (int, float, np.number)) and max_elevation > 2500:
                return 'Verano (junio-septiembre) - Alta montaña'
            else:
                return 'Primavera y Otoño'
        
        elif 'Pirineo' in mountain_range:
            if isinstance(max_elevation, (int, float, np.number)) and max_elevation > 2000:
                return 'Verano (julio-septiembre)'
            else:
                return 'Mayo a Octubre'
        
        elif 'Guadarrama' in mountain_range:
            return 'Todo el año (evitar horas centrales en verano)'
        
        else:
            return 'Primavera y Otoño recomendadas'
    
    @staticmethod
    def classify_technical_level(row) -> str:
        """Clasifica la ruta por su dificultad técnica"""
        intensity = row.get('intensity', 0)
        avg_slope = row.get('avg_slope', 0)
        difficulty = row.get('difficulty', 'Moderada')
        
        # Clasificación basada en múltiples factores
        technical_score = 0
        
        # Factor dificultad
        if difficulty == 'Difícil':
            technical_score += 3
        elif difficulty == 'Moderada':
            technical_score += 2
        else:
            technical_score += 1
        
        # Factor intensidad
        if isinstance(intensity, (int, float, np.number)):
            if intensity > 80:
                technical_score += 3
            elif intensity > 60:
                technical_score += 2
            elif intensity > 30:
                technical_score += 1
        
        # Factor pendiente
        if isinstance(avg_slope, (int, float, np.number)):
            if avg_slope > 15:
                technical_score += 2
            elif avg_slope > 10:
                technical_score += 1
        
        # Determinar nivel técnico
        if technical_score >= 6:
            return 'Técnica (experiencia avanzada)'
        elif technical_score >= 4:
            return 'Media-Alta (experiencia media)'
        elif technical_score >= 2:
            return 'Media (senderista habitual)'
        else:
            return 'Baja (accesible para principiantes)'
    
    @staticmethod
    def generate_route_score(row) -> float:
        """
        Genera un score de calidad/atractivo de la ruta (0-100)
        Basado en múltiples factores ponderados
        """
        score = 0.0
        
        # 1. Distancia (óptimo 8-20km)
        distance = row.get('distance_km', 0)
        if isinstance(distance, (int, float, np.number)):
            if 8 <= distance <= 20:
                score += 25
            elif 5 <= distance < 8 or 20 < distance <= 25:
                score += 15
            else:
                score += 5
        else:
            score += 5
        
        # 2. Desnivel (óptimo 400-1200m)
        ascent = row.get('ascent_m', 0)
        if isinstance(ascent, (int, float, np.number)):
            if 400 <= ascent <= 1200:
                score += 25
            elif 200 <= ascent < 400 or 1200 < ascent <= 2000:
                score += 15
            else:
                score += 5
        else:
            score += 5
        
        # 3. Intensidad (óptimo 30-80 m/km)
        intensity = row.get('intensity', 0)
        if isinstance(intensity, (int, float, np.number)):
            if 30 <= intensity <= 80:
                score += 20
            elif 15 <= intensity < 30 or 80 < intensity <= 120:
                score += 12
            else:
                score += 4
        else:
            score += 4
        
        # 4. Completitud de datos
        data_score = 0
        max_elevation = row.get('max_elevation', 0)
        if isinstance(max_elevation, (int, float, np.number)) and max_elevation > 0:
            data_score += 5
        
        avg_slope = row.get('avg_slope', 0)
        if isinstance(avg_slope, (int, float, np.number)) and avg_slope > 0:
            data_score += 5
        
        coordinates = row.get('coordinates')
        if coordinates and isinstance(coordinates, list):
            data_score += 5
        
        score += data_score
        
        # 5. Interés técnico (basado en variación de pendiente y elevación)
        elevation_range = row.get('elevation_range', 0)
        if isinstance(elevation_range, (int, float, np.number)):
            if elevation_range > 1000:
                score += 15
            elif elevation_range > 500:
                score += 10
            elif elevation_range > 200:
                score += 5
        
        # Normalizar a 100
        return min(100.0, round(score, 2))
    
    @staticmethod
    def create_advanced_features(df: pd.DataFrame) -> pd.DataFrame:
        """Crear características avanzadas para análisis y ML"""
        if df.empty:
            return df
        
        # Verificar columnas requeridas
        required_cols = ['distance_km', 'ascent_m', 'difficulty']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            logger.warning(f"Columnas faltantes para features avanzados: {missing_cols}")
            # Crear columnas dummy
            for col in missing_cols:
                if 'km' in col or 'm' in col:
                    df[col] = 0
                else:
                    df[col] = 'Desconocido'
        
        df_features = df.copy()
        
        # 1. Mejor época (estacionalidad)
        df_features['best_season'] = df_features.apply(
            FeatureEngineer.calculate_seasonality, axis=1
        )
        
        # 2. Nivel técnico
        df_features['technical_level'] = df_features.apply(
            FeatureEngineer.classify_technical_level, axis=1
        )
        
        # 3. Score de calidad
        df_features['route_score'] = df_features.apply(
            FeatureEngineer.generate_route_score, axis=1
        )
        
        # 4. Categoría combinada
        def create_combined_category(row):
            length_cat = row.get('length_category', '')
            ascent_cat = row.get('ascent_category', '')
            if length_cat and ascent_cat:
                return f"{length_cat} - {ascent_cat}"
            return "No categorizada"
        
        df_features['combined_category'] = df_features.apply(create_combined_category, axis=1)
        
        # 5. Amigable para fin de semana - CON SEGURIDAD
        if 'estimated_hours' in df_features.columns:
            try:
                # Convertir a numérico si es necesario
                if df_features['estimated_hours'].dtype == object:
                    df_features['estimated_hours'] = pd.to_numeric(df_features['estimated_hours'], errors='coerce')
                df_features['weekend_friendly'] = df_features['estimated_hours'] <= 8
                # Rellenar NaN con False
                df_features['weekend_friendly'] = df_features['weekend_friendly'].fillna(False)
            except Exception as e:
                logger.warning(f"Error calculando weekend_friendly: {e}")
                df_features['weekend_friendly'] = False
        
        # 6. Amigable para familias - CORREGIDO CON np.where
        if all(col in df_features.columns for col in ['difficulty', 'distance_km', 'ascent_m']):
            try:
                # Asegurar tipos de datos correctos
                df_features['distance_km'] = pd.to_numeric(df_features['distance_km'], errors='coerce')
                df_features['ascent_m'] = pd.to_numeric(df_features['ascent_m'], errors='coerce')
                
                # Usar np.where para operación vectorizada segura
                df_features['family_friendly'] = np.where(
                    (df_features['difficulty'] == 'Fácil') & 
                    (df_features['distance_km'] <= 8) & 
                    (df_features['ascent_m'] <= 300),
                    True,
                    False
                )
                
                # Rellenar NaN con False
                df_features['family_friendly'] = df_features['family_friendly'].fillna(False)
                
            except Exception as e:
                logger.warning(f"Error calculando family_friendly: {e}")
                df_features['family_friendly'] = False
        else:
            df_features['family_friendly'] = False
        
        # 7. Ruta para entrenamiento - CORREGIDO CON np.where
        if all(col in df_features.columns for col in ['intensity', 'distance_km']):
            try:
                # Asegurar tipos de datos correctos
                df_features['intensity'] = pd.to_numeric(df_features['intensity'], errors='coerce')
                df_features['distance_km'] = pd.to_numeric(df_features['distance_km'], errors='coerce')
                
                # Usar np.where para operación vectorizada segura
                df_features['training_route'] = np.where(
                    (df_features['intensity'] > 50) & (df_features['distance_km'] > 10),
                    True,
                    False
                )
                
                # Rellenar NaN con False
                df_features['training_route'] = df_features['training_route'].fillna(False)
                
            except Exception as e:
                logger.warning(f"Error calculando training_route: {e}")
                df_features['training_route'] = False
        else:
            df_features['training_route'] = False
        
        # 8. Clasificación por score
        def classify_by_score(score):
            try:
                s = float(score)
                if s >= 80:
                    return 'Excelente'
                elif s >= 60:
                    return 'Muy buena'
                elif s >= 40:
                    return 'Buena'
                elif s >= 20:
                    return 'Regular'
                else:
                    return 'Básica'
            except (ValueError, TypeError):
                return 'No calificada'
        
        df_features['quality_class'] = df_features['route_score'].apply(classify_by_score)
        
        # 9. Descripción enriquecida automática
        def generate_enriched_description(row):
            try:
                name = row.get('name', 'Ruta de montaña')
                mountain_range = row.get('mountain_range', '')
                distance = row.get('distance_km', 0)
                ascent = row.get('ascent_m', 0)
                difficulty = row.get('difficulty', 'Moderada')
                best_season = row.get('best_season', 'Primavera y Otoño')
                
                desc = f"{name} - {distance:.1f}km en {mountain_range}. "
                desc += f"Desnivel: {ascent:.0f}m, Dificultad: {difficulty}. "
                desc += f"Recomendada en {best_season}. "
                
                if 'route_score' in row:
                    quality = classify_by_score(row['route_score'])
                    desc += f"Calificación: {quality} ({row['route_score']}/100)."
                
                return desc
            except Exception:
                return "Descripción no disponible"
        
        df_features['enriched_description'] = df_features.apply(generate_enriched_description, axis=1)
        
        logger.info(f"Características avanzadas creadas: {len(df_features)} registros")
        return df_features
# ============================================================================
# ANÁLISIS Y REPORTING
# ============================================================================

class DataAnalyzer:
    """Clase para análisis de datos y generación de reportes"""
    
    @staticmethod
    def analyze_routes_data(df: pd.DataFrame) -> Dict[str, Any]:
        """Análisis estadístico completo de las rutas"""
        if df.empty:
            return {}
        
        stats = {
            'general': {},
            'by_region': {},
            'by_difficulty': {},
            'by_quality': {},
            'correlations': {},
            'timestamps': {
                'analysis_date': datetime.now().isoformat(),
                'data_points': len(df)
            }
        }
        
        # Estadísticas generales
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        try:
            stats['general'] = {
                'total_routes': len(df),
                'avg_distance_km': float(df['distance_km'].mean()) if 'distance_km' in numeric_cols else 0,
                'avg_ascent_m': float(df['ascent_m'].mean()) if 'ascent_m' in numeric_cols else 0,
                'avg_intensity': float(df['intensity'].mean()) if 'intensity' in numeric_cols else 0,
                'avg_route_score': float(df['route_score'].mean()) if 'route_score' in numeric_cols else 0,
                'total_distance_km': float(df['distance_km'].sum()) if 'distance_km' in numeric_cols else 0,
                'total_ascent_m': float(df['ascent_m'].sum()) if 'ascent_m' in numeric_cols else 0
            }
        except Exception as e:
            logger.warning(f"Error calculando estadísticas generales: {e}")
            stats['general'] = {'total_routes': len(df)}
        
        # Por región
        if 'mountain_range' in df.columns:
            for region in df['mountain_range'].unique():
                region_df = df[df['mountain_range'] == region]
                try:
                    stats['by_region'][region] = {
                        'count': len(region_df),
                        'avg_distance': float(region_df['distance_km'].mean()) if 'distance_km' in region_df.columns else 0,
                        'avg_ascent': float(region_df['ascent_m'].mean()) if 'ascent_m' in region_df.columns else 0,
                        'avg_score': float(region_df['route_score'].mean()) if 'route_score' in region_df.columns else 0,
                        'most_common_difficulty': region_df['difficulty'].mode().iloc[0] if not region_df['difficulty'].mode().empty else 'N/A'
                    }
                except Exception:
                    stats['by_region'][region] = {'count': len(region_df)}
        
        # Por dificultad
        if 'difficulty' in df.columns:
            for diff in df['difficulty'].unique():
                diff_df = df[df['difficulty'] == diff]
                try:
                    stats['by_difficulty'][diff] = {
                        'count': len(diff_df),
                        'avg_distance': float(diff_df['distance_km'].mean()) if 'distance_km' in diff_df.columns else 0,
                        'avg_ascent': float(diff_df['ascent_m'].mean()) if 'ascent_m' in diff_df.columns else 0,
                        'avg_intensity': float(diff_df['intensity'].mean()) if 'intensity' in diff_df.columns else 0,
                        'avg_score': float(diff_df['route_score'].mean()) if 'route_score' in diff_df.columns else 0
                    }
                except Exception:
                    stats['by_difficulty'][diff] = {'count': len(diff_df)}
        
        # Por calidad
        if 'quality_class' in df.columns:
            for quality in df['quality_class'].unique():
                quality_df = df[df['quality_class'] == quality]
                try:
                    stats['by_quality'][quality] = {
                        'count': len(quality_df),
                        'avg_distance': float(quality_df['distance_km'].mean()) if 'distance_km' in quality_df.columns else 0,
                        'avg_ascent': float(quality_df['ascent_m'].mean()) if 'ascent_m' in quality_df.columns else 0
                    }
                except Exception:
                    stats['by_quality'][quality] = {'count': len(quality_df)}
        
        # Correlaciones (solo si hay suficientes datos)
        if len(numeric_cols) > 1 and len(df) > 10:
            try:
                corr_matrix = df[numeric_cols].corr()
                # Convertir a formato serializable
                stats['correlations'] = corr_matrix.where(
                    np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
                ).stack().to_dict()
            except Exception:
                stats['correlations'] = {}
        
        return stats
    
    @staticmethod
    def generate_analysis_report(df: pd.DataFrame, filename: Optional[str] = None) -> Dict:
        """
        Generar reporte analítico completo
        """
        if df.empty:
            return {}
        
        report = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_routes': len(df),
                'data_sources': df['_ingestion_metadata.source'].unique().tolist() if '_ingestion_metadata.source' in df.columns else ['unknown']
            },
            'summary': {},
            'recommendations': [],
            'top_routes': [],
            'warnings': []
        }
        
        # Resumen estadístico
        stats = DataAnalyzer.analyze_routes_data(df)
        report['summary'] = stats['general']
        
        # Top rutas por score
        if 'route_score' in df.columns and len(df) > 0:
            try:
                top_routes = df.nlargest(10, 'route_score')
                report['top_routes'] = top_routes[[
                    'name', 'mountain_range', 'distance_km', 'ascent_m', 
                    'difficulty', 'route_score', 'quality_class'
                ]].to_dict('records')
            except Exception as e:
                logger.warning(f"Error obteniendo top rutas: {e}")
        
        # Recomendaciones
        if 'family_friendly' in df.columns:
            family_routes = df[df['family_friendly']]
            if len(family_routes) > 0:
                report['recommendations'].append({
                    'type': 'family_friendly',
                    'count': len(family_routes),
                    'example': family_routes.iloc[0]['name'] if len(family_routes) > 0 else 'N/A'
                })
        
        if 'weekend_friendly' in df.columns:
            weekend_routes = df[df['weekend_friendly']]
            if len(weekend_routes) > 0:
                report['recommendations'].append({
                    'type': 'weekend_friendly',
                    'count': len(weekend_routes),
                    'example': weekend_routes.iloc[0]['name'] if len(weekend_routes) > 0 else 'N/A'
                })
        
        # Warnings (calidad de datos)
        missing_data = {}
        for col in ['distance_km', 'ascent_m', 'coordinates']:
            if col in df.columns:
                missing_pct = (df[col].isna().sum() / len(df)) * 100
                if missing_pct > 10:
                    report['warnings'].append({
                        'type': f'missing_data_{col}',
                        'percentage': round(missing_pct, 2),
                        'message': f'Alto porcentaje de datos faltantes en {col}'
                    })
        
        # Guardar reporte si se especifica filename
        if filename:
            try:
                os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(report, f, ensure_ascii=False, indent=2)
                logger.info(f"Reporte guardado en {filename}")
            except Exception as e:
                logger.error(f"Error guardando reporte: {e}")
        
        return report

# ============================================================================
# FUNCIONES DE GUARDADO
# ============================================================================

class DataExporter:
    """Clase para exportar datos en múltiples formatos"""
    
    @staticmethod
    def save_to_json(df: pd.DataFrame, filename: str) -> bool:
        """Guardar DataFrame como JSON"""
        try:
            os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)
            records = df.to_dict('records')
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            logger.info(f"JSON guardado: {filename} ({len(records)} registros)")
            return True
        except Exception as e:
            logger.error(f"Error guardando JSON: {e}")
            return False
    
    @staticmethod
    def save_to_csv(df: pd.DataFrame, filename: str) -> bool:
        """Guardar DataFrame como CSV"""
        try:
            os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)
            df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"CSV guardado: {filename} ({len(df)} registros)")
            return True
        except Exception as e:
            logger.error(f"Error guardando CSV: {e}")
            return False
    
    @staticmethod
    def save_to_parquet(df: pd.DataFrame, filename: str) -> bool:
        """Guardar DataFrame como Parquet (más eficiente)"""
        try:
            os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)
            df.to_parquet(filename, index=False, engine='pyarrow')
            logger.info(f"Parquet guardado: {filename} ({len(df)} registros)")
            return True
        except Exception as e:
            logger.error(f"Error guardando Parquet: {e}")
            return False

# ============================================================================
# PIPELINE PRINCIPAL UNIFICADO
# ============================================================================

class UnifiedETLPipeline:
    """
    Pipeline ETL unificado que integra:
    1. Extracción desde MongoDB
    2. Limpieza básica
    3. Feature engineering básico
    4. Transformaciones avanzadas
    5. Análisis y reporting
    6. Exportación múltiple
    """
    
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017/", 
                 database: str = "hiking_trails_tfm"):
        """Inicializar pipeline con conexión a MongoDB"""
        self.mongo = MongoDBConnector(mongo_uri, database)
        self.stats = {
            'extraction': {},
            'transformation': {},
            'loading': {},
            'errors': []
        }
        
        # Verificar que existe la colección 'routes'
        if 'routes' not in self.mongo.db.list_collection_names():
            error_msg = "No existe colección 'routes'. Ejecuta ingesta primero."
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Verificar que hay datos
        count = self.mongo.db['routes'].count_documents({})
        if count == 0:
            warning_msg = "Colección 'routes' está vacía. Ejecuta ingesta primero."
            logger.warning(warning_msg)
    
    def extract(self, limit: Optional[int] = None, 
                incremental: bool = False) -> pd.DataFrame:
        """
        Extraer datos desde MongoDB
        
        Args:
            limit: Límite de documentos
            incremental: Si True, extrae solo datos de las últimas 24h
        
        Returns:
            DataFrame con datos crudos
        """
        try:
            start_time = datetime.now()
            
            if incremental:
                # Extraer solo datos recientes
                end_date = datetime.now()
                start_date = end_date - timedelta(days=1)
                raw_data = self.mongo.get_routes_by_date_range(start_date, end_date)
            else:
                # Extraer todos los datos
                raw_data = self.mongo.get_all_routes(limit)
            
            df = pd.DataFrame(raw_data)
            
            self.stats['extraction'] = {
                'duration': (datetime.now() - start_time).total_seconds(),
                'records': len(df),
                'incremental': incremental,
                'success': True
            }
            
            logger.info(f"Extracción completada: {len(df)} registros")
            return df
            
        except Exception as e:
            error_msg = f"Error en extracción: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return pd.DataFrame()
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transformar datos: limpieza + feature engineering
        
        Returns:
            DataFrame transformado
        """
        try:
            start_time = datetime.now()
            
            if df.empty:
                logger.warning("No hay datos para transformar")
                return df
            
            # Verificar columnas mínimas
            min_cols = ['name', 'distance_km', 'ascent_m']
            if not all(col in df.columns for col in min_cols):
                missing = [col for col in min_cols if col not in df.columns]
                logger.error(f"Faltan columnas esenciales: {missing}. Disponibles: {list(df.columns)}")
                return pd.DataFrame()
            
            # FASE 1: Limpieza básica
            logger.info("Fase 1: Limpieza básica")
            df_clean = DataCleaner.clean_dataframe(df)
            
            # FASE 2: Extraer datos de elevación
            logger.info("Fase 2: Extracción de datos de elevación")
            df_with_elevation = DataCleaner.extract_elevation_data(df_clean)
            
            # FASE 3: Métricas básicas
            logger.info("Fase 3: Cálculo de métricas básicas")
            df_metrics = FeatureEngineer.calculate_basic_metrics(df_with_elevation)
            
            # FASE 4: Características avanzadas
            logger.info("Fase 4: Creación de características avanzadas")
            df_final = FeatureEngineer.create_advanced_features(df_metrics)
            
            # Añadir metadata de transformación
            df_final['_etl_metadata'] = {
                'pipeline_version': '3.0',
                'transformed_at': datetime.now().isoformat(),
                'transformation_steps': ['clean', 'elevation', 'metrics', 'advanced_features']
            }
            
            self.stats['transformation'] = {
                'duration': (datetime.now() - start_time).total_seconds(),
                'input_records': len(df),
                'output_records': len(df_final),
                'columns_added': len(df_final.columns) - len(df.columns),
                'success': True
            }
            
            logger.info(f"Transformación completada: {len(df_final)} registros, {len(df_final.columns)} columnas")
            return df_final
            
        except Exception as e:
            error_msg = f"Error en transformación: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return pd.DataFrame()
    
    def load(self, df: pd.DataFrame, 
             save_to_mongo: bool = True,
             output_formats: List[str] = ['json'],
             output_prefix: str = 'transformed_routes') -> Dict:
        """
        Cargar datos transformados a destinos
        
        Returns:
            Diccionario con resultados de cada operación
        """
        results = {
            'mongodb': {'success': False, 'count': 0},
            'files': {}
        }
        
        try:
            start_time = datetime.now()
            
            if df.empty:
                logger.warning("No hay datos para cargar")
                return results
            
            # Guardar en MongoDB
            if save_to_mongo:
                logger.info("Guardando en MongoDB...")
                inserted, updated = self.mongo.save_transformed_data(df, 'transformed_routes')
                results['mongodb'] = {
                    'success': True,
                    'count': inserted,
                    'updated': updated
                }
            
            # Guardar en archivos
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            for fmt in output_formats:
                filename = f"{output_prefix}_{timestamp}.{fmt}"
                
                if fmt == 'json':
                    success = DataExporter.save_to_json(df, filename)
                    results['files'][filename] = success
                elif fmt == 'csv':
                    success = DataExporter.save_to_csv(df, filename)
                    results['files'][filename] = success
                elif fmt == 'parquet':
                    success = DataExporter.save_to_parquet(df, filename)
                    results['files'][filename] = success
            
            self.stats['loading'] = {
                'duration': (datetime.now() - start_time).total_seconds(),
                'mongodb_success': save_to_mongo,
                'files_generated': len(results['files']),
                'success': True
            }
            
            logger.info(f"Carga completada: MongoDB={save_to_mongo}, Archivos={len(results['files'])}")
            return results
            
        except Exception as e:
            error_msg = f"Error en carga: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return results
    
    def analyze(self, df: pd.DataFrame, 
                generate_report: bool = True) -> Dict:
        """
        Analizar datos y generar reportes
        
        Returns:
            Diccionario con análisis y reporte
        """
        try:
            start_time = datetime.now()
            
            if df.empty:
                logger.warning("No hay datos para analizar")
                return {}
            
            # Análisis estadístico
            stats = DataAnalyzer.analyze_routes_data(df)
            
            # Generar reporte
            report = {}
            if generate_report:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                report_filename = f"analysis_report_{timestamp}.json"
                report = DataAnalyzer.generate_analysis_report(df, report_filename)
            
            analysis_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Análisis completado en {analysis_time:.2f}s")
            
            return {
                'statistics': stats,
                'report': report,
                'analysis_time': analysis_time
            }
            
        except Exception as e:
            error_msg = f"Error en análisis: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return {}
    
    def run_pipeline(self, 
                    limit: Optional[int] = None,
                    incremental: bool = False,
                    save_to_mongo: bool = True,
                    output_formats: List[str] = ['json', 'csv'],
                    generate_report: bool = True,
                    validate_data: bool = True) -> Dict:
        """
        Ejecutar pipeline completo ETL
        
        Returns:
            Estadísticas completas del pipeline
        """
        logger.info("=" * 70)
        logger.info("INICIANDO PIPELINE ETL UNIFICADO")
        logger.info("=" * 70)
        
        pipeline_start = datetime.now()
        
        try:
            # 1. EXTRACCIÓN
            logger.info("PASO 1: Extracción de datos")
            raw_df = self.extract(limit, incremental)
            
            if raw_df.empty:
                logger.error("No se pudieron extraer datos")
                return self.stats
            
            # 2. TRANSFORMACIÓN
            logger.info("PASO 2: Transformación de datos")
            transformed_df = self.transform(raw_df)
            
            if transformed_df.empty:
                logger.error("Error en transformación de datos")
                return self.stats
            
            # 3. VALIDACIÓN (opcional)
            if validate_data:
                logger.info("PASO 3: Validación de datos")
                validation_results = self.validate_data(transformed_df)
                self.stats['validation'] = validation_results
            
            # 4. ANÁLISIS
            logger.info("PASO 4: Análisis y reporting")
            analysis_results = self.analyze(transformed_df, generate_report)
            self.stats['analysis'] = analysis_results
            
            # 5. CARGA
            logger.info("PASO 5: Carga a destinos")
            load_results = self.load(
                transformed_df, 
                save_to_mongo, 
                output_formats
            )
            self.stats['load_results'] = load_results
            
            # Calcular tiempo total
            total_time = (datetime.now() - pipeline_start).total_seconds()
            self.stats['pipeline'] = {
                'total_duration': total_time,
                'start_time': pipeline_start.isoformat(),
                'end_time': datetime.now().isoformat(),
                'success': True,
                'errors_count': len(self.stats['errors'])
            }
            
            # Mostrar resumen
            self.print_summary()
            
            return self.stats
            
        except Exception as e:
            error_msg = f"Error en pipeline: {str(e)}"
            logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            self.stats['pipeline'] = {'success': False, 'error': error_msg}
            return self.stats
        
        finally:
            self.mongo.close()
    
    def validate_data(self, df: pd.DataFrame) -> Dict:
        """Validar calidad de datos transformados"""
        validation = {
            'passed': True,
            'checks': [],
            'warnings': []
        }
        
        # Check 1: Columnas requeridas
        required_columns = ['name', 'mountain_range', 'distance_km', 'ascent_m', 'difficulty']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            validation['passed'] = False
            validation['checks'].append({
                'check': 'required_columns',
                'status': 'FAILED',
                'message': f'Columnas faltantes: {missing_columns}'
            })
        else:
            validation['checks'].append({
                'check': 'required_columns',
                'status': 'PASSED'
            })
        
        # Check 2: Valores nulos
        for col in required_columns:
            if col in df.columns:
                null_count = df[col].isna().sum()
                null_pct = (null_count / len(df)) * 100
                
                if null_pct > 5:
                    validation['warnings'].append({
                        'column': col,
                        'null_percentage': round(null_pct, 2),
                        'message': f'Alto porcentaje de nulos en {col}'
                    })
        
        # Check 3: Valores fuera de rango
        if 'distance_km' in df.columns:
            invalid_distance = df[(df['distance_km'] <= 0) | (df['distance_km'] > 100)]
            if len(invalid_distance) > 0:
                validation['warnings'].append({
                    'check': 'distance_range',
                    'invalid_count': len(invalid_distance),
                    'message': 'Distancias fuera de rango razonable (0-100km)'
                })
        
        return validation
    
    def print_summary(self):
        """Imprimir resumen del pipeline"""
        logger.info("=" * 70)
        logger.info("PIPELINE COMPLETADO")
        logger.info("=" * 70)
        
        if 'pipeline' in self.stats and self.stats['pipeline'].get('success'):
            logger.info(f"Estado: EXITOSO")
            logger.info(f"Tiempo total: {self.stats['pipeline']['total_duration']:.2f}s")
            
            if 'extraction' in self.stats:
                logger.info(f"Extracción: {self.stats['extraction']['records']} registros")
            
            if 'transformation' in self.stats:
                logger.info(f"Transformación: +{self.stats['transformation']['columns_added']} columnas")
            
            if 'load_results' in self.stats:
                mongo_count = self.stats['load_results']['mongodb']['count']
                file_count = len(self.stats['load_results']['files'])
                logger.info(f"Carga: {mongo_count} docs en MongoDB, {file_count} archivos")
            
            if self.stats['errors']:
                logger.warning(f"Errores encontrados: {len(self.stats['errors'])}")
                for error in self.stats['errors'][:3]:
                    logger.warning(f"   • {error}")
        else:
            logger.error("Pipeline falló")
            for error in self.stats['errors']:
                logger.error(f"   • {error}")

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal del pipeline"""
    parser = argparse.ArgumentParser(description='Pipeline ETL Unificado para Rutas de Montaña')
    
    parser.add_argument('--mongo-uri', default='mongodb://localhost:27017/',
                       help='URI de conexión a MongoDB')
    parser.add_argument('--database', default='hiking_trails_tfm',
                       help='Nombre de la base de datos')
    parser.add_argument('--limit', type=int, default=None,
                       help='Límite de rutas a procesar')
    parser.add_argument('--incremental', action='store_true',
                       help='Procesar solo datos de las últimas 24h')
    parser.add_argument('--output', nargs='+', default=['json', 'csv'],
                       choices=['json', 'csv', 'parquet'],
                       help='Formatos de salida')
    parser.add_argument('--no-mongo', action='store_true',
                       help='No guardar en MongoDB')
    parser.add_argument('--no-report', action='store_true',
                       help='No generar reporte de análisis')
    parser.add_argument('--validate', action='store_true',
                       help='Ejecutar validación de datos')
    parser.add_argument('--stats', action='store_true',
                       help='Solo mostrar estadísticas de la base de datos')
    
    args = parser.parse_args()
    
    # Mostrar configuración
    logger.info("CONFIGURACIÓN:")
    logger.info(f"   • MongoDB: {args.mongo_uri}/{args.database}")
    logger.info(f"   • Límite: {args.limit if args.limit else 'Sin límite'}")
    logger.info(f"   • Incremental: {'Sí' if args.incremental else 'No'}")
    logger.info(f"   • Formatos salida: {', '.join(args.output)}")
    logger.info(f"   • Guardar en MongoDB: {'No' if args.no_mongo else 'Sí'}")
    logger.info(f"   • Generar reporte: {'No' if args.no_report else 'Sí'}")
    
    try:
        # Crear pipeline
        pipeline = UnifiedETLPipeline(args.mongo_uri, args.database)
    except ValueError as e:
        logger.error(f"No se pudo inicializar el pipeline: {e}")
        return
    
    # Modo solo estadísticas
    if args.stats:
        stats = pipeline.mongo.get_database_stats()
        print("\nESTADÍSTICAS DE LA BASE DE DATOS:")
        print("=" * 50)
        print(f"Total rutas: {stats.get('routes', 0):,}")
        print(f"Rutas transformadas: {stats.get('transformed_routes', 0):,}")
        print(f"Tamaño BD: {stats.get('database_size', 0):,} bytes")
        print(f"Colecciones: {', '.join(stats.get('collections', []))}")
        if stats.get('last_ingestion'):
            print(f"Última ingesta: {stats['last_ingestion']}")
        if stats.get('last_transformation'):
            print(f"Última transformación: {stats['last_transformation']}")
        pipeline.mongo.close()
        return
    
    # Ejecutar pipeline completo
    results = pipeline.run_pipeline(
        limit=args.limit,
        incremental=args.incremental,
        save_to_mongo=not args.no_mongo,
        output_formats=args.output,
        generate_report=not args.no_report,
        validate_data=args.validate
    )
    
    # Mostrar estadísticas finales
    if results.get('pipeline', {}).get('success'):
        print("\nPIPELINE COMPLETADO CON ÉXITO")
        print(f"Tiempo total: {results['pipeline']['total_duration']:.2f}s")
        
        if 'analysis' in results and 'statistics' in results['analysis']:
            stats = results['analysis']['statistics']['general']
            print(f"Rutas procesadas: {stats.get('total_routes', 0):,}")
            print(f"Distancia media: {stats.get('avg_distance_km', 0):.1f} km")
            print(f"Desnivel medio: {stats.get('avg_ascent_m', 0):.0f} m")
            print(f"Score medio: {stats.get('avg_route_score', 0):.1f}/100")
    else:
        print("\nPIPELINE FALLÓ")
        for error in results.get('errors', []):
            print(f"   • {error}")

if __name__ == "__main__":
    main()
