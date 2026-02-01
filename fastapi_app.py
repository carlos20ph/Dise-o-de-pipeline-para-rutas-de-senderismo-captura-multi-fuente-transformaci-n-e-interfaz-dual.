"""
FASTAPI PARA TFM DATA ENGINEERING - HIKING TRAILS API (VERSIÓN CORREGIDA)
Integración REAL con el pipeline ETL
"""

import uvicorn
import json
import asyncio
from pathlib import Path
import pandas as pd
import numpy as np
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import logging
import sys
from contextlib import asynccontextmanager
import time
import psutil
from datetime import datetime
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ConfigDict
from bson import ObjectId

# ============================================================================
# IMPORTACIÓN DEL PIPELINE ETL REAL
# ============================================================================

try:
    # Intentar importar el pipeline ETL real
    from pipeline_etl_momgo import UnifiedETLPipeline
    ETL_MODULE_AVAILABLE = True
    print("✅ Módulo ETL disponible - Modo REAL activado")
except ImportError as e:
    ETL_MODULE_AVAILABLE = False
    print(f"⚠️  Módulo ETL no disponible - Modo SIMULACIÓN: {e}")

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "hiking_trails_tfm"
LOG_DIR = "api_logs"

# Configurar logging
Path(LOG_DIR).mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/fastapi_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("FastAPI_TFM")

# ============================================================================
# MODELOS PYDANTIC
# ============================================================================

class RouteBase(BaseModel):
    name: str
    region: str
    difficulty: str
    distance_km: float = Field(..., gt=0, description="Distancia en kilómetros")
    elevation_gain: int = Field(..., ge=0, description="Desnivel positivo en metros")
    duration_min: int = Field(..., gt=0, description="Duración estimada en minutos")
    source: Optional[str] = None
    tags: List[str] = []

class RouteResponse(RouteBase):
    id: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    intensity_score: Optional[float] = None
    
    model_config = ConfigDict(from_attributes=True)

class ETLSchedule(BaseModel):
    limit: Optional[int] = Field(100, ge=1, le=100000)
    incremental: bool = True
    save_to_mongo: bool = True
    generate_reports: bool = True
    validate_data: bool = True

class ETLResponse(BaseModel):
    job_id: str
    status: str
    scheduled_at: datetime
    estimated_duration: Optional[int] = None
    message: str

class StatisticsResponse(BaseModel):
    total_routes: int
    routes_by_region: Dict[str, int]
    routes_by_difficulty: Dict[str, int]
    avg_distance: float
    avg_elevation: float
    avg_duration: float
    last_update: datetime

class HealthCheck(BaseModel):
    status: str
    timestamp: datetime
    mongo_connected: bool
    mongo_latency_ms: Optional[float]
    api_version: str = "1.0.0"
    system_resources: Dict[str, Any]
    etl_module_available: bool = False

# ============================================================================
# CONEXIÓN MONGODB
# ============================================================================

class MongoDBManager:
    def __init__(self):
        self.client = None
        self.db = None
        self.connect()
    
    def connect(self):
        try:
            self.client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client[DATABASE_NAME]
            logger.info(f"Conectado a MongoDB: {DATABASE_NAME}")
            return True
        except ConnectionFailure as e:
            logger.error(f"Error conectando a MongoDB: {e}")
            return False
    
    def get_collection(self, collection_name: str):
        if self.db is None:
            self.connect()
        return self.db[collection_name]
    
    def close(self):
        if self.client:
            self.client.close()
            logger.info("Conexión MongoDB cerrada")

# ============================================================================
# ESTADO DE LA API
# ============================================================================

class APIManager:
    def __init__(self):
        self.etl_jobs = {}
        self.system_stats = {}
        self.cache = {}
        self.cache_timeout = 300
        self.startup_time = datetime.now()
    
    def start_etl_job(self, params: dict) -> str:
        job_id = f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.etl_jobs[job_id] = {
            'params': params,
            'status': 'pending',
            'start_time': datetime.now(),
            'progress': 0,
            'logs': [],
            'mode': 'REAL' if ETL_MODULE_AVAILABLE else 'SIMULATED'
        }
        return job_id
    
    def update_etl_job(self, job_id: str, status: str, progress: int = 0, log: str = None):
        if job_id in self.etl_jobs:
            self.etl_jobs[job_id]['status'] = status
            self.etl_jobs[job_id]['progress'] = progress
            if status in ['completed', 'failed']:
                self.etl_jobs[job_id]['end_time'] = datetime.now()
            if log:
                self.etl_jobs[job_id]['logs'].append({
                    'timestamp': datetime.now(),
                    'message': log
                })
    
    def get_etl_job(self, job_id: str) -> Optional[dict]:
        return self.etl_jobs.get(job_id)

# ============================================================================
# FUNCIONES AUXILIARES DEL PIPELINE
# ============================================================================

def run_pipeline_real(params: dict) -> Dict[str, Any]:
    """Ejecutar el pipeline ETL real (función síncrona para usar en threads)"""
    try:
        if not ETL_MODULE_AVAILABLE:
            return {"error": "Módulo ETL no disponible"}
        
        logger.info(f"Iniciando pipeline ETL REAL con parámetros: {params}")
        
        # Crear pipeline
        pipeline = UnifiedETLPipeline()
        
        # Mapear parámetros (generate_reports -> generate_report)
        pipeline_params = {
            'limit': params.get('limit', 100),
            'incremental': params.get('incremental', True),
            'save_to_mongo': params.get('save_to_mongo', True),
            'generate_report': params.get('generate_reports', True),  # Nota: sin 's'
            'validate_data': params.get('validate_data', True),
            'output_formats': ['json']  # Formato simple para no generar muchos archivos
        }
        
        # Ejecutar pipeline
        results = pipeline.run_pipeline(**pipeline_params)
        
        # Cerrar conexiones
        pipeline.mongo.close()
        
        return {
            "success": True,
            "results": results,
            "summary": {
                "records_processed": results.get('extraction', {}).get('records', 0),
                "duration_seconds": results.get('pipeline', {}).get('total_duration', 0),
                "errors": len(results.get('errors', []))
            }
        }
        
    except Exception as e:
        logger.error(f"Error en pipeline real: {e}", exc_info=True)
        return {"error": str(e), "success": False}

async def run_etl_in_background(job_id: str, params: dict, api_manager: APIManager):
    """Ejecutar pipeline ETL en background (REAL o simulado)"""
    
    if ETL_MODULE_AVAILABLE:
        # ===== MODO REAL =====
        try:
            api_manager.update_etl_job(job_id, 'running', 10, 
                                      "🚀 Iniciando pipeline ETL REAL...")
            
            # Ejecutar en thread separado para no bloquear
            api_manager.update_etl_job(job_id, 'running', 30, 
                                      "📥 Extrayendo datos de MongoDB...")
            
            results = await asyncio.to_thread(run_pipeline_real, params)
            
            if results.get('success'):
                summary = results.get('summary', {})
                api_manager.update_etl_job(
                    job_id, 
                    'completed', 
                    100,
                    f"✅ Pipeline ETL REAL completado. "
                    f"Procesados: {summary.get('records_processed', 0)} registros, "
                    f"Duración: {summary.get('duration_seconds', 0):.1f}s"
                )
            else:
                api_manager.update_etl_job(
                    job_id,
                    'failed',
                    0,
                    f"❌ Error en pipeline REAL: {results.get('error', 'Error desconocido')}"
                )
                
        except Exception as e:
            api_manager.update_etl_job(
                job_id,
                'failed',
                0,
                f"❌ Excepción en pipeline REAL: {str(e)[:200]}"
            )
    
    else:
        # ===== MODO SIMULADO (fallback) =====
        try:
            api_manager.update_etl_job(job_id, 'running', 10, 
                                      "[SIMULACIÓN] Iniciando pipeline ETL...")
            await asyncio.sleep(2)
            
            api_manager.update_etl_job(job_id, 'running', 30, 
                                      "[SIMULACIÓN] Extrayendo datos de fuentes...")
            await asyncio.sleep(3)
            
            api_manager.update_etl_job(job_id, 'running', 60, 
                                      "[SIMULACIÓN] Transformando y enriqueciendo datos...")
            await asyncio.sleep(2)
            
            api_manager.update_etl_job(job_id, 'running', 80, 
                                      "[SIMULACIÓN] Guardando en base de datos...")
            await asyncio.sleep(1)
            
            api_manager.update_etl_job(job_id, 'completed', 100, 
                                      "[SIMULACIÓN] Pipeline completado exitosamente")
            
        except Exception as e:
            api_manager.update_etl_job(job_id, 'failed', 0, 
                                      f"[SIMULACIÓN] Error: {str(e)[:100]}")

def get_mongo_connection():
    """Obtener conexión a MongoDB"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DATABASE_NAME]
        client.admin.command('ping')
        return client, db
    except Exception as e:
        logger.error(f"Error de conexión MongoDB: {e}")
        raise HTTPException(status_code=503, detail="MongoDB no disponible")

def calculate_intensity(distance_km: float, elevation_gain: int, duration_min: int) -> float:
    """Calcular intensidad de la ruta"""
    intensity = (distance_km * 2) + (elevation_gain / 100) + (duration_min / 60)
    return round(intensity, 2)

# ============================================================================
# LIFESPAN DE LA APP
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Iniciando FastAPI TFM Data Engineering")
    app.state.mongo_manager = MongoDBManager()
    app.state.api_manager = APIManager()
    
    yield
    
    logger.info("Apagando FastAPI")
    if hasattr(app.state, 'mongo_manager'):
        app.state.mongo_manager.close()

# ============================================================================
# APLICACIÓN FASTAPI
# ============================================================================

app = FastAPI(
    title="TFM Data Engineering - Hiking Trails API",
    description="API REST para gestión y análisis de rutas de senderismo",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Configurar CORS (en producción limitar orígenes)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción cambiar a orígenes específicos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# ENDPOINTS PRINCIPALES
# ============================================================================

@app.get("/", tags=["Root"])
async def root():
    return {
        "message": "API TFM Data Engineering - Hiking Trails",
        "version": "1.0.0",
        "status": "operational",
        "etl_mode": "REAL" if ETL_MODULE_AVAILABLE else "SIMULATED",
        "docs": "/docs",
        "endpoints": {
            "health": "/api/health",
            "routes": "/api/routes",
            "statistics": "/api/statistics",
            "etl": "/api/etl"
        }
    }

@app.get("/api/health", response_model=HealthCheck, tags=["Monitoring"])
async def health_check():
    try:
        start = time.time()
        client, db = get_mongo_connection()
        latency = (time.time() - start) * 1000
        
        system_stats = {
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_percent": psutil.disk_usage('.').percent,
            "uptime_seconds": (datetime.now() - app.state.api_manager.startup_time).total_seconds(),
            "etl_module_available": ETL_MODULE_AVAILABLE
        }
        
        client.close()
        
        return HealthCheck(
            status="healthy",
            timestamp=datetime.now(),
            mongo_connected=True,
            mongo_latency_ms=round(latency, 2),
            system_resources=system_stats,
            etl_module_available=ETL_MODULE_AVAILABLE
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return HealthCheck(
            status="degraded",
            timestamp=datetime.now(),
            mongo_connected=False,
            mongo_latency_ms=None,
            system_resources={"error": str(e)},
            etl_module_available=ETL_MODULE_AVAILABLE
        )

@app.get("/api/routes", tags=["Routes"])
async def get_routes(
    skip: int = Query(0, ge=0, description="Número de registros a saltar"),
    limit: int = Query(50, ge=1, le=1000, description="Límite de registros"),
    region: Optional[str] = Query(None, description="Filtrar por región"),
    difficulty: Optional[str] = Query(None, description="Filtrar por dificultad"),
    min_distance: Optional[float] = Query(None, ge=0, description="Distancia mínima (km)"),
    max_distance: Optional[float] = Query(None, ge=0, description="Distancia máxima (km)")
):
    try:
        client, db = get_mongo_connection()
        collection = db['routes']
        
        filter_query = {}
        if region:
            filter_query['region'] = region
        if difficulty:
            filter_query['difficulty'] = difficulty
        if min_distance is not None or max_distance is not None:
            filter_query['distance_km'] = {}
            if min_distance is not None:
                filter_query['distance_km']['$gte'] = min_distance
            if max_distance is not None:
                filter_query['distance_km']['$lte'] = max_distance
        
        total = collection.count_documents(filter_query)
        
        cursor = collection.find(filter_query, {'_id': 0}).skip(skip).limit(limit)
        routes = list(cursor)
        
        for route in routes:
            if 'distance_km' in route and 'elevation_gain' in route and 'duration_min' in route:
                route['intensity'] = calculate_intensity(
                    route['distance_km'],
                    route['elevation_gain'],
                    route['duration_min']
                )
        
        client.close()
        
        return {
            "total": total,
            "skip": skip,
            "limit": limit,
            "filters": filter_query,
            "data": routes
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo rutas: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/routes/{route_id}", tags=["Routes"])
async def get_route_by_id(route_id: str):
    try:
        client, db = get_mongo_connection()
        collection = db['routes']
        
        try:
            route = collection.find_one({'_id': ObjectId(route_id)}, {'_id': 0})
        except:
            route = collection.find_one({'route_id': route_id}, {'_id': 0})
        
        client.close()
        
        if not route:
            raise HTTPException(status_code=404, detail="Ruta no encontrada")
        
        if 'distance_km' in route and 'elevation_gain' in route and 'duration_min' in route:
            route['intensity'] = calculate_intensity(
                route['distance_km'],
                route['elevation_gain'],
                route['duration_min']
            )
        
        return route
        
    except Exception as e:
        logger.error(f"Error obteniendo ruta {route_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/statistics", response_model=StatisticsResponse, tags=["Statistics"])
async def get_statistics():
    try:
        client, db = get_mongo_connection()
        collection = db['routes']
        
        total_routes = collection.count_documents({})
        
        pipeline_region = [
            {"$group": {"_id": "$region", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        routes_by_region = {
            str(item['_id']): item['count'] 
            for item in collection.aggregate(pipeline_region)
            if item['_id'] is not None
        }
        
        pipeline_difficulty = [
            {"$group": {"_id": "$difficulty", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        routes_by_difficulty = {
            str(item['_id']): item['count'] 
            for item in collection.aggregate(pipeline_difficulty)
            if item['_id'] is not None
        }
        
        pipeline_avg = [
            {"$group": {
                "_id": None,
                "avg_distance": {"$avg": "$distance_km"},
                "avg_elevation": {"$avg": "$elevation_gain"},
                "avg_duration": {"$avg": "$duration_min"}
            }}
        ]
        
        avg_result = list(collection.aggregate(pipeline_avg))
        if avg_result:
            avg_data = avg_result[0]
            avg_distance = round(avg_data.get('avg_distance', 0), 2)
            avg_elevation = round(avg_data.get('avg_elevation', 0), 2)
            avg_duration = round(avg_data.get('avg_duration', 0), 2)
        else:
            avg_distance = avg_elevation = avg_duration = 0
        
        client.close()
        
        return StatisticsResponse(
            total_routes=total_routes,
            routes_by_region=routes_by_region,
            routes_by_difficulty=routes_by_difficulty,
            avg_distance=avg_distance,
            avg_elevation=avg_elevation,
            avg_duration=avg_duration,
            last_update=datetime.now()
        )
        
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/etl/run", response_model=ETLResponse, tags=["ETL"])
async def run_etl(
    params: ETLSchedule,
    background_tasks: BackgroundTasks
):
    try:
        job_id = app.state.api_manager.start_etl_job(params.dict())
        
        background_tasks.add_task(
            run_etl_in_background,
            job_id,
            params.dict(),
            app.state.api_manager
        )
        
        mode = "REAL" if ETL_MODULE_AVAILABLE else "SIMULADO"
        
        return ETLResponse(
            job_id=job_id,
            status="scheduled",
            scheduled_at=datetime.now(),
            estimated_duration=300 if ETL_MODULE_AVAILABLE else 60,
            message=f"ETL programado en modo {mode} con job_id: {job_id}"
        )
        
    except Exception as e:
        logger.error(f"Error programando ETL: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/etl/status/{job_id}", tags=["ETL"])
async def get_etl_status(job_id: str):
    job = app.state.api_manager.get_etl_job(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail="Trabajo no encontrado")
    
    duration = None
    if 'end_time' in job and 'start_time' in job:
        duration = (job['end_time'] - job['start_time']).total_seconds()
    
    return {
        "job_id": job_id,
        "status": job['status'],
        "progress": job['progress'],
        "mode": job.get('mode', 'unknown'),
        "start_time": job['start_time'],
        "end_time": job.get('end_time'),
        "duration_seconds": duration,
        "params": job['params'],
        "logs": job['logs'][-10:] if job['logs'] else []
    }

@app.get("/api/etl/jobs", tags=["ETL"])
async def list_etl_jobs(limit: int = Query(10, ge=1, le=100)):
    jobs = list(app.state.api_manager.etl_jobs.values())
    jobs.sort(key=lambda x: x['start_time'], reverse=True)
    
    return {
        "total_jobs": len(jobs),
        "jobs": jobs[:limit]
    }

@app.get("/api/etl/test-real", tags=["ETL"])
async def test_etl_real():
    """Endpoint para probar el ETL REAL directamente"""
    try:
        if not ETL_MODULE_AVAILABLE:
            return {
                "status": "error", 
                "message": "Módulo ETL no disponible - ejecutando en modo SIMULADO"
            }
        
        # Parámetros de prueba
        test_params = {
            'limit': 10,
            'incremental': False,
            'save_to_mongo': True,
            'generate_reports': True,
            'validate_data': True
        }
        
        results = await asyncio.to_thread(run_pipeline_real, test_params)
        
        if results.get('success'):
            summary = results.get('summary', {})
            return {
                "status": "success",
                "message": "✅ ETL REAL ejecutado correctamente",
                "mode": "REAL",
                "results": summary
            }
        else:
            return {
                "status": "error",
                "message": f"❌ Error en ETL REAL: {results.get('error', 'Desconocido')}",
                "mode": "REAL"
            }
            
    except Exception as e:
        logger.error(f"Error en test ETL real: {e}", exc_info=True)
        return {
            "status": "error",
            "message": f"Excepción: {str(e)}",
            "mode": "REAL" if ETL_MODULE_AVAILABLE else "SIMULATED"
        }

@app.get("/api/regions", tags=["Metadata"])
async def get_regions():
    try:
        client, db = get_mongo_connection()
        collection = db['routes']
        
        regions = collection.distinct('region')
        regions = [r for r in regions if r is not None]
        
        client.close()
        
        return {
            "total_regions": len(regions),
            "regions": sorted(regions)
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo regiones: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/difficulties", tags=["Metadata"])
async def get_difficulties():
    try:
        client, db = get_mongo_connection()
        collection = db['routes']
        
        difficulties = collection.distinct('difficulty')
        difficulties = [d for d in difficulties if d is not None]
        
        client.close()
        
        return {
            "total_difficulties": len(difficulties),
            "difficulties": sorted(difficulties)
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo dificultades: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# ENDPOINTS DE ADMINISTRACIÓN
# ============================================================================

@app.get("/admin/metrics", tags=["Admin"])
async def get_metrics():
    process = psutil.Process()
    
    metrics = {
        "api": {
            "uptime_seconds": (datetime.now() - app.state.api_manager.startup_time).total_seconds(),
            "total_etl_jobs": len(app.state.api_manager.etl_jobs),
            "active_etl_jobs": sum(1 for j in app.state.api_manager.etl_jobs.values() if j['status'] == 'running'),
            "etl_mode": "REAL" if ETL_MODULE_AVAILABLE else "SIMULATED"
        },
        "process": {
            "cpu_percent": process.cpu_percent(),
            "memory_mb": round(process.memory_info().rss / 1024 / 1024, 2),
            "threads": process.num_threads()
        },
        "system": {
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_percent": psutil.disk_usage('.').percent
        }
    }
    
    return metrics

# ============================================================================
# ENDPOINT DE EXPORTACIÓN
# ============================================================================

@app.get("/api/export/csv", tags=["Export"])
async def export_csv(
    region: Optional[str] = None,
    difficulty: Optional[str] = None
):
    try:
        client, db = get_mongo_connection()
        collection = db['routes']
        
        filter_query = {}
        if region:
            filter_query['region'] = region
        if difficulty:
            filter_query['difficulty'] = difficulty
        
        cursor = collection.find(filter_query, {'_id': 0})
        routes = list(cursor)
        
        if not routes:
            raise HTTPException(status_code=404, detail="No hay datos para exportar")
        
        df = pd.DataFrame(routes)
        
        if 'distance_km' in df.columns and 'elevation_gain' in df.columns and 'duration_min' in df.columns:
            df['intensity'] = df.apply(
                lambda row: calculate_intensity(
                    row['distance_km'],
                    row['elevation_gain'],
                    row['duration_min']
                ), axis=1
            )
        
        csv_content = df.to_csv(index=False)
        
        client.close()
        
        filename = f"routes_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        if region:
            filename = f"routes_{region}_{datetime.now().strftime('%Y%m%d')}.csv"
        
        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        logger.error(f"Error exportando CSV: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# EJECUCIÓN
# ============================================================================

if __name__ == "__main__":
    # Para ejecutar con reload, usar en la terminal:
    # uvicorn fastapi_app:app --reload
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")