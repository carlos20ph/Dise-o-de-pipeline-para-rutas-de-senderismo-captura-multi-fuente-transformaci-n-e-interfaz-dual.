"""
SCHEDULER PARA TFM DATA ENGINEERING - VERSIÓN CORREGIDA
Orquestación simple sin Airflow
"""

import os
import sys
import schedule
import time
import subprocess
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scheduler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_ingestion():
    """Ejecutar ingesta diaria"""
    logger.info("Ejecutando ingesta diaria...")
    try:
        # COMANDO CORREGIDO: sin --incremental, con --test-mode y --limit para rapidez
        result = subprocess.run(
            [sys.executable, "ingesta.py", "--all", "--test-mode", "--limit", "50"],
            capture_output=True,
            text=True,
            timeout=300  # 5 minutos timeout
        )
        
        if result.returncode == 0:
            logger.info("Ingesta completada exitosamente")
            # Extraer estadísticas del output
            for line in result.stdout.split('\n'):
                if "rutas" in line.lower() or "insertadas" in line.lower() or "resumen" in line.lower():
                    logger.info(f"   {line.strip()}")
        else:
            logger.error(f"Error en ingesta: {result.stderr[:200]}")
            
    except subprocess.TimeoutExpired:
        logger.error("Ingesta timeout (5 minutos)")
    except Exception as e:
        logger.error(f"Error ejecutando ingesta: {e}")

def run_etl_pipeline():
    """Ejecutar pipeline ETL transformación"""
    logger.info("Ejecutando pipeline ETL...")
    try:
        # Mantenemos el comando que funcionó, pero añadimos --limit para coincidir con la ingesta
        result = subprocess.run(
            [sys.executable, "pipeline_etl_momgo.py", "--incremental", "--validate", "--limit", "50"],
            capture_output=True,
            text=True,
            timeout=600  # 10 minutos timeout
        )
        
        if result.returncode == 0:
            logger.info("Pipeline ETL completado")
            # Mostrar algunas líneas de salida
            for line in result.stdout.split('\n'):
                if "pipeline" in line.lower() or "registros" in line.lower() or "completado" in line.lower():
                    logger.info(f"   {line.strip()}")
        else:
            logger.error(f"Error en pipeline: {result.stderr[:200]}")
            
    except subprocess.TimeoutExpired:
        logger.error("Pipeline timeout (10 minutos)")
    except Exception as e:
        logger.error(f"Error ejecutando pipeline: {e}")

def run_full_workflow():
    """Workflow completo: ingesta + transformación"""
    logger.info("=" * 60)
    logger.info(f"INICIANDO WORKFLOW COMPLETO - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    run_ingestion()
    time.sleep(5)  # Esperar 5 segundos
    run_etl_pipeline()
    
    logger.info(f"WORKFLOW COMPLETADO - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    """Función principal del scheduler"""
    logger.info("=" * 60)
    logger.info("SCHEDULER TFM DATA ENGINEERING - INICIANDO")
    logger.info("=" * 60)
    
    # CONFIGURACIÓN DE HORARIOS
    # -------------------------
    
    # 1. Workflow completo diario a las 2:00 AM
    schedule.every().day.at("02:00").do(run_full_workflow).tag("daily", "full")
    
    # 2. Solo ingesta cada 6 horas (para datos más frescos)
    schedule.every(6).hours.do(run_ingestion).tag("frequent", "ingestion")
    
    # 3. Pipeline ETL cada 12 horas
    schedule.every(12).hours.do(run_etl_pipeline).tag("frequent", "etl")
    
    logger.info("Configuración:")
    logger.info("  - Workflow completo: Diario a las 02:00")
    logger.info("  - Ingesta: Cada 6 horas")
    logger.info("  - ETL: Cada 12 horas")
    logger.info("  - Presiona Ctrl+C para salir")
    
    # Ejecutar inmediatamente al inicio (opcional)
    if len(sys.argv) > 1 and sys.argv[1] == "--run-now":
        logger.info("Ejecución inmediata solicitada...")
        run_full_workflow()
    
    # Mantener el scheduler activo
    try:
        while True:
            schedule.run_pending()
            # Mostrar próxima ejecución cada hora
            if datetime.now().minute == 0:
                jobs = schedule.get_jobs()
                if jobs:
                    logger.info(f"Proxima ejecucion programada: {jobs[0].next_run}")
            time.sleep(60)  # Revisar cada minuto
            
    except KeyboardInterrupt:
        logger.info("Scheduler detenido por usuario")
    except Exception as e:
        logger.error(f"Error en scheduler: {e}")

if __name__ == "__main__":
    main()