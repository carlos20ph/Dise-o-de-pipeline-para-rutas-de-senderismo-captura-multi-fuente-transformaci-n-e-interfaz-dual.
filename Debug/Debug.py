"""
Debug, monitoring y validación completa del pipeline ETL
"""

import json
import sys
import time
import traceback
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np
from dataclasses import dataclass, asdict
from enum import Enum
import logging
from colorama import init, Fore, Back, Style
import argparse
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import requests
import psutil
import subprocess

# Inicializar colorama para colores en terminal
init(autoreset=True)

# ============================================================================
# CONFIGURACIÓN Y CONSTANTES
# ============================================================================

class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

@dataclass
class DebugConfig:
    """Configuración de debugging"""
    mongo_uri: str = "mongodb://localhost:27017/"
    database: str = "hiking_trails_tfm"
    timeout_seconds: int = 10
    log_level: LogLevel = LogLevel.INFO
    enable_colors: bool = True
    max_errors_to_show: int = 10
    check_external_apis: bool = False
    save_reports: bool = True
    reports_dir: str = "debug_reports"

class ColorFormatter:
    """Formateador de colores para logging"""
    
    COLORS = {
        LogLevel.DEBUG: Fore.CYAN,
        LogLevel.INFO: Fore.GREEN,
        LogLevel.WARNING: Fore.YELLOW,
        LogLevel.ERROR: Fore.RED,
        LogLevel.CRITICAL: Fore.RED + Back.WHITE + Style.BRIGHT
    }
    
    @classmethod
    def format(cls, message: str, level: LogLevel = LogLevel.INFO) -> str:
        """Formatear mensaje con colores"""
        if not cls.COLORS.get(level):
            return message
        return f"{cls.COLORS[level]}{message}{Style.RESET_ALL}"

# ============================================================================
# LOGGING MEJORADO
# ============================================================================

class DebugLogger:
    """Logger avanzado para debugging"""
    
    def __init__(self, config: DebugConfig):
        self.config = config
        self.log_file = f"debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.setup_logging()
    
    def setup_logging(self):
        """Configurar logging con handlers múltiples"""
        self.logger = logging.getLogger('TFM_Debug')
        self.logger.setLevel(logging.DEBUG)
        
        # Handler para consola con colores
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.config.log_level.value)
        console_formatter = ColoredFormatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        
        # Handler para archivo
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
    
    def log(self, message: str, level: LogLevel = LogLevel.INFO, 
            exc_info: bool = False, **kwargs):
        """Log con nivel personalizado"""
        log_method = {
            LogLevel.DEBUG: self.logger.debug,
            LogLevel.INFO: self.logger.info,
            LogLevel.WARNING: self.logger.warning,
            LogLevel.ERROR: self.logger.error,
            LogLevel.CRITICAL: self.logger.critical
        }.get(level, self.logger.info)
        
        if kwargs:
            message = f"{message} | {json.dumps(kwargs, default=str)}"
        
        log_method(message, exc_info=exc_info)
    
    def section(self, title: str):
        """Log de sección con formato especial"""
        self.log("\n" + "="*60, LogLevel.INFO)
        self.log(f"  {title.upper()}", LogLevel.INFO)
        self.log("="*60, LogLevel.INFO)

class ColoredFormatter(logging.Formatter):
    """Formateador con colores para logging"""
    
    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.RED + Back.WHITE + Style.BRIGHT
    }
    
    def format(self, record):
        """Formatear record con colores"""
        log_message = super().format(record)
        color = self.COLORS.get(record.levelname, '')
        return f"{color}{log_message}{Style.RESET_ALL}"

# ============================================================================
# CHECKERS ESPECÍFICOS
# ============================================================================

class ModuleChecker:
    """Verificador de módulos"""
    
    def __init__(self, logger: DebugLogger):
        self.logger = logger
        self.required_modules = [
            'ingesta',
            'pipeline_etl_momgo', 
            'streamlitmongo'
        ]
        self.dependencies = [
            'pandas', 'numpy', 'pymongo', 'streamlit',
            'requests', 'logging', 'argparse'
        ]
    
    def check_all_modules(self) -> Dict[str, Any]:
        """Verificar todos los módulos"""
        results = {}
        
        for module in self.required_modules:
            results[module] = self._check_module_file(module)
        
        results['dependencies'] = self._check_dependencies()
        results['imports'] = self._test_imports()
        
        return results
    
    def _check_module_file(self, module_name: str) -> Dict[str, Any]:
        """Verificar archivo de módulo"""
        filename = f"{module_name}.py"
        result = {
            'exists': False,
            'size_bytes': 0,
            'last_modified': None,
            'hash': None,
            'lines': 0
        }
        
        try:
            path = Path(filename)
            if path.exists():
                result['exists'] = True
                result['size_bytes'] = path.stat().st_size
                result['last_modified'] = datetime.fromtimestamp(
                    path.stat().st_mtime
                ).isoformat()
                
                # Calcular hash del archivo
                with open(path, 'rb') as f:
                    result['hash'] = hashlib.md5(f.read()).hexdigest()
                
                # Contar líneas
                with open(path, 'r', encoding='utf-8') as f:
                    result['lines'] = sum(1 for _ in f)
                
                self.logger.log(f" Módulo {module_name}: {result['lines']} líneas", 
                              LogLevel.INFO)
            else:
                self.logger.log(f" Módulo {module_name} no encontrado", 
                              LogLevel.ERROR)
                
        except Exception as e:
            self.logger.log(f"Error verificando {module_name}: {e}", 
                          LogLevel.ERROR, exc_info=True)
            result['error'] = str(e)
        
        return result
    
    def _check_dependencies(self) -> Dict[str, Any]:
        """Verificar dependencias instaladas"""
        results = {}
        
        for dep in self.dependencies:
            try:
                __import__(dep)
                version = sys.modules[dep].__version__ if hasattr(
                    sys.modules[dep], '__version__') else 'unknown'
                results[dep] = {
                    'installed': True,
                    'version': version
                }
                self.logger.log(f" Dependencia {dep}: {version}", LogLevel.INFO)
            except ImportError:
                results[dep] = {'installed': False}
                self.logger.log(f" Dependencia {dep} no instalada", 
                              LogLevel.WARNING)
        
        return results
    
    def _test_imports(self) -> Dict[str, Any]:
        """Probar importación de módulos propios"""
        results = {}
        
        import_tests = [
            ('ingesta', ['main', 'collect_data']),
            ('pipeline_etl_momgo', ['UnifiedETLPipeline', 'main']),
            ('streamlitmongo', ['main', 'load_data'])
        ]
        
        for module_name, expected_attrs in import_tests:
            try:
                module = __import__(module_name)
                results[module_name] = {
                    'imported': True,
                    'attributes': {}
                }
                
                for attr in expected_attrs:
                    has_attr = hasattr(module, attr)
                    results[module_name]['attributes'][attr] = has_attr
                    
                    if has_attr:
                        self.logger.log(f" {module_name}.{attr} encontrado", 
                                      LogLevel.INFO)
                    else:
                        self.logger.log(f"  {module_name}.{attr} no encontrado", 
                                      LogLevel.WARNING)
                        
            except Exception as e:
                results[module_name] = {
                    'imported': False,
                    'error': str(e)
                }
                self.logger.log(f" Error importando {module_name}: {e}", 
                              LogLevel.ERROR, exc_info=True)
        
        return results

class MongoChecker:
    """Verificador de MongoDB"""
    
    def __init__(self, config: DebugConfig, logger: DebugLogger):
        self.config = config
        self.logger = logger
    
    def check_connection(self) -> Dict[str, Any]:
        """Verificar conexión a MongoDB"""
        result = {
            'connected': False,
            'latency_ms': None,
            'server_info': None,
            'error': None
        }
        
        try:
            start_time = time.time()
            client = MongoClient(
                self.config.mongo_uri,
                serverSelectionTimeoutMS=self.config.timeout_seconds * 1000
            )
            
            # Test de conexión
            client.admin.command('ping')
            latency = (time.time() - start_time) * 1000
            
            # Obtener info del servidor
            server_info = client.server_info()
            
            result.update({
                'connected': True,
                'latency_ms': round(latency, 2),
                'server_info': {
                    'version': server_info.get('version'),
                    'host': client.address[0],
                    'port': client.address[1]
                }
            })
            
            self.logger.log(
                f" MongoDB conectado: v{server_info.get('version')} "
                f"({latency:.2f}ms)",
                LogLevel.INFO
            )
            
            client.close()
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            result['error'] = str(e)
            self.logger.log(f" Error conectando a MongoDB: {e}", 
                          LogLevel.ERROR)
        except Exception as e:
            result['error'] = str(e)
            self.logger.log(f" Error inesperado: {e}", 
                          LogLevel.ERROR, exc_info=True)
        
        return result
    
    def check_database(self) -> Dict[str, Any]:
        """Verificar base de datos y colecciones"""
        result = {
            'database_exists': False,
            'collections': [],
            'counts': {},
            'indexes': {},
            'storage_stats': {}
        }
        
        try:
            client = MongoClient(self.config.mongo_uri)
            db = client[self.config.database]
            
            # Verificar si existe la BD
            result['database_exists'] = self.config.database in client.list_database_names()
            
            if result['database_exists']:
                # Listar colecciones
                result['collections'] = db.list_collection_names()
                
                # Contar documentos
                for collection_name in result['collections']:
                    count = db[collection_name].count_documents({})
                    result['counts'][collection_name] = count
                    
                    # Obtener índices
                    indexes = list(db[collection_name].list_indexes())
                    result['indexes'][collection_name] = [
                        {'name': idx['name'], 'keys': idx['key']} 
                        for idx in indexes
                    ]
                
                # Stats de storage
                stats = db.command('dbStats')
                result['storage_stats'] = {
                    'dataSize': stats.get('dataSize', 0),
                    'storageSize': stats.get('storageSize', 0),
                    'indexSize': stats.get('indexSize', 0),
                    'objects': stats.get('objects', 0)
                }
                
                self.logger.log(
                    f" Base de datos '{self.config.database}': "
                    f"{len(result['collections'])} colecciones, "
                    f"{sum(result['counts'].values())} documentos totales",
                    LogLevel.INFO
                )
            
            client.close()
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.log(f" Error verificando BD: {e}", 
                          LogLevel.ERROR, exc_info=True)
        
        return result
    
    def check_data_quality(self) -> Dict[str, Any]:
        """Verificar calidad de datos en colecciones"""
        result = {
            'routes_quality': {},
            'transformed_routes_quality': {},
            'issues': []
        }
        
        try:
            client = MongoClient(self.config.mongo_uri)
            db = client[self.config.database]
            
            # Verificar colección 'routes'
            if 'routes' in db.list_collection_names():
                routes_stats = self._analyze_collection_quality(db.routes)
                result['routes_quality'] = routes_stats
                
                if routes_stats['null_percentage'] > 20:
                    result['issues'].append(
                        f"Alto porcentaje de nulos en 'routes': "
                        f"{routes_stats['null_percentage']}%"
                    )
            
            # Verificar colección 'transformed_routes'
            if 'transformed_routes' in db.list_collection_names():
                transformed_stats = self._analyze_collection_quality(
                    db.transformed_routes
                )
                result['transformed_routes_quality'] = transformed_stats
            
            client.close()
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def _analyze_collection_quality(self, collection) -> Dict[str, Any]:
        """Analizar calidad de una colección"""
        try:
            # Muestra aleatoria para análisis
            sample_size = min(100, collection.count_documents({}))
            pipeline = [
                {'$sample': {'size': sample_size}},
                {'$project': {
                    '_id': 0,
                    'field_count': {'$size': {'$objectToArray': '$$ROOT'}},
                    'has_name': {'$cond': [
                        {'$and': [
                            {'$ifNull': ['$name', False]},
                            {'$ne': ['$name', '']}
                        ]}, True, False
                    ]}
                }}
            ]
            
            sample = list(collection.aggregate(pipeline))
            
            if not sample:
                return {'sample_size': 0, 'null_percentage': 0}
            
            df = pd.DataFrame(sample)
            null_percentage = ((len(df) - df['has_name'].sum()) / len(df)) * 100
            
            return {
                'sample_size': len(df),
                'null_percentage': round(null_percentage, 2),
                'avg_fields': round(df['field_count'].mean(), 2)
            }
            
        except Exception as e:
            return {'error': str(e), 'sample_size': 0}

class PipelineChecker:
    """Verificador de pipeline ETL"""
    
    def __init__(self, config: DebugConfig, logger: DebugLogger):
        self.config = config
        self.logger = logger
    
    def test_ingestion(self, limit: int = 10, test_mode: bool = True) -> Dict[str, Any]:
        """Probar módulo de ingesta"""
        result = {
            'success': False,
            'duration_seconds': 0,
            'records_processed': 0,
            'output': '',
            'error': None
        }
        
        try:
            self.logger.section("TEST DE INGESTA")
            
            # Construir comando
            cmd = ['python', 'ingesta.py', '--test-mode', '--limit', str(limit)]
            
            self.logger.log(f"Comando: {' '.join(cmd)}", LogLevel.DEBUG)
            
            start_time = time.time()
            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.config.timeout_seconds * 3
            )
            duration = time.time() - start_time
            
            result.update({
                'success': process.returncode == 0,
                'duration_seconds': round(duration, 2),
                'output': process.stdout,
                'error': process.stderr if process.returncode != 0 else None
            })
            
            if process.returncode == 0:
                self.logger.log(
                    f" Ingesta completada en {duration:.2f}s",
                    LogLevel.INFO
                )
                
                # Extraer estadísticas de la salida
                if "procesados" in process.stdout.lower():
                    lines = process.stdout.split('\n')
                    for line in lines:
                        if any(word in line.lower() for word in 
                              ['procesados', 'insertados', 'guardados']):
                            self.logger.log(f" {line.strip()}", LogLevel.INFO)
            else:
                self.logger.log(
                    f" Ingesta falló: {process.stderr[:200]}",
                    LogLevel.ERROR
                )
            
        except subprocess.TimeoutExpired:
            result['error'] = f"Timeout después de {self.config.timeout_seconds * 3}s"
            self.logger.log(f" Ingesta timeout", LogLevel.ERROR)
        except Exception as e:
            result['error'] = str(e)
            self.logger.log(f" Error en ingesta: {e}", 
                          LogLevel.ERROR, exc_info=True)
        
        return result
    
    def test_etl_pipeline(self, incremental: bool = False, 
                         limit: int = 5) -> Dict[str, Any]:
        """Probar pipeline ETL"""
        result = {
            'success': False,
            'duration_seconds': 0,
            'steps': {},
            'error': None
        }
        
        try:
            self.logger.section("TEST DE PIPELINE ETL")
            
            # Construir comando
            cmd = ['python', 'pipeline_etl_momgo.py', '--limit', str(limit)]
            
            if incremental:
                cmd.append('--incremental')
            
            cmd.extend(['--no-mongo', '--no-report', '--validate'])
            
            self.logger.log(f"Comando: {' '.join(cmd)}", LogLevel.DEBUG)
            
            start_time = time.time()
            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.config.timeout_seconds * 5
            )
            duration = time.time() - start_time
            
            result.update({
                'success': process.returncode == 0,
                'duration_seconds': round(duration, 2),
                'output': process.stdout,
                'error': process.stderr if process.returncode != 0 else None
            })
            
            if process.returncode == 0:
                self.logger.log(
                    f" Pipeline ETL completado en {duration:.2f}s",
                    LogLevel.INFO
                )
                
                # Parsear estadísticas del output
                stats = self._parse_etl_output(process.stdout)
                result['steps'] = stats
                
                for step, data in stats.items():
                    self.logger.log(
                        f" {step}: {data}",
                        LogLevel.INFO
                    )
            else:
                self.logger.log(
                    f" Pipeline ETL falló: {process.stderr[:200]}",
                    LogLevel.ERROR
                )
            
        except subprocess.TimeoutExpired:
            result['error'] = f"Timeout después de {self.config.timeout_seconds * 5}s"
            self.logger.log(f" Pipeline timeout", LogLevel.ERROR)
        except Exception as e:
            result['error'] = str(e)
            self.logger.log(f" Error en pipeline: {e}", 
                          LogLevel.ERROR, exc_info=True)
        
        return result
    
    def _parse_etl_output(self, output: str) -> Dict[str, Any]:
        """Parsear output del pipeline ETL"""
        stats = {}
        lines = output.split('\n')
        
        for line in lines:
            line_lower = line.lower()
            
            if 'extracción' in line_lower:
                if 'registros' in line_lower:
                    # Extraer número de registros
                    import re
                    match = re.search(r'(\d+)\s*registros', line_lower)
                    if match:
                        stats['extraction'] = {'records': int(match.group(1))}
            
            elif 'transformación' in line_lower:
                if 'columnas' in line_lower:
                    import re
                    match = re.search(r'\+(\d+)\s*columnas', line_lower)
                    if match:
                        stats['transformation'] = {'columns_added': int(match.group(1))}
        
        return stats

class SystemChecker:
    """Verificador del sistema"""
    
    def __init__(self, logger: DebugLogger):
        self.logger = logger
    
    def check_system_resources(self) -> Dict[str, Any]:
        """Verificar recursos del sistema"""
        result = {
            'cpu': {},
            'memory': {},
            'disk': {},
            'python': {},
            'network': {}
        }
        
        try:
            # CPU
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_count = psutil.cpu_count()
            result['cpu'] = {
                'percent': cpu_percent,
                'count': cpu_count,
                'frequency': psutil.cpu_freq().current if hasattr(
                    psutil.cpu_freq(), 'current') else None
            }
            
            # Memoria
            memory = psutil.virtual_memory()
            result['memory'] = {
                'total_gb': round(memory.total / (1024**3), 2),
                'available_gb': round(memory.available / (1024**3), 2),
                'percent': memory.percent
            }
            
            # Disco
            disk = psutil.disk_usage('.')
            result['disk'] = {
                'total_gb': round(disk.total / (1024**3), 2),
                'free_gb': round(disk.free / (1024**3), 2),
                'percent': disk.percent
            }
            
            # Python
            result['python'] = {
                'version': sys.version,
                'executable': sys.executable,
                'platform': sys.platform
            }
            
            # Network
            net_io = psutil.net_io_counters()
            result['network'] = {
                'bytes_sent': net_io.bytes_sent,
                'bytes_recv': net_io.bytes_recv
            }
            
            self.logger.log(
                f" Sistema: CPU {cpu_percent}%, "
                f"Memoria {memory.percent}%, "
                f"Disco {disk.percent}%",
                LogLevel.INFO
            )
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.log(f"  Error verificando sistema: {e}", 
                          LogLevel.WARNING)
        
        return result

# ============================================================================
# DEBUGGER PRINCIPAL
# ============================================================================

class TFMDebugger:
    """Debugger principal para TFM Data Engineering"""
    
    def __init__(self, config: Optional[DebugConfig] = None):
        self.config = config or DebugConfig()
        self.logger = DebugLogger(self.config)
        
        # Inicializar checkers
        self.module_checker = ModuleChecker(self.logger)
        self.mongo_checker = MongoChecker(self.config, self.logger)
        self.pipeline_checker = PipelineChecker(self.config, self.logger)
        self.system_checker = SystemChecker(self.logger)
        
        # Resultados
        self.results = {}
        self.start_time = None
        self.end_time = None
    
    def run_comprehensive_check(self) -> Dict[str, Any]:
        """Ejecutar verificación completa"""
        self.start_time = datetime.now()
        self.logger.section("INICIANDO DEBUG COMPLETO - TFM DATA ENGINEERING")
        
        # 1. Verificar sistema
        self.results['system'] = self.system_checker.check_system_resources()
        
        # 2. Verificar módulos
        self.results['modules'] = self.module_checker.check_all_modules()
        
        # 3. Verificar MongoDB
        self.results['mongodb'] = {
            'connection': self.mongo_checker.check_connection(),
            'database': self.mongo_checker.check_database(),
            'data_quality': self.mongo_checker.check_data_quality()
        }
        
        # 4. Probar ingesta (solo si MongoDB está conectado)
        if self.results['mongodb']['connection']['connected']:
            self.results['ingestion_test'] = self.pipeline_checker.test_ingestion(
                limit=5, test_mode=True
            )
        
        # 5. Probar pipeline ETL
        self.results['etl_test'] = self.pipeline_checker.test_etl_pipeline(
            incremental=True, limit=5
        )
        
        # 6. Verificar streamlit
        self.results['streamlit'] = self._check_streamlit()
        
        # 7. Generar resumen
        self.results['summary'] = self._generate_summary()
        
        self.end_time = datetime.now()
        self.results['metadata'] = {
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'duration_seconds': round(
                (self.end_time - self.start_time).total_seconds(), 2
            ),
            'debugger_version': '2.0.0'
        }
        
        # Guardar reporte
        if self.config.save_reports:
            self._save_report()
        
        # Mostrar resumen
        self._print_summary()
        
        return self.results
    
    def _check_streamlit(self) -> Dict[str, Any]:
        """Verificar módulo streamlit"""
        result = {
            'importable': False,
            'version': None,
            'functions': []
        }
        
        try:
            import streamlit as st
            result.update({
                'importable': True,
                'version': st.__version__
            })
            
            # Verificar funciones en streamlitmongo
            try:
                import streamlitmongo as sm
                result['functions'] = [f for f in dir(sm) if not f.startswith('_')]
            except:
                pass
            
            self.logger.log(f" Streamlit v{st.__version__} instalado", 
                          LogLevel.INFO)
            
        except ImportError:
            self.logger.log(" Streamlit no instalado", LogLevel.WARNING)
        
        return result
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generar resumen de la verificación"""
        summary = {
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0,
            'warnings': 0,
            'critical_issues': [],
            'recommendations': [],
            'overall_status': 'UNKNOWN'
        }
        
        # Contar checks
        checks = []
        
        # Check módulos
        if 'modules' in self.results:
            modules = self.results['modules']
            if 'routes' in modules.get('module_files', {}):
                if modules['module_files']['routes']['exists']:
                    checks.append(('Module: routes', True))
                else:
                    checks.append(('Module: routes', False))
        
        # Check MongoDB
        mongodb = self.results.get('mongodb', {})
        if mongodb.get('connection', {}).get('connected'):
            checks.append(('MongoDB Connection', True))
        else:
            checks.append(('MongoDB Connection', False))
            summary['critical_issues'].append("MongoDB no conectado")
        
        # Check ingesta
        ingestion = self.results.get('ingestion_test', {})
        if ingestion.get('success'):
            checks.append(('Ingestion Test', True))
        else:
            checks.append(('Ingestion Test', False))
            summary['warnings'] += 1
        
        # Check ETL
        etl = self.results.get('etl_test', {})
        if etl.get('success'):
            checks.append(('ETL Pipeline Test', True))
        else:
            checks.append(('ETL Pipeline Test', False))
            summary['warnings'] += 1
        
        # Calcular estadísticas
        summary['total_checks'] = len(checks)
        summary['passed_checks'] = sum(1 for _, passed in checks if passed)
        summary['failed_checks'] = summary['total_checks'] - summary['passed_checks']
        
        # Determinar estado general
        if summary['failed_checks'] == 0 and summary['warnings'] == 0:
            summary['overall_status'] = 'EXCELLENT'
        elif summary['failed_checks'] == 0:
            summary['overall_status'] = 'GOOD'
        elif summary['failed_checks'] < summary['total_checks'] / 2:
            summary['overall_status'] = 'WARNING'
        else:
            summary['overall_status'] = 'CRITICAL'
        
        # Generar recomendaciones
        if summary['failed_checks'] > 0:
            summary['recommendations'].append(
                "Revisar los checks fallidos antes de continuar"
            )
        
        if self.results.get('system', {}).get('memory', {}).get('percent', 0) > 80:
            summary['recommendations'].append(
                "La memoria está al 80%+, considerar liberar recursos"
            )
        
        return summary
    
    def _save_report(self):
        """Guardar reporte completo"""
        try:
            # Crear directorio si no existe
            Path(self.config.reports_dir).mkdir(exist_ok=True)
            
            # Nombre de archivo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.config.reports_dir}/debug_report_{timestamp}.json"
            
            # Guardar como JSON
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, indent=2, default=str)
            
            # También guardar versión resumida
            summary_filename = f"{self.config.reports_dir}/summary_{timestamp}.txt"
            with open(summary_filename, 'w', encoding='utf-8') as f:
                f.write(self._format_summary_text())
            
            self.logger.log(f" Reporte guardado en: {filename}", LogLevel.INFO)
            
        except Exception as e:
            self.logger.log(f" Error guardando reporte: {e}", 
                          LogLevel.ERROR)
    
    def _format_summary_text(self) -> str:
        """Formatear resumen como texto"""
        summary = self.results.get('summary', {})
        
        text = f"""
{'='*60}
TFM DATA ENGINEERING - REPORTE DE DEBUGGING
{'='*60}

FECHA: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
DURACIÓN: {self.results.get('metadata', {}).get('duration_seconds', 0)}s
ESTADO GENERAL: {summary.get('overall_status', 'UNKNOWN')}

{'='*60}
RESUMEN DE CHECKS
{'='*60}
Checks totales: {summary.get('total_checks', 0)}
✓ Checks pasados: {summary.get('passed_checks', 0)}
✗ Checks fallados: {summary.get('failed_checks', 0)}
  Advertencias: {summary.get('warnings', 0)}

{'='*60}
MONGODB
{'='*60}
"""
        mongodb = self.results.get('mongodb', {})
        if mongodb.get('connection', {}).get('connected'):
            text += f" Conectado: {mongodb['connection'].get('latency_ms', 0)}ms\n"
            text += f" Base de datos: {self.config.database}\n"
            text += f" Documentos totales: {sum(mongodb.get('database', {}).get('counts', {}).values())}\n"
        else:
            text += " NO CONECTADO\n"
        
        text += f"""
{'='*60}
MÓDULOS
{'='*60}
"""
        modules = self.results.get('modules', {}).get('module_files', {})
        for module_name, data in modules.items():
            status = "" if data.get('exists') else ""
            text += f"{status} {module_name}: {data.get('lines', 0)} líneas\n"
        
        text += f"""
{'='*60}
TESTS
{'='*60}
"""
        ingestion = self.results.get('ingestion_test', {})
        text += f"Ingesta: {'Done' if ingestion.get('success') else 'Fail'} "
        text += f"({ingestion.get('duration_seconds', 0)}s)\n"
        
        etl = self.results.get('etl_test', {})
        text += f"ETL Pipeline: {'Done' if etl.get('success') else 'Fail'} "
        text += f"({etl.get('duration_seconds', 0)}s)\n"
        
        text += f"""
{'='*60}
RECURSOS DEL SISTEMA
{'='*60}
"""
        system = self.results.get('system', {})
        text += f"CPU: {system.get('cpu', {}).get('percent', 0)}%\n"
        text += f"Memoria: {system.get('memory', {}).get('percent', 0)}%\n"
        text += f"Disco: {system.get('disk', {}).get('percent', 0)}%\n"
        
        if summary.get('critical_issues'):
            text += f"""
{'='*60}
  ISSUES CRÍTICOS
{'='*60}
"""
            for issue in summary['critical_issues']:
                text += f"• {issue}\n"
        
        if summary.get('recommendations'):
            text += f"""
{'='*60}
 RECOMENDACIONES
{'='*60}
"""
            for rec in summary['recommendations']:
                text += f"• {rec}\n"
        
        return text
    
    def _print_summary(self):
        """Imprimir resumen en consola"""
        summary = self.results.get('summary', {})
        
        self.logger.section("RESUMEN FINAL")
        
        status_color = {
            'EXCELLENT': Fore.GREEN + Style.BRIGHT,
            'GOOD': Fore.GREEN,
            'WARNING': Fore.YELLOW,
            'CRITICAL': Fore.RED + Style.BRIGHT,
            'UNKNOWN': Fore.WHITE
        }.get(summary.get('overall_status', 'UNKNOWN'), Fore.WHITE)
        
        self.logger.log(
            f"ESTADO: {status_color}{summary.get('overall_status')}{Style.RESET_ALL}",
            LogLevel.INFO
        )
        
        self.logger.log(
            f"Checks: ✓{summary.get('passed_checks', 0)}/"
            f"✗{summary.get('failed_checks', 0)}/"
            f"{summary.get('warnings', 0)}",
            LogLevel.INFO
        )
        
        duration = self.results.get('metadata', {}).get('duration_seconds', 0)
        self.logger.log(f"Duración: {duration}s", LogLevel.INFO)
        
        # Mostrar issues críticos
        if summary.get('critical_issues'):
            self.logger.log("\n ISSUES CRÍTICOS:", LogLevel.CRITICAL)
            for issue in summary['critical_issues']:
                self.logger.log(f"  • {issue}", LogLevel.CRITICAL)
        
        # Mostrar recomendaciones
        if summary.get('recommendations'):
            self.logger.log("\n RECOMENDACIONES:", LogLevel.WARNING)
            for rec in summary['recommendations']:
                self.logger.log(f"  • {rec}", LogLevel.WARNING)
        
        self.logger.log("\n" + "="*60, LogLevel.INFO)
        self.logger.log("Debugging completado ", LogLevel.INFO)

# ============================================================================
# INTERFAZ CLI
# ============================================================================

def main():
    """Función principal del debugger"""
    parser = argparse.ArgumentParser(
        description='Debugger completo para TFM Data Engineering',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  %(prog)s                          # Debug completo
  %(prog)s --quick                  # Debug rápido (solo essentials)
  %(prog)s --check mongo           # Solo verificar MongoDB
  %(prog)s --check modules         # Solo verificar módulos
  %(prog)s --check system          # Solo verificar sistema
  %(prog)s --test ingestion        # Probar módulo de ingesta
  %(prog)s --test etl              # Probar pipeline ETL
  %(prog)s --mongo-uri mongodb://localhost:27017/ --database mydb
        """
    )
    
    parser.add_argument('--quick', action='store_true',
                       help='Ejecutar verificación rápida')
    parser.add_argument('--check', choices=['all', 'mongo', 'modules', 'system', 'pipeline'],
                       default='all', help='Tipo de verificación a realizar')
    parser.add_argument('--test', choices=['ingestion', 'etl', 'streamlit', 'all'],
                       help='Ejecutar tests específicos')
    parser.add_argument('--mongo-uri', default='mongodb://localhost:27017/',
                       help='URI de conexión a MongoDB')
    parser.add_argument('--database', default='hiking_trails_tfm',
                       help='Nombre de la base de datos')
    parser.add_argument('--no-colors', action='store_true',
                       help='Desactivar colores en la salida')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Modo verboso')
    parser.add_argument('--save-report', action='store_true',
                       help='Guardar reporte detallado')
    parser.add_argument('--output-format', choices=['json', 'yaml', 'text'],
                       default='text', help='Formato de salida')
    
    args = parser.parse_args()
    
    # Configurar debugger
    config = DebugConfig(
        mongo_uri=args.mongo_uri,
        database=args.database,
        enable_colors=not args.no_colors,
        log_level=LogLevel.DEBUG if args.verbose else LogLevel.INFO,
        save_reports=args.save_report
    )
    
    debugger = TFMDebugger(config)
    
    # Ejecutar según parámetros
    if args.quick:
        # Debug rápido
        config.timeout_seconds = 5
        debugger.run_comprehensive_check()
    
    elif args.test:
        # Ejecutar tests específicos
        if args.test in ['ingestion', 'all']:
            debugger.pipeline_checker.test_ingestion(limit=5, test_mode=True)
        
        if args.test in ['etl', 'all']:
            debugger.pipeline_checker.test_etl_pipeline(incremental=True, limit=5)
        
        if args.test in ['streamlit', 'all']:
            debugger._check_streamlit()
    
    elif args.check != 'all':
        # Checks específicos
        if args.check == 'mongo':
            debugger.logger.section("CHECK MONGODB")
            debugger.results['mongodb'] = {
                'connection': debugger.mongo_checker.check_connection(),
                'database': debugger.mongo_checker.check_database()
            }
            debugger._print_summary()
        
        elif args.check == 'modules':
            debugger.logger.section("CHECK MÓDULOS")
            debugger.results['modules'] = debugger.module_checker.check_all_modules()
            debugger._print_summary()
        
        elif args.check == 'system':
            debugger.logger.section("CHECK SISTEMA")
            debugger.results['system'] = debugger.system_checker.check_system_resources()
            debugger._print_summary()
        
        elif args.check == 'pipeline':
            debugger.logger.section("CHECK PIPELINE")
            debugger.results['ingestion_test'] = debugger.pipeline_checker.test_ingestion(
                limit=3, test_mode=True
            )
            debugger.results['etl_test'] = debugger.pipeline_checker.test_etl_pipeline(
                incremental=True, limit=3
            )
            debugger._print_summary()
    
    else:
        # Debug completo
        debugger.run_comprehensive_check()
    
    # Salir con código apropiado
    summary = debugger.results.get('summary', {})
    if summary.get('overall_status') in ['CRITICAL', 'UNKNOWN']:
        sys.exit(1)
    elif summary.get('overall_status') == 'WARNING':
        sys.exit(2)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()
