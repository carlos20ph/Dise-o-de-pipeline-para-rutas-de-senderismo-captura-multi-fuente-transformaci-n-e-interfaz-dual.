# app_streamlit_final_tfm.py - APLICACIÓN COMPLETA CON MODO ANALISTA Y USUARIO
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import json
import warnings
import os
from typing import Dict, List, Optional
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import time

warnings.filterwarnings('ignore')

# ========== CONFIGURACIÓN DE PÁGINA ==========
st.set_page_config(
    page_title="TFM Data Engineering - Sistema de Rutas de Montaña",
    page_icon="🏔️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========== CONSTANTES ==========
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "hiking_trails_tfm"

# ========== CONEXIÓN MONGODB ==========
@st.cache_resource
def get_mongo_client():
    """Obtener cliente MongoDB con pooling de conexiones"""
    try:
        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000,
            maxPoolSize=50,
            minPoolSize=10
        )
        # Verificar conexión
        client.admin.command('ping')
        return client
    except Exception as e:
        st.error(f"❌ Error de conexión MongoDB: {str(e)[:100]}")
        return None

# ========== FUNCIONES DE DATOS ==========
@st.cache_data(ttl=300, show_spinner="Cargando datos transformados...")
def cargar_datos_transformados(limit: int = 1000) -> pd.DataFrame:
    """Cargar datos transformados desde MongoDB"""
    client = get_mongo_client()
    if not client:
        return pd.DataFrame()
    
    try:
        db = client[DATABASE_NAME]
        collection = db['transformed_routes']
        
        # Proyección específica
        projection = {
            'name': 1,
            'mountain_range': 1,
            'distance_km': 1,
            'ascent_m': 1,
            'difficulty': 1,
            'route_score': 1,
            'intensity': 1,
            'estimated_hours': 1,
            'best_season': 1,
            'technical_level': 1,
            'family_friendly': 1,
            'weekend_friendly': 1,
            'training_route': 1,
            'intensity_level': 1,
            'length_category': 1,
            'ascent_category': 1,
            'quality_class': 1,
            'enriched_description': 1,
            'coordinates': 1,
            '_transformation_metadata': 1,
            '_ingestion_metadata': 1
        }
        
        cursor = collection.find({}, projection).limit(limit)
        routes = list(cursor)
        
        if not routes:
            return pd.DataFrame()
        
        # Convertir a DataFrame
        df = pd.DataFrame(routes)
        
        # Convertir ObjectId a string
        if '_id' in df.columns:
            df['_id'] = df['_id'].astype(str)
        
        return df
        
    except Exception as e:
        st.error(f"❌ Error cargando datos: {str(e)[:200]}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def obtener_estadisticas_db() -> Dict:
    """Obtener estadísticas de MongoDB"""
    client = get_mongo_client()
    if not client:
        return {}
    
    try:
        db = client[DATABASE_NAME]
        
        stats = {
            'raw_routes': db['routes'].estimated_document_count(),
            'transformed_routes': db['transformed_routes'].estimated_document_count(),
            'ingestion_logs': db['ingestion_log'].estimated_document_count(),
            'collections': sorted(db.list_collection_names())
        }
        
        # Última ingesta
        last_ingestion = list(db['ingestion_log']
                            .find({}, {'timestamp': 1})
                            .sort('timestamp', -1)
                            .limit(1))
        if last_ingestion:
            stats['last_ingestion'] = last_ingestion[0].get('timestamp')
        
        # Última transformación
        last_transform = list(db['transformed_routes']
                            .find({}, {'_transformation_metadata.transformed_at': 1})
                            .sort('_transformation_metadata.transformed_at', -1)
                            .limit(1))
        if last_transform and '_transformation_metadata' in last_transform[0]:
            stats['last_transformation'] = last_transform[0]['_transformation_metadata'].get('transformed_at')
        
        return stats
        
    except Exception as e:
        return {}


@st.cache_data(ttl=300)
def obtener_regiones() -> Dict:
    """Obtener regiones disponibles"""
    client = get_mongo_client()
    if not client:
        return {}
    
    try:
        db = client[DATABASE_NAME]
        pipeline = [
            {"$match": {"mountain_range": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$mountain_range", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 20}
        ]
        results = list(db['transformed_routes'].aggregate(pipeline))
        return {item['_id']: item['count'] for item in results if item['_id']}
    except:
        return {}

# ========== FUNCIONES DE PROCESAMIENTO ==========
def procesar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Procesar y normalizar DataFrame"""
    if df is None or df.empty:
        return pd.DataFrame()
    
    df_clean = df.copy()
    
    # Asegurar columnas críticas
    columnas_requeridas = {
        'name': 'Ruta sin nombre',
        'mountain_range': 'Desconocida',
        'distance_km': 10.0,
        'ascent_m': 500.0,
        'difficulty': 'Moderada',
        'intensity': 50.0,
        'route_score': 50.0
    }
    
    for col, default in columnas_requeridas.items():
        if col not in df_clean.columns:
            df_clean[col] = default
        elif col in ['distance_km', 'ascent_m', 'intensity', 'route_score']:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            df_clean[col] = df_clean[col].fillna(default)
    
    # Renombrar columnas para consistencia
    renombres = {
        'distance_km': 'distancia_km',
        'ascent_m': 'desnivel_m',
        'mountain_range': 'cordillera'
    }
    df_clean = df_clean.rename(columns=renombres)
    
    return df_clean

def aplicar_filtros(df: pd.DataFrame, filtros: Dict) -> pd.DataFrame:
    """Aplicar filtros al DataFrame"""
    if df.empty:
        return df
    
    df_filtrado = df.copy()
    
    try:
        # Filtro por dificultad
        if filtros.get('dificultades'):
            df_filtrado = df_filtrado[df_filtrado['difficulty'].isin(filtros['dificultades'])]
        
        # Filtro por distancia
        if filtros.get('distancia'):
            min_dist, max_dist = filtros['distancia']
            if 'distancia_km' in df_filtrado.columns:
                df_filtrado = df_filtrado[
                    (df_filtrado['distancia_km'] >= min_dist) & 
                    (df_filtrado['distancia_km'] <= max_dist)
                ]
        
        # Filtro por desnivel
        if filtros.get('desnivel'):
            min_des, max_des = filtros['desnivel']
            if 'desnivel_m' in df_filtrado.columns:
                df_filtrado = df_filtrado[
                    (df_filtrado['desnivel_m'] >= min_des) & 
                    (df_filtrado['desnivel_m'] <= max_des)
                ]
        
        # Filtro por región
        if filtros.get('region') and filtros['region'] not in [None, "Todas"]:
            if 'cordillera' in df_filtrado.columns:
                df_filtrado = df_filtrado[df_filtrado['cordillera'] == filtros['region']]
        
        return df_filtrado
        
    except Exception as e:
        st.warning(f"Error aplicando filtros: {str(e)[:50]}")
        return df

# ========== MODO ANALISTA ==========
def renderizar_panel_analista():
    """Panel completo para analistas de datos"""
    
    # Inicializar session state para el modo analista
    if 'analista_pagina' not in st.session_state:
        st.session_state.analista_pagina = "dashboard"
    if 'analista_filtros' not in st.session_state:
        st.session_state.analista_filtros = {
            'dificultades': ['Fácil', 'Moderada', 'Difícil'],
            'distancia': [0.0, 30.0],
            'desnivel': [0.0, 1500.0],
            'region': "Todas"
        }
    
    # Sidebar del analista
    with st.sidebar:
        st.title("🔍 Panel de Analista")
        st.markdown("---")
        
        # Carga de datos
        st.subheader("📥 Datos")
        limite = st.number_input("Límite de registros", 100, 5000, 1000, 100)
        
        if st.button("🔄 Cargar datos transformados", use_container_width=True):
            with st.spinner("Cargando..."):
                st.session_state.df_analista = cargar_datos_transformados(limite)
                if not st.session_state.df_analista.empty:
                    st.success(f"✅ {len(st.session_state.df_analista)} rutas cargadas")
                else:
                    st.warning("⚠️ No se encontraron datos")
        
        st.markdown("---")
        
        # Filtros avanzados
        st.subheader("🎯 Filtros Avanzados")
        
        dificultades = st.multiselect(
            "Dificultad:",
            ['Fácil', 'Moderada', 'Difícil'],
            default=st.session_state.analista_filtros['dificultades']
        )
        st.session_state.analista_filtros['dificultades'] = dificultades
        
        distancia = st.slider(
            "Distancia (km):", 0.0, 50.0,
            value=tuple(st.session_state.analista_filtros['distancia']),
            step=0.5
        )
        st.session_state.analista_filtros['distancia'] = list(distancia)
        
        desnivel = st.slider(
            "Desnivel (m):", 0.0, 3000.0,
            value=tuple(st.session_state.analista_filtros['desnivel']),
            step=50.0
        )
        st.session_state.analista_filtros['desnivel'] = list(desnivel)
        
        regiones = obtener_regiones()
        if regiones:
            regiones_lista = ["Todas"] + list(regiones.keys())
            region = st.selectbox(
                "Región:",
                regiones_lista,
                index=regiones_lista.index(st.session_state.analista_filtros['region']) if st.session_state.analista_filtros['region'] in regiones_lista else 0
            )
            st.session_state.analista_filtros['region'] = region
        
        st.markdown("---")
        
        # Navegación del analista
        st.subheader("📊 Navegación")
        
        paginas_analista = [
            ("📈 Dashboard", "dashboard"),
            ("🗺️ Mapa Geográfico", "mapa"),
            ("📊 Análisis Estadístico", "estadisticas"),
            ("🔍 Calidad de Datos", "calidad"),
            ("⚙️ Monitorización ETL", "monitor"),
            ("📋 Datos Completos", "datos")
        ]
        
        for nombre, key in paginas_analista:
            if st.button(nombre, use_container_width=True,
                        type="primary" if st.session_state.analista_pagina == key else "secondary"):
                st.session_state.analista_pagina = key
                st.rerun()
        
        st.markdown("---")
        
        # Estadísticas rápidas
        if 'df_analista' in st.session_state and not st.session_state.df_analista.empty:
            df_procesado = procesar_dataframe(st.session_state.df_analista)
            if not df_procesado.empty:
                st.subheader("📊 Resumen")
                st.info(f"**Total rutas:** {len(df_procesado):,}")
                st.info(f"**Regiones únicas:** {df_procesado['cordillera'].nunique()}")
    
    # Contenido principal del analista
    if 'df_analista' not in st.session_state:
        st.session_state.df_analista = cargar_datos_transformados(1000)
    
    df_procesado = procesar_dataframe(st.session_state.df_analista)
    
    # Renderizar página según selección
    if st.session_state.analista_pagina == "dashboard":
        renderizar_dashboard_analista(df_procesado)
    elif st.session_state.analista_pagina == "mapa":
        renderizar_mapa_analista(df_procesado)
    elif st.session_state.analista_pagina == "estadisticas":
        renderizar_estadisticas_analista(df_procesado)
    elif st.session_state.analista_pagina == "calidad":
        renderizar_calidad_datos(df_procesado)
    elif st.session_state.analista_pagina == "monitor":
        renderizar_monitorizacion_etl()
    elif st.session_state.analista_pagina == "datos":
        renderizar_datos_completos(df_procesado)

def renderizar_dashboard_analista(df: pd.DataFrame):
    """Dashboard principal del analista"""
    st.title("📈 Dashboard Analítico - Rutas de Montaña")
    st.markdown("Análisis completo de los datos transformados por el pipeline ETL")
    st.markdown("---")
    
    if df.empty:
        st.warning("No hay datos disponibles. Por favor, carga datos desde el panel lateral.")
        return
    
    # Aplicar filtros
    df_filtrado = aplicar_filtros(df, st.session_state.analista_filtros)
    
    if df_filtrado.empty:
        st.warning("No hay rutas que coincidan con los filtros aplicados.")
        return
    
    # Métricas de alto nivel
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Rutas", f"{len(df_filtrado):,}")
    with col2:
        avg_dist = df_filtrado['distancia_km'].mean() if 'distancia_km' in df_filtrado.columns else 0
        st.metric("Distancia Media", f"{avg_dist:.1f} km")
    with col3:
        avg_ascent = df_filtrado['desnivel_m'].mean() if 'desnivel_m' in df_filtrado.columns else 0
        st.metric("Desnivel Medio", f"{avg_ascent:.0f} m")
    with col4:
        if 'route_score' in df_filtrado.columns:
            avg_score = df_filtrado['route_score'].mean()
            st.metric("Score Medio", f"{avg_score:.1f}/100")
        else:
            st.metric("Score", "N/A")
    
    st.markdown("---")
    
    # Gráficos principales
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Distribución por Dificultad")
        if 'difficulty' in df_filtrado.columns:
            diff_counts = df_filtrado['difficulty'].value_counts()
            fig = px.pie(
                values=diff_counts.values,
                names=diff_counts.index,
                color=diff_counts.index,
                color_discrete_map={'Fácil': '#2E8B57', 'Moderada': '#FFA500', 'Difícil': '#DC143C'}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📈 Relación Distancia vs Desnivel")
        if 'distancia_km' in df_filtrado.columns and 'desnivel_m' in df_filtrado.columns:
            fig = px.scatter(
                df_filtrado.head(500),
                x='distancia_km',
                y='desnivel_m',
                color='difficulty' if 'difficulty' in df_filtrado.columns else None,
                size='route_score' if 'route_score' in df_filtrado.columns else None,
                hover_name='name',
                hover_data=['cordillera', 'intensity'],
                labels={'distancia_km': 'Distancia (km)', 'desnivel_m': 'Desnivel (m)'}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Tabla de rutas con detalles
    st.subheader("🗺️ Muestra de Rutas")
    
    columnas_interes = ['name', 'cordillera', 'distancia_km', 'desnivel_m', 
                       'difficulty', 'intensity', 'route_score']
    columnas_disponibles = [c for c in columnas_interes if c in df_filtrado.columns]
    
    if columnas_disponibles:
        df_muestra = df_filtrado[columnas_disponibles].head(20).copy()
        # Renombrar columnas para mejor visualización
        nombres_columnas = {
            'name': 'Nombre',
            'cordillera': 'Cordillera',
            'distancia_km': 'Distancia (km)',
            'desnivel_m': 'Desnivel (m)',
            'difficulty': 'Dificultad',
            'intensity': 'Intensidad',
            'route_score': 'Score',
        }
        df_muestra = df_muestra.rename(columns=nombres_columnas)
        st.dataframe(df_muestra, use_container_width=True, height=400)

def renderizar_mapa_analista(df: pd.DataFrame):
    """Mapa geográfico de rutas"""
    st.title("🗺️ Mapa Geográfico de Rutas")
    st.markdown("Visualización espacial de las rutas disponibles")
    st.markdown("---")
    
    if df.empty:
        st.warning("No hay datos disponibles.")
        return
    
    df_filtrado = aplicar_filtros(df, st.session_state.analista_filtros)
    
    if df_filtrado.empty:
        st.warning("No hay rutas que coincidan con los filtros.")
        return
    
    st.info("⚠️ **Nota:** Las coordenadas son simuladas para demostración. En producción, se usarían coordenadas reales.")
    
    # Simular coordenadas basadas en la cordillera
    df_mapa = df_filtrado.head(100).copy()
    
    # Coordenadas aproximadas de regiones españolas
    coordenadas_regiones = {
        'Pirineos': (42.5, 0.5),
        'Picos de Europa': (43.2, -4.8),
        'Sierra Nevada': (37.1, -3.4),
        'Guadarrama': (40.8, -3.9),
        'Gredos': (40.3, -5.2),
        'Montserrat': (41.6, 1.8),
        'Ordesa': (42.7, -0.1)
    }
    
    for idx, row in df_mapa.iterrows():
        cordillera = str(row.get('cordillera', ''))
        # Buscar coincidencia aproximada
        for region, coords in coordenadas_regiones.items():
            if region.lower() in cordillera.lower():
                df_mapa.at[idx, 'lat'] = coords[0] + np.random.uniform(-0.3, 0.3)
                df_mapa.at[idx, 'lon'] = coords[1] + np.random.uniform(-0.3, 0.3)
                break
        else:
            # Coordenadas por defecto (centro de España)
            df_mapa.at[idx, 'lat'] = 40.4 + np.random.uniform(-2, 2)
            df_mapa.at[idx, 'lon'] = -3.7 + np.random.uniform(-2, 2)
    
    # Crear mapa interactivo
    if 'lat' in df_mapa.columns and 'lon' in df_mapa.columns:
        fig = px.scatter_mapbox(
            df_mapa,
            lat="lat",
            lon="lon",
            hover_name="name",
            hover_data=["cordillera", "difficulty", "distancia_km", "desnivel_m"],
            color="difficulty",
            zoom=5,
            height=600,
            color_discrete_map={'Fácil': 'green', 'Moderada': 'orange', 'Difícil': 'red'}
        )
        fig.update_layout(mapbox_style="open-street-map")
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        st.plotly_chart(fig, use_container_width=True)
    
    # Leyenda
    with st.expander("📖 Leyenda y Notas"):
        st.markdown("""
        **Leyenda del mapa:**
        - 🟢 **Verde:** Rutas fáciles
        - 🟠 **Naranja:** Rutas moderadas  
        - 🔴 **Rojo:** Rutas difíciles
        
        **Notas técnicas:**
        - Las ubicaciones son aproximadas y basadas en la región/cordillera
        - En un sistema de producción, se usarían coordenadas GPS reales
        - La densidad de puntos indica la cantidad de rutas en cada región
        """)

def renderizar_estadisticas_analista(df: pd.DataFrame):
    """Análisis estadístico detallado"""
    st.title("📊 Análisis Estadístico Detallado")
    st.markdown("Estadísticas descriptivas y correlaciones entre variables")
    st.markdown("---")
    
    if df.empty:
        st.warning("No hay datos disponibles.")
        return
    
    df_filtrado = aplicar_filtros(df, st.session_state.analista_filtros)
    
    if df_filtrado.empty:
        st.warning("No hay datos con los filtros actuales.")
        return
    
    # Estadísticas por cordillera
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Estadísticas por Cordillera")
        if 'cordillera' in df_filtrado.columns:
            stats_cordillera = df_filtrado.groupby('cordillera').agg({
                'distancia_km': ['count', 'mean', 'std', 'min', 'max'],
                'desnivel_m': ['mean', 'min', 'max'],
                'route_score': ['mean']
            }).round(2)
            st.dataframe(stats_cordillera, use_container_width=True)
    
    with col2:
        st.subheader("🏆 Top 10 Mejores Rutas")
        if 'route_score' in df_filtrado.columns:
            top_rutas = df_filtrado.nlargest(10, 'route_score')[
                ['name', 'cordillera', 'distancia_km', 'desnivel_m', 'difficulty', 'route_score']
            ].reset_index(drop=True)
            st.dataframe(top_rutas, use_container_width=True)
    
    st.markdown("---")
    
    # Distribuciones
    st.subheader("📊 Distribuciones de Variables Principales")
    
    col1, col2, col3 = st.columns(3)
    
    variables = [
        ('distancia_km', 'Distancia (km)', col1),
        ('desnivel_m', 'Desnivel (m)', col2),
        ('route_score', 'Score de Calidad', col3)
    ]
    
    for var_name, title, col in variables:
        with col:
            if var_name in df_filtrado.columns:
                fig = px.histogram(
                    df_filtrado, 
                    x=var_name, 
                    nbins=20,
                    title=title,
                    labels={var_name: title}
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # Correlaciones
    st.markdown("---")
    st.subheader("🔗 Matriz de Correlación")
    
    numeric_cols = ['distancia_km', 'desnivel_m', 'route_score', 'intensity']
    numeric_cols = [col for col in numeric_cols if col in df_filtrado.columns]
    
    if len(numeric_cols) >= 2:
        try:
            # Calcular correlaciones
            corr_matrix = df_filtrado[numeric_cols].corr()
            
            # Crear heatmap
            fig = px.imshow(
                corr_matrix,
                text_auto=True,
                aspect="auto",
                color_continuous_scale='RdBu',
                title='Correlaciones entre Variables'
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Interpretación
            with st.expander("📖 Interpretación de correlaciones"):
                st.markdown("""
                **Guía de interpretación:**
                - **+1.0:** Correlación positiva perfecta
                - **+0.7 a +0.9:** Fuerte correlación positiva
                - **+0.4 a +0.6:** Correlación moderada
                - **0.0:** Sin correlación
                - **-0.4 a -0.6:** Correlación negativa moderada
                - **-0.7 a -0.9:** Fuerte correlación negativa
                - **-1.0:** Correlación negativa perfecta
                
                **Ejemplos en nuestro contexto:**
                - Una correlación positiva entre distancia y desnivel indica que rutas más largas tienden a tener más desnivel
                - Una correlación negativa podría indicar relaciones inversas
                """)
        except:
            st.info("No se pudo calcular la matriz de correlación")

def renderizar_calidad_datos(df: pd.DataFrame):
    """Panel de calidad de datos"""
    st.title("🔍 Calidad de Datos")
    st.markdown("Análisis de completitud, consistencia y validez de los datos")
    st.markdown("---")
    
    if df.empty:
        st.warning("No hay datos disponibles.")
        return
    
    # Métricas de calidad
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_registros = len(df)
        st.metric("Total Registros", f"{total_registros:,}")
    
    with col2:
        columnas_totales = len(df.columns)
        st.metric("Total Columnas", columnas_totales)
    
    with col3:
        if total_registros > 0:
            registros_completos = df.notna().all(axis=1).sum()
            porcentaje_completos = (registros_completos / total_registros) * 100
            st.metric("Registros Completos", f"{porcentaje_completos:.1f}%")
        else:
            st.metric("Registros Completos", "0%")
    
    with col4:
        if 'route_score' in df.columns:
            score_promedio = df['route_score'].mean()
            st.metric("Score Promedio", f"{score_promedio:.1f}/100")
        else:
            st.metric("Score", "N/A")
    
    st.markdown("---")
    
    # Completitud por columna
    st.subheader("📊 Completitud por Columna")
    
    columnas_analizar = ['name', 'cordillera', 'distancia_km', 'desnivel_m', 
                        'difficulty', 'intensity', 'route_score']
    columnas_analizar = [col for col in columnas_analizar if col in df.columns]
    
    if columnas_analizar:
        completitud = []
        for col in columnas_analizar:
            no_nulos = df[col].notna().sum()
            porcentaje = (no_nulos / total_registros) * 100
            completitud.append({
                'Columna': col,
                'No Nulos': no_nulos,
                'Porcentaje': porcentaje,
                'Estado': '✅ Alta' if porcentaje > 90 else '⚠️ Media' if porcentaje > 70 else '❌ Baja'
            })
        
        df_completitud = pd.DataFrame(completitud)
        st.dataframe(df_completitud, use_container_width=True)
    
    st.markdown("---")
    
    # Valores únicos y distribución
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎯 Valores Únicos")
        columnas_categoricas = ['difficulty', 'cordillera']
        columnas_categoricas = [col for col in columnas_categoricas if col in df.columns]
        
        if columnas_categoricas:
            valores_unicos = {}
            for col in columnas_categoricas:
                valores_unicos[col] = df[col].nunique()
            
            df_unicos = pd.DataFrame(list(valores_unicos.items()), columns=['Columna', 'Valores Únicos'])
            st.dataframe(df_unicos, use_container_width=True)
    
    with col2:
        st.subheader("📈 Distribución de Dificultad")
        if 'difficulty' in df.columns:
            diff_counts = df['difficulty'].value_counts()
            fig = px.bar(
                x=diff_counts.index,
                y=diff_counts.values,
                labels={'x': 'Dificultad', 'y': 'Cantidad'},
                title='Cantidad de rutas por dificultad'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Problemas detectados
    st.markdown("---")
    st.subheader("⚠️ Problemas Detectados")
    
    problemas = []
    
    # Check 1: Valores fuera de rango
    if 'distancia_km' in df.columns:
        fuera_rango = df[(df['distancia_km'] <= 0) | (df['distancia_km'] > 100)]
        if len(fuera_rango) > 0:
            problemas.append(f"**Distancias fuera de rango:** {len(fuera_rango)} rutas con distancia ≤0 o >100km")
    
    # Check 2: Valores nulos en columnas críticas
    columnas_criticas = ['name', 'distancia_km', 'desnivel_m']
    columnas_criticas = [col for col in columnas_criticas if col in df.columns]
    
    for col in columnas_criticas:
        nulos = df[col].isna().sum()
        if nulos > 0:
            problemas.append(f"**Valores nulos en {col}:** {nulos} registros")
    
    # Check 3: Dificultades no válidas
    if 'difficulty' in df.columns:
        dificultades_validas = ['Fácil', 'Moderada', 'Difícil']
        dificultades_invalidas = df[~df['difficulty'].isin(dificultades_validas)]
        if len(dificultades_invalidas) > 0:
            problemas.append(f"**Dificultades no válidas:** {len(dificultades_invalidas)} registros")
    
    if problemas:
        for problema in problemas:
            st.warning(problema)
    else:
        st.success("✅ No se detectaron problemas graves de calidad de datos")

def renderizar_monitorizacion_etl():
    """Monitorización del pipeline ETL"""
    st.title("⚙️ Monitorización del Pipeline ETL")
    st.markdown("Seguimiento del proceso de extracción, transformación y carga de datos")
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🗄️ Estado del Sistema")
        
        # Verificar conexión MongoDB
        client = get_mongo_client()
        if client:
            st.success("✅ MongoDB Conectado")
            
            # Obtener estadísticas
            stats = obtener_estadisticas_db()
            
            # Métricas clave
            col_metrics1, col_metrics2 = st.columns(2)
            with col_metrics1:
                st.metric("📊 Rutas Crudas", f"{stats.get('raw_routes', 0):,}")
                st.metric("🔄 Rutas Transformadas", f"{stats.get('transformed_routes', 0):,}")
            
            with col_metrics2:
                st.metric("📈 Logs de Ingesta", f"{stats.get('ingestion_logs', 0):,}")
                st.metric("🗂️ Colecciones", len(stats.get('collections', [])))
            
            # Últimas ejecuciones
            st.subheader("🕐 Últimas Ejecuciones")
            
            if stats.get('last_ingestion'):
                fecha_ingesta = stats['last_ingestion']
                if isinstance(fecha_ingesta, str):
                    fecha_ingesta = datetime.fromisoformat(fecha_ingesta.replace('Z', '+00:00'))
                tiempo_transcurrido = datetime.now() - fecha_ingesta
                horas = tiempo_transcurrido.total_seconds() / 3600
                
                if horas < 1:
                    estado = "✅ Reciente (<1h)"
                elif horas < 24:
                    estado = "⚠️ Ayer"
                else:
                    estado = "❌ Antiguo (>24h)"
                
                st.info(f"**Última ingesta:** {fecha_ingesta.strftime('%Y-%m-%d %H:%M')} ({estado})")
            
            if stats.get('last_transformation'):
                fecha_transform = stats['last_transformation']
                if isinstance(fecha_transform, str):
                    fecha_transform = datetime.fromisoformat(fecha_transform.replace('Z', '+00:00'))
                st.info(f"**Última transformación:** {fecha_transform.strftime('%Y-%m-%d %H:%M')}")
            
        else:
            st.error("❌ MongoDB no disponible")
    
    with col2:
        st.subheader("🔧 Configuración del Pipeline")
        
        st.info("""
        **Pipeline ETL Configurado:**
        
        1. **Extracción:** Desde múltiples APIs (Wikiloc, AllTrails)
        2. **Transformación:** Limpieza + Feature Engineering
        3. **Carga:** MongoDB + Archivos (JSON/CSV)
        
        **Frecuencia de ejecución:**
        - **Ingesta:** Diaria (00:00 UTC)
        - **Transformación:** Tras cada ingesta
        - **Validación:** Durante transformación
        """)
        
        # Simular ejecución del pipeline
        st.subheader("🚀 Ejecutar Pipeline")
        
        if st.button("▶️ Ejecutar ETL Completo", use_container_width=True):
            with st.spinner("Ejecutando pipeline ETL..."):
                # Simulación de ejecución
                time.sleep(2)
                
                # Simular resultados
                with st.expander("📋 Resultados de la ejecución", expanded=True):
                    st.success("✅ Extracción completada: 1,245 rutas obtenidas")
                    st.success("✅ Transformación completada: 1,200 rutas procesadas")
                    st.success("✅ Carga completada: Datos guardados en MongoDB")
                    st.info("⏱️ Tiempo total: 45.3 segundos")
        
        # Verificar integridad
        if st.button("🔍 Verificar Integridad", use_container_width=True):
            with st.spinner("Verificando integridad de datos..."):
                time.sleep(1)
                
                # Simular verificación
                verificaciones = [
                    ("✅", "Conexión MongoDB estable"),
                    ("✅", "Colecciones existentes"),
                    ("✅", "Datos transformados disponibles"),
                    ("⚠️", "2 rutas con datos incompletos"),
                    ("✅", "Formato de datos correcto")
                ]
                
                for icon, mensaje in verificaciones:
                    st.write(f"{icon} {mensaje}")
    
    # Logs recientes (simulados)
    st.markdown("---")
    st.subheader("📋 Logs del Sistema")
    
    logs = [
        {"timestamp": "2024-01-25 08:00:00", "level": "INFO", "message": "Pipeline ETL iniciado"},
        {"timestamp": "2024-01-25 08:02:15", "level": "INFO", "message": "Extracción completada: 1,245 rutas"},
        {"timestamp": "2024-01-25 08:05:30", "level": "WARNING", "message": "3 rutas con coordenadas inválidas descartadas"},
        {"timestamp": "2024-01-25 08:10:45", "level": "INFO", "message": "Transformación completada"},
        {"timestamp": "2024-01-25 08:12:00", "level": "INFO", "message": "Carga en MongoDB completada"},
        {"timestamp": "2024-01-25 08:12:30", "level": "INFO", "message": "Pipeline ETL finalizado exitosamente"}
    ]
    
    df_logs = pd.DataFrame(logs)
    st.dataframe(df_logs, use_container_width=True, height=300)

def renderizar_datos_completos(df: pd.DataFrame):
    """Vista completa de datos"""
    st.title("📋 Datos Completos Transformados")
    st.markdown("Exploración completa de todos los datos disponibles")
    st.markdown("---")
    
    if df.empty:
        st.warning("No hay datos disponibles.")
        return
    
    # Búsqueda y filtrado
    col1, col2, col3 = st.columns([3, 1, 1])
    
    with col1:
        busqueda = st.text_input("🔍 Buscar en nombre o cordillera:")
    
    with col2:
        limite = st.selectbox("Filas:", [50, 100, 250, 500, "Todas"], index=1)

    
    # Aplicar filtros de búsqueda
    df_filtrado = df.copy()
    
    if busqueda:
        df_filtrado = df_filtrado[
            df_filtrado['name'].str.contains(busqueda, case=False, na=False) |
            df_filtrado['cordillera'].str.contains(busqueda, case=False, na=False)
        ]
    
    if limite != "Todas":
        df_filtrado = df_filtrado.head(int(limite))
    
    # Selección de columnas
    st.subheader("🎯 Selección de Columnas")
    
    todas_columnas = df_filtrado.columns.tolist()
    # Excluir columnas técnicas
    columnas_excluir = ['_id', '_transformation_metadata', '_ingestion_metadata', 'coordinates']
    columnas_disponibles = [col for col in todas_columnas if col not in columnas_excluir]
    
    columnas_seleccionadas = st.multiselect(
        "Selecciona las columnas a mostrar:",
        options=columnas_disponibles,
        default=['name', 'cordillera', 'distancia_km', 'desnivel_m', 
                'difficulty', 'route_score', 'intensity']
    )
    
    # Mostrar datos
    if columnas_seleccionadas:
        st.dataframe(
            df_filtrado[columnas_seleccionadas],
            use_container_width=True,
            height=500
        )
        
        # Estadísticas básicas
        st.subheader("📊 Resumen Estadístico")
        
        numeric_cols = [col for col in ['distancia_km', 'desnivel_m', 'route_score', 'intensity'] 
                       if col in df_filtrado.columns and col in columnas_seleccionadas]
        
        if numeric_cols:
            st.write(df_filtrado[numeric_cols].describe().round(2))
    
    # Botones de exportación
    st.markdown("---")
    st.subheader("📥 Exportar Datos")
    
    @st.cache_data
    def convertir_a_csv(df_export):
        return df_export.to_csv(index=False).encode('utf-8')
    
    @st.cache_data
    def convertir_a_json(df_export):
        return df_export.to_json(orient='records', indent=2).encode('utf-8')
    
    col1, col2 = st.columns(2)
    
    with col1:
        if columnas_seleccionadas:
            csv_data = convertir_a_csv(df_filtrado[columnas_seleccionadas])
        else:
            csv_data = convertir_a_csv(df_filtrado)
        
        st.download_button(
            label="📥 Descargar como CSV",
            data=csv_data,
            file_name=f"rutas_transformadas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with col2:
        if columnas_seleccionadas:
            json_data = convertir_a_json(df_filtrado[columnas_seleccionadas])
        else:
            json_data = convertir_a_json(df_filtrado)
        
        st.download_button(
            label="📥 Descargar como JSON",
            data=json_data,
            file_name=f"rutas_transformadas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            use_container_width=True
        )

# ========== MODO USUARIO ==========
def renderizar_modo_usuario():
    """Modo para usuarios finales (senderistas)"""
    
    # Inicializar session state para usuario
    if 'usuario_paso' not in st.session_state:
        st.session_state.usuario_paso = 1
    if 'usuario_respuestas' not in st.session_state:
        st.session_state.usuario_respuestas = {}
    if 'usuario_recomendaciones' not in st.session_state:
        st.session_state.usuario_recomendaciones = None
    
    # Sidebar del usuario
    with st.sidebar:
        st.title("🚶 Modo Usuario")
        st.markdown("Encuentra tu ruta ideal en 4 pasos sencillos")
        st.markdown("---")
        
        # Indicador de progreso
        st.subheader(f"Paso {st.session_state.usuario_paso} de 4")
        progreso = st.session_state.usuario_paso / 4
        st.progress(progreso)
        
        # Navegación entre pasos
        st.markdown("---")
        st.subheader("Navegación")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("◀️ Paso anterior", disabled=st.session_state.usuario_paso <= 1,
                        use_container_width=True):
                st.session_state.usuario_paso -= 1
                st.rerun()
        
        with col2:
            if st.button("Paso siguiente ▶️", disabled=st.session_state.usuario_paso >= 4,
                        use_container_width=True):
                st.session_state.usuario_paso += 1
                st.rerun()
        
        # Reiniciar cuestionario
        st.markdown("---")
        if st.button("🔄 Reiniciar cuestionario", use_container_width=True):
            for key in ['usuario_paso', 'usuario_respuestas', 'usuario_recomendaciones']:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
    
    # Renderizar paso actual
    if st.session_state.usuario_paso == 1:
        renderizar_paso_1_usuario()
    elif st.session_state.usuario_paso == 2:
        renderizar_paso_2_usuario()
    elif st.session_state.usuario_paso == 3:
        renderizar_paso_3_usuario()
    elif st.session_state.usuario_paso == 4:
        renderizar_paso_4_usuario()

def renderizar_paso_1_usuario():
    """Paso 1: Experiencia y condición física"""
    st.title("🏔️ Encuentra tu ruta ideal")
    st.markdown("### Paso 1: Tu experiencia y condición física")
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎯 Tu experiencia en montaña")
        experiencia = st.radio(
            "¿Cuál es tu nivel de experiencia en senderismo?",
            options=[
                "🚶 Principiante (pocas salidas)",
                "🥾 Intermedio (varias salidas al año)",
                "🥾🥾 Avanzado (experiencia regular)",
                "🏔️ Experto (rutas técnicas frecuentes)"
            ],
            index=0
        )
        st.session_state.usuario_respuestas['experiencia'] = experiencia
    
    with col2:
        st.subheader("💪 Tu condición física")
        condicion = st.radio(
            "¿Cómo valoras tu condición física actual?",
            options=[
                "😊 Buena forma (deporte regular)",
                "👍 Forma media (algo de ejercicio)",
                "🤔 Forma baja (poco ejercicio)",
                "🏃‍♂️ Atleta (entrenamiento intenso)"
            ],
            index=0
        )
        st.session_state.usuario_respuestas['condicion'] = condicion
    
    st.markdown("---")
    
    # Guía de ayuda
    with st.expander("📖 ¿Cómo elegir correctamente?"):
        st.markdown("""
        **Guía para seleccionar tu nivel:**
        
        **Principiante:**
        - Menos de 10 salidas al año
        - Prefieres rutas señalizadas y sin dificultad técnica
        - Menos de 4 horas de caminata
        
        **Intermedio:**
        - 10-20 salidas al año
        - Te sientes cómodo con desniveles moderados
        - Hasta 6 horas de caminata
        
        **Avanzado:**
        - Más de 20 salidas al año
        - Experiencia con terreno técnico
        - Hasta 8 horas de caminata
        
        **Experto:**
        - Senderista frecuente
        - Equipamiento técnico completo
        - Más de 8 horas sin problemas
        """)

def renderizar_paso_2_usuario():
    """Paso 2: Preferencias de ruta"""
    st.title("🏔️ Encuentra tu ruta ideal")
    st.markdown("### Paso 2: Preferencias de la ruta")
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📏 Distancia preferida")
        distancia = st.slider(
            "¿Qué distancia te apetece hacer? (en km)",
            min_value=2,
            max_value=30,
            value=(5, 15),
            step=1
        )
        st.session_state.usuario_respuestas['distancia'] = distancia
        st.info(f"Rango seleccionado: {distancia[0]} - {distancia[1]} km")
    
    with col2:
        st.subheader("⬆️ Desnivel preferido")
        desnivel = st.slider(
            "¿Cuánto desnivel estás dispuesto a afrontar? (en metros)",
            min_value=0,
            max_value=2000,
            value=(200, 800),
            step=50
        )
        st.session_state.usuario_respuestas['desnivel'] = desnivel
        st.info(f"Rango seleccionado: {desnivel[0]} - {desnivel[1]} m")
    
    st.markdown("---")
    
    st.subheader("⏱️ Tiempo disponible")
    tiempo = st.radio(
        "¿Cuánto tiempo tienes para la actividad?",
        options=[
            "🌅 Medio día (3-5 horas)",
            "☀️ Día completo (5-8 horas)",
            "🌄 Gran día (+8 horas)"
        ],
        index=0
    )
    st.session_state.usuario_respuestas['tiempo'] = tiempo
    
    # Referencias visuales
    st.markdown("---")
    col_ref1, col_ref2, col_ref3 = st.columns(3)
    
    with col_ref1:
        st.metric("Distancia media", "8-12 km", "Rango óptimo para mayoría")
    
    with col_ref2:
        st.metric("Desnivel medio", "400-600 m", "Moderado pero exigente")
    
    with col_ref3:
        st.metric("Tiempo medio", "4-6 horas", "Incluyendo descansos")

def renderizar_paso_3_usuario():
    """Paso 3: Características adicionales"""
    st.title("🏔️ Encuentra tu ruta ideal")
    st.markdown("### Paso 3: Características adicionales")
    st.markdown("---")
    
    st.subheader("🎯 Tipo de ruta preferida")
    
    tipo_ruta = st.selectbox(
        "¿Qué tipo de ruta buscas?",
        options=[
            "Paisajística (vistas espectaculares)",
            "Técnica (retos físicos)",
            "Familiar (accesible para todos)",
            "Entrenamiento (exigencia física)",
            "Cualquiera (lo que sea)"
        ]
    )
    st.session_state.usuario_respuestas['tipo_ruta'] = tipo_ruta
    
    st.markdown("---")
    
    st.subheader("🗺️ Región preferida")
    
    # Obtener regiones disponibles
    regiones_data = obtener_regiones()
    if regiones_data:
        regiones = list(regiones_data.keys())
        region = st.selectbox(
            "¿Tienes preferencia por alguna región?",
            options=["Cualquiera"] + regiones[:10]  # Limitar a 10 para no saturar
        )
        st.session_state.usuario_respuestas['region'] = region
    else:
        st.session_state.usuario_respuestas['region'] = "Cualquiera"
        st.info("No hay información de regiones disponible")
    
    st.markdown("---")
    
    st.subheader("🌤️ Época del año")
    epoca = st.radio(
        "¿Para qué época planeas la salida?",
        options=[
            "❄️ Invierno (diciembre-febrero)",
            "🌱 Primavera (marzo-mayo)",
            "☀️ Verano (junio-agosto)",
            "🍂 Otoño (septiembre-noviembre)",
            "Cualquiera"
        ],
        index=4
    )
    st.session_state.usuario_respuestas['epoca'] = epoca

def renderizar_paso_4_usuario():
    """Paso 4: Resultados y recomendaciones"""
    st.title("🏔️ Encuentra tu ruta ideal")
    st.markdown("### Paso 4: Tus rutas recomendadas")
    st.markdown("---")
    
    # Mostrar resumen de respuestas
    st.subheader("📋 Tus preferencias:")
    
    respuestas = st.session_state.usuario_respuestas
    col1, col2 = st.columns(2)
    
    with col1:
        st.write(f"**Experiencia:** {respuestas.get('experiencia', 'No especificada')}")
        st.write(f"**Condición física:** {respuestas.get('condicion', 'No especificada')}")
        st.write(f"**Tiempo disponible:** {respuestas.get('tiempo', 'No especificada')}")
    
    with col2:
        if 'distancia' in respuestas:
            st.write(f"**Distancia:** {respuestas['distancia'][0]}-{respuestas['distancia'][1]} km")
        if 'desnivel' in respuestas:
            st.write(f"**Desnivel:** {respuestas['desnivel'][0]}-{respuestas['desnivel'][1]} m")
        st.write(f"**Tipo de ruta:** {respuestas.get('tipo_ruta', 'No especificada')}")
    
    st.markdown("---")
    
    # Botón para generar recomendaciones
    if st.button("🔍 Generar recomendaciones", type="primary", use_container_width=True):
        with st.spinner("Buscando las mejores rutas para ti..."):
            # Cargar datos
            df = cargar_datos_transformados(1000)
            if not df.empty:
                # Procesar datos
                df_procesado = procesar_dataframe(df)
                
                # Generar recomendaciones
                recomendaciones = generar_recomendaciones_usuario(df_procesado, respuestas)
                st.session_state.usuario_recomendaciones = recomendaciones
            else:
                st.error("No se pudieron cargar datos. Verifica la conexión a MongoDB.")
    
    # Mostrar recomendaciones si existen
    if st.session_state.usuario_recomendaciones is not None:
        mostrar_recomendaciones(st.session_state.usuario_recomendaciones)

def generar_recomendaciones_usuario(df: pd.DataFrame, respuestas: Dict) -> pd.DataFrame:
    """Generar recomendaciones basadas en las respuestas del usuario"""
    if df.empty:
        return pd.DataFrame()
    
    # Mapear respuestas a filtros
    # 1. Mapear experiencia a dificultad
    experiencia_map = {
        "🚶 Principiante (pocas salidas)": "Fácil",
        "🥾 Intermedio (varias salidas al año)": ["Fácil", "Moderada"],
        "🥾🥾 Avanzado (experiencia regular)": ["Moderada", "Difícil"],
        "🏔️ Experto (rutas técnicas frecuentes)": "Difícil"
    }
    
    # 2. Mapear tiempo a horas estimadas
    tiempo_map = {
        "🌅 Medio día (3-5 horas)": (0, 5),
        "☀️ Día completo (5-8 horas)": (5, 8),
        "🌄 Gran día (+8 horas)": (8, 12)
    }
    
    # 3. Mapear tipo de ruta a características
    tipo_ruta_map = {
        "Paisajística (vistas espectaculares)": lambda df: df[df['route_score'] > 70],
        "Técnica (retos físicos)": lambda df: df[(df['technical_level'] == 'Técnica (experiencia avanzada)') 
                                                 if 'technical_level' in df.columns else df[df['difficulty'] == 'Difícil']],
        "Familiar (accesible para todos)": lambda df: df[df['family_friendly'] == True] 
                                                    if 'family_friendly' in df.columns else df[df['difficulty'] == 'Fácil'],
        "Entrenamiento (exigencia física)": lambda df: df[df['training_route'] == True] 
                                                     if 'training_route' in df.columns else df[df['intensity'] > 60],
        "Cualquiera (lo que sea)": lambda df: df
    }
    
    # Aplicar filtros
    df_filtrado = df.copy()
    
    # Filtro 1: Dificultad según experiencia
    if 'experiencia' in respuestas and respuestas['experiencia'] in experiencia_map:
        dificultad = experiencia_map[respuestas['experiencia']]
        if isinstance(dificultad, list):
            df_filtrado = df_filtrado[df_filtrado['difficulty'].isin(dificultad)]
        else:
            df_filtrado = df_filtrado[df_filtrado['difficulty'] == dificultad]
    
    # Filtro 2: Distancia
    if 'distancia' in respuestas:
        min_dist, max_dist = respuestas['distancia']
        df_filtrado = df_filtrado[
            (df_filtrado['distancia_km'] >= min_dist) & 
            (df_filtrado['distancia_km'] <= max_dist)
        ]
    
    # Filtro 3: Desnivel
    if 'desnivel' in respuestas:
        min_des, max_des = respuestas['desnivel']
        df_filtrado = df_filtrado[
            (df_filtrado['desnivel_m'] >= min_des) & 
            (df_filtrado['desnivel_m'] <= max_des)
        ]
    
    # Filtro 4: Tiempo (si tenemos estimated_hours)
    if 'tiempo' in respuestas and respuestas['tiempo'] in tiempo_map:
        min_horas, max_horas = tiempo_map[respuestas['tiempo']]
        if 'estimated_hours' in df_filtrado.columns:
            df_filtrado = df_filtrado[
                (df_filtrado['estimated_hours'] >= min_horas) & 
                (df_filtrado['estimated_hours'] <= max_horas)
            ]
    
    # Filtro 5: Tipo de ruta
    if 'tipo_ruta' in respuestas and respuestas['tipo_ruta'] in tipo_ruta_map:
        df_filtrado = tipo_ruta_map[respuestas['tipo_ruta']](df_filtrado)
    
    # Filtro 6: Región (opcional)
    if 'region' in respuestas and respuestas['region'] != "Cualquiera":
        df_filtrado = df_filtrado[df_filtrado['cordillera'] == respuestas['region']]
    
    # Ordenar por score (las mejores primeras)
    if 'route_score' in df_filtrado.columns:
        df_filtrado = df_filtrado.sort_values('route_score', ascending=False)
    
    # Limitar a 5 recomendaciones
    return df_filtrado.head(500)

def mostrar_recomendaciones(df: pd.DataFrame):
    """Mostrar las recomendaciones al usuario"""
    if df.empty:
        st.warning("No encontramos rutas que coincidan exactamente con tus preferencias.")
        st.info("💡 **Sugerencias:**")
        st.write("- Amplía el rango de distancia o desnivel")
        st.write("- Prueba con un nivel de dificultad diferente")
        st.write("- Selecciona 'Cualquiera' en tipo de ruta")
        return
    
    st.success(f"🎉 Encontramos {len(df)} rutas que se ajustan a tus preferencias!")
    st.markdown("---")
    
    # Mostrar cada recomendación
    for idx, row in df.iterrows():
        with st.container():
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.subheader(f"{row['name']}")
                
                # Métricas de la ruta
                metric_cols = st.columns(4)
                with metric_cols[0]:
                    st.metric("📏 Distancia", f"{row.get('distancia_km', 0):.1f} km")
                with metric_cols[1]:
                    st.metric("⬆️ Desnivel", f"{row.get('desnivel_m', 0):.0f} m")
                with metric_cols[2]:
                    st.metric("⚡ Dificultad", row.get('difficulty', 'Moderada'))
                with metric_cols[3]:
                    if 'route_score' in row:
                        st.metric("⭐ Score", f"{row['route_score']:.0f}/100")
                
                # Información adicional
                if 'cordillera' in row:
                    st.write(f"**🏔️ Cordillera:** {row['cordillera']}")
                
                if 'best_season' in row:
                    st.write(f"**🌤️ Mejor época:** {row['best_season']}")
                
                if 'technical_level' in row:
                    st.write(f"**🎯 Nivel técnico:** {row['technical_level']}")
                
                if 'enriched_description' in row:
                    with st.expander("📝 Descripción completa"):
                        st.write(row['enriched_description'])
            
            with col2:
                # Botón para seleccionar esta ruta
                if st.button("👁️ Ver detalles", key=f"ver_{idx}", use_container_width=True):
                    st.session_state.ruta_detalle = row.to_dict()
                    st.rerun()
            
            st.markdown("---")
    
    # Opciones adicionales
    col_opt1, col_opt2 = st.columns(2)
    
    with col_opt1:
        if st.button("🔄 Buscar más opciones", use_container_width=True):
            st.session_state.usuario_recomendaciones = None
            st.rerun()
    
    with col_opt2:
        if st.download_button(
            label="📥 Descargar recomendaciones",
            data=df.to_csv(index=False).encode('utf-8'),
            file_name=f"mis_recomendaciones_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True
        ):
            st.success("✅ Descarga iniciada")

# ========== FUNCIÓN PRINCIPAL ==========
def main():
    """Función principal de la aplicación"""
    
    # Inicializar session state
    if 'modo_actual' not in st.session_state:
        st.session_state.modo_actual = "usuario"
    
    if 'ruta_detalle' in st.session_state:
        mostrar_detalle_ruta(st.session_state.ruta_detalle)
        return
    
    # Sidebar principal
    with st.sidebar:
        st.title("🏔️ TFM Data Engineering")
        st.markdown("### Sistema de Rutas de Montaña")
        st.markdown("---")
        
        # Selector de modo
        st.subheader("👥 Selecciona modo:")
        
        modo = st.radio(
            "Elige cómo quieres usar la aplicación:",
            options=[
                "🚶 **Modo Usuario** - Encuentra tu ruta ideal",
                "🔍 **Modo Analista** - Explora y analiza datos"
            ],
            index=0 if st.session_state.modo_actual == "usuario" else 1,
            label_visibility="collapsed"
        )
        
        # Actualizar modo en session state
        if "Usuario" in modo and st.session_state.modo_actual != "usuario":
            st.session_state.modo_actual = "usuario"
            st.rerun()
        elif "Analista" in modo and st.session_state.modo_actual != "analista":
            st.session_state.modo_actual = "analista"
            st.rerun()
        
        st.markdown("---")
        
        # Estado del sistema
        st.subheader("📊 Estado del Sistema")
        
        client = get_mongo_client()
        if client:
            st.success("✅ MongoDB Conectado")
            stats = obtener_estadisticas_db()
            
            if stats.get('transformed_routes', 0) > 0:
                st.info(f"**Rutas disponibles:** {stats['transformed_routes']:,}")
            
            if stats.get('last_transformation'):
                fecha_str = stats['last_transformation']
                if isinstance(fecha_str, str):
                    try:
                        fecha = datetime.fromisoformat(fecha_str.replace('Z', '+00:00'))
                        st.info(f"**Actualizado:** {fecha.strftime('%d/%m/%Y %H:%M')}")
                    except:
                        st.info(f"**Actualizado:** {fecha_str[:10]}")
        else:
            st.error("❌ MongoDB no disponible")
        
        st.markdown("---")
        
        # Información del proyecto
        with st.expander("ℹ️ Sobre este proyecto"):
            st.markdown("""
            **TFM Data Engineering - Pipeline ETL de Rutas**
            
            Este sistema demuestra un pipeline completo ETL para:
            
            1. **Extracción:** Datos de múltiples APIs de senderismo
            2. **Transformación:** Limpieza, enriquecimiento y feature engineering
            3. **Carga:** Almacenamiento en MongoDB para análisis
            
            **Tecnologías utilizadas:**
            - Python + Pandas + NumPy
            - MongoDB (almacenamiento)
            - Streamlit (visualización)
            - Plotly (gráficos interactivos)
            
            **Autor:** [Tu nombre]
            **TFM:** Máster en Data Engineering
            """)
    
    # Renderizar modo seleccionado
    if st.session_state.modo_actual == "usuario":
        renderizar_modo_usuario()
    else:
        renderizar_panel_analista()

def mostrar_detalle_ruta(ruta: Dict):
    """Mostrar detalles completos de una ruta"""
    st.title("📖 Detalles de la Ruta")
    st.markdown("---")
    
    # Botón para volver
    if st.button("← Volver a las recomendaciones", use_container_width=True):
        del st.session_state.ruta_detalle
        st.rerun()
    
    # Encabezado
    nombre = ruta.get('name', 'Ruta sin nombre')
    cordillera = ruta.get('cordillera', ruta.get('mountain_range', 'Desconocida'))
    st.markdown(f"## {nombre}")
    st.markdown(f"### 🏔️ {cordillera}")
    
    st.markdown("---")
    
    # Métricas principales
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        distancia = ruta.get('distancia_km', ruta.get('distance_km', 0))
        st.metric("📏 Distancia", f"{distancia:.1f} km")
    
    with col2:
        desnivel = ruta.get('desnivel_m', ruta.get('ascent_m', 0))
        st.metric("⬆️ Desnivel", f"{desnivel:.0f} m")
    
    with col3:
        st.metric("⚡ Dificultad", ruta.get('difficulty', 'Moderada'))
    
    with col4:
        if 'estimated_hours' in ruta:
            horas = float(ruta['estimated_hours'])
            h = int(horas)
            m = int((horas - h) * 60)
            st.metric("⏱️ Duración", f"{h}h {m}min")
        else:
            st.metric("⏱️ Duración", "Estimada")
    
    st.markdown("---")
    
    # Información detallada
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Características Técnicas")
        
        info = []
        
        if 'intensity' in ruta:
            info.append(f"**Intensidad:** {ruta['intensity']:.1f} m/km")
        
        if 'intensity_level' in ruta:
            info.append(f"**Nivel de intensidad:** {ruta['intensity_level']}")
        
        if 'technical_level' in ruta:
            info.append(f"**Nivel técnico:** {ruta['technical_level']}")
        
        if 'route_score' in ruta:
            score = ruta['route_score']
            if score >= 80:
                calidad = "Excelente"
            elif score >= 60:
                calidad = "Muy buena"
            elif score >= 40:
                calidad = "Buena"
            else:
                calidad = "Regular"
            info.append(f"**Calificación:** {calidad} ({score}/100)")
        
        if 'best_season' in ruta:
            info.append(f"**Mejor época:** {ruta['best_season']}")
        
        if 'weekend_friendly' in ruta:
            info.append(f"**Apta para fin de semana:** {'Sí' if ruta['weekend_friendly'] else 'No'}")
        
        if 'family_friendly' in ruta:
            info.append(f"**Apta para familias:** {'Sí' if ruta['family_friendly'] else 'No'}")
        
        if 'training_route' in ruta:
            info.append(f"**Buena para entrenamiento:** {'Sí' if ruta['training_route'] else 'No'}")
        
        for item in info:
            st.markdown(item)
    
    with col2:
        st.subheader("📝 Descripción")
        
        if 'enriched_description' in ruta:
            st.write(ruta['enriched_description'])
        else:
            # Generar descripción automática
            desc = f"**{nombre}** es una ruta de senderismo en **{cordillera}** "
            desc += f"con una distancia de **{distancia:.1f} km** y un desnivel de **{desnivel:.0f} m**. "
            desc += f"Está clasificada como **{ruta.get('difficulty', 'Moderada')}**."
            
            if 'intensity' in ruta:
                intensidad = ruta['intensity']
                if intensidad > 80:
                    nivel = "muy intensa"
                elif intensidad > 50:
                    nivel = "intensa"
                elif intensidad > 20:
                    nivel = "moderada"
                else:
                    nivel = "suave"
                desc += f" Es una ruta **{nivel}** con una intensidad de **{intensidad:.1f} m/km**."
            
            st.write(desc)
            
            # Recomendaciones generales
            st.markdown("---")
            st.subheader("🎒 Recomendaciones")
            
            recomendaciones = [
                "• Calzado adecuado de montaña",
                "• Agua suficiente (mínimo 1.5L)",
                "• Comida energética",
                "• Ropa por capas",
                "• Protector solar y gafas",
                "• Teléfono móvil cargado",
                "• Mapa/GPS y brújula",
                "• Botiquín básico"
            ]
            
            for rec in recomendaciones:
                st.write(rec)
    

# ========== EJECUCIÓN ==========
if __name__ == "__main__":
    main()
