"""
ORQUESTADOR - Versión Windows
"""
import subprocess
import schedule
import time
from datetime import datetime
import os

def ejecutar_modulo(nombre, comando):
    print(f"\n{'='*50}")
    print(f"EJECUTANDO: {nombre}")
    print(f"Hora: {datetime.now().strftime('%H:%M:%S')}")
    print(f"Comando: {comando}")
    print('='*50)
    
    try:
        # Usar 'py' en lugar de 'python' para Windows
        resultado = subprocess.run(
            comando,
            shell=True,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='ignore'
        )
        
        # Mostrar salida
        if resultado.stdout:
            print("SALIDA:", resultado.stdout[:300])
        
        if resultado.stderr:
            print("ERRORES:", resultado.stderr[:300])
        
        if resultado.returncode == 0:
            print(f"[OK] {nombre} - EXITOSO")
            return True
        else:
            print(f"[ERROR] {nombre} - FALLIDO (código: {resultado.returncode})")
            return False
            
    except Exception as e:
        print(f"[ERROR] en {nombre}: {str(e)}")
        return False

def pipeline_completo():
    print("\n" + "="*60)
    print("INICIANDO PIPELINE TFM")
    print("="*60)
    
    # Cambiar al directorio TFM
    os.chdir(r"C:\Users\Carlos\TFM")
    
    # 1. INGESTA - Usar 'py' en lugar de 'python'
    if not ejecutar_modulo("INGESTA", "py ingesta.py --limit 100"):
        print("\n[ATENCION] Pipeline detenido por error en INGESTA")
        return
    
    # 2. ETL - Usar 'py' en lugar de 'python'
    if not ejecutar_modulo("ETL", "py pipeline_etl_mongo.py --validate"):
        print("\n[ATENCION] Error en ETL, pero continuando...")
    
    print("\n" + "="*60)
    print("PIPELINE COMPLETADO")
    print("="*60)

def menu_principal():
    while True:
        print("\n" + "="*50)
        print("ORQUESTADOR TFM - DATA ENGINEERING")
        print("="*50)
        print("1. Ejecutar pipeline completo AHORA")
        print("2. Programar ejecución semanal (Lunes 2 AM)")
        print("3. Salir")
        print("-"*50)
        
        opcion = input("Selecciona (1-3): ").strip()
        
        if opcion == "1":
            pipeline_completo()
            input("\nPresiona Enter para continuar...")
        elif opcion == "2":
            print("\n[INFO] Programando ejecución semanal...")
            print("[INFO] Mantén esta ventana abierta")
            print("[INFO] Se ejecutará cada Lunes a las 2:00 AM")
            
            # Programar cada Lunes a las 2 AM
            schedule.every().monday.at("02:00").do(pipeline_completo)
            
            # Ejecutar ahora
            pipeline_completo()
            
            print("\n[INFO] Programador activado.")
            print("[INFO] Próxima ejecución automática: Lunes 2:00 AM")
            print("[INFO] Presiona Ctrl+C para salir")
            
            try:
                while True:
                    schedule.run_pending()
                    time.sleep(60)  # Esperar 1 minuto
            except KeyboardInterrupt:
                print("\n[INFO] Programador detenido")
        elif opcion == "3":
            print("\n[INFO] Saliendo...")
            break
        else:
            print("\n[ERROR] Opción no válida")

if __name__ == "__main__":
    print("[INFO] Iniciando Orquestador TFM...")
    menu_principal()
