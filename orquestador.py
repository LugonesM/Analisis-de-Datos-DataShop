"""
==============================================
ORQUESTADOR MAESTRO - DATA WAREHOUSE
==============================================
Ejecuta todos los scripts en el orden correcto:
1. SQLQuerySTAGING.sql - Crea tablas STAGING
2. SQLQueryINT.sql - Crea tablas INT
3. SQLQueryCreateDW.sql - Crea tablas DW (Dimensiones y Fact)
4. SQLQueryStoreProcedures.sql - Crea Stored Procedures
5. extract_data.py - Extrae CSV -> STAGING
6. load_STG_to_INT.py - Carga STAGING -> INT
7. dw_loader.py - Carga INT -> DW
"""

import pyodbc
from configparser import ConfigParser
import os
import sys
import subprocess
from datetime import datetime

# Configura codificación UTF-8 para la consola
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
os.system('chcp 65001 >nul 2>&1')

class DWMasterOrchestrator:
    def __init__(self, config_file='config.ini'):
        self.config = ConfigParser()
        self.config.optionxform = str
        
        # Verifica si el archivo existe 
        if not os.path.exists(config_file):
            current_dir = os.path.abspath('.')
            raise FileNotFoundError(f"Archivo '{config_file}' no encontrado en: {current_dir}")
        
        # Intenta leer el archivo
        try:
            files_read = self.config.read(config_file, encoding='utf-8')
            if not files_read:
                raise Exception(f"No se pudo leer el archivo '{config_file}'")
            
            # Verificar que tenga las secciones necesarias
            if not self.config.has_section('DATABASE'):
                raise Exception("El archivo config.ini no tiene la sección [DATABASE]")
                
        except Exception as e:
            raise Exception(f"Error leyendo config.ini: {e}")
        
        self.connection = None
        
        # Define rutas de archivos
        self.sql_files = [
            'SQLQuerySTAGING.sql',
            'SQLQueryINT.sql',
            'SQLQueryCreateDW.sql',
            'SQLQueryStoreProcedures.sql'
        ]
        
        self.python_scripts = [
            'extract_data.py',
            'load_STG_to_INT.py',
            'dw_loader.py'
        ]
        
    def log(self, message, level="INFO"):
        """Log con timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        prefix = {
            "INFO": "INFO |",
            "SUCCESS": "OK |",
            "ERROR": "ERROR |",
            "WARNING": "WARN |",
            "PROCESS": "PROC |",
            "STEP": "STEP |"
        }.get(level, "")
        
        separator = "=" * 60
        if level == "STEP":
            print(f"\n{separator}")
            print(f"[{timestamp}] {prefix} {message}")
            print(f"{separator}")
        else:
            print(f"[{timestamp}] {prefix} {message}")
    
    def connect_db(self):
        """Conectar a SQL Server"""
        try:
            server = self.config.get('DATABASE', 'server').strip()
            database = self.config.get('DATABASE', 'database').strip()
            trusted_connection = self.config.get('DATABASE', 'trusted_connection').strip()
            driver = self.config.get('DATABASE', 'driver', fallback='ODBC Driver 17 for SQL Server').strip()
            
            self.log(f"Conectando a {server} | BD: {database}", "PROCESS")
            
            connection_string = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"Trusted_Connection={trusted_connection};"
            )
            
            self.connection = pyodbc.connect(connection_string, timeout=30)
            self.connection.autocommit = False
            self.log("Conexión exitosa a SQL Server", "SUCCESS")
            
        except Exception as e:
            self.log(f"Error de conexión: {e}", "ERROR")
            raise
    
    def execute_sql_file(self, sql_file):
        """Ejecutar archivo SQL completo"""
        self.log(f"Ejecutando archivo SQL: {sql_file}", "PROCESS")
        
        if not os.path.exists(sql_file):
            self.log(f"Archivo no encontrado: {sql_file}", "ERROR")
            raise FileNotFoundError(f"No se encontró el archivo: {sql_file}")
        
        try:
            # Intentar leer con diferentes codificaciones por si acaso
            sql_content = None
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-8-sig']
            
            for encoding in encodings:
                try:
                    with open(sql_file, 'r', encoding=encoding) as f:
                        sql_content = f.read()
                    self.log(f"Archivo leído con codificación: {encoding}", "INFO")
                    break
                except UnicodeDecodeError:
                    continue
            
            if sql_content is None:
                raise Exception(f"No se pudo leer el archivo con ninguna codificación estándar")
            
            # Dividir por GO (separador de lotes en SQL Server) y Usar split case-insensitive
            import re
            batches = re.split(r'\bGO\b', sql_content, flags=re.IGNORECASE)
            
            cursor = self.connection.cursor()
            batch_count = 0
            
            for batch in batches:
                batch = batch.strip()
                if batch:  # Solo ejecutar si hay contenido
                    try:
                        cursor.execute(batch)
                        self.connection.commit()
                        batch_count += 1
                    except Exception as e:
                        self.log(f"Error en lote {batch_count + 1}: {str(e)[:200]}", "WARNING")
                        self.connection.rollback()
                        # Continuar con el siguiente lote
                        continue
            
            self.log(f"{sql_file} ejecutado exitosamente ({batch_count} lotes)", "SUCCESS")
            
        except Exception as e:
            self.log(f"Error ejecutando {sql_file}: {e}", "ERROR")
            self.connection.rollback()
            raise
    
    def execute_python_script(self, script_name):
        """Ejecutar script Python"""
        self.log(f"Ejecutando script Python: {script_name}", "PROCESS")
    
        try:
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'
            env['PYTHONUTF8'] = '1'
        
            # Ejecutar sin capturar output (o capturar pero no mostrar todo)
            result = subprocess.run(
                [sys.executable, script_name],
                capture_output=False,
                text=True,
                encoding='utf-8',
                errors='replace',
                env=env
            )
        
            # Solo mostrarmuestra errores si los hay
            if result.returncode != 0:
                self.log(f"Error ejecutando {script_name} (código: {result.returncode})", "ERROR")
                raise Exception(f"El script {script_name} falló con código {result.returncode}")
        
            self.log(f"{script_name} ejecutado exitosamente", "SUCCESS")
        
        except Exception as e:
            self.log(f"Error ejecutando {script_name}: {e}", "ERROR")
            raise
    
    def verify_files(self):
        """Verificar que todos los archivos necesarios existan"""
        self.log("Verificando archivos necesarios...", "PROCESS")
        
        missing_files = []
        
        # Verificar archivos de SQL
        for sql_file in self.sql_files:
            if os.path.exists(sql_file):
                self.log(f"   {sql_file}", "INFO") 
            else:
                self.log(f"   {sql_file} NO ENCONTRADO", "ERROR") 
                missing_files.append(sql_file)
        
        # Verificar scripts de Python
        for py_script in self.python_scripts:
            if os.path.exists(py_script):
                self.log(f"   {py_script}", "INFO") 
            else:
                self.log(f"   {py_script} NO ENCONTRADO", "ERROR") 
                missing_files.append(py_script)
        
        # Verificar archivo config.ini
        if os.path.exists('config.ini'):
            self.log(f"   config.ini", "INFO") 
            
            # Verificar contenido básico
            try:
                with open('config.ini', 'r', encoding='utf-8') as f:
                    content = f.read()
                if '[DATABASE]' in content:
                    self.log(f"   config.ini tiene sección [DATABASE]", "INFO") 
                else:
                    self.log(f"   config.ini NO tiene sección [DATABASE]", "WARNING") 
            except Exception as e:
                self.log(f"   Error leyendo config.ini: {e}", "WARNING") 
        else:
            self.log(f"   config.ini NO ENCONTRADO", "ERROR") 
            missing_files.append('config.ini')
        
        if missing_files:
            self.log(f"Faltan {len(missing_files)} archivo(s)", "ERROR")
            raise FileNotFoundError(f"Archivos faltantes: {', '.join(missing_files)}")
        
        self.log("Todos los archivos verificados correctamente", "SUCCESS")         
    
    def run(self):
        """Ejecutar todo el proceso de orquestación"""
        try:
            print("\n" + "=" * 60)
            print("          INICIANDO ORQUESTACIÓN DEL DATA WAREHOUSE")
            print("=" * 60 + "\n")
            
            # Diagnóstico inicial
            self.log("DIAGNÓSTICO INICIAL", "STEP")
            current_dir = os.path.abspath('.')
            self.log(f"Directorio de trabajo: {current_dir}", "INFO")
            
            # Verificar archivos
            self.log("PASO 0: VERIFICACIÓN DE ARCHIVOS", "STEP")
            self.verify_files()
            
            # Conectar a BD
            self.log("PASO 1: CONEXIÓN A BASE DE DATOS", "STEP")
            self.connect_db()
            
            # Ejecutar scripts SQL para crear tablas y SPs
            self.log("PASO 2: CREACIÓN DE ESTRUCTURA (Tablas y SPs)", "STEP")
            
            for i, sql_file in enumerate(self.sql_files, 1):
                self.log(f"2.{i} - Ejecutando {sql_file}", "INFO")
                self.execute_sql_file(sql_file)
            
            # Cerrar conexión antes de ejecutar scripts Python
            if self.connection:
                self.connection.close()
                self.log("Conexión a BD cerrada", "INFO")
            
            # Ejecutar scripts Python 
            self.log("PASO 3: EJECUCIÓN DE PROCESOS ETL", "STEP")
            
            for i, py_script in enumerate(self.python_scripts, 1):
                self.log(f"3.{i} - Ejecutando {py_script}", "INFO")
                self.execute_python_script(py_script)
            
            # Finalización exitosa
            print("\n" + "=" * 60)
            print("        ORQUESTACIÓN COMPLETADA EXITOSAMENTE")
            print("=" * 60)
            print("\n Resumen:")
            print(f"    Archivos SQL ejecutados: {len(self.sql_files)}")
            print(f"    Scripts Python ejecutados: {len(self.python_scripts)}")
            print(f"    Estado: COMPLETADO")
            print("\n" + "=" * 60 + "\n")
            
        except Exception as e:
            print("\n" + "=" * 60)
            print("           ORQUESTACIÓN FALLIDA")
            print("=" * 60)
            print(f"\n Error: {e}")
            print("\n🔍 Revisa los logs anteriores para más detalles.")
            print("=" * 60 + "\n")
            sys.exit(1)
        
        finally:
            if self.connection:
                try:
                    self.connection.close()
                    self.log("Conexión final cerrada", "INFO")
                except:
                    pass



if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         ORQUESTADOR MAESTRO - DATA WAREHOUSE             ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    try:
        # Cambia al directorio del script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)
        
        print(" Ejecutando diagnóstico inicial...")
        current_dir = os.path.abspath('.')
        print(f" Directorio actual: {current_dir}")
        print(f" Archivos encontrados:")
        files = [f for f in os.listdir('.') if f.endswith(('.py', '.sql', '.ini'))]
        for file in files:
            print(f"   - {file}")
        
        orchestrator = DWMasterOrchestrator()
        orchestrator.run()
        
    except FileNotFoundError as e:
        print(f"\n ERROR DE ARCHIVO: {e}")
        print(f" Asegúrate de que todos los archivos estén en: {os.path.abspath('.')}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n Proceso interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n Error fatal: {e}")
        sys.exit(1)