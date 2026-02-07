import pandas as pd
import pyodbc
from configparser import ConfigParser
from datetime import datetime
import os

class CSVToSQLServer:
    """
    Clase para Extracción (E) y Carga (L) de datos CSV a tablas STAGING en SQL Server.
    """
    def __init__(self, config_file='config.ini'):
        self.config = ConfigParser()
        self.config.optionxform = str
        
        #  Verificar que el archivo de configuración exista y se lea
        if not os.path.exists(config_file) or not self.config.read(config_file):
            print(f" ERROR: No se pudo leer el archivo de configuración: {config_file}")
            raise FileNotFoundError(f"Archivo '{config_file}' no encontrado o vacío.")

        self.connection = None
        self.dataset_folder = 'DATASET'
        
        # Definición explícita de columnas del CSV que conincide con las tablas STG
        self.column_mapping = {
            'clientes.csv': ['CodCliente', 'RazonSocial', 'Telefono', 'Mail', 'Direccion', 'Localidad', 'Provincia', 'CP'],
            'productos.csv': ['CodigoProducto', 'Descripcion', 'Categoria', 'Marca', 'PrecioCosto', 'PrecioVentaSugerido'],
            'tiendas.csv': ['CodigoTienda', 'Descripcion', 'Direccion', 'Localidad', 'Provincia', 'CP', 'TipoTienda'],
            'ventas.csv': ['FechaVenta', 'CodigoProducto', 'Producto', 'Cantidad', 'PrecioVenta', 'CodigoCliente', 'Cliente', 'CodigoTienda', 'Tienda'],
            'ventas_add.csv': ['FechaVenta', 'CodigoProducto', 'Producto', 'Cantidad', 'PrecioVenta', 'CodigoCliente', 'Cliente', 'CodigoTienda', 'Tienda']
        }
        
    def connect_db(self):
        """Conectar a base de datos SQL."""
        try:
            
            server = self.config.get('DATABASE', 'server', fallback='').strip()
            database = self.config.get('DATABASE', 'database', fallback='').strip()
            trusted_connection = self.config.get('DATABASE', 'trusted_connection', fallback='').strip()
            driver = self.config.get('DATABASE', 'driver', fallback='ODBC Driver 17 for SQL Server').strip()
            
            # Validación de datos críticos
            if not all([server, database, trusted_connection]):
                 raise ValueError("Faltan parámetros críticos (server, database, trusted_connection) en config.ini.")
            
            print(f" Conectando a: {server} | BD: {database} usando {driver}")
            
            connection_string = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"Trusted_Connection={trusted_connection};"
            )
            
            self.connection = pyodbc.connect(connection_string, timeout=10)
            self.connection.autocommit = False 
            print(" Conexión exitosa!")
            
        except Exception as e:
            print(f" Error de conexión: {e}")
            raise
    
    def get_csv_path(self, filename):
        """Obtener ruta completa del archivo CSV en carpeta DATASET"""
        return os.path.join(self.dataset_folder, filename)
    
    def run_etl(self):
        """Ejecutar proceso de extracción y carga."""
        print("\n INICIANDO PROCESO DE EXTRACCION Y CARGA")
        
        try:
            # 1. Verificar la carpeta de datos
            if not os.path.exists(self.dataset_folder):
                print(f" Carpeta '{self.dataset_folder}' no encontrada. Creala y coloca los CSV.")
                return
            
            print(f" Leyendo archivos desde: {self.dataset_folder}")
            
            # 2. Conectar a BD
            self.connect_db()
            cursor = self.connection.cursor()
            cursor.fast_executemany = True # Optimización para inserción masiva
            
            # 3. Procesar y Cargar archivos
            print("\n CARGA DE DATOS...")
            
            # Mapeo de archivos CSV a tablas STAGING
            csv_to_staging = {
                'clientes.csv': 'STG_Clientes',
                'productos.csv': 'STG_Productos', 
                'tiendas.csv': 'STG_Tiendas',
                'ventas.csv': 'STG_Ventas',
                'ventas_add.csv': 'STG_Ventas_Add'
            }
            
            for csv_file, table_name in csv_to_staging.items():
                csv_path = self.get_csv_path(csv_file)
                
                if csv_file not in self.column_mapping:
                    print(f" ERROR interno: Falta mapeo de columnas para {csv_file}")
                    continue
                
                expected_columns = self.column_mapping[csv_file]
                
                if os.path.exists(csv_path):
                    
                    # header=0 indica que la primera fila es el encabezado.
                    # names=expected_columns re-nombra las columnas según mapeo STAGING.
                    df = pd.read_csv(csv_path, usecols=expected_columns) # Solo lee las columnas esperadas
                    
                    # Asegura que todas las columnas sean texto y maneja NaN/NULL
                    df = df.astype(str).fillna('')
                    
                    df['Fecha_Carga'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Truncar tabla
                    cursor.execute(f"TRUNCATE TABLE {table_name}")
                    
                    # Insertar datos
                    # Usa TODAS las columnas del DF, incluyendo Fecha_Carga
                    columns = ', '.join(df.columns) 
                    placeholders = ', '.join(['?' for _ in df.columns])
                    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    
                    data_to_insert = [tuple(row) for row in df.values]
                    
                    cursor.executemany(query, data_to_insert)
                    print(f" {csv_file} → {table_name} ({len(df)} registros cargados)")
                else:
                    print(f" {csv_file} no encontrado en {self.dataset_folder}. Saltando.")
            
           
            self.connection.commit()
            print("\n COMPLETADO EXITOSAMENTE!")
            
        except Exception as e:
            print(f" ERROR FATAL en el proceso ETL. Haciendo ROLLBACK: {e}")
            if self.connection:
                self.connection.rollback()
        finally:
            if self.connection:
                self.connection.close()
                print(" Conexión cerrada")




# EJECUCIÓN
if __name__ == "__main__":
    etl = CSVToSQLServer()
    etl.run_etl()
    