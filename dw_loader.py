import pyodbc
from configparser import ConfigParser
import os

class ELTDataWarehouseLoader:
    """
    Clase para la orquestación y ejecución de Stored Procedures
    en SQL Server para cargar el Data Warehouse (INT -> DIM/FACT).
    """
    
    
    def __init__(self, config_file='config.ini'):
        self.config = ConfigParser()
        self.config.optionxform = str 
        
        # Validar y leer el archivo de configuración
        if not os.path.exists(config_file) or not self.config.read(config_file):
            raise FileNotFoundError(f" Error: Archivo '{config_file}' no encontrado.")
            
        self.connection = None
        
        # Secuencia de ejecución: 
        # 1. Cargar Dimensiones (para que las IDs estén disponibles).
        # 2. Cargar Hechos (Fact) usando las IDs de las dimensiones.
        self.execution_sequence = [
            'dbo.sp_Dim_CargarCliente',
            'dbo.sp_Dim_CargarProducto',
            'dbo.sp_Dim_CargarTienda',
            'dbo.sp_Fact_CargarVentas' 
        ]

    def connect_db(self):
        """Conectar a base de datos SQL."""
        try:
            # Lectura de la configuración 
            server = self.config.get('DATABASE', 'server').strip()
            database = self.config.get('DATABASE', 'database').strip()
            trusted_connection = self.config.get('DATABASE', 'trusted_connection').strip()
            driver = self.config.get('DATABASE', 'driver').strip()
            
            connection_string = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"Trusted_Connection={trusted_connection};"
            )
            
            self.connection = pyodbc.connect(connection_string, timeout=10)
            self.connection.autocommit = False 
            print(" Conexión a BD establecida.")
            
        except Exception as e:
            print(f" Error de conexión: {e}")
            # Re-lanzar la excepción para detener la ejecución si no hay conexión
            raise

    def execute_stored_procedure(self, sp_name):
        """Ejecuta un SP y maneja la transacción (commit/rollback)."""
        if not self.connection:
            print(" La conexión a la BD no está establecida.")
            return

        print(f"Ejecutando SP: {sp_name}...")
        try:
            cursor = self.connection.cursor()
            # Ejecuta el Stored Procedure
            cursor.execute(f"EXEC {sp_name}") 
            
            # Commit la transacción después de una ejecución exitosa
            self.connection.commit()
            print(f"SP {sp_name} ejecutado y transacción confirmada.")
            
        except pyodbc.Error as ex:
            # Rollback la transacción en caso de error SQL
            self.connection.rollback()
            print(f"Error SQL al ejecutar {sp_name}: {ex}")
            raise
        except Exception as e:
            # Rollback en caso de error Python o general
            self.connection.rollback()
            print(f"Error general al ejecutar {sp_name}: {e}")
            raise

    def run_load_dw(self):
        """Ejecuta la secuencia completa de carga del Data Warehouse."""
        print("INICIANDO CARGA DE DATOS AL DATA WAREHOUSE")
        
        try:
            self.connect_db()

            for sp in self.execution_sequence:
                self.execute_stored_procedure(sp)

            print("\n CARGA DEL DATA WAREHOUSE COMPLETADA EXITOSAMENTE!")
            
        except Exception as e:
            print(f" El proceso de carga terminó con un error. Verifique logs.")
        finally:
            if self.connection:
                self.connection.close()
                print(" Conexión a BD cerrada.")

# EJECUCIÓN
if __name__ == "__main__":
   
    loader = ELTDataWarehouseLoader()
    loader.run_load_dw()