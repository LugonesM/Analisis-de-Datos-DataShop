import pyodbc
from configparser import ConfigParser
import os

class DWLoader:
    def __init__(self, config_file='config.ini'):
        self.config = ConfigParser()
        self.config.optionxform = str
        
        if not os.path.exists(config_file) or not self.config.read(config_file):
            print(f" ERROR: No se pudo leer el archivo de configuración: {config_file}")
            raise FileNotFoundError(f"Archivo '{config_file}' no encontrado o vacío.")
        
        self.connection = None

    def connect_db(self):
        """Conectar a base SQL Server"""
        try:
            server = self.config.get('DATABASE', 'server').strip()
            database = self.config.get('DATABASE', 'database').strip()
            trusted_connection = self.config.get('DATABASE', 'trusted_connection').strip()
            driver = self.config.get('DATABASE', 'driver', fallback='ODBC Driver 17 for SQL Server').strip()

            if not all([server, database, trusted_connection]):
                raise ValueError("Faltan parámetros críticos en config.ini.")

            print(f" Conectando a: {server} | BD: {database}")

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

    def check_ventas_problematic_data(self):
        """Verificar datos problemáticos en ventas usando stored procedure"""
        cursor = self.connection.cursor()
        
        print(" Verificando datos problemáticos en STG_Ventas...")
        
        try:
            # Llamar al stored procedure para mostrar problemas encontrados
            cursor.execute("EXEC sp_CheckVentasProblematicData")
            stats = cursor.fetchone()
            
            total_problemas = stats[0] or 0
            cantidad_invalida = stats[1] or 0
            precio_invalido = stats[2] or 0
            precio_grande = stats[3] or 0
            fecha_invalida = stats[4] or 0

            
            if total_problemas > 0:
                print(f"  Detalle de problemas:")
                print(f"   - Cantidad inválida: {cantidad_invalida}")
                print(f"   - Precio inválido: {precio_invalido}")
                print(f"   - Precio demasiado grande: {precio_grande}")
                print(f"   - Fecha inválida: {fecha_invalida}")
                
                # Llamar al stored procedure para ejemplos
                cursor.execute("EXEC sp_GetVentasProblematicExamples")
                problematic_rows = cursor.fetchall()
                
                if problematic_rows:
                    print("   Ejemplos de datos problemáticos:")
                    for row in problematic_rows:
                        print(f"     - {row.CodigoProducto}: Fecha='{row.FechaVenta}', Cantidad='{row.Cantidad}', Precio='{row.PrecioVenta}' ({row.Tipo_Problema})")
                else:
                    print("    No se encontraron ejemplos específicos")
            else:
                print(" No se encontraron datos problemáticos en ventas")
            
            return total_problemas
            
        except Exception as e:
            print(f" Error al verificar datos problemáticos: {e}")
            # Fallback a consulta directa si el SP no existe
            return self._check_ventas_problematic_data_fallback()

    def _check_ventas_problematic_data_fallback(self):
        """Fallback si los stored procedures no existen"""
        cursor = self.connection.cursor()
        
        print("  Usando fallback para verificación de datos...")
        
        cursor.execute("""
            SELECT 
                SUM(CASE 
                    WHEN TRY_CONVERT(INT, Cantidad) IS NULL 
                    OR TRY_CONVERT(DECIMAL(30,10), PrecioVenta) IS NULL 
                    OR TRY_CONVERT(DECIMAL(30,10), PrecioVenta) > 99999999999999.99 
                    OR TRY_CONVERT(DATE, FechaVenta) IS NULL
                    THEN 1 ELSE 0 
                END) as Total_Problemas
            FROM (
                SELECT * FROM STG_Ventas
                UNION ALL 
                SELECT * FROM STG_Ventas_Add
            ) AS Ventas
        """)
        
        result = cursor.fetchone()
        return result[0] or 0

    def check_stored_procedures_exist(self):
        """Verificar que los stored procedures existen """
        cursor = self.connection.cursor()
        
        expected_procedures = [
            "sp_Cargar_INT_Clientes",
            "sp_Cargar_INT_Productos", 
            "sp_Cargar_INT_Tiendas",
            "sp_Cargar_INT_Ventas",
            "sp_CheckVentasProblematicData",  
            "sp_GetVentasProblematicExamples",
            "sp_CheckStoredProceduresExist"
        ]
        
        print(" Verificando stored procedures...")
        
        missing_procedures = []
        
        for sp in expected_procedures:
            try:
                cursor.execute("EXEC sp_CheckStoredProceduresExist @ProcedureName = ?", sp)
                result = cursor.fetchone()
                exists = result[0] > 0 if result else False
            except:
                # Fallback si el SP de verificación no existe
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.ROUTINES 
                    WHERE ROUTINE_NAME = ? AND ROUTINE_TYPE = 'PROCEDURE'
                """, sp)
                exists = cursor.fetchone()[0] > 0
            
            status = " EXISTE" if exists else " NO EXISTE"
            print(f"   {sp}: {status}")
            
            if not exists and sp.startswith('sp_Cargar_INT_'):
                missing_procedures.append(sp)
        
        if missing_procedures:
            raise Exception(f"Stored procedures faltantes: {', '.join(missing_procedures)}")
        
        print(" Todos los stored procedures verificados")

    def run_dw_procedures(self):
        """Ejecutar stored procedures con manejo individual de errores"""
        cursor = self.connection.cursor()

        stored_procedures = [
            "sp_Cargar_INT_Clientes",
            "sp_Cargar_INT_Productos",
            "sp_Cargar_INT_Tiendas", 
            "sp_Cargar_INT_Ventas"
        ]

        print("\n Ejecutando carga del Data Warehouse...\n")

        successful_procedures = 0
        
        for sp in stored_procedures:
            try:
                print(f" Ejecutando: {sp} ...")
                cursor.execute(f"EXEC {sp}")
                
                # Capturar mensajes de PRINT de SQL
                while cursor.nextset():
                    pass
                    
                print(f"    {sp} ejecutado correctamente\n")
                successful_procedures += 1
                
            except Exception as e:
                print(f"    ERROR en {sp}: {e}")
                
                # Preguntar si continuar
                if sp != stored_procedures[-1]:
                    continue_anyway = input("   ¿Continuar con los siguientes procedures? (s/n): ").strip().lower()
                    if continue_anyway != 's':
                        raise
                else:
                    print(f"     Último procedure falló, continuando...")
        
        print(f" {successful_procedures}/{len(stored_procedures)} SP ejecutados exitosamente")

    def run(self):
        try:
            self.connect_db()
            
            # Verificar procedures
            self.check_stored_procedures_exist()
            
            # Verificar datos problemáticos en ventas
            problem_count = self.check_ventas_problematic_data()
            
            if problem_count > 0:
                user_input = input(f"  Se encontraron {problem_count} problemas. ¿Continuar con la carga? (s/n): ").strip().lower()
                if user_input != 's':
                    print(" Carga cancelada por el usuario")
                    return
            
            # Ejecutar procedures
            self.run_dw_procedures()

            self.connection.commit()
            print("\n CARGA DE TABLAS INT COMPLETADA!")

        except Exception as e:
            print(f" ERROR FATAL: {e}. Realizando rollback.")
            if self.connection:
                self.connection.rollback()

        finally:
            if self.connection:
                self.connection.close()
                print(" Conexión cerrada.")


# EJECUCIÓN
if __name__ == "__main__":
    loader = DWLoader()
    loader.run()