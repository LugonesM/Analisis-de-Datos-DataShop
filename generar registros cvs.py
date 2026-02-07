import csv
import random
from datetime import datetime, timedelta
import os
import sys

# Obtener el directorio del script para guardarlos en el mismo directorio
try:
    # sys.argv[0] contiene la ruta del script actual.
    # os.path.abspath lo convierte en una ruta absoluta.
    # os.path.dirname extrae solo la carpeta.
    SCRIPT_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
except:
    # Fallback para entornos donde sys.argv[0] no está disponible
    SCRIPT_DIR = os.getcwd() 


# Rango de fechas
START_DATE = datetime(2020, 1, 1)
END_DATE = datetime(2025, 10, 31)

# Lista de nombres/categorías para simular datos 
PROVINCIAS = ["Buenos Aires", "Córdoba", "Santa Fe", "Mendoza", "Tucumán", "Santiago del Estero"]
LOCALIDADES = ["Centro", "Belgrano", "Alberdi", "Nueva Córdoba", "Godoy Cruz", "Palermo"]
TIPOS_TIENDA = ["Online", "Sucursal Centro", "Outlet", "RetailMall"]
MARCAS = ["Samsung", "LG", "Sony", "Apple", "Lenovo", "Xiaomi", "HP", "Dell", "Bose", "JBL"]
CATEGORIAS = ["Televisores", "Celulares", "Computadoras", "Audio", "Accesorios", "Electrodomésticos"]

# Diccionario de productos para asegurar coherencia de precios
PRODUCTOS_INFO = {
    "P001": ("Televisor OLED 65", "Televisores", "Samsung", 1200.00, 1699.99),
    "P002": ("Smartphone G-Series", "Celulares", "LG", 450.00, 799.99),
    "P003": ("Laptop UltraSlim X", "Computadoras", "Lenovo", 800.00, 1199.99),
    "P004": ("Auriculares Inalámbricos Pro", "Audio", "Sony", 100.00, 149.99),
    "P005": ("Monitor 4K 27 Pulgadas", "Computadoras", "Dell", 350.00, 529.99),
    "P006": ("Smartwatch V2", "Accesorios", "Apple", 200.00, 399.99),
    "P007": ("Televisor LED 55", "Televisores", "Samsung", 400.00, 500.00),
    "P008": ("Tablet Pro 11", "Celulares", "Xiaomi", 300.00, 450.00),
    "P009": ("Cafetera Espresso", "Electrodomésticos", "LG", 150.00, 250.00),
    "P010": ("Parlante Bluetooth", "Audio", "JBL", 50.00, 89.99),
    "P011": ("Refrigerador No Frost 400L", "Electrodomésticos", "Samsung", 700.00, 1050.00),
    "P012": ("Cámara Mirrorless A7", "Cámaras", "Sony", 1500.00, 2200.00),
    "P013": ("Consola de Videojuegos X", "Gaming", "Microsoft", 450.00, 599.99),
    "P014": ("Mouse Ergonómico Inalámbrico", "Accesorios", "Logitech", 25.00, 45.00),
    "P015": ("Aspiradora Robot S9", "Electrodomésticos", "Xiaomi", 380.00, 550.00),
    "P016": ("Router Wi-Fi 6 AX1800", "Computadoras", "TP-Link", 60.00, 95.00),
    "P017": ("Auriculares Diadema Noise Cancelling", "Audio", "Bose", 220.00, 329.00),
    "P018": ("Smart TV 75 Pulgadas QLED", "Televisores", "TCL", 950.00, 1400.00),
    "P019": ("Freidora de Aire Digital", "Electrodomésticos", "Philips", 90.00, 150.00),
    "P020": ("Batería Externa 20000 mAh", "Accesorios", "Anker", 30.00, 55.00),
    "P021": ("Lavadora Carga Frontal 9kg", "Electrodomésticos", "Whirlpool", 550.00, 750.00),
    "P022": ("Smartphone P-Series Lite", "Celulares", "Huawei", 280.00, 420.00),
    "P023": ("Teclado Mecánico RGB", "Computadoras", "Razer", 90.00, 135.00),
    "P024": ("Barra de Sonido Dolby Atmos", "Audio", "Samsung", 350.00, 599.00),
    "P025": ("Impresora Multifunción Láser", "Computadoras", "HP", 180.00, 289.00),
    "P026": ("Drone P-Series Mini", "Cámaras", "DJI", 300.00, 499.00),
    "P027": ("Altavoz Inteligente con Pantalla", "Accesorios", "Google", 75.00, 129.99),
    "P028": ("Licuadora de Alta Potencia", "Electrodomésticos", "Oster", 60.00, 99.00),
    "P029": ("Portátil Gaming 15p", "Computadoras", "Asus", 1100.00, 1599.00),
    "P030": ("Audífonos Deportivos BT", "Audio", "Skullcandy", 40.00, 69.99),
    "P031": ("Secadora de Ropa con Sensor", "Electrodomésticos", "LG", 480.00, 650.00),
    "P032": ("Teléfono Fijo Inalámbrico", "Accesorios", "Panasonic", 20.00, 35.00),
    "P033": ("Monitor Curvo Gaming 34p", "Computadoras", "AOC", 450.00, 680.00),
    "P034": ("Smart TV 85 Pulgadas 8K", "Televisores", "Samsung", 2500.00, 3999.00),
    "P035": ("Proyector Portátil 1080p", "Audio", "Epson", 400.00, 620.00),
    "P036": ("Robot de Cocina Multifunción", "Electrodomésticos", "Taurus", 320.00, 480.00),
    "P037": ("Cargador Rápido USB-C 65W", "Accesorios", "UGREEN", 15.00, 25.00),
    "P038": ("Smartphone Z-Fold", "Celulares", "Samsung", 900.00, 1499.00),
    "P039": ("Tarjeta Gráfica RTX-4070", "Gaming", "NVIDIA", 600.00, 850.00),
    "P040": ("Sistema de Malla WiFi (Mesh)", "Computadoras", "Netgear", 150.00, 249.00),
}


# GENERACIÓN DE DIMENSIONES
def generate_clientes():
    """Genera datos para clientes.csv"""
    clientes = []
    headers = ["CodCliente", "RazonSocial", "Telefono", "Mail", "Direccion", "Localidad", "Provincia", "CP"]
    
    for i in range(1, 181): # 180 clientes
        cod_cliente = f"C{i:03d}"
        razon_social = random.choice(["ACME Corp", "Globex Corporation", "Inversiones Delta", "TecnoSoluciones", "Distribuidora Sur", "Comercio Norte"]) + f" {i}"
        telefono = f"54911{random.randint(40000000, 59999999)}"
        mail = f"contacto_{i}@ejemplo.com"
        provincia = random.choice(PROVINCIAS)
        localidad = random.choice(LOCALIDADES)
        direccion = f"Calle {random.randint(100, 999)}"
        cp = f"{random.randint(1000, 9999)}"
        
        clientes.append({
            "CodCliente": cod_cliente,
            "RazonSocial": razon_social,
            "Telefono": telefono,
            "Mail": mail,
            "Direccion": direccion,
            "Localidad": localidad,
            "Provincia": provincia,
            "CP": cp
        })
    return clientes, headers

def generate_productos():
    """Genera datos para productos.csv"""
    productos = []
    headers = ["CodigoProducto", "Descripcion", "Categoria", "Marca", "PrecioCosto", "PrecioVentaSugerido"]
    
    for cod, (desc, cat, marca, costo, venta_sugerida) in PRODUCTOS_INFO.items():
        productos.append({
            "CodigoProducto": cod,
            "Descripcion": desc,
            "Categoria": cat,
            "Marca": marca,
            "PrecioCosto": f"{costo:.2f}",
            "PrecioVentaSugerido": f"{venta_sugerida:.2f}"
        })
    return productos, headers

def generate_tiendas():
    """Genera datos para tiendas.csv"""
    tiendas = []
    headers = ["CodigoTienda", "Descripcion", "Direccion", "Localidad", "Provincia", "CP", "TipoTienda"]
    
    for i in range(1, 9): # 8 tiendas
        cod_tienda = f"T{i:02d}"
        tipo_tienda = random.choice(TIPOS_TIENDA)
        
        if tipo_tienda == "Online":
            descripcion = f"ElectroShop Online - {cod_tienda}" 
            direccion = "Avenida Internet 123"
            localidad = "Virtual"
            provincia = "Virtual"
            cp = "00000"
        else:
            descripcion = f"Tienda {tipo_tienda.split(' ')[-1]} - SUCURSAL {i}"
            provincia = random.choice(PROVINCIAS)
            localidad = random.choice(LOCALIDADES)
            direccion = f"Calle Ficticia {random.randint(100, 999)}"
            cp = f"{random.randint(1000, 9999)}"

        tiendas.append({
            "CodigoTienda": cod_tienda,
            "Descripcion": descripcion,
            "Direccion": direccion,
            "Localidad": localidad,
            "Provincia": provincia,
            "CP": cp,
            "TipoTienda": tipo_tienda
        })
    return tiendas, headers


# GENERACIÓN DE HECHOS (VENTAS)
def generate_sales_data(clientes, productos, tiendas):
    """Genera todos los registros de ventas para el período completo."""
    sales_data = []
    
    client_map = {c['CodCliente']: c['RazonSocial'] for c in clientes}
    product_map = {p['CodigoProducto']: (p['Descripcion'], float(p['PrecioVentaSugerido'])) for p in productos}
    store_map = {t['CodigoTienda']: t['Descripcion'] for t in tiendas}

    current_date = START_DATE
    while current_date <= END_DATE:
        # Solo días laborables (Lunes=0 a Viernes=4)
        if current_date.weekday() < 5:
            # Número de transacciones por día (variabilidad para patrones)
            num_transactions = random.randint(15, 45) 
            
            for _ in range(num_transactions):
                # Horario de 9hs a 20hs
                hour = random.randint(9, 19) # 9am a 7pm (para terminar antes de las 20hs)
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                
                # Combinar fecha y hora
                fecha_venta = current_date.replace(hour=hour, minute=minute, second=second, microsecond=0)
                
                # Seleccionar dimensiones aleatorias
                cod_cliente = random.choice(list(client_map.keys()))
                cod_producto = random.choice(list(product_map.keys()))
                cod_tienda = random.choice(list(store_map.keys()))
                
                # Obtener detalles del producto
                desc_producto, precio_sugerido = product_map[cod_producto]
                
                # Generar cantidad vendida
                cantidad = random.randint(1, 5) 
                
                # el precio de venta unitario puede variar ligeramente
                precio_unitario_real = precio_sugerido * random.uniform(0.95, 1.05)
                
                # Calcular PrecioVenta
                total_linea_venta = precio_unitario_real * cantidad
                
                sales_data.append({
                    "FechaVenta": fecha_venta.strftime("%Y-%m-%d %H:%M:%S"), 
                    "CodigoProducto": cod_producto,
                    "Producto": desc_producto,
                    "Cantidad": cantidad,
                    "PrecioVenta": f"{total_linea_venta:.2f}",
                    "CodigoCliente": cod_cliente,
                    "Cliente": client_map[cod_cliente],
                    "CodigoTienda": cod_tienda,
                    "Tienda": store_map[cod_tienda]
                })

        # Avanzar al día siguiente
        current_date += timedelta(days=1)
        
    return sales_data

def split_and_save_sales(sales_data):
    """Divide las ventas en ventas.csv y ventas_add.csv y las guarda."""
    headers = [
        "FechaVenta", "CodigoProducto", "Producto", "Cantidad", "PrecioVenta", 
        "CodigoCliente", "Cliente", "CodigoTienda", "Tienda"
    ]
    
    # El mes de octubre de 2025 es el mes adicional (ventas_add.csv)
    split_date = datetime(2025, 10, 1)
    
    ventas_main = []
    ventas_add = []
    
    # Ordena por fecha para asegurar la división correcta
    sales_data.sort(key=lambda x: datetime.strptime(x['FechaVenta'], "%Y-%m-%d %H:%M:%S"))
    
    for sale in sales_data:
        # Extrae solo la fecha para la comparación
        sale_date_obj = datetime.strptime(sale['FechaVenta'].split(' ')[0], "%Y-%m-%d") 
        if sale_date_obj < split_date:
            ventas_main.append(sale)
        else:
            ventas_add.append(sale)

    # Función local para guardar con manejo de errores
    def safe_save(filename, data):
        # **Asegura la ruta absoluta**
        full_path = os.path.join(SCRIPT_DIR, filename) 
        
        try:
            with open(full_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(data)
            print(f" Creado {filename} con {len(data)} registros en: {full_path}")
        except Exception as e:
            print(f" ERROR al guardar el archivo {filename} en {full_path}: {e}", file=sys.stderr)
            print("Asegúrate de tener permisos de escritura en el directorio.", file=sys.stderr)

    safe_save('ventas.csv', ventas_main)
    safe_save('ventas_add.csv', ventas_add)


def save_dimension(data, headers, filename):
    """Función genérica para guardar dimensiones con manejo de errores."""
    # **Asegura la ruta absoluta**
    full_path = os.path.join(SCRIPT_DIR, filename) 

    try:
        with open(full_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
        print(f" Creado {filename} con {len(data)} registros en: {full_path}")
    except Exception as e:
        print(f" ERROR al guardar el archivo {filename} en {full_path}: {e}", file=sys.stderr)
        print("Asegúrate de tener permisos de escritura en el directorio.", file=sys.stderr)



# EJECUCIÓN PRINCIPAL
if __name__ == "__main__":
    print("---------------------------------------------------------")
    print(f"Directorio donde se guardarán los archivos: {SCRIPT_DIR}")
    print("---------------------------------------------------------")
    print(f"Iniciando generación de datos DW para el período: {START_DATE.date()} a {END_DATE.date()}")
    
    # Generar y guardar Dimensiones
    clientes_data, c_headers = generate_clientes()
    save_dimension(clientes_data, c_headers, 'clientes.csv')
    
    productos_data, p_headers = generate_productos()
    save_dimension(productos_data, p_headers, 'productos.csv')
    
    tiendas_data, t_headers = generate_tiendas()
    save_dimension(tiendas_data, t_headers, 'tiendas.csv')
    
    # Generar todos los Hechos
    print("\nGenerando registros de ventas... (Esto puede tardar unos segundos)")
    all_sales_data = generate_sales_data(clientes_data, productos_data, tiendas_data)
    
    # Dividir y guardar Hechos
    print("\nDividiendo y guardando archivos de ventas...")
    split_and_save_sales(all_sales_data)
    
    # Resumen
    total_records = len(all_sales_data)
    expected_files = ['clientes.csv', 'productos.csv', 'tiendas.csv', 'ventas.csv', 'ventas_add.csv']
    print("\n Generación de datasets DW completada.")
    print(f"Total de transacciones generadas: {total_records}")
    print(f"Busca los siguientes archivos en la carpeta del script: {', '.join(expected_files)}")