-- STAGING: Clientes
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'STG_Clientes')
BEGIN
    CREATE TABLE STG_Clientes (
        CodCliente VARCHAR(100),
        RazonSocial VARCHAR(500),
        Telefono VARCHAR(100),
        Mail VARCHAR(500),
        Direccion VARCHAR(500),
        Localidad VARCHAR(200),
        Provincia VARCHAR(200),
        CP VARCHAR(50),
        Fecha_Carga DATETIME DEFAULT GETDATE()
    );
    PRINT ' Tabla STG_Clientes creada exitosamente.';
END
ELSE
    PRINT '  Tabla STG_Clientes ya existe.';
GO

-- STAGING: Productos
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'STG_Productos')
BEGIN
    CREATE TABLE STG_Productos (
        CodigoProducto VARCHAR(100),
        Descripcion VARCHAR(500),
        Categoria VARCHAR(200),
        Marca VARCHAR(200),
        PrecioCosto VARCHAR(100),  -- Se guarda como texto inicialmente
        PrecioVentaSugerido VARCHAR(100),
        Fecha_Carga DATETIME DEFAULT GETDATE()
    );
    PRINT ' Tabla STG_Productos creada exitosamente.';
END
ELSE
    PRINT '  Tabla STG_Productos ya existe.';
GO

-- STAGING: Tiendas
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'STG_Tiendas')
BEGIN
    CREATE TABLE STG_Tiendas (
        CodigoTienda VARCHAR(100),
        Descripcion VARCHAR(500),
        Direccion VARCHAR(500),
        Localidad VARCHAR(500),
        Provincia VARCHAR(200),
        CP VARCHAR(50),
        TipoTienda VARCHAR(100),
        Fecha_Carga DATETIME DEFAULT GETDATE()
    );
    PRINT ' Tabla STG_Tiendas creada exitosamente.';
END
ELSE
    PRINT '  Tabla STG_Tiendas ya existe.';
GO

-- STAGING: Ventas
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'STG_Ventas')
BEGIN
    CREATE TABLE STG_Ventas (
        FechaVenta VARCHAR(100),  -- Texto para manejar diferentes formatos
        CodigoProducto VARCHAR(100),
        Producto VARCHAR(500),   -- Descripción redundante
        Cantidad VARCHAR(50),    -- Texto para manejar posibles errores
        PrecioVenta VARCHAR(100), -- Texto para manejar decimales/formato
        CodigoCliente VARCHAR(100),
        Cliente VARCHAR(500),    -- Descripción redundante
        CodigoTienda VARCHAR(100),
        Tienda VARCHAR(500),     -- Descripción redundante
        Fecha_Carga DATETIME DEFAULT GETDATE()
    );
    PRINT ' Tabla STG_Ventas creada exitosamente.';
END
ELSE
    PRINT ' Tabla STG_Ventas ya existe.';
GO

-- STAGING: Ventas_Add (estructura idéntica a Ventas)
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'STG_Ventas_Add')
BEGIN
    CREATE TABLE STG_Ventas_Add (
        FechaVenta VARCHAR(100),
        CodigoProducto VARCHAR(100),
        Producto VARCHAR(500),
        Cantidad VARCHAR(50),
        PrecioVenta VARCHAR(100),
        CodigoCliente VARCHAR(100),
        Cliente VARCHAR(500),
        CodigoTienda VARCHAR(100),
        Tienda VARCHAR(500),
        Fecha_Carga DATETIME DEFAULT GETDATE()
    );
    PRINT ' Tabla STG_Ventas_Add creada exitosamente.';
END
ELSE
    PRINT '  Tabla STG_Ventas_Add ya existe.';
GO

PRINT ' Verificación de tablas STAGING completada.';
