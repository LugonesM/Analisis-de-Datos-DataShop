-- TABLAS INT

-- TABLA INT: CLIENTES
-- Destino: Dim_Cliente
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'INT_Cliente')
BEGIN
    CREATE TABLE INT_Cliente (  
        CodCliente VARCHAR(100) PRIMARY KEY NOT NULL,
        RazonSocial VARCHAR(300) NOT NULL,
        Telefono VARCHAR(100) NULL,
        Mail VARCHAR(300) NULL,
        Direccion VARCHAR(300) NULL,
        Localidad VARCHAR(150) NOT NULL,
        Provincia VARCHAR(150) NOT NULL,
        CP VARCHAR(20) NULL,
        FechaCreacion DATETIME NOT NULL
    );
    PRINT ' Tabla INT_Cliente creada exitosamente.';
END
ELSE
    PRINT '  Tabla INT_Cliente ya existe.';
GO


-- TABLA INT: PRODUCTOS
-- Destino: Dim_Producto
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'INT_Producto')
BEGIN
    CREATE TABLE INT_Producto (
        CodigoProducto VARCHAR(100) PRIMARY KEY NOT NULL, 
        Descripcion VARCHAR(300) NOT NULL,
        Categoria VARCHAR(150) NOT NULL,
        Marca VARCHAR(150) NOT NULL,
        PrecioCosto DECIMAL(18,2) NOT NULL, 
        PrecioVentaSugerido DECIMAL(18,2) NOT NULL,
        FechaCreacion DATETIME NOT NULL
    );
    PRINT ' Tabla INT_Producto creada exitosamente.';
END
ELSE
    PRINT '  Tabla INT_Producto ya existe.';
GO


-- TABLA INT: TIENDAS
-- Destino: Dim_Tienda
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'INT_Tienda')
BEGIN
    CREATE TABLE INT_Tienda (
        CodigoTienda VARCHAR(100) PRIMARY KEY NOT NULL,
        Descripcion VARCHAR(300) NOT NULL,
        Direccion VARCHAR(300) NULL,
        Localidad VARCHAR(150) NOT NULL,
        Provincia VARCHAR(150) NOT NULL,
        CP VARCHAR(20) NULL,
        TipoTienda VARCHAR(100) NOT NULL,
        FechaCreacion DATETIME NOT NULL
    );
    PRINT ' Tabla INT_Tienda creada exitosamente.';
END
ELSE
    PRINT '  Tabla INT_Tienda ya existe.';
GO


-- TABLA INT: VENTAS
-- Destino: Fact_Ventas
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'INT_Ventas')
BEGIN
    CREATE TABLE INT_Ventas (
        FechaVenta DATE NOT NULL,
        CodigoProducto VARCHAR(100) NOT NULL,
        CodigoCliente VARCHAR(100) NOT NULL,
        CodigoTienda VARCHAR(100) NOT NULL,

        Cantidad INT NOT NULL, 
        PrecioVenta DECIMAL(18,2) NOT NULL,
        Total_IVA DECIMAL(18,2) NOT NULL,

        PRIMARY KEY (
            FechaVenta, 
            CodigoProducto, 
            CodigoCliente, 
            CodigoTienda, 
            Cantidad, 
            PrecioVenta
        ),

        FechaCarga DATETIME NOT NULL
    );
    PRINT ' Tabla INT_Ventas creada exitosamente.';
END
ELSE
    PRINT '  Tabla INT_Ventas ya existe.';
GO

PRINT ' Verificación de tablas INT completada.';

