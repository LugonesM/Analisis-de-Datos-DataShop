-- SCRIPT DEL DATA WAREHOUSE
SET NOCOUNT ON;
SET XACT_ABORT ON; 

-- Verificar y crear la base de datos si no existe
IF NOT EXISTS(SELECT name FROM sys.databases WHERE name = 'DW_DataShop1')
BEGIN
    CREATE DATABASE DW_DataShop1;
    PRINT 'Base de datos DW_DataShop1 creada exitosamente';
END
ELSE
    PRINT 'Base de datos DW_DataShop1 ya existe';
GO

USE DW_DataShop1;
GO

PRINT '1. Iniciando eliminación de objetos existentes...';
GO

-- Eliminación en orden correcto considerando dependencias
IF OBJECT_ID('Fact_Ventas') IS NOT NULL
BEGIN
    -- Eliminar FKs primero
    WHILE EXISTS (SELECT * FROM sys.foreign_keys WHERE parent_object_id = OBJECT_ID('Fact_Ventas'))
    BEGIN
        DECLARE @fk_name NVARCHAR(128);
        SELECT TOP 1 @fk_name = name FROM sys.foreign_keys WHERE parent_object_id = OBJECT_ID('Fact_Ventas');
        EXEC('ALTER TABLE Fact_Ventas DROP CONSTRAINT ' + @fk_name);
    END
    DROP TABLE Fact_Ventas;
    PRINT 'Tabla Fact_Ventas eliminada.';
END
GO

-- Eliminar procedures
IF OBJECT_ID('[dbo].[Sp_Genera_Dim_Tiempo]') IS NOT NULL
BEGIN
    DROP PROCEDURE [dbo].[Sp_Genera_Dim_Tiempo];
    PRINT 'Stored Procedure Sp_Genera_Dim_Tiempo eliminado.';
END
GO

IF OBJECT_ID('[dbo].[usp_Poblar_Dimension_Tiempo]') IS NOT NULL
BEGIN
    DROP PROCEDURE [dbo].[usp_Poblar_Dimension_Tiempo];
    PRINT 'Stored Procedure usp_Poblar_Dimension_Tiempo eliminado.';
END
GO

-- Eliminar dimensiones (sin dependencias entre ellas)
DECLARE @TablesToDrop TABLE (TableName NVARCHAR(128));
INSERT INTO @TablesToDrop VALUES 
('Dim_Tiempo'), ('Dim_Producto'), ('Dim_Cliente'), ('Dim_Tienda');

DECLARE @TableName NVARCHAR(128);
DECLARE table_cursor CURSOR FOR SELECT TableName FROM @TablesToDrop;

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID(@TableName) IS NOT NULL
    BEGIN
        EXEC('DROP TABLE ' + @TableName);
        PRINT 'Tabla ' + @TableName + ' eliminada.';
    END
    FETCH NEXT FROM table_cursor INTO @TableName;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;
GO

-- PASO 2: CREACIÓN DE TABLAS DE DIMENSIÓN
PRINT '2. Creando tablas de dimensión...';
GO

-- Dimensión TIEMPO
CREATE TABLE Dim_Tiempo (
    Tiempo_Key smalldatetime PRIMARY KEY,
    Anio INT NOT NULL,
    Mes INT NOT NULL,
    Mes_Nombre VARCHAR(20) NOT NULL,
    Semestre INT NOT NULL,
    Trimestre INT NOT NULL,
    Semana_Anio INT NOT NULL,
    Semana_Nro_Mes INT NOT NULL,
    Dia INT NOT NULL,
    Dia_Nombre VARCHAR(20) NOT NULL,
    Dia_Semana_Nro INT NOT NULL 
);

CREATE INDEX IDX_AnioMes ON Dim_Tiempo (Anio, Mes);
PRINT 'Tabla Dim_Tiempo creada con Tiempo_Key (smalldatetime) como PK.';
GO

-- Dimensión PRODUCTO
CREATE TABLE Dim_Producto (
    ID_Producto INT PRIMARY KEY IDENTITY(1,1),
    CodigoProducto VARCHAR(100) NOT NULL UNIQUE,
    Descripcion VARCHAR(255) NOT NULL,
    Categoria VARCHAR(100) NOT NULL,
    Marca VARCHAR(100) NOT NULL,
    PrecioCosto DECIMAL(18,2) NOT NULL DEFAULT 0,
    PrecioVentaSugerido DECIMAL(18,2) NOT NULL DEFAULT 0,
    FechaCreacion DATETIME DEFAULT GETDATE()
);

CREATE INDEX IDX_CodigoProducto ON Dim_Producto (CodigoProducto);
PRINT 'Tabla Dim_Producto creada.';
GO

-- Dimensión CLIENTE
CREATE TABLE Dim_Cliente (
    ID_Cliente INT PRIMARY KEY IDENTITY(1,1),
    CodCliente VARCHAR(50) NOT NULL UNIQUE,
    RazonSocial VARCHAR(255) NOT NULL,
    Telefono VARCHAR(50) NULL,
    Mail VARCHAR(255) NULL,
    Direccion VARCHAR(255) NULL,
    Localidad VARCHAR(100) NOT NULL,
    Provincia VARCHAR(100) NOT NULL,
    CP VARCHAR(20) NULL,
    FechaCreacion DATETIME DEFAULT GETDATE()
);

CREATE INDEX IDX_CodCliente ON Dim_Cliente (CodCliente);
PRINT 'Tabla Dim_Cliente creada.';
GO

-- Dimensión TIENDA
CREATE TABLE Dim_Tienda (
    ID_Tienda INT PRIMARY KEY IDENTITY(1,1),
    CodigoTienda VARCHAR(50) NOT NULL UNIQUE,
    Descripcion VARCHAR(255) NOT NULL,
    Direccion VARCHAR(255) NULL,
    Localidad VARCHAR(100) NOT NULL,
    Provincia VARCHAR(100) NOT NULL,
    CP VARCHAR(20) NULL,
    TipoTienda VARCHAR(50) NOT NULL,
    FechaCreacion DATETIME DEFAULT GETDATE()
);

CREATE INDEX IDX_CodigoTienda ON Dim_Tienda (CodigoTienda);
PRINT 'Tabla Dim_Tienda creada.';
GO

-- PASO 3: CREACIÓN DE LA TABLA DE HECHOS
PRINT '3. Creando Fact_Ventas...';
GO

CREATE TABLE Fact_Ventas (
    ID_Venta BIGINT PRIMARY KEY IDENTITY(1,1),
    Tiempo_Key smalldatetime NOT NULL,
    ID_Producto INT NOT NULL,
    ID_Cliente INT NOT NULL,
    ID_Tienda INT NOT NULL,
    Cantidad INT NOT NULL CHECK (Cantidad > 0),
    PrecioVenta DECIMAL(18,2) NOT NULL CHECK (PrecioVenta >= 0),
    Total_IVA DECIMAL(18,2) NOT NULL,
    FechaCarga DATETIME DEFAULT GETDATE(),

    FOREIGN KEY (Tiempo_Key) REFERENCES Dim_Tiempo(Tiempo_Key),
    FOREIGN KEY (ID_Producto) REFERENCES Dim_Producto(ID_Producto),
    FOREIGN KEY (ID_Cliente) REFERENCES Dim_Cliente(ID_Cliente),
    FOREIGN KEY (ID_Tienda) REFERENCES Dim_Tienda(ID_Tienda)
);

CREATE INDEX IDX_Fecha ON Fact_Ventas (Tiempo_Key);
CREATE INDEX IDX_Producto ON Fact_Ventas (ID_Producto);
CREATE INDEX IDX_Cliente ON Fact_Ventas (ID_Cliente);
CREATE INDEX IDX_Tienda ON Fact_Ventas (ID_Tienda);
PRINT 'Tabla Fact_Ventas creada.';
GO

-- PASO 4: CREACIÓN DEL STORED PROCEDURE DIM TIEMPO
PRINT '4. Creando Stored Procedure Sp_Genera_Dim_Tiempo...';
GO

IF OBJECT_ID('[dbo].[Sp_Genera_Dim_Tiempo]') IS NOT NULL
    DROP PROCEDURE [dbo].[Sp_Genera_Dim_Tiempo];
GO

CREATE PROCEDURE [dbo].[Sp_Genera_Dim_Tiempo]
    @anio INT
AS
BEGIN
SET NOCOUNT ON;
SET ARITHABORT OFF;
SET ARITHIGNORE ON;
SET DATEFIRST 1;
SET DATEFORMAT mdy;
/**************/
/* Variables */
/**************/
DECLARE @dia smallint;
DECLARE @mes smallint;
DECLARE @f_txt varchar(10);
DECLARE @fecha smalldatetime;
DECLARE @key int;
DECLARE @vacio smallint;
DECLARE @fin smallint;
DECLARE @fin_mes int;
DECLARE @anioperiodicidad int;
SELECT @dia = 1, @mes = 1;
SELECT @f_txt = Convert(char(2), @mes) + '/' + Convert(char(2), @dia) + '/' +
Convert(char(4), @anio);
SELECT @fecha = Convert(smalldatetime, @f_txt);
SELECT @anioperiodicidad = @anio;

/************************************/
/* Se chequea que el año a procesar */
/* no exista en la tabla TIME */
/************************************/
IF (SELECT Count(*) FROM Dim_Tiempo WHERE anio = @anio) > 0
BEGIN
PRINT 'El año que ingresó ya existe en la tabla Dim_Tiempo.';
PRINT 'Procedimiento CANCELADO.................';
RETURN 0;
END;
/*************************/
/* Se inserta día a día */
/* hasta terminar el año */
/*************************/
SELECT @fin = @anio + 1;
WHILE (Year(@fecha) < @fin)
BEGIN
--Armo la fecha
SET @f_txt = Convert(char(4), Datepart(yyyy, @fecha)) +
RIGHT('0' + Convert(varchar(2), Datepart(mm, @fecha)), 2) +
RIGHT('0' + Convert(varchar(2), Datepart(dd, @fecha)), 2);
--Calculo el último día del mes
SET @fin_mes = Day(Dateadd(d, -1, Dateadd(m, 1, Dateadd(d, - Day(@fecha)
+ 1, @fecha))));
--Inserto el registro
INSERT Dim_Tiempo (Tiempo_Key, Anio, Mes, Mes_Nombre, Semestre,
Trimestre, Semana_Anio ,Semana_Nro_Mes, Dia, Dia_Nombre,
Dia_Semana_Nro)
SELECT
tiempo_key = @fecha,
anio = Datepart(yyyy, @fecha),
mes = Datepart(mm, @fecha),

mes_nombre = CASE Datename(mm, @fecha)
WHEN 'January' THEN 'Enero'
WHEN 'February' THEN 'Febrero'
WHEN 'March' THEN 'Marzo'
WHEN 'April' THEN 'Abril'
WHEN 'May' THEN 'Mayo'
WHEN 'June' THEN 'Junio'
WHEN 'July' THEN 'Julio'
WHEN 'August' THEN 'Agosto'
WHEN 'September' THEN 'Septiembre'
WHEN 'October' THEN 'Octubre'
WHEN 'November' THEN 'Noviembre'
WHEN 'December' THEN 'Diciembre'
ELSE Datename(mm, @fecha)
END,
semestre = CASE WHEN Datepart(mm, @fecha) BETWEEN 1 AND 6 THEN 1 ELSE
2 END,
trimestre = Datepart(qq, @fecha),
semana_anio = Datepart(wk, @fecha),
semana_nro_mes = Datepart(wk, @fecha) - Datepart(week, Dateadd(dd,
-Day(@fecha)+1, @fecha)) + 1,
dia = Datepart(dd, @fecha),
dia_nombre = CASE Datename(dw, @fecha)
WHEN 'Monday' THEN 'Lunes'
WHEN 'Tuesday' THEN 'Martes'
WHEN 'Wednesday' THEN 'Miércoles'
WHEN 'Thursday' THEN 'Jueves'
WHEN 'Friday' THEN 'Viernes'
WHEN 'Saturday' THEN 'Sábado'
WHEN 'Sunday' THEN 'Domingo'
ELSE Datename(dw, @fecha)
END,
dia_semana_nro = Datepart(dw, @fecha);
-- Incremento la fecha
SELECT @fecha = Dateadd(dd, 1, @fecha);
END;
END;
GO

-- PASO 5: EJECUCIÓN CON MANEJO DE ERRORES
PRINT '5. Ejecutando procedimiento para poblar Dim_Tiempo (2020 a 2025)...';
GO

DECLARE @Years TABLE (YearValue INT);

-- Inserción de todos los años requeridos para la carga de Fact_Ventas
INSERT INTO @Years VALUES (2020), (2021), (2022), (2023), (2024), (2025);

DECLARE @CurrentYear INT;
DECLARE year_cursor CURSOR FOR SELECT YearValue FROM @Years;

OPEN year_cursor;
FETCH NEXT FROM year_cursor INTO @CurrentYear;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        EXEC dbo.Sp_Genera_Dim_Tiempo @anio = @CurrentYear;
    END TRY
    BEGIN CATCH
        PRINT 'Error procesando año ' + CAST(@CurrentYear AS VARCHAR) + ': ' + ERROR_MESSAGE();
    END CATCH
    
    FETCH NEXT FROM year_cursor INTO @CurrentYear;
END

CLOSE year_cursor;
DEALLOCATE year_cursor;
GO

PRINT '6. Dim_Tiempo poblada exitosamente para el rango 2020-2025.';