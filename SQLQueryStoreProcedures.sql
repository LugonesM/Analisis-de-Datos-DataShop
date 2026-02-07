CREATE OR ALTER PROCEDURE sp_Cargar_INT_Clientes
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRAN;

        MERGE INT_Cliente AS T
        USING (
            SELECT
                CodCliente,
                RazonSocial,
                Telefono,
                Mail,
                Direccion,
                Localidad,
                Provincia,
                CP,
                GETDATE() AS FechaCreacion
            FROM STG_Clientes
        ) AS S
        ON S.CodCliente = T.CodCliente

        WHEN MATCHED THEN
            UPDATE SET 
                T.RazonSocial = S.RazonSocial,
                T.Telefono = S.Telefono,
                T.Mail = S.Mail,
                T.Direccion = S.Direccion,
                T.Localidad = S.Localidad,
                T.Provincia = S.Provincia,
                T.CP = S.CP

        WHEN NOT MATCHED THEN
            INSERT (CodCliente, RazonSocial, Telefono, Mail, Direccion, Localidad, Provincia, CP, FechaCreacion)
            VALUES (S.CodCliente, S.RazonSocial, S.Telefono, S.Mail, S.Direccion, S.Localidad, S.Provincia, S.CP, S.FechaCreacion);

        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        THROW;
    END CATCH
END;
GO

CREATE OR ALTER PROCEDURE sp_Cargar_INT_Productos
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRAN;

        MERGE INT_Producto AS T
        USING (
            SELECT
                CodigoProducto,
                Descripcion,
                Categoria,
                Marca,
                COALESCE(TRY_CONVERT(DECIMAL(18,2), NULLIF(PrecioCosto, '')), 0.00) AS PrecioCosto,
                COALESCE(TRY_CONVERT(DECIMAL(18,2), NULLIF(PrecioVentaSugerido, '')), 0.00) AS PrecioVentaSugerido,
                GETDATE() AS FechaCreacion
            FROM STG_Productos
            WHERE CodigoProducto IS NOT NULL 
                AND CodigoProducto != ''
                AND Descripcion IS NOT NULL
                AND Descripcion != ''
        ) AS S
        ON T.CodigoProducto = S.CodigoProducto

        WHEN MATCHED THEN 
            UPDATE SET 
                T.Descripcion = S.Descripcion,
                T.Categoria = S.Categoria,
                T.Marca = S.Marca,
                T.PrecioCosto = S.PrecioCosto,
                T.PrecioVentaSugerido = S.PrecioVentaSugerido

        WHEN NOT MATCHED THEN
            INSERT (CodigoProducto, Descripcion, Categoria, Marca, PrecioCosto, PrecioVentaSugerido, FechaCreacion)
            VALUES (S.CodigoProducto, S.Descripcion, S.Categoria, S.Marca, S.PrecioCosto, S.PrecioVentaSugerido, S.FechaCreacion);

        COMMIT;
        
        DECLARE @inserted INT = @@ROWCOUNT;
        PRINT ' INT_Producto: ' + CAST(@inserted AS VARCHAR) + ' registros procesados';
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        PRINT ' Error en sp_Cargar_INT_Productos: ' + @ErrorMessage;
        THROW;
    END CATCH
END;
GO

CREATE OR ALTER PROCEDURE sp_Cargar_INT_Tiendas
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRAN;

        MERGE INT_Tienda AS T
        USING (
            SELECT
                CodigoTienda,
                Descripcion,
                Direccion,
                Localidad,
                Provincia,
                CP,
                TipoTienda,
                GETDATE() AS FechaCreacion
            FROM STG_Tiendas
        ) AS S
        ON T.CodigoTienda = S.CodigoTienda

        WHEN MATCHED THEN
            UPDATE SET 
                T.Descripcion = S.Descripcion,
                T.Direccion = S.Direccion,
                T.Localidad = S.Localidad,
                T.Provincia = S.Provincia,
                T.CP = S.CP,
                T.TipoTienda = S.TipoTienda

        WHEN NOT MATCHED THEN
            INSERT (CodigoTienda, Descripcion, Direccion, Localidad, Provincia, CP, TipoTienda, FechaCreacion)
            VALUES (S.CodigoTienda, S.Descripcion, S.Direccion, S.Localidad, S.Provincia, S.CP, S.TipoTienda, S.FechaCreacion);

        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        THROW;
    END CATCH
END;
GO

CREATE OR ALTER PROCEDURE sp_Cargar_INT_Ventas
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRAN;

        ;WITH STG AS (
            SELECT * FROM STG_Ventas
            UNION ALL
            SELECT * FROM STG_Ventas_Add
        ),
        TRANSFORM AS (
            SELECT
                TRY_CONVERT(DATE, FechaVenta) AS FechaVenta,
                CodigoProducto,
                CodigoCliente,
                CodigoTienda,
                CASE 
                    WHEN TRY_CONVERT(INT, Cantidad) BETWEEN -2147483648 AND 2147483647 
                    THEN TRY_CONVERT(INT, Cantidad)
                    ELSE NULL 
                END AS Cantidad,
                CASE 
                    WHEN TRY_CONVERT(DECIMAL(30,10), PrecioVenta) BETWEEN -99999999999999.99 AND 99999999999999.99
                    THEN TRY_CONVERT(DECIMAL(18,2), TRY_CONVERT(DECIMAL(30,10), PrecioVenta))
                    ELSE NULL 
                END AS PrecioVenta,
                GETDATE() AS FechaCarga
            FROM STG
            WHERE 
                TRY_CONVERT(DATE, FechaVenta) IS NOT NULL
        )
        MERGE INT_Ventas AS T
        USING TRANSFORM AS S
        ON T.FechaVenta = S.FechaVenta
        AND T.CodigoProducto = S.CodigoProducto
        AND T.CodigoCliente = S.CodigoCliente
        AND T.CodigoTienda = S.CodigoTienda
        AND T.Cantidad = S.Cantidad
        AND T.PrecioVenta = S.PrecioVenta

        WHEN NOT MATCHED AND S.Cantidad IS NOT NULL AND S.PrecioVenta IS NOT NULL THEN
            INSERT (
                FechaVenta, CodigoProducto, CodigoCliente, CodigoTienda,
                Cantidad, PrecioVenta, Total_IVA, FechaCarga
            )
            VALUES (
                S.FechaVenta, S.CodigoProducto, S.CodigoCliente, S.CodigoTienda,
                S.Cantidad, S.PrecioVenta, 
                CASE 
                    WHEN S.PrecioVenta BETWEEN -99999999999999.99 AND 99999999999999.99
                    THEN S.PrecioVenta * 1.21
                    ELSE NULL 
                END,
                S.FechaCarga
            );

        DECLARE @inserted INT = @@ROWCOUNT;
        DECLARE @filtered_out INT;
        
        SELECT @filtered_out = COUNT(*)
        FROM (
            SELECT * FROM STG_Ventas
            UNION ALL SELECT * FROM STG_Ventas_Add
        ) s
        WHERE TRY_CONVERT(DATE, FechaVenta) IS NULL 
           OR TRY_CONVERT(INT, Cantidad) IS NULL 
           OR TRY_CONVERT(DECIMAL(18,2), PrecioVenta) IS NULL;

        PRINT ' INT_Ventas: ' + CAST(@inserted AS VARCHAR) + ' registros insertados';
        PRINT '  INT_Ventas: ' + CAST(@filtered_out AS VARCHAR) + ' registros filtrados (datos inválidos)';

        COMMIT;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        PRINT ' Error en sp_Cargar_INT_Ventas: ' + @ErrorMessage;
        THROW;
    END CATCH
END;
GO

-- SP para verificar datos problemáticos en ventas
CREATE OR ALTER PROCEDURE sp_CheckVentasProblematicData
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        SUM(CASE 
            WHEN TRY_CONVERT(INT, Cantidad) IS NULL 
            OR TRY_CONVERT(DECIMAL(30,10), PrecioVenta) IS NULL 
            OR TRY_CONVERT(DECIMAL(30,10), PrecioVenta) > 99999999999999.99 
            OR TRY_CONVERT(DATE, FechaVenta) IS NULL
            THEN 1 ELSE 0 
        END) as Total_Problemas,
        
        SUM(CASE WHEN TRY_CONVERT(INT, Cantidad) IS NULL THEN 1 ELSE 0 END) as Cantidad_Invalida,
        SUM(CASE WHEN TRY_CONVERT(DECIMAL(30,10), PrecioVenta) IS NULL THEN 1 ELSE 0 END) as Precio_Invalido,
        SUM(CASE WHEN TRY_CONVERT(DECIMAL(30,10), PrecioVenta) > 99999999999999.99 THEN 1 ELSE 0 END) as Precio_Demasiado_Grande,
        SUM(CASE WHEN TRY_CONVERT(DATE, FechaVenta) IS NULL THEN 1 ELSE 0 END) as Fecha_Invalida,
        
        COUNT(*) as Total_Registros
    FROM (
        SELECT * FROM STG_Ventas
        UNION ALL 
        SELECT * FROM STG_Ventas_Add
    ) AS Ventas;
END
GO

-- SP para obtener ejemplos de datos problemáticos
CREATE OR ALTER PROCEDURE sp_GetVentasProblematicExamples
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT TOP 5
        FechaVenta, CodigoProducto, Cantidad, PrecioVenta,
        CASE 
            WHEN TRY_CONVERT(DATE, FechaVenta) IS NULL THEN 'FECHA_INVALIDA'
            WHEN TRY_CONVERT(INT, Cantidad) IS NULL THEN 'CANTIDAD_INVALIDA'
            WHEN TRY_CONVERT(DECIMAL(30,10), PrecioVenta) IS NULL THEN 'PRECIO_INVALIDO'
            WHEN TRY_CONVERT(DECIMAL(30,10), PrecioVenta) > 99999999999999.99 THEN 'PRECIO_DEMASIADO_GRANDE'
            ELSE 'OTRO_PROBLEMA'
        END as Tipo_Problema
    FROM (
        SELECT * FROM STG_Ventas
        UNION ALL SELECT * FROM STG_Ventas_Add
    ) v
    WHERE 
        TRY_CONVERT(DATE, FechaVenta) IS NULL
        OR TRY_CONVERT(INT, Cantidad) IS NULL
        OR TRY_CONVERT(DECIMAL(30,10), PrecioVenta) IS NULL
        OR TRY_CONVERT(DECIMAL(30,10), PrecioVenta) > 99999999999999.99;
END
GO

-- SP para verificar existencia de stored procedures
CREATE OR ALTER PROCEDURE sp_CheckStoredProceduresExist
    @ProcedureName NVARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @ProcedureName IS NULL
    BEGIN
        SELECT 
            ROUTINE_NAME as ProcedureName,
            CASE 
                WHEN ROUTINE_NAME IN ('sp_Cargar_INT_Clientes', 'sp_Cargar_INT_Productos', 
                                    'sp_Cargar_INT_Tiendas', 'sp_Cargar_INT_Ventas')
                THEN 1 ELSE 0 
            END as IsRequired
        FROM INFORMATION_SCHEMA.ROUTINES 
        WHERE ROUTINE_TYPE = 'PROCEDURE'
        AND ROUTINE_NAME IN ('sp_Cargar_INT_Clientes', 'sp_Cargar_INT_Productos', 
                           'sp_Cargar_INT_Tiendas', 'sp_Cargar_INT_Ventas');
    END
    ELSE
    BEGIN
        SELECT COUNT(*) as ExistsFlag
        FROM INFORMATION_SCHEMA.ROUTINES 
        WHERE ROUTINE_NAME = @ProcedureName AND ROUTINE_TYPE = 'PROCEDURE';
    END
END
GO

------------------------------------------------------------------------------------------------------------

-- Carga o actualiza registros en Dim_Cliente a partir de INT_Cliente
CREATE OR ALTER PROCEDURE dbo.sp_Dim_CargarCliente
AS
BEGIN
    SET NOCOUNT ON;
    
    MERGE Dim_Cliente AS Target
    USING INT_Cliente AS Source
    ON (Target.CodCliente = Source.CodCliente)
    
    WHEN MATCHED THEN
        UPDATE SET
            Target.RazonSocial = Source.RazonSocial,
            Target.Telefono = Source.Telefono,
            Target.Mail = Source.Mail,
            Target.Direccion = Source.Direccion,
            Target.Localidad = Source.Localidad,
            Target.Provincia = Source.Provincia,
            Target.CP = Source.CP
    
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (CodCliente, RazonSocial, Telefono, Mail, Direccion, Localidad, Provincia, CP, FechaCreacion)
        VALUES (Source.CodCliente, Source.RazonSocial, Source.Telefono, Source.Mail, Source.Direccion, Source.Localidad, Source.Provincia, Source.CP, Source.FechaCreacion);

    TRUNCATE TABLE INT_Cliente;
END
GO

-- Carga o actualiza registros en Dim_Producto a partir de INT_Producto
CREATE OR ALTER PROCEDURE dbo.sp_Dim_CargarProducto
AS
BEGIN
    SET NOCOUNT ON;

    MERGE Dim_Producto AS Target
    USING INT_Producto AS Source
    ON (Target.CodigoProducto = Source.CodigoProducto)
    
    WHEN MATCHED THEN
        UPDATE SET
            Target.Descripcion = Source.Descripcion,
            Target.Categoria = Source.Categoria,
            Target.Marca = Source.Marca,
            Target.PrecioCosto = Source.PrecioCosto,
            Target.PrecioVentaSugerido = Source.PrecioVentaSugerido
            
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (CodigoProducto, Descripcion, Categoria, Marca, PrecioCosto, PrecioVentaSugerido, FechaCreacion)
        VALUES (Source.CodigoProducto, Source.Descripcion, Source.Categoria, Source.Marca, Source.PrecioCosto, Source.PrecioVentaSugerido, Source.FechaCreacion);

    TRUNCATE TABLE INT_Producto;
END
GO

-- Carga o actualiza registros en Dim_Tienda a partir de INT_Tienda
CREATE OR ALTER PROCEDURE dbo.sp_Dim_CargarTienda
AS
BEGIN
    SET NOCOUNT ON;

    MERGE Dim_Tienda AS Target
    USING INT_Tienda AS Source
    ON (Target.CodigoTienda = Source.CodigoTienda)
    
    WHEN MATCHED THEN
        UPDATE SET
            Target.Descripcion = Source.Descripcion,
            Target.Direccion = Source.Direccion,
            Target.Localidad = Source.Localidad,
            Target.Provincia = Source.Provincia,
            Target.CP = Source.CP,
            Target.TipoTienda = Source.TipoTienda
            
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (CodigoTienda, Descripcion, Direccion, Localidad, Provincia, CP, TipoTienda, FechaCreacion)
        VALUES (Source.CodigoTienda, Source.Descripcion, Source.Direccion, Source.Localidad, Source.Provincia, Source.CP, Source.TipoTienda, Source.FechaCreacion);

    TRUNCATE TABLE INT_Tienda;
END
GO

-- Carga registros en Fact_Ventas a partir de INT_Ventas
CREATE OR ALTER PROCEDURE dbo.sp_Fact_CargarVentas
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO Fact_Ventas (
        Tiempo_Key,        -- reemplaza ID_Fecha
        ID_Producto, 
        ID_Cliente, 
        ID_Tienda,
        Cantidad, 
        PrecioVenta, 
        Total_IVA, 
        FechaCarga
    )
    SELECT
        DT.Tiempo_Key,       -- corresponde a Tiempo_Key en Fact
        DP.ID_Producto,
        DC.ID_Cliente,
        TD.ID_Tienda,
        IV.Cantidad,
        IV.PrecioVenta,
        IV.Total_IVA,
        IV.FechaCarga
    FROM 
        INT_Ventas IV
    INNER JOIN 
        Dim_Cliente DC 
        ON IV.CodigoCliente = DC.CodCliente
    INNER JOIN 
        Dim_Producto DP 
        ON IV.CodigoProducto = DP.CodigoProducto
    INNER JOIN 
        Dim_Tienda TD 
        ON IV.CodigoTienda = TD.CodigoTienda
    INNER JOIN 
        Dim_Tiempo DT 
        ON IV.FechaVenta = DT.Tiempo_Key;

    TRUNCATE TABLE INT_Ventas;
END
GO
