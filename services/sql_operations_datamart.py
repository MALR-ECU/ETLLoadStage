import logging
import pymssql
import os

sql_server = os.environ["SQL_SERVER"]  # Dirección del servidor SQL
sql_username = os.environ["SQL_USERNAME"]  # Usuario de SQL Server
sql_password = os.environ["SQL_PASSWORD"]  # Contraseña de SQL Server
sql_table_string = "LandingDB.DataMart"
sql_database, schema_name = sql_table_string.split('.') 
Name_table_Staging = "Staging.StagingInspecciones"

Name_table_Hechos = "FactInspecciones"
Name_table_Tiempos = "DimTiempo"
Name_table_Productos= "DimProducto"
Name_table_Lado = "DimLado"
Name_table_Estado = "DimEstado"
Name_table_Operador = "DimOperador"

def Crear_Modelo_Estrella():
    try:
        # Conexión a SQL Server
        with pymssql.connect(
            server=sql_server,
            user=sql_username,
            password=sql_password,
            database=sql_database
        ) as conn:
            cursor = conn.cursor()

            # Tabla DimTiempo
            create_table_query = f"""
            IF NOT EXISTS (
                SELECT 1 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{Name_table_Tiempos}'
            )
            BEGIN
                CREATE TABLE {schema_name}.{Name_table_Tiempos} (
                    FechaKey INT IDENTITY(1,1) PRIMARY KEY,
                    Year INT NOT NULL,
                    Month INT NOT NULL,
                    Day INT NULL,
                    NombreDia VARCHAR(20) NULL,        
                    NombreMes VARCHAR(20) NULL           
                )
            END
            """
            crear_tabla(cursor, schema_name, Name_table_Tiempos, create_table_query)
            crear_indices(cursor, schema_name, Name_table_Tiempos, "IX_FechaKey", "FechaKey")
            conn.commit()

            # Tabla DimProducto
            create_table_query = f"""
            IF NOT EXISTS (
                SELECT 1 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{Name_table_Productos}'
            )
            BEGIN
                CREATE TABLE {schema_name}.{Name_table_Productos} (
                    Codigo_Unico NVARCHAR(100) PRIMARY KEY,
                    Diametro VARCHAR(50) NULL,
                    Conexion VARCHAR(50) NULL,
                    Orden_de_Produccion INT NULL         
                )
            END
            """
            crear_tabla(cursor, schema_name, Name_table_Productos, create_table_query)
            crear_indices(cursor, schema_name, Name_table_Productos, "IX_Codigo_Unico", "Codigo_Unico")
            conn.commit()

            # Tabla DimLado
            create_table_query = f"""
            IF NOT EXISTS (
                SELECT 1 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{Name_table_Lado}'
            )
            BEGIN
                CREATE TABLE {schema_name}.{Name_table_Lado} (
                    LadoKey INT IDENTITY(1,1) PRIMARY KEY,
                    Lado VARCHAR(50) NULL        
                )
            END
            """
            crear_tabla(cursor, schema_name, Name_table_Lado, create_table_query)
            crear_indices(cursor, schema_name, Name_table_Lado, "IX_LadoKey", "LadoKey")
            conn.commit()

            # Tabla DimEstado
            create_table_query = f"""
            IF NOT EXISTS (
                SELECT 1 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{Name_table_Estado}'
            )
            BEGIN
                CREATE TABLE {schema_name}.{Name_table_Estado} (
                    EstadoKey INT IDENTITY(1,1) PRIMARY KEY,
                    Perfil_de_Rosca VARCHAR(50) NULL,
                    Estado VARCHAR(50) NULL,   
                    Motivo_Descarte VARCHAR(50) NULL,
                    Comentario NVARCHAR(MAX) NULL      
                )
            END
            """
            crear_tabla(cursor, schema_name, Name_table_Estado, create_table_query)
            crear_indices(cursor, schema_name, Name_table_Estado, "IX_EstadoKey", "EstadoKey")
            conn.commit()

            # Tabla DimOperador
            create_table_query = f"""
            IF NOT EXISTS (
                SELECT 1 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{Name_table_Operador}'
            )
            BEGIN
                CREATE TABLE {schema_name}.{Name_table_Operador} (
                    OperadorKey INT IDENTITY(1,1) PRIMARY KEY,
                    Operador VARCHAR(100) NULL
                )
            END
            """
            crear_tabla(cursor, schema_name, Name_table_Operador, create_table_query)
            crear_indices(cursor, schema_name, Name_table_Operador, "IX_OperadorKey", "OperadorKey")
            conn.commit()

            # Tabla de hechos
            create_table_query = f"""
            IF NOT EXISTS (
                SELECT 1 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{Name_table_Hechos}'
            )
            BEGIN
                CREATE TABLE {schema_name}.{Name_table_Hechos} (
                    ID_Inspeccion BIGINT PRIMARY KEY,
                    FechaKey INT NOT NULL,          
                    Codigo_Unico NVARCHAR(100) NOT NULL,           
                    OperadorKey INT NOT NULL,          
                    LadoKey INT NOT NULL,            
                    EstadoKey INT NOT NULL,             
                    Variacion_de_Diametro FLOAT NULL,
                    Ovalidad FLOAT NULL,
                    Paso FLOAT NULL,
                    Conicidad FLOAT NULL,
                    Longitud_de_Rosca FLOAT NULL,
                    Altura_de_Rosca FLOAT NULL,
                    Espesor_de_Cara VARCHAR(50) NULL,
                    FechaCargaBlob DATETIME NOT NULL,
                    FOREIGN KEY (OperadorKey) REFERENCES DataMart.DimOperador(OperadorKey),
                    FOREIGN KEY (Codigo_Unico) REFERENCES DataMart.DimProducto(Codigo_Unico),
                    FOREIGN KEY (FechaKey) REFERENCES DataMart.DimTiempo(FechaKey),
                    FOREIGN KEY (LadoKey) REFERENCES DataMart.DimLado(LadoKey),
                    FOREIGN KEY (EstadoKey) REFERENCES DataMart.DimEstado(EstadoKey)
                )
            END
            """
            crear_tabla(cursor, schema_name, Name_table_Hechos, create_table_query)
            crear_indices(cursor, schema_name, Name_table_Hechos, "IX_FechaKey", "FechaKey")
            crear_indices(cursor, schema_name, Name_table_Hechos, "IX_Codigo_Unico", "Codigo_Unico")
            crear_indices(cursor, schema_name, Name_table_Hechos, "IX_OperadorKey", "OperadorKey")
            crear_indices(cursor, schema_name, Name_table_Hechos, "IX_LadoKey", "LadoKey")
            crear_indices(cursor, schema_name, Name_table_Hechos, "IX_EstadoKey", "EstadoKey")
            conn.commit()

            # Confirmación final
            logging.info("Todas las tablas han sido verificadas o creadas exitosamente.")

    except pymssql.OperationalError as e:
        logging.error(f"Error de conexión a SQL Server: {e}")
        raise
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
        raise

def crear_tabla(cursor, schema_name, nombre_tabla, query):
    try:
        cursor.execute(query)
        logging.info(f"Tabla '{schema_name}.{nombre_tabla}' verificada/creada exitosamente.")
    except Exception as e:
        logging.error(f"Error al crear/verificar la tabla '{schema_name}.{nombre_tabla}': {e}")
        raise

def crear_indices(cursor, schema_name, nombre_tabla, nombre_indice, campo):
    try:
        cursor.execute(f"""
        IF NOT EXISTS (
            SELECT 1
            FROM sys.indexes
            WHERE name = '{nombre_indice}' AND object_id = OBJECT_ID('{schema_name}.{nombre_tabla}')
        )
        BEGIN
            CREATE INDEX {nombre_indice} ON {schema_name}.{nombre_tabla} ({campo})
        END
        """)
        logging.info(f"Índice '{nombre_indice}' creado/verificado en la tabla '{schema_name}.{nombre_tabla}'.")
    except Exception as e:
        logging.error(f"Error al crear el índice '{nombre_indice}' en '{schema_name}.{nombre_tabla}': {e}")
        raise

def Carga_Modelo_Estrella():
    try:
        # Conexión a SQL Server
        with pymssql.connect(
            server=sql_server,
            user=sql_username,
            password=sql_password,
            database=sql_database
        ) as conn:
            cursor = conn.cursor()

            # Insertar en DimProducto
            insertar_dim_producto_query = f"""
                INSERT INTO {schema_name}.{Name_table_Productos}  (Codigo_Unico, Diametro, Conexion, Orden_de_Produccion)
                SELECT DISTINCT 
                    Codigo_Unico,
                    Diametro,
                    Conexion,
                    Orden_de_Produccion
                FROM {Name_table_Staging}
                WHERE Codigo_Unico IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 
                    FROM {schema_name}.{Name_table_Productos} AS prod
                    WHERE prod.Codigo_Unico = {Name_table_Staging}.Codigo_Unico
                );
            """
            Ejecutar_Consulta(cursor, insertar_dim_producto_query, Name_table_Productos)
            conn.commit()

            # Insertar en DimTiempo
            insertar_dim_tiempo_query = f"""
                INSERT INTO {schema_name}.{Name_table_Tiempos}  (Year, Month, Day, NombreDia, NombreMes)
                SELECT DISTINCT 
                    Year, 
                    Month, 
                    Day,
                    DATENAME(WEEKDAY, CAST(CONCAT(Year, '-', Month, '-', Day) AS DATE)) AS NombreDia,
                    DATENAME(MONTH, CAST(CONCAT(Year, '-', Month, '-', Day) AS DATE)) AS NombreMes
                FROM {Name_table_Staging}
                WHERE Year IS NOT NULL AND Month IS NOT NULL AND Day IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 
                    FROM {schema_name}.{Name_table_Tiempos} AS tiempo
                    WHERE tiempo.Year = {Name_table_Staging}.Year
                    AND tiempo.Month = {Name_table_Staging}.Month
                    AND tiempo.Day = {Name_table_Staging}.Day
                );
            """
            Ejecutar_Consulta(cursor, insertar_dim_tiempo_query, Name_table_Tiempos)
            conn.commit()

            # Insertar en DimLado
            insertar_dim_lado_query = f"""
                INSERT INTO {schema_name}.{Name_table_Lado} (Lado)
                SELECT DISTINCT 
                    Lado
                FROM {Name_table_Staging}
                WHERE Lado IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 
                    FROM {schema_name}.{Name_table_Lado} AS lado
                    WHERE lado.Lado = {Name_table_Staging}.Lado
                );
            """
            Ejecutar_Consulta(cursor, insertar_dim_lado_query, Name_table_Lado)
            conn.commit()

            # Insertar en DimEstado
            insertar_dim_estado_query = f"""
                INSERT INTO {schema_name}.{Name_table_Estado} (Perfil_de_Rosca, Estado, Motivo_Descarte, Comentario)
                SELECT DISTINCT 
                    Perfil_de_Rosca,
                    Estado,
                    Motivo_Descarte,
                    Comentario
                FROM {Name_table_Staging}
                WHERE Estado IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 
                    FROM {schema_name}.{Name_table_Estado} AS estado
                    WHERE estado.Estado = {Name_table_Staging}.Estado
                );
            """
            Ejecutar_Consulta(cursor, insertar_dim_estado_query, Name_table_Estado)
            conn.commit()

            # Insertar en DimOperador
            insertar_dim_operador_query = f"""
                INSERT INTO {schema_name}.{Name_table_Operador} (Operador)
                SELECT DISTINCT 
                    Operador
                FROM {Name_table_Staging}
                WHERE Operador IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 
                    FROM {schema_name}.{Name_table_Operador} AS operador
                    WHERE operador.Operador = {Name_table_Staging}.Operador
                );
            """
            Ejecutar_Consulta(cursor, insertar_dim_operador_query, Name_table_Operador)
            conn.commit()

            # Insertar en la tabla de hechos (Fact)
            insertar_fact_query = f"""
                WITH CTE AS (
                    SELECT 
                        S.ID_Inspeccion,
                        T.FechaKey,
                        S.Codigo_Unico,
                        O.OperadorKey,
                        L.LadoKey,
                        E.EstadoKey,
                        S.Variacion_de_Diametro, 
                        S.Ovalidad, 
                        S.Paso, 
                        S.Conicidad, 
                        S.Longitud_de_Rosca, 
                        S.Altura_de_Rosca,
                        S.Espesor_de_Cara,
                        S.FechaCargaBlob,
                        ROW_NUMBER() OVER (PARTITION BY S.ID_Inspeccion ORDER BY S.FechaCargaBlob DESC) AS row_num
                    FROM {Name_table_Staging} AS S
                    JOIN {schema_name}.DimTiempo AS T 
                        ON T.Year = S.Year AND T.Month = S.Month AND T.Day = S.Day
                    JOIN {schema_name}.DimOperador AS O 
                        ON O.Operador = S.Operador
                    JOIN {schema_name}.DimLado AS L 
                        ON L.Lado = S.Lado
                    JOIN {schema_name}.DimEstado AS E 
                        ON E.Estado = S.Estado
                    WHERE S.Codigo_Unico IS NOT NULL
                    AND S.FechaCargaBlob IS NOT NULL
                )
                INSERT INTO {schema_name}.{Name_table_Hechos} (
                    ID_Inspeccion,
                    FechaKey, 
                    Codigo_Unico, 
                    OperadorKey, 
                    LadoKey, 
                    EstadoKey, 
                    Variacion_de_Diametro, 
                    Ovalidad, 
                    Paso, 
                    Conicidad, 
                    Longitud_de_Rosca, 
                    Altura_de_Rosca, 
                    Espesor_de_Cara, 
                    FechaCargaBlob
                )
                SELECT 
                    ID_Inspeccion,
                    FechaKey, 
                    Codigo_Unico, 
                    OperadorKey, 
                    LadoKey, 
                    EstadoKey, 
                    Variacion_de_Diametro, 
                    Ovalidad, 
                    Paso, 
                    Conicidad, 
                    Longitud_de_Rosca, 
                    Altura_de_Rosca, 
                    Espesor_de_Cara, 
                    FechaCargaBlob
                FROM CTE
                WHERE row_num = 1
                and not exists (
                    select 1
                    from {schema_name}.{Name_table_Hechos} as fact
                    where fact.ID_Inspeccion = CTE.ID_Inspeccion
                );
            """
            Ejecutar_Consulta(cursor, insertar_fact_query, Name_table_Hechos)

            # Confirmar cambios
            conn.commit()
            logging.info("Datos insertados correctamente en las tablas del modelo estrella.")

    except pymssql.OperationalError as e:
        logging.error(f"Error de conexión a SQL Server: {e}")
        raise
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
        raise


def Ejecutar_Consulta(cursor, query, tabla):
    try:
        cursor.execute(query)
        logging.info(f"Consulta ejecutada correctamente: '{tabla}' ")
    except Exception as e:
        logging.error(f"Error al ejecutar la consulta: {e}")
        raise