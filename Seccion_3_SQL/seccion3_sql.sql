-- =============================================================================
-- SECCION 3 - SQL
-- Prueba Tecnica - Analista de Procesos ETL
-- Base de datos: PostgreSQL 15
-- =============================================================================
--
-- DECISION DE MOTOR: Se eligio PostgreSQL porque soporta nativamente
-- INSERT ... ON CONFLICT DO UPDATE (UPSERT), stored procedures con PL/pgSQL,
-- COPY para carga masiva desde CSV, y DISTINCT ON para deduplicacion elegante.
-- Ademas, se levanta facilmente con Docker (ver docker-compose.yml).
--
-- ARCHIVOS USADOS: flights_5000.csv y flights_10000.csv (separados por coma,
-- headers correctos Col_1 a Col_19). Estos son los archivos para la seccion SQL,
-- distintos a los archivos v2 usados en Python/Excel.
-- =============================================================================


-- =============================================================================
-- 3.1 CREACION DE TABLAS Y CARGA DE DATOS
-- =============================================================================

-- Se eliminan las tablas si ya existen para que el script sea RE-EJECUTABLE.
-- Esto permite correr el script multiples veces durante desarrollo/pruebas
-- sin errores por tablas duplicadas.
DROP TABLE IF EXISTS FlightsBase;
DROP TABLE IF EXISTS FlightsNew;

-- DECISION DE TIPOS: Todas las columnas se definen como VARCHAR porque:
-- 1. Los datos vienen de CSV sin tipos definidos, y hay mezcla de formatos
--    (ej: Col_4 tiene valores como "998E" que no son numericos puros).
-- 2. Usar VARCHAR evita errores de carga por datos inesperados.
-- 3. Col_8 y Col_14 usan VARCHAR(500) porque contienen valores mas largos
--    (emails con padding de ~200 espacios en los archivos v2, y Col_14
--    contiene cadenas compuestas con pipes como "10591512|125|23601231|E").

-- FlightsBase: tabla destino principal que recibira flights_5000.csv
-- y luego sera actualizada con los datos de FlightsNew via UPSERT.
CREATE TABLE FlightsBase (
    Col_1  VARCHAR(100),   -- Identificador del vuelo (llave de negocio para dedup)
    Col_2  VARCHAR(100),   -- Codigo de aerolinea (ej: EK, AA)
    Col_3  VARCHAR(100),   -- Aeropuerto origen
    Col_4  VARCHAR(100),   -- Codigo de vuelo (puede contener letras, ej: "998E")
    Col_5  VARCHAR(100),   -- Hora
    Col_6  VARCHAR(100),   -- Aeropuerto origen (repetido en datos)
    Col_7  VARCHAR(100),   -- Aeropuerto destino
    Col_8  VARCHAR(500),   -- Email o dato de contacto (VARCHAR largo por padding)
    Col_9  VARCHAR(100),   -- Valor numerico
    Col_10 VARCHAR(100),   -- Valor numerico (almacenado como texto en fuente)
    Col_11 VARCHAR(100),   -- Telefono
    Col_12 VARCHAR(100),   -- Codigo de estado
    Col_13 VARCHAR(100),   -- Timestamp
    Col_14 VARCHAR(500),   -- Cadena compuesta con pipes (necesita mas espacio)
    Col_15 VARCHAR(100),   -- ID numerico
    Col_16 VARCHAR(100),   -- Flag numerico
    Col_17 VARCHAR(100),   -- Timestamp
    Col_18 VARCHAR(100),   -- Usuario del sistema
    Col_19 VARCHAR(100)    -- Tipo de operacion (INSERT/UPDATE)
);

-- FlightsNew: tabla temporal de carga para flights_10000.csv.
-- Misma estructura que FlightsBase para permitir el UPSERT.
CREATE TABLE FlightsNew (
    Col_1  VARCHAR(100),
    Col_2  VARCHAR(100),
    Col_3  VARCHAR(100),
    Col_4  VARCHAR(100),
    Col_5  VARCHAR(100),
    Col_6  VARCHAR(100),
    Col_7  VARCHAR(100),
    Col_8  VARCHAR(500),
    Col_9  VARCHAR(100),
    Col_10 VARCHAR(100),
    Col_11 VARCHAR(100),
    Col_12 VARCHAR(100),
    Col_13 VARCHAR(100),
    Col_14 VARCHAR(500),
    Col_15 VARCHAR(100),
    Col_16 VARCHAR(100),
    Col_17 VARCHAR(100),
    Col_18 VARCHAR(100),
    Col_19 VARCHAR(100)
);

-- CARGA DE DATOS con COPY:
-- Se usa COPY en lugar de INSERT porque es el metodo mas eficiente de PostgreSQL
-- para carga masiva desde archivos. COPY lee directamente del filesystem del servidor
-- (dentro del contenedor Docker), por eso los archivos deben estar en /data/.
--
-- NOTA: La ruta /data/ corresponde al volumen montado en docker-compose.yml
-- que mapea la carpeta del proyecto al contenedor.

COPY FlightsBase
FROM '/data/flights_5000.csv'
WITH (
    FORMAT csv,        -- Formato CSV estandar
    HEADER true,       -- La primera fila contiene los nombres de columna
    DELIMITER ',',     -- Separador de coma (archivos sin "v2" usan coma)
    ENCODING 'UTF8'    -- Codificacion UTF-8 para caracteres especiales
);

COPY FlightsNew
FROM '/data/flights_10000.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    ENCODING 'UTF8'
);

-- VERIFICACION DE CARGA: Confirmar que se cargaron las cantidades esperadas.
-- Esperado: FlightsBase = 5000, FlightsNew = 10000.
-- Se usa UNION ALL (no UNION) para mostrar ambos conteos sin eliminar duplicados.
SELECT 'FlightsBase' AS tabla, COUNT(*) AS total_registros FROM FlightsBase
UNION ALL
SELECT 'FlightsNew' AS tabla, COUNT(*) AS total_registros FROM FlightsNew;


-- =============================================================================
-- 3.2 STORED PROCEDURE: sp_upsert_flights
-- =============================================================================
--
-- OBJETIVO: Fusionar los datos de FlightsNew hacia FlightsBase sin duplicados.
-- Si un registro de FlightsNew ya existe en FlightsBase (mismo Col_1),
-- se ACTUALIZA. Si no existe, se INSERTA.
--
-- DECISION DE ESTRATEGIA: Se usa INSERT ... ON CONFLICT DO UPDATE (UPSERT nativo
-- de PostgreSQL) en lugar de un MERGE o DELETE+INSERT porque:
-- 1. Es atomico: cada fila se inserta o actualiza en una sola operacion.
-- 2. Es eficiente: PostgreSQL lo optimiza internamente.
-- 3. Es idempotente: ejecutar el SP multiples veces da el mismo resultado.
--
-- PASOS DEL PROCEDIMIENTO:
-- 1. Registrar conteos iniciales (auditoria)
-- 2. Limpiar duplicados internos de FlightsBase (prerequisito para UNIQUE)
-- 3. Agregar constraint UNIQUE en Col_1 (requerido por ON CONFLICT)
-- 4. Deduplicar FlightsNew en tabla temporal
-- 5. Calcular estadisticas previas (inserciones vs actualizaciones)
-- 6. Ejecutar el UPSERT
-- 7. Reportar resultados
-- =============================================================================

CREATE OR REPLACE PROCEDURE sp_upsert_flights()
LANGUAGE plpgsql
AS $$
DECLARE
    -- Variables para auditoria y reporte del proceso.
    -- Se usan para dar trazabilidad al resultado del UPSERT,
    -- algo esencial en cualquier proceso ETL de produccion.
    v_total_base_antes     INT;  -- Registros en FlightsBase antes del proceso
    v_total_base_despues   INT;  -- Registros en FlightsBase despues del proceso
    v_duplicados_base      INT;  -- Duplicados internos eliminados de FlightsBase
    v_registros_new        INT;  -- Total de registros en FlightsNew
    v_registros_new_unicos INT;  -- Registros unicos en FlightsNew (sin duplicados)
    v_insertados           INT;  -- Registros nuevos que se insertaron
    v_actualizados         INT;  -- Registros existentes que se actualizaron
BEGIN
    -- =========================================================================
    -- PASO 1: Snapshot de conteos iniciales
    -- Se capturan ANTES de cualquier modificacion para poder reportar
    -- el delta al final del proceso (buena practica ETL).
    -- =========================================================================
    SELECT COUNT(*) INTO v_total_base_antes FROM FlightsBase;
    SELECT COUNT(*) INTO v_registros_new FROM FlightsNew;

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'INICIO DEL PROCESO UPSERT';
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'FlightsBase - Registros iniciales: %', v_total_base_antes;
    RAISE NOTICE 'FlightsNew  - Registros totales:   %', v_registros_new;

    -- =========================================================================
    -- PASO 2: Eliminar duplicados internos de FlightsBase
    -- =========================================================================
    -- PORQUE: FlightsBase puede tener registros con Col_1 repetido (se detecto
    -- en el analisis de calidad). El constraint UNIQUE del paso 3 fallaria si
    -- existen duplicados. Ademas, tener duplicados en la tabla base compromete
    -- la integridad del UPSERT.
    --
    -- COMO: Se usa ctid (identificador fisico de fila en PostgreSQL) para
    -- distinguir filas con el mismo Col_1. Se conserva la primera ocurrencia
    -- (MIN ctid) y se eliminan las demas.
    -- Se eligio ctid sobre ROW_NUMBER() porque es mas directo y eficiente
    -- para esta operacion especifica de deduplicacion en PostgreSQL.
    DELETE FROM FlightsBase
    WHERE ctid NOT IN (
        SELECT MIN(ctid)
        FROM FlightsBase
        GROUP BY Col_1
    );

    -- GET DIAGNOSTICS captura el numero de filas afectadas por el ultimo comando.
    -- Es la forma estandar en PL/pgSQL de obtener el ROW_COUNT.
    GET DIAGNOSTICS v_duplicados_base = ROW_COUNT;
    RAISE NOTICE 'FlightsBase - Duplicados internos eliminados: %', v_duplicados_base;

    -- =========================================================================
    -- PASO 3: Agregar constraint UNIQUE en Col_1
    -- =========================================================================
    -- PORQUE: ON CONFLICT requiere un indice unico o constraint UNIQUE para
    -- saber contra que columna detectar el conflicto. Sin esto, PostgreSQL
    -- no puede determinar si un registro es "nuevo" o "existente".
    --
    -- Se hace DROP + ADD (en lugar de IF NOT EXISTS) para garantizar que
    -- el constraint refleje el estado actual de los datos despues de la
    -- deduplicacion del paso 2. Esto hace el SP re-ejecutable (idempotente).
    ALTER TABLE FlightsBase DROP CONSTRAINT IF EXISTS uq_flightsbase_col1;
    ALTER TABLE FlightsBase ADD CONSTRAINT uq_flightsbase_col1 UNIQUE (Col_1);

    RAISE NOTICE 'Constraint UNIQUE en Col_1 agregado correctamente.';

    -- =========================================================================
    -- PASO 4: Crear tabla temporal con registros unicos de FlightsNew
    -- =========================================================================
    -- PORQUE: FlightsNew tambien puede tener duplicados internos por Col_1.
    -- Si se intentara el UPSERT directamente, los duplicados podrian causar
    -- conflictos inesperados o actualizaciones redundantes.
    --
    -- Se usa DISTINCT ON (Col_1) que es una extension de PostgreSQL que
    -- selecciona la primera fila de cada grupo (segun ORDER BY).
    -- Es mas limpio y eficiente que un ROW_NUMBER() + filtro.
    --
    -- Se usa tabla TEMPORAL porque solo se necesita durante la ejecucion
    -- del SP. Se elimina automaticamente al cerrar la sesion, y tambien
    -- la eliminamos explicitamente al final para mayor limpieza.
    DROP TABLE IF EXISTS tmp_flights_new_unicos;
    CREATE TEMP TABLE tmp_flights_new_unicos AS
    SELECT DISTINCT ON (Col_1) *
    FROM FlightsNew
    ORDER BY Col_1;

    SELECT COUNT(*) INTO v_registros_new_unicos FROM tmp_flights_new_unicos;
    RAISE NOTICE 'FlightsNew  - Registros unicos:    %', v_registros_new_unicos;

    -- =========================================================================
    -- PASO 5: Calcular estadisticas previas al UPSERT
    -- =========================================================================
    -- PORQUE: Es importante para la auditoria saber cuantos registros seran
    -- actualizaciones (ya existen en FlightsBase) vs inserciones nuevas.
    -- Esto se calcula ANTES del UPSERT porque despues ya no es posible
    -- distinguirlos.
    --
    -- Un INNER JOIN encuentra las coincidencias (actualizaciones).
    -- Las inserciones = total unicos - actualizaciones.
    SELECT COUNT(*) INTO v_actualizados
    FROM tmp_flights_new_unicos t
    INNER JOIN FlightsBase f ON f.Col_1 = t.Col_1;

    v_insertados := v_registros_new_unicos - v_actualizados;

    RAISE NOTICE '------------------------------------------';
    RAISE NOTICE 'Registros a ACTUALIZAR: %', v_actualizados;
    RAISE NOTICE 'Registros a INSERTAR:   %', v_insertados;

    -- =========================================================================
    -- PASO 6: Ejecutar UPSERT (INSERT ... ON CONFLICT DO UPDATE)
    -- =========================================================================
    -- PORQUE se usa ON CONFLICT DO UPDATE y no otras alternativas:
    --
    -- vs DELETE + INSERT: El UPSERT es atomico por fila y no requiere
    --    eliminar registros existentes, lo que mantiene la integridad
    --    referencial si hubiera FKs apuntando a esta tabla.
    --
    -- vs MERGE (SQL estandar): PostgreSQL no soportaba MERGE hasta la v15,
    --    y ON CONFLICT es la forma idiomatica y mas optimizada en PostgreSQL.
    --
    -- EXCLUDED: Es una tabla virtual de PostgreSQL que contiene los valores
    -- del registro que se intento insertar y causo el conflicto. Se usa
    -- para actualizar todas las columnas con los datos nuevos de FlightsNew.
    INSERT INTO FlightsBase (Col_1, Col_2, Col_3, Col_4, Col_5, Col_6, Col_7,
                             Col_8, Col_9, Col_10, Col_11, Col_12, Col_13,
                             Col_14, Col_15, Col_16, Col_17, Col_18, Col_19)
    SELECT Col_1, Col_2, Col_3, Col_4, Col_5, Col_6, Col_7,
           Col_8, Col_9, Col_10, Col_11, Col_12, Col_13,
           Col_14, Col_15, Col_16, Col_17, Col_18, Col_19
    FROM tmp_flights_new_unicos
    ON CONFLICT (Col_1) DO UPDATE SET
        Col_2  = EXCLUDED.Col_2,
        Col_3  = EXCLUDED.Col_3,
        Col_4  = EXCLUDED.Col_4,
        Col_5  = EXCLUDED.Col_5,
        Col_6  = EXCLUDED.Col_6,
        Col_7  = EXCLUDED.Col_7,
        Col_8  = EXCLUDED.Col_8,
        Col_9  = EXCLUDED.Col_9,
        Col_10 = EXCLUDED.Col_10,
        Col_11 = EXCLUDED.Col_11,
        Col_12 = EXCLUDED.Col_12,
        Col_13 = EXCLUDED.Col_13,
        Col_14 = EXCLUDED.Col_14,
        Col_15 = EXCLUDED.Col_15,
        Col_16 = EXCLUDED.Col_16,
        Col_17 = EXCLUDED.Col_17,
        Col_18 = EXCLUDED.Col_18,
        Col_19 = EXCLUDED.Col_19;

    -- =========================================================================
    -- PASO 7: Reportar resultados finales
    -- =========================================================================
    -- Se muestra un resumen del proceso para verificacion y auditoria.
    -- En un ETL de produccion, estos valores se guardarian en una tabla
    -- de log (etl_audit_log) para trazabilidad historica.
    SELECT COUNT(*) INTO v_total_base_despues FROM FlightsBase;

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'RESULTADO FINAL';
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'FlightsBase - Registros antes:   %', v_total_base_antes;
    RAISE NOTICE 'FlightsBase - Registros despues:  %', v_total_base_despues;
    RAISE NOTICE 'Inserciones realizadas:           %', v_insertados;
    RAISE NOTICE 'Actualizaciones realizadas:       %', v_actualizados;
    RAISE NOTICE '==========================================';

    -- Limpieza explicita de la tabla temporal.
    -- Aunque las tablas temporales se eliminan al cerrar la sesion,
    -- es buena practica liberarla inmediatamente para no consumir memoria.
    DROP TABLE IF EXISTS tmp_flights_new_unicos;
END;
$$;

-- Ejecutar el stored procedure
CALL sp_upsert_flights();


-- =============================================================================
-- 3.3 VERIFICACION
-- =============================================================================
-- Estas queries validan que el UPSERT se ejecuto correctamente.
-- Son el equivalente a los "controles de calidad post-carga" en un ETL real.
-- =============================================================================

-- VERIFICACION 1: Comparar COUNT vs COUNT DISTINCT.
-- Si ambos son iguales, significa que no hay duplicados en Col_1.
-- Esta es la prueba mas directa de exito del proceso.
SELECT
    COUNT(*) AS total_registros,
    COUNT(DISTINCT Col_1) AS registros_unicos
FROM FlightsBase;

-- VERIFICACION 2: Buscar duplicados explicitamente.
-- Debe devolver 0 filas. Si devuelve algo, el proceso fallo.
-- Se usa HAVING COUNT(*) > 1 para mostrar solo los Col_1 repetidos.
SELECT Col_1, COUNT(*) AS cantidad
FROM FlightsBase
GROUP BY Col_1
HAVING COUNT(*) > 1;

-- VERIFICACION 3: Muestra de datos para inspeccion visual.
-- Permite verificar que los datos se cargaron correctamente
-- y que las columnas tienen valores coherentes.
SELECT * FROM FlightsBase LIMIT 10;

-- VERIFICACION 4: Resultado en texto claro (OK/ERROR).
-- Util como paso final de validacion automatica.
-- Un sistema de monitoreo podria parsear este resultado para alertar.
SELECT
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT Col_1)
        THEN 'OK - No hay duplicados en FlightsBase'
        ELSE 'ERROR - Se encontraron duplicados'
    END AS verificacion_duplicados
FROM FlightsBase;
