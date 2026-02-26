# Prueba Tecnica - Analista de Procesos ETL

Desarrollo completo de la prueba tecnica para el cargo de **Analista de Procesos ETL**, abordando las cuatro secciones solicitadas: Excel, Python, SQL y Logica ETL.

## Estructura del Repositorio

```
├── Seccion_1_Excel/          # Power Query, tabla dinamica y analisis de duplicados
├── Seccion_2_Python/         # ETL con pandas: carga, deduplicacion, validacion y exportacion
├── Seccion_3_SQL/            # PostgreSQL con Docker: Store Procedure UPSERT
├── Seccion_4_Logica_ETL/     # Diseno de flujo ETL, criterios de calidad y mejora continua
└── Documento_Explicativo_Prueba_Tecnica.pdf
```

## Seccion 1 - Excel

- Conexion de archivos CSV en Power Query con correccion de headers corruptos
- Append de `flights_5000v2.csv` y `flights_10000v2.csv` (15,000 registros)
- Conversion de `Col_10` de texto a numero
- Tabla dinamica por `Col_2` con cantidad y suma de `Col_10`
- Identificacion del valor mas duplicado en `Col_1`

## Seccion 2 - Python

Notebook Jupyter (`ETL_Prueba_Tecnica.ipynb`) con:

1. **Carga e integracion**: Union de ambos CSV en un DataFrame de 15,000 registros
2. **Eliminacion de duplicados**: Deduplicacion por `Col_1` conservando la primera ocurrencia
3. **Validacion de emails** (`Col_8`): Regex con limpieza previa de padding
4. **Validacion de telefonos** (`Col_11`): Reglas de numeracion colombiana (celular/fijo)
5. **Exportacion**: `flights_unificado_limpio.csv` con columnas depuradas
6. **Conexion a BD**: Documentacion de `pyodbc` y `SQLAlchemy` para SQL Server

## Seccion 3 - SQL

- **Motor**: PostgreSQL 15 ejecutado con Docker
- **Carga**: `COPY` desde CSV a tablas `FlightsBase` (5,000) y `FlightsNew` (10,000)
- **Store Procedure** (`sp_upsert_flights`): UPSERT con `INSERT ... ON CONFLICT DO UPDATE`, deduplicacion previa y auditoria por pasos
- **Verificacion**: 10,000 registros unicos sin duplicados

### Ejecucion rapida

```bash
cd Seccion_3_SQL
docker compose up -d
docker exec -i etl_postgres psql -U etl_user -d etl_prueba -f /data/seccion3_sql.sql
```

## Seccion 4 - Logica ETL

Desarrollo teorico de un proceso ETL para integrar ERP, CRM y archivos externos al Data Warehouse:

- **Parte A**: Flujo de 6 etapas (extraccion, validacion, transformacion, carga, errores, monitoreo)
- **Parte B**: Controles de calidad, validacion, idempotencia y deteccion de anomalias
- **Parte C**: Manejo de fallas, documentacion y mejoras futuras

## Herramientas Utilizadas

| Herramienta | Uso |
|---|---|
| Python + pandas | ETL y validacion de datos |
| Jupyter Notebook | Organizacion del codigo por secciones |
| PostgreSQL 15 | Motor de base de datos para la seccion SQL |
| Docker | Despliegue del ambiente de base de datos |
| Excel + Power Query | Conexion de archivos y tabla dinamica |

## Prerequisitos

- Python 3.8+
- Docker Desktop (para la seccion SQL)
- Excel 2016+ (para la seccion Excel)
- VS Code con extension Jupyter

```bash
pip install pandas jupyter
```
