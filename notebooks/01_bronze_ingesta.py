# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze · Ingesta de hurto a personas
# MAGIC
# MAGIC ## Qué hace este notebook
# MAGIC Lee el CSV descargado de datos.gov.co desde un Volume de Unity Catalog y lo guarda **tal cual** como tabla Delta. Es la primera capa de la arquitectura *medallion* del proyecto:
# MAGIC
# MAGIC | Capa | Tabla | Notebook | Qué contiene |
# MAGIC |---|---|---|---|
# MAGIC | **Bronze** | `bronze_hurto_personas` | **este** | Datos crudos, sin interpretar |
# MAGIC | Silver | `silver_hurto_personas` | 02 | Datos tipados y con banderas de calidad |
# MAGIC | Gold | `gold_*` | 03 | Tablas agregadas para análisis y modelado |
# MAGIC
# MAGIC ## Fuente
# MAGIC Ministerio de Defensa Nacional, Observatorio MDN, vía datos.gov.co, dataset **HURTO PERSONAS**, ID `4rxi-8m8d`, licencia CC BY-SA 4.0. Cada fila es un día y un municipio, y `cantidad` mide **víctimas**.
# MAGIC
# MAGIC ## Decisiones de diseño
# MAGIC 1. **Todas las columnas se leen como texto** (`inferSchema=False`). Así nada se interpreta en silencio al cargar; por ejemplo, una fecha `03/04/2020` podría leerse como 3 de abril o 4 de marzo. El tipado se hace en Silver, después de ver el formato real.
# MAGIC 2. **Lo único que se modifica es el nombre de las columnas** (minúsculas y guion bajo), porque Delta no admite bien espacios en los nombres. Por ejemplo, `FECHA HECHO` pasa a `fecha_hecho`.
# MAGIC 3. **Se agregan dos columnas de trazabilidad:** `_source_file` (de qué archivo vino) y `_ingested_at` (cuándo se cargó).
# MAGIC 4. **La escritura es idempotente:** se puede ejecutar de nuevo y la tabla se reemplaza sin duplicar filas.
# MAGIC
# MAGIC ## Requisitos previos
# MAGIC - Schema `workspace.hurtos_co` y Volume `raw` creados.
# MAGIC - El archivo `hurto_personas_20261016.csv` subido a `/Volumes/workspace/hurtos_co/raw/`. Las instrucciones para descargarlo están en `data/README.md` del repositorio.
# MAGIC - Cómputo **Serverless**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros y verificación del archivo
# MAGIC Se definen en un solo lugar el catálogo, el schema, el nombre del archivo y la tabla de destino. Si descargas el dataset otro día, **solo cambia `FILE`**. Después se lista el Volume para confirmar que el archivo existe y que su tamaño es el esperado (unos 35 millones de bytes).

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "hurtos_co"
FILE = "hurto_personas_20261016.csv"
SRC = f"/Volumes/{CATALOG}/{SCHEMA}/raw/{FILE}"
TABLE = f"{CATALOG}.{SCHEMA}.bronze_hurto_personas"

display(dbutils.fs.ls(f"/Volumes/{CATALOG}/{SCHEMA}/raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Inspección del archivo crudo
# MAGIC Antes de leer, se imprimen los primeros 800 bytes. Aquí se confirman tres cosas que condicionan todo lo demás:
# MAGIC - **Encabezados:** `FECHA HECHO`, `COD_DEPTO`, `DEPARTAMENTO`, `COD_MUNI`, `MUNICIPIO`, `CANTIDAD`.
# MAGIC - **Separador y comillas:** comas, con todos los valores entre comillas dobles.
# MAGIC - **Formato de fecha:** día/mes/año (`31/08/2026`).

# COMMAND ----------

# Vista del archivo crudo: encabezados, separador y formato de fecha (dd/MM/yyyy)
print(dbutils.fs.head(SRC, 800))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lectura y renombrado de columnas
# MAGIC Se lee el CSV con `header=True` y **`inferSchema=False`**, de modo que las seis columnas quedan como `string`. La función `snake` quita tildes, reemplaza espacios y símbolos por guion bajo y pasa a minúsculas. Luego se agregan `_source_file` y `_ingested_at`.
# MAGIC
# MAGIC En el esquema impreso debes ver seis columnas `string`, más `_source_file` (`string`) y `_ingested_at` (`timestamp`).
# MAGIC
# MAGIC Los códigos de departamento y municipio **deben seguir siendo texto**: así se conservan los ceros a la izquierda (`05001` es Medellín; como número sería `5001`).

# COMMAND ----------

import re, unicodedata
from pyspark.sql import functions as F

df_raw = (spark.read
          .option("header", True)
          .option("inferSchema", False)
          .option("encoding", "UTF-8")
          .csv(SRC))

def snake(c):
    c = unicodedata.normalize("NFKD", c).encode("ascii", "ignore").decode()
    return re.sub(r"[^0-9a-zA-Z]+", "_", c.strip()).strip("_").lower()

df = (df_raw.toDF(*[snake(c) for c in df_raw.columns])
            .withColumn("_source_file", F.lit(FILE))
            .withColumn("_ingested_at", F.current_timestamp()))

df.printSchema()
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Escritura de la tabla Delta
# MAGIC Se guarda `bronze_hurto_personas` en formato Delta con `mode("overwrite")`. La opción `overwriteSchema` permite reemplazar también el esquema si cambia, por ejemplo cuando el publicador agrega una columna. Como el modo es *overwrite*, ejecutar la celda otra vez deja la tabla igual en contenido, sin filas duplicadas.

# COMMAND ----------

(df.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(TABLE))
print("Tabla creada:", TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validación del conteo de filas
# MAGIC Se compara con la cifra de la ficha del dataset en datos.gov.co: **651.200 filas**. Una diferencia indicaría una carga incompleta o un archivo distinto. Si descargas el dataset en otra fecha, el número será mayor, porque la fuente se actualiza cada mes.

# COMMAND ----------

# Validación de conteo (la ficha del dataset reportaba 651.200 filas)
n = spark.table(TABLE).count()
print(f"Filas cargadas: {n:,}  (la ficha reportaba ≈ 651.200)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Control de calidad básico
# MAGIC Una sola consulta revisa nulos en las columnas clave y cuenta valores distintos. Se espera **cero nulos**. Los 1.113 valores de `cod_muni` y los 34 de `departamento` incluyen la categoría `SIN ESTABLECER` (registros sin ubicación), que en Colombia no es un departamento ni un municipio real. Colombia tiene 32 departamentos más Bogotá, es decir 33. Ese tratamiento se hace en Silver.

# COMMAND ----------

# Control de calidad básico
display(spark.sql(f"""
SELECT count(*)                         AS filas,
       count_if(fecha_hecho IS NULL)    AS fecha_nula,
       count_if(cod_muni IS NULL)       AS muni_nulo,
       count_if(cantidad IS NULL)       AS cantidad_nula,
       count(DISTINCT cod_muni)         AS municipios,
       count(DISTINCT departamento)     AS departamentos
FROM {TABLE}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Formato de fecha
# MAGIC Se reemplaza cada dígito por un `9` y se cuentan los patrones resultantes. Si hay **un solo patrón**, el formato es uniforme y se puede convertir a fecha con una única máscara. Si hubiera varios, habría que tratarlos por separado en Silver.

# COMMAND ----------

# Patrones de formato de fecha (cada dígito se reemplaza por 9)
display(spark.sql(f"""
SELECT regexp_replace(fecha_hecho, '[0-9]', '9') AS patron, count(*) AS filas
FROM {TABLE}
GROUP BY 1
ORDER BY filas DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultados verificados (ejecución del 16/10/2026)
# MAGIC | Verificación | Resultado |
# MAGIC |---|---|
# MAGIC | Archivo | 35.062.496 bytes |
# MAGIC | Filas cargadas | **651.200** (coincide con la ficha) |
# MAGIC | Nulos en fecha, municipio y cantidad | **0** |
# MAGIC | Formatos de fecha | **Uno solo:** `99/99/9999` (día/mes/año) |
# MAGIC | Valores distintos | 1.113 de `cod_muni` y 34 de `departamento`, ambos con `SIN ESTABLECER` |
# MAGIC
# MAGIC ## Qué se traslada a Silver
# MAGIC - **Tipar** `fecha_hecho` (dd/MM/yyyy) y `cantidad` (entero).
# MAGIC - **Marcar** los registros `SIN ESTABLECER`. Una revisión posterior mostró que los 365 están en 2008 y concentran el 96,5 % de las víctimas de ese año, por lo que 2008 no es utilizable por municipio.
# MAGIC - **Medir duplicados** por fecha y municipio antes de decidir qué hacer con ellos.
# MAGIC
# MAGIC **Siguiente notebook:** `02_silver_limpieza`.
