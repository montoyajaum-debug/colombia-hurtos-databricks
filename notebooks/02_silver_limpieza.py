# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver · Limpieza y tipado de hurto a personas
# MAGIC **Entrada:** `bronze_hurto_personas` (todo texto). **Salida:** `silver_hurto_personas`.
# MAGIC
# MAGIC Reglas: no se elimina ninguna fila. Se tipan fecha y cantidad, se estandariza el texto y se agregan banderas de calidad:
# MAGIC - `ubicacion_definida`: falso cuando departamento o municipio es `SIN ESTABLECER`.
# MAGIC - `apto_municipal`: verdadero solo si la ubicación está definida **y** el año no es 2008 (en 2008 el 96,5 % de las víctimas no tiene municipio y solo hay 77 municipios identificados).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "hurtos_co"
BRONZE = f"{CATALOG}.{SCHEMA}.bronze_hurto_personas"
SILVER = f"{CATALOG}.{SCHEMA}.silver_hurto_personas"

b = spark.table(BRONZE)
n_bronze = b.count()
print(f"Bronze: {n_bronze:,} filas")

# COMMAND ----------

s = (b.select(
        F.expr("CAST(try_to_timestamp(trim(fecha_hecho), 'dd/MM/yyyy') AS DATE)").alias("fecha"),
        F.upper(F.trim("cod_depto")).alias("cod_depto"),
        F.upper(F.trim("departamento")).alias("departamento"),
        F.upper(F.trim("cod_muni")).alias("cod_muni"),
        F.upper(F.trim("municipio")).alias("municipio"),
        F.expr("try_cast(trim(cantidad) AS INT)").alias("cantidad"),
        "_source_file", "_ingested_at")
     .withColumn("anio", F.year("fecha"))
     .withColumn("ubicacion_definida",
                 ~((F.col("cod_muni") == "SIN ESTABLECER") | (F.col("cod_depto") == "SIN ESTABLECER")))
     .withColumn("apto_municipal", F.col("ubicacion_definida") & (F.col("anio") != 2008)))

s.printSchema()
display(s.limit(5))

# COMMAND ----------

# Diagnóstico general. Valores esperados según la revisión local del CSV:
# fecha_no_parseada=0, cantidad_no_numerica=0, fecha_min=2003-01-01, fecha_max=2026-08-31,
# cantidad_min=1, cantidad_max=718, sin_ubicacion=365
display(s.agg(
    F.count("*").alias("filas"),
    F.sum(F.col("fecha").isNull().cast("int")).alias("fecha_no_parseada"),
    F.sum(F.col("cantidad").isNull().cast("int")).alias("cantidad_no_numerica"),
    F.sum((F.col("cantidad") <= 0).cast("int")).alias("cantidad_menor_igual_cero"),
    F.sum((~F.col("ubicacion_definida")).cast("int")).alias("sin_ubicacion"),
    F.min("fecha").alias("fecha_min"),
    F.max("fecha").alias("fecha_max"),
    F.min("cantidad").alias("cantidad_min"),
    F.max("cantidad").alias("cantidad_max")))

# COMMAND ----------

# Duplicados: se espera 0 combinaciones repetidas de (fecha, cod_muni)
n_dup = (s.groupBy("fecha", "cod_muni").count().where("count > 1").count())
print("Combinaciones fecha-municipio repetidas:", n_dup)

# COMMAND ----------

# Impacto de las banderas de calidad
display(s.groupBy("ubicacion_definida", "apto_municipal")
          .agg(F.count("*").alias("filas"),
               F.sum("cantidad").alias("victimas"),
               F.countDistinct("cod_muni").alias("municipios")))

# COMMAND ----------

# Resumen por año: cobertura y posibles quiebres (2008 y 2016-2019)
display(s.groupBy("anio")
          .agg(F.count("*").alias("filas"),
               F.sum("cantidad").alias("victimas"),
               F.countDistinct("cod_muni").alias("municipios"))
          .orderBy("anio"))

# COMMAND ----------

# Reconciliación con la cifra externa (no verificada): 156.116 para enero-julio de 2026
total = (s.where("fecha BETWEEN '2026-01-01' AND '2026-07-31'")
          .agg(F.sum("cantidad")).first()[0])
print(f"Enero-julio 2026: {total:,} víctimas  (esperado en esta extracción: 160,998; referencia externa: 156,116)")

# COMMAND ----------

(s.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(SILVER))

n_silver = spark.table(SILVER).count()
print(f"Silver: {n_silver:,} filas | Bronze: {n_bronze:,} | filas perdidas: {n_bronze - n_silver}")

spark.sql(f"""COMMENT ON TABLE {BRONZE} IS
'Hurto a personas, victimas por dia y municipio. Fuente: Ministerio de Defensa Nacional, Observatorio MDN, datos.gov.co, ID 4rxi-8m8d. Licencia CC BY-SA 4.0. Extraccion: 16/10/2026. Capa Bronze, sin transformar.'""")
spark.sql(f"""COMMENT ON TABLE {SILVER} IS
'Capa Silver de hurto a personas: fecha y cantidad tipadas, texto estandarizado, banderas ubicacion_definida y apto_municipal (excluye 2008). Sin filas eliminadas. Misma fuente y licencia que Bronze.'""")

# COMMAND ----------

# Verificación final contra los valores calculados localmente sobre el CSV
chk = spark.table(SILVER).agg(
    F.sum("cantidad").alias("victimas_total"),
    F.sum(F.col("apto_municipal").cast("int")).alias("filas_apto_municipal"),
    F.sum(F.when(F.col("apto_municipal"), F.col("cantidad")).otherwise(0)).alias("victimas_apto_municipal")).first()
print("victimas_total            :", f"{chk['victimas_total']:,}", "(esperado 3,774,742)")
print("filas_apto_municipal      :", f"{chk['filas_apto_municipal']:,}", "(esperado 650,385)")
print("victimas_apto_municipal   :", f"{chk['victimas_apto_municipal']:,}", "(esperado 3,708,212)")
