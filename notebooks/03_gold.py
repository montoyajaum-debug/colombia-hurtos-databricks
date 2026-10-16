# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold · Tablas analíticas de hurto a personas
# MAGIC **Entrada:** `silver_hurto_personas`. **Salidas** (granularidad mensual, salvo la diaria nacional):
# MAGIC | Tabla | Contenido |
# MAGIC |---|---|
# MAGIC | `gold_dim_municipio` | Un registro por municipio: nombre y departamento más recientes, primer y último mes con casos |
# MAGIC | `gold_hurto_mensual_municipio` | Panel municipio × mes **con ceros**, desde el primer mes en que el municipio aparece. Solo filas `apto_municipal` (excluye 2008 y `SIN ESTABLECER`) |
# MAGIC | `gold_hurto_mensual_depto` | Panel departamento × mes con ceros, mismas reglas |
# MAGIC | `gold_hurto_mensual_nacional` | Serie nacional mensual, **incluye 2008** |
# MAGIC | `gold_hurto_diario_nacional` | Serie nacional diaria, incluye 2008 |
# MAGIC
# MAGIC **Decisión sobre los ceros:** el dataset solo trae filas con al menos una víctima. Se completa con 0 desde el **primer mes en que cada municipio aparece**, porque rellenar desde 2003 inventaría ceros en municipios que quizá aún no reportaban. 2008 queda fuera del panel (sin dato, no cero).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "hurtos_co"
SILVER = f"{CATALOG}.{SCHEMA}.silver_hurto_personas"
def T(name): return f"{CATALOG}.{SCHEMA}.{name}"

s = spark.table(SILVER).withColumn("fecha_mes", F.trunc("fecha", "month"))
apto = s.where("apto_municipal")
print("Silver:", f"{s.count():,}", "filas | aptas:", f"{apto.count():,}")

# COMMAND ----------

# Calendario mensual completo, sin 2008
rng = s.agg(F.min("fecha_mes").alias("mn"), F.max("fecha_mes").alias("mx"))
meses = (rng.select(F.explode(F.sequence("mn", "mx", F.expr("INTERVAL 1 MONTH"))).alias("fecha_mes"))
            .where(F.year("fecha_mes") != 2008))
print("Meses en el calendario (sin 2008):", meses.count())

# COMMAND ----------

# Dimensión de municipios
dim_muni = (apto.groupBy("cod_muni")
    .agg(F.max_by("municipio", "fecha").alias("municipio"),
         F.max_by("cod_depto", "fecha").alias("cod_depto"),
         F.max_by("departamento", "fecha").alias("departamento"),
         F.min("fecha_mes").alias("primer_mes"),
         F.max("fecha_mes").alias("ultimo_mes")))
dim_muni.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("gold_dim_municipio"))

# Panel municipio x mes con ceros
m_agg = apto.groupBy("fecha_mes", "cod_muni").agg(F.sum("cantidad").alias("victimas"))
panel_muni = (dim_muni.select("cod_muni", "primer_mes")
    .crossJoin(meses).where(F.col("fecha_mes") >= F.col("primer_mes")).drop("primer_mes")
    .join(m_agg, ["fecha_mes", "cod_muni"], "left")
    .withColumn("victimas", F.coalesce("victimas", F.lit(0)).cast("int"))
    .withColumn("anio", F.year("fecha_mes")).withColumn("mes", F.month("fecha_mes")))
panel_muni.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("gold_hurto_mensual_municipio"))

# COMMAND ----------

# Panel departamento x mes con ceros
dim_dep = (apto.groupBy("cod_depto")
    .agg(F.max_by("departamento", "fecha").alias("departamento"), F.min("fecha_mes").alias("primer_mes")))
d_agg = apto.groupBy("fecha_mes", "cod_depto").agg(F.sum("cantidad").alias("victimas"))
panel_dep = (dim_dep.crossJoin(meses).where(F.col("fecha_mes") >= F.col("primer_mes")).drop("primer_mes")
    .join(d_agg, ["fecha_mes", "cod_depto"], "left")
    .withColumn("victimas", F.coalesce("victimas", F.lit(0)).cast("int"))
    .withColumn("anio", F.year("fecha_mes")).withColumn("mes", F.month("fecha_mes")))
panel_dep.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("gold_hurto_mensual_depto"))

# COMMAND ----------

# Series nacionales (incluyen 2008 y las filas SIN ESTABLECER)
nac_m = (s.groupBy("fecha_mes")
    .agg(F.sum("cantidad").alias("victimas"),
         F.sum(F.when(F.col("apto_municipal"), F.col("cantidad")).otherwise(0)).alias("victimas_aptas_municipal"),
         F.countDistinct(F.when(F.col("apto_municipal"), F.col("cod_muni"))).alias("municipios_con_casos"))
    .withColumn("anio", F.year("fecha_mes")).withColumn("mes", F.month("fecha_mes")))
nac_m.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("gold_hurto_mensual_nacional"))

nac_d = s.groupBy("fecha").agg(F.sum("cantidad").alias("victimas"))
nac_d.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("gold_hurto_diario_nacional"))

# COMMAND ----------

# Documentación de tablas
fuente = "Fuente: Ministerio de Defensa Nacional, Observatorio MDN, datos.gov.co, ID 4rxi-8m8d. Licencia CC BY-SA 4.0."
coment = {
 "gold_dim_municipio": "Dimension de municipios: nombre y departamento mas recientes, primer y ultimo mes con casos. Excluye 2008 y SIN ESTABLECER.",
 "gold_hurto_mensual_municipio": "Panel municipio x mes de victimas de hurto a personas, con ceros desde el primer mes de aparicion. Excluye 2008 y SIN ESTABLECER.",
 "gold_hurto_mensual_depto": "Panel departamento x mes de victimas de hurto a personas, con ceros. Excluye 2008 y SIN ESTABLECER.",
 "gold_hurto_mensual_nacional": "Serie nacional mensual de victimas de hurto a personas. Incluye 2008 y filas sin ubicacion.",
 "gold_hurto_diario_nacional": "Serie nacional diaria de victimas de hurto a personas. Incluye 2008 y filas sin ubicacion.",
}
for t, c in coment.items():
    spark.sql(f"COMMENT ON TABLE {T(t)} IS '{c} {fuente}'")
print("Tablas documentadas")

# COMMAND ----------

# Verificación contra los valores calculados localmente sobre el CSV
def v(label, actual, esperado):
    print(f"{label:<38}: {actual:>12,}  (esperado {esperado:,})  {'OK' if actual == esperado else '<<< REVISAR'}")

pm = spark.table(T("gold_hurto_mensual_municipio"))
pd_ = spark.table(T("gold_hurto_mensual_depto"))
nm = spark.table(T("gold_hurto_mensual_nacional"))
v("municipios en la dimension",           spark.table(T("gold_dim_municipio")).count(), 1112)
v("panel municipio-mes: filas",           pm.count(), 279379)
v("panel municipio-mes: filas con casos", pm.where("victimas > 0").count(), 114450)
v("panel municipio-mes: victimas",        pm.agg(F.sum("victimas")).first()[0], 3708212)
v("panel depto-mes: filas",               pd_.count(), 8974)
v("panel depto-mes: filas con casos",     pd_.where("victimas > 0").count(), 8697)
v("departamentos",                        pd_.select("cod_depto").distinct().count(), 33)
v("nacional mensual: filas",              nm.count(), 284)
v("nacional mensual: victimas",           nm.agg(F.sum("victimas")).first()[0], 3774742)
v("nacional diario: filas",               spark.table(T("gold_hurto_diario_nacional")).count(), 8644)

# COMMAND ----------

# Vista rápida: serie nacional mensual de los últimos 14 meses
display(nm.orderBy(F.desc("fecha_mes")).limit(14))
