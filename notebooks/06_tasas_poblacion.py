# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Población (DANE) y tasas de hurto por 100 mil habitantes
# MAGIC
# MAGIC **Problema:** comparar municipios por número de víctimas es engañoso: Bogotá "gana" por tamaño. La tasa por 100 mil habitantes normaliza.
# MAGIC
# MAGIC **Qué hace este notebook**
# MAGIC 1. Carga `poblacion_municipal_dane.csv` (Volume `raw`) → tabla **Bronze** `bronze_poblacion_dane`.
# MAGIC 2. Construye la tabla **Gold** `gold_poblacion_municipio` (tipos correctos, solo `Total` por municipio y año).
# MAGIC 3. Verifica la cobertura contra el panel de hurtos.
# MAGIC 4. Calcula `gold_tasa_anual_municipio` y `gold_tasa_anual_nacional` (años completos 2018-2025).
# MAGIC 5. Compara el ranking por **conteo** vs por **tasa**.
# MAGIC
# MAGIC **Decisiones y advertencias**
# MAGIC - Solo años **completos** (2018-2025). 2026 está incompleto (hasta julio y con agosto provisional).
# MAGIC - Población: serie DANE 2005-2017 empalmada con la 2018-2042 (hay un quiebre leve en 2017→2018, +1,8 % nacional frente a ~1,2 % habitual).
# MAGIC - Municipios pequeños: la tasa es muy volátil. Se marca `pob_100k` y los rankings se hacen solo con municipios de **100 mil habitantes o más**.
# MAGIC - La tasa usa población **residente**. Ciudades con mucha población flotante (Bogotá) o municipios dormitorio/turísticos (Chía, Funza, Girardot) pueden verse infladas o distorsionadas.
# MAGIC - Bogotá tiene una anomalía de registro en nov-dic 2025: su tasa de 2025 está subestimada. La referencia para rankings es **2024**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros y Bronze

# COMMAND ----------

import numpy as np, pandas as pd
import matplotlib.pyplot as plt

CATALOG, SCHEMA = "workspace", "hurtos_co"
G = f"{CATALOG}.{SCHEMA}"
RUTA = f"/Volumes/{CATALOG}/{SCHEMA}/raw/poblacion_municipal_dane.csv"
ANIOS = list(range(2018, 2026))        # años completos
ANIO_REF = 2024

sdf = (spark.read.option("header", True).option("inferSchema", False).csv(RUTA))
print("Filas leídas:", sdf.count(), "| columnas:", sdf.columns)
sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{G}.bronze_poblacion_dane")
spark.sql(f"COMMENT ON TABLE {G}.bronze_poblacion_dane IS 'Bronze: poblacion municipal DANE (total por municipio y anio), tal cual el CSV preparado'")
print("OK bronze_poblacion_dane")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gold: población por municipio y año
# MAGIC Esperado: **42.661 filas**, **1.123 municipios**, años 2005-2042 y **cero duplicados** (cod_muni, anio).

# COMMAND ----------

pob = spark.table(f"{G}.bronze_poblacion_dane").toPandas()
pob["cod_muni"] = pob["cod_muni"].astype(str).str.strip().str.zfill(5)
pob["anio"] = pob["anio"].astype(int)
pob["poblacion"] = pob["poblacion"].astype(float).astype(int)
pob = pob[["cod_muni", "municipio_dane", "anio", "poblacion", "fuente"]]

print("Filas:", len(pob), "| municipios:", pob.cod_muni.nunique(), "| años:", pob.anio.min(), "-", pob.anio.max())
print("Duplicados (cod_muni, anio):", pob.duplicated(["cod_muni", "anio"]).sum())
print("Población nacional 2017 / 2018 / 2024:",
      [f"{pob[pob.anio == a].poblacion.sum():,}" for a in (2017, 2018, 2024)])

spark.createDataFrame(pob).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{G}.gold_poblacion_municipio")
spark.sql(f"COMMENT ON TABLE {G}.gold_poblacion_municipio IS 'Poblacion total DANE por municipio y anio (2005-2042). Serie 2005-2017 empalmada con 2018-2042'")
print("OK gold_poblacion_municipio")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cobertura frente a los hurtos
# MAGIC Todo municipio con víctimas 2018-2025 debe tener población. Esperado: **0 víctimas sin población**.

# COMMAND ----------

pm = spark.table(f"{G}.gold_hurto_mensual_municipio").toPandas()
dim = spark.table(f"{G}.gold_dim_municipio").toPandas()
pm["fecha_mes"] = pd.to_datetime(pm["fecha_mes"])
pm["anio"] = pm["fecha_mes"].dt.year

hz = pm[pm.anio.isin(ANIOS)].groupby(["cod_muni", "anio"], as_index=False)["victimas"].sum()
chk = hz.merge(pob[["cod_muni", "anio", "poblacion"]], on=["cod_muni", "anio"], how="left")
sin_pob = chk[chk.poblacion.isna()]
print("Víctimas 2018-2025 sin población:", int(sin_pob.victimas.sum()), "de", int(hz.victimas.sum()))
if len(sin_pob):
    display(sin_pob.head(10))
else:
    print("OK: todos los municipios con víctimas tienen población asociada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tasas anuales por 100 mil habitantes
# MAGIC `tasa_100k = víctimas del año / población del año × 100 000`

# COMMAND ----------

nombre = dim.set_index("cod_muni")[["municipio", "departamento"]]
T = chk.dropna(subset=["poblacion"]).copy()
T["poblacion"] = T["poblacion"].astype(int)
T["tasa_100k"] = T["victimas"] / T["poblacion"] * 1e5
T["pob_100k"] = T["poblacion"] >= 100_000
T = T.merge(nombre, left_on="cod_muni", right_index=True, how="left")
T = T[["cod_muni", "municipio", "departamento", "anio", "victimas", "poblacion", "tasa_100k", "pob_100k"]]

# Nacional: víctimas con ubicación municipal válida / población de todos los municipios
nv = hz.groupby("anio")["victimas"].sum()
npob = pob[pob.anio.isin(ANIOS)].groupby("anio")["poblacion"].sum()
N = pd.DataFrame({"victimas": nv, "poblacion": npob}).reset_index()
N["tasa_100k"] = N["victimas"] / N["poblacion"] * 1e5
N["var_tasa_pct"] = N["tasa_100k"].pct_change() * 100
display(N.round(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Guardar tablas Gold de tasas

# COMMAND ----------

def guardar(df, tabla, comentario):
    d = df.copy()
    for c in d.columns:
        if str(d[c].dtype).startswith("datetime"): d[c] = d[c].dt.date
        elif d[c].dtype == object: d[c] = d[c].astype(str)
    spark.createDataFrame(d).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{G}.{tabla}")
    spark.sql(f"COMMENT ON TABLE {G}.{tabla} IS '{comentario}'")
    print("OK", tabla, len(d))

guardar(T, "gold_tasa_anual_municipio", "Victimas, poblacion DANE y tasa por 100 mil hab. por municipio y anio completo 2018-2025")
guardar(N, "gold_tasa_anual_nacional", "Tasa nacional por 100 mil hab. por anio completo 2018-2025")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Conteo vs tasa: ¿cambia el ranking? (año de referencia 2024, municipios ≥ 100 mil hab.)
# MAGIC Si el ranking por conteo y por tasa fueran iguales, normalizar no aportaría nada.

# COMMAND ----------

R = T[(T.anio == ANIO_REF) & T.pob_100k].copy()
R["rank_conteo"] = R["victimas"].rank(ascending=False, method="min").astype(int)
R["rank_tasa"] = R["tasa_100k"].rank(ascending=False, method="min").astype(int)
print(f"Municipios ≥100 mil hab. en {ANIO_REF}: {len(R)}")
print("Correlación de Spearman conteo vs tasa:", round(R["victimas"].corr(R["tasa_100k"], method="spearman"), 2))
print("\nTop 15 por TASA:")
display(R.sort_values("tasa_100k", ascending=False).head(15)[["municipio", "departamento", "victimas", "poblacion", "tasa_100k", "rank_conteo", "rank_tasa"]].round(1))
print("Top 10 por CONTEO:")
display(R.sort_values("victimas", ascending=False).head(10)[["municipio", "victimas", "poblacion", "tasa_100k", "rank_conteo", "rank_tasa"]].round(1))
print("Ibagué:")
display(R[R.cod_muni == "73001"][["municipio", "victimas", "poblacion", "tasa_100k", "rank_conteo", "rank_tasa"]].round(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gráfico: top 15 por tasa (2024)

# COMMAND ----------

top = R.sort_values("tasa_100k", ascending=False).head(15).iloc[::-1]
fig, axs = plt.subplots(1, 2, figsize=(12, 5.5), sharey=True)
axs[0].barh(top.municipio, top.tasa_100k, color="#1f4e79"); axs[0].set_title(f"Tasa por 100 mil hab. ({ANIO_REF})")
axs[1].barh(top.municipio, top.victimas, color="#9aa5b1"); axs[1].set_title(f"Víctimas ({ANIO_REF})")
for a in axs: a.grid(axis="x", alpha=0.3)
fig.suptitle("Top 15 municipios (≥100 mil hab.) por tasa de hurto a personas", y=1.0)
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verificación final
# MAGIC Esperado: `gold_poblacion_municipio` 42.661 · `gold_tasa_anual_nacional` 8 filas · `gold_tasa_anual_municipio` > 8.000 filas.

# COMMAND ----------

for t in ["bronze_poblacion_dane", "gold_poblacion_municipio", "gold_tasa_anual_municipio", "gold_tasa_anual_nacional"]:
    print(f"{t}: {spark.table(f'{G}.{t}').count():,} filas")
