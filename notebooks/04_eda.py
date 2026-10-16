# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · EDA · Hurto a personas en Colombia
# MAGIC **Entrada:** tablas `gold_*` (pequeñas: la mayor tiene 279.379 filas, caben en pandas).
# MAGIC
# MAGIC **Preguntas que responde este notebook**
# MAGIC 1. ¿Los saltos de 2016 y 2017 son crecimiento real o cambios de registro? ¿Vienen de Bogotá?
# MAGIC 2. ¿Qué pasó en nov-dic de 2025 y en agosto de 2026?
# MAGIC 3. ¿Desde cuándo es razonable entrenar el modelo?
# MAGIC 4. ¿Hay estacionalidad mensual y semanal?
# MAGIC 5. ¿Qué tan concentrado está el hurto y qué tan dispersa (llena de ceros) es la serie por municipio?
# MAGIC
# MAGIC Nada de lo que se concluya aquí es causal: son patrones en datos de hechos **denunciados**.

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

CATALOG, SCHEMA = "workspace", "hurtos_co"
def T(name): return f"{CATALOG}.{SCHEMA}.{name}"

nac = spark.table(T("gold_hurto_mensual_nacional")).toPandas()
pm  = spark.table(T("gold_hurto_mensual_municipio")).toPandas()
dim = spark.table(T("gold_dim_municipio")).toPandas()
dia = spark.table(T("gold_hurto_diario_nacional")).toPandas()

for d, c in [(nac, "fecha_mes"), (pm, "fecha_mes"), (dia, "fecha")]:
    d[c] = pd.to_datetime(d[c])
for d in (nac, pm, dia):
    d["victimas"] = d["victimas"].astype(int)

nac = nac.set_index("fecha_mes").sort_index()
IDX = pd.date_range(nac.index.min(), nac.index.max(), freq="MS")   # calendario completo (2008 queda como hueco en el panel)
print("nacional:", nac.shape, "| panel municipal:", pm.shape, "| diario:", dia.shape)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Serie nacional mensual (todos los años, incluido 2008)

# COMMAND ----------

ser = nac["victimas"].reindex(IDX)
fig, ax = plt.subplots(figsize=(12, 4.5))
ax.plot(ser.index, ser.values, lw=1.5, color="#1f4e79")
for fecha, texto in [("2016-01-01", "ene 2016"), ("2017-08-01", "ago 2017"), ("2020-03-01", "mar 2020")]:
    ax.axvline(pd.Timestamp(fecha), color="#c0392b", ls="--", lw=0.9)
    ax.text(pd.Timestamp(fecha), ser.max() * 0.98, " " + texto, color="#c0392b", fontsize=9, va="top")
ax.set_title("Víctimas de hurto a personas por mes, Colombia")
ax.set_ylabel("Víctimas"); ax.grid(alpha=0.25)
plt.tight_layout(); plt.show()

anual = nac.groupby("anio")["victimas"].sum().to_frame("victimas")
anual["var_anual_%"] = (anual["victimas"].pct_change() * 100).round(1)
display(anual.reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ¿De dónde vienen los saltos? Bogotá frente al resto del país
# MAGIC Se usa el panel municipal, que excluye 2008 y las filas sin municipio.

# COMMAND ----------

tot_mes = pm.groupby("fecha_mes")["victimas"].sum().reindex(IDX)
bog = pm[pm["cod_muni"] == "11001"].set_index("fecha_mes")["victimas"].reindex(IDX)
resto = tot_mes - bog

fig, ax = plt.subplots(figsize=(12, 4.5))
ax.plot(IDX, bog.values, lw=1.5, color="#c0392b", label="Bogotá D.C.")
ax.plot(IDX, resto.values, lw=1.5, color="#1f4e79", label="Resto del país")
ax.set_title("Víctimas por mes: Bogotá frente al resto del país (sin 2008)")
ax.set_ylabel("Víctimas"); ax.grid(alpha=0.25); ax.legend()
plt.tight_layout(); plt.show()

def escalon(serie, fecha, ventana):
    """Media mensual en `ventana` meses antes frente a `ventana` meses desde `fecha` (inclusive)."""
    f = pd.Timestamp(fecha)
    antes = serie[f - pd.DateOffset(months=ventana): f - pd.DateOffset(months=1)].mean()
    despues = serie[f: f + pd.DateOffset(months=ventana - 1)].mean()
    return antes, despues

filas = []
for nombre, fecha, vent in [("ene 2016", "2016-01-01", 12), ("ago 2017", "2017-08-01", 6)]:
    for serie_nombre, serie in [("Nacional (panel)", tot_mes), ("Bogotá", bog), ("Resto", resto)]:
        a, d = escalon(serie, fecha, vent)
        filas.append({"escalon": nombre, "serie": serie_nombre, "media_antes": round(a), "media_despues": round(d),
                      "cambio_%": round((d / a - 1) * 100, 1)})
display(pd.DataFrame(filas))

por_anio = pd.DataFrame({"bogota": bog.groupby(bog.index.year).sum(), "resto": resto.groupby(resto.index.year).sum()})
por_anio["pct_bogota"] = (por_anio["bogota"] / (por_anio["bogota"] + por_anio["resto"]) * 100).round(1)
display(por_anio.reset_index().rename(columns={"index": "anio", "fecha_mes": "anio"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Anomalías recientes: nov-dic 2025 y agosto 2026

# COMMAND ----------

ult = pd.DataFrame({"total": tot_mes, "bogota": bog, "resto": resto})["2024-09-01":]
ult["pct_bogota"] = (ult["bogota"] / ult["total"] * 100).round(1)
display(ult.reset_index().rename(columns={"index": "mes"}))

fig, ax = plt.subplots(figsize=(12, 4))
ax.plot(ult.index, ult["bogota"], lw=1.6, color="#c0392b", label="Bogotá")
ax.plot(ult.index, ult["resto"], lw=1.6, color="#1f4e79", label="Resto")
ax.axvspan(pd.Timestamp("2025-11-01"), pd.Timestamp("2025-12-31"), color="#f5b041", alpha=0.3, label="nov-dic 2025")
ax.axvline(pd.Timestamp("2026-08-01"), color="gray", ls=":", lw=1)
ax.set_title("Últimos meses: Bogotá y resto del país"); ax.grid(alpha=0.25); ax.legend()
plt.tight_layout(); plt.show()

ref_bog = bog["2025-01-01":"2025-10-01"].mean()
ref_resto = resto["2025-01-01":"2025-10-01"].mean()
print(f"Bogotá nov-25 / media ene-oct 2025 : {bog['2025-11-01'] / ref_bog:.2f}")
print(f"Bogotá dic-25 / media ene-oct 2025 : {bog['2025-12-01'] / ref_bog:.2f}")
print(f"Resto  nov-25 / media ene-oct 2025 : {resto['2025-11-01'] / ref_resto:.2f}")
print(f"Resto  dic-25 / media ene-oct 2025 : {resto['2025-12-01'] / ref_resto:.2f}")
print(f"Bogotá ago-26 / media may-jul 2026 : {bog['2026-08-01'] / bog['2026-05-01':'2026-07-01'].mean():.2f}")
print(f"Resto  ago-26 / media may-jul 2026 : {resto['2026-08-01'] / resto['2026-05-01':'2026-07-01'].mean():.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ventana de entrenamiento y estacionalidad mensual
# MAGIC Propuesta de ventana: **desde 2017-09** (después del segundo escalón). Para medir estacionalidad se usan solo años completos y sin eventos raros: 2018, 2019, 2021, 2022, 2023, 2024 (se excluyen 2020 por la pandemia y 2025 por la anomalía de nov-dic).

# COMMAND ----------

INICIO = pd.Timestamp("2017-09-01")
anios_limpios = [2018, 2019, 2021, 2022, 2023, 2024]
v = nac["victimas"]
idx_est = {}
for a in anios_limpios:
    s = v[v.index.year == a]
    idx_est[a] = (s / s.mean()).values
est = pd.DataFrame(idx_est, index=range(1, 13))
est["indice_medio"] = est.mean(axis=1)
est["desv"] = est[anios_limpios].std(axis=1)
nombres = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]

fig, ax = plt.subplots(figsize=(10, 4))
ax.bar(nombres, est["indice_medio"], color="#1f4e79", yerr=est["desv"], capsize=3, ecolor="#7f8c8d")
ax.axhline(1, color="#c0392b", lw=1, ls="--")
ax.set_ylim(0.7, 1.3)
ax.set_title("Índice estacional mensual (1 = promedio del año), años 2018, 2019, 2021-2024")
ax.grid(axis="y", alpha=0.25)
plt.tight_layout(); plt.show()
display(est.round(3).reset_index().rename(columns={"index": "mes"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Estacionalidad semanal (día del hecho)

# COMMAND ----------

d = dia[(dia["fecha"] >= "2018-01-01") & (dia["fecha"] <= "2024-12-31") & (dia["fecha"].dt.year != 2020)].copy()
d["dow"] = d["fecha"].dt.dayofweek
sem = d.groupby("dow")["victimas"].mean()
sem_idx = sem / sem.mean()
dias_nombre = ["lun", "mar", "mié", "jue", "vie", "sáb", "dom"]

fig, ax = plt.subplots(figsize=(9, 3.8))
ax.bar(dias_nombre, sem_idx.values, color="#1f4e79")
ax.axhline(1, color="#c0392b", lw=1, ls="--")
ax.set_ylim(0.7, 1.3)
ax.set_title("Índice por día de la semana (1 = promedio diario), 2018-2024 sin 2020")
ax.grid(axis="y", alpha=0.25)
plt.tight_layout(); plt.show()
print((sem_idx.round(3)).to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Concentración geográfica (Pareto) en 2018-2024

# COMMAND ----------

w = pm[(pm["fecha_mes"] >= "2018-01-01") & (pm["fecha_mes"] <= "2024-12-01")]
por_muni = w.groupby("cod_muni")["victimas"].sum().sort_values(ascending=False)
acum = por_muni.cumsum() / por_muni.sum()
for p in (0.5, 0.8, 0.9):
    print(f"Municipios necesarios para el {int(p*100)} % de las víctimas: {int((acum < p).sum() + 1)} de {len(por_muni)}")

top = (por_muni.head(10).to_frame("victimas").join(dim.set_index("cod_muni")[["municipio", "departamento"]])
       .assign(pct=lambda x: (x["victimas"] / por_muni.sum() * 100).round(1)))
display(top.reset_index())

fig, ax = plt.subplots(figsize=(8, 4))
ax.plot(np.arange(1, len(acum) + 1), acum.values * 100, color="#1f4e79")
ax.set_xscale("log"); ax.set_xlabel("Número de municipios (escala log, de mayor a menor)")
ax.set_ylabel("% acumulado de víctimas"); ax.grid(alpha=0.25)
ax.set_title("Curva de concentración, 2018-2024")
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Dispersión por municipio (¿se puede predecir cada municipio por separado?)

# COMMAND ----------

wv = pm[(pm["fecha_mes"] >= INICIO) & (pm["fecha_mes"] <= "2026-07-01")]
res = wv.groupby("cod_muni").agg(victimas=("victimas", "sum"), meses=("victimas", "size"),
                                  meses_cero=("victimas", lambda x: (x == 0).sum()))
res["pct_meses_cero"] = res["meses_cero"] / res["meses"] * 100
res = res.sort_values("victimas", ascending=False).reset_index()
res["rango"] = np.arange(1, len(res) + 1)
res["grupo"] = pd.cut(res["rango"], [0, 10, 50, 200, 10**6], labels=["Top 10", "11-50", "51-200", "201+"])
resumen = (res.groupby("grupo", observed=True)
              .agg(municipios=("cod_muni", "size"), victimas=("victimas", "sum"),
                   mediana_pct_meses_cero=("pct_meses_cero", "median")))
resumen["pct_victimas"] = (resumen["victimas"] / resumen["victimas"].sum() * 100).round(1)
resumen["mediana_pct_meses_cero"] = resumen["mediana_pct_meses_cero"].round(1)
display(resumen.reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Descomposición STL de la serie nacional (2017-09 a 2025-10)
# MAGIC Se corta en octubre de 2025 para no contaminar la descomposición con la anomalía de nov-dic.

# COMMAND ----------

try:
    from statsmodels.tsa.seasonal import STL
    y = v["2017-09-01":"2025-10-01"].astype(float)
    y.index = pd.DatetimeIndex(y.index, freq="MS")
    r = STL(y, period=12, robust=True).fit()
    fig, axs = plt.subplots(3, 1, figsize=(12, 7), sharex=True)
    axs[0].plot(r.trend, color="#1f4e79"); axs[0].set_title("Tendencia")
    axs[1].plot(r.seasonal, color="#1f4e79"); axs[1].set_title("Estacionalidad")
    axs[2].plot(r.resid, color="#7f8c8d"); axs[2].set_title("Residuo")
    for a in axs: a.grid(alpha=0.25)
    plt.tight_layout(); plt.show()
    fs = max(0, 1 - r.resid.var() / (r.seasonal + r.resid).var())
    ft = max(0, 1 - r.resid.var() / (r.trend + r.resid).var())
    print(f"Fuerza de estacionalidad: {fs:.2f} | fuerza de tendencia: {ft:.2f} (0 a 1)")
except ImportError:
    print("statsmodels no está disponible en este entorno: omitir esta celda o ejecutar %pip install statsmodels")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decisiones a confirmar con estos resultados
# MAGIC 1. **Ventana de entrenamiento**: desde 2017-09 si los escalones de 2016 y 2017 se confirman como cambios de nivel concentrados en Bogotá.
# MAGIC 2. **Nov-dic de 2025**: tratarlas como anomalía (enmascarar o imputar), no como señal.
# MAGIC 3. **Agosto de 2026**: excluir de la evaluación por posible registro incompleto.
# MAGIC 4. **Alcance del modelo municipal**: modelar por separado solo los municipios con volumen suficiente (según la sección 7) y agrupar el resto.
# MAGIC 5. **Estacionalidad**: incluir mes y día de la semana como variables si los índices se separan claramente de 1.
