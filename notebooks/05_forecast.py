# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Predicción (forecasting) de hurto a personas
# MAGIC
# MAGIC **Objetivo:** pronosticar las víctimas mensuales de hurto a personas a **6 meses** (nacional y top 50 municipios) y **medir honestamente** si un modelo sofisticado aporta algo frente a métodos simples.
# MAGIC
# MAGIC **Metodología (decisiones tomadas tras el EDA):**
# MAGIC - Ventana de datos: **2017-09 → 2026-07** (después del escalón de registro de 2017; agosto 2026 se excluye por estar incompleto).
# MAGIC - Se **enmascaran** (se interpolan solo para entrenar) el **COVID (2020-03..08)** y la **anomalía de Bogotá (2025-11 y 2025-12)**. Esos meses **no se usan para puntuar**.
# MAGIC - **Backtest de origen móvil** (rolling origin): 38 orígenes (2022-12 → 2026-01), horizontes 1-6 meses. Nunca se usa información posterior al origen.
# MAGIC - Métrica: **WAPE** = Σ|error| / Σ real (robusta con series pequeñas). Los municipios con pocas víctimas son muy ruidosos.
# MAGIC - Candidatos: ingenuo, media móvil 3 y 12, estacional ingenuo, **ETS amortiguado**, **LightGBM global** y mezcla **MM3+ETS**.
# MAGIC
# MAGIC > Resultado esperado a validar al ejecutar: ganan los métodos simples; LightGBM empeora. Eso se documenta, no se esconde.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Instalar librerías
# MAGIC Databricks serverless no trae `statsmodels` ni `lightgbm`. Esta celda las instala y la siguiente reinicia Python (es normal que se pierdan las variables; este notebook empieza después).

# COMMAND ----------

# MAGIC %pip install statsmodels lightgbm

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros y carga desde Gold

# COMMAND ----------

import warnings, numpy as np, pandas as pd
import matplotlib.pyplot as plt
warnings.filterwarnings("ignore")

CATALOG, SCHEMA = "workspace", "hurtos_co"
G = f"{CATALOG}.{SCHEMA}"
INI, FIN = pd.Timestamp("2017-09-01"), pd.Timestamp("2026-07-01")   # FIN = último mes confiable
H = 6
COVID = pd.date_range("2020-03-01", "2020-08-01", freq="MS")
ANOM_BOG = pd.to_datetime(["2025-11-01", "2025-12-01"])

pm = spark.table(f"{G}.gold_hurto_mensual_municipio").toPandas()
dia = spark.table(f"{G}.gold_hurto_diario_nacional").toPandas()
dim = spark.table(f"{G}.gold_dim_municipio").toPandas()
pm["fecha_mes"] = pd.to_datetime(pm["fecha_mes"])
dia["fecha"] = pd.to_datetime(dia["fecha"])
print("Panel municipal:", pm.shape, "| meses:", pm.fecha_mes.min().date(), "→", pm.fecha_mes.max().date())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Matriz mensual (serie × mes) y series a pronosticar
# MAGIC Se toman **NACIONAL** (suma de municipios con ubicación válida) y los **50 municipios con más víctimas en 2018-2024**.
# MAGIC El resto del país queda dentro de NACIONAL.

# COMMAND ----------

MESES = pd.date_range(INI, FIN, freq="MS")
wide = (pm.pivot_table(index="fecha_mes", columns="cod_muni", values="victimas", aggfunc="sum")
          .reindex(MESES).fillna(0.0))
T = len(MESES)

top = (pm[(pm.fecha_mes >= "2018-01-01") & (pm.fecha_mes <= "2024-12-01")]
       .groupby("cod_muni")["victimas"].sum().sort_values(ascending=False).head(50))
TOP = list(top.index)
nombre = dim.set_index("cod_muni")["municipio"].to_dict()

# Serie ajustada: se interpola COVID (todas las series) y Bogotá 2025-11/12 solo para entrenar
wide_adj = wide.copy()
for m in ANOM_BOG: wide_adj.loc[m, "11001"] = np.nan
for m in COVID:    wide_adj.loc[m, :] = np.nan
wide_adj = wide_adj.interpolate(method="linear", limit_direction="both")

obs = {"NACIONAL": wide.sum(axis=1)};  adj = {"NACIONAL": wide_adj.sum(axis=1)}
for c in TOP: obs[c] = wide[c]; adj[c] = wide_adj[c]
SERIES = list(obs)
GRUPO = {s: ("nacional" if s == "NACIONAL" else ("top10" if TOP.index(s) < 10 else "11-50")) for s in SERIES}
print("Series:", len(SERIES), "| meses:", T, MESES[0].date(), "→", MESES[-1].date())
display(pd.DataFrame({"municipio": [nombre.get(c, c) for c in TOP[:10]], "victimas_2018_2024": top.head(10).values}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Efecto calendario (composición de días de la semana del mes)
# MAGIC Índice por día de la semana estimado **solo con datos hasta 2022-11** (antes del primer origen → sin fuga). Se usa como variable del modelo LightGBM.

# COMMAND ----------

base = dia[(dia.fecha >= "2018-01-01") & (dia.fecha <= "2022-11-30") & (dia.fecha.dt.year != 2020)].set_index("fecha")["victimas"]
dow_idx = (base.groupby(base.index.dayofweek).mean() / base.mean()).values
def cal_eff(m):
    d = pd.date_range(m, m + pd.offsets.MonthEnd(0), freq="D")
    return float(np.mean(dow_idx[d.dayofweek]))
CAL = np.array([cal_eff(m) for m in list(MESES) + [FIN + pd.offsets.MonthBegin(k) for k in range(1, H + 1)]])
print("Índice por día (lun..dom):", np.round(dow_idx, 3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Modelos
# MAGIC - **naive**: último valor. **ma3 / ma12**: promedio de 3 / 12 meses. **snaive**: mismo mes del año anterior.
# MAGIC - **ETS amortiguado**: nivel + tendencia amortiguada (sin estacionalidad: el EDA mostró estacionalidad débil).
# MAGIC - **mezcla MM3+ETS**: promedio de ma3 y ETS.

# COMMAND ----------

from statsmodels.tsa.holtwinters import ExponentialSmoothing

def pron_simples(y, o):
    """y: serie ajustada (np.array); o: índice del origen. Devuelve dict modelo -> array(H)."""
    out = {"naive": np.repeat(y[o], H),
           "ma3": np.repeat(y[o - 2:o + 1].mean(), H),
           "ma12": np.repeat(y[o - 11:o + 1].mean(), H)}
    out["snaive"] = np.array([y[o + h - 12] if o + h - 12 >= 0 and o + h - 12 <= o else np.nan for h in range(1, H + 1)])
    try:
        out["ets"] = ExponentialSmoothing(y[:o + 1], trend="add", damped_trend=True, seasonal=None,
                                          initialization_method="estimated").fit(optimized=True).forecast(H)
    except Exception:
        out["ets"] = np.repeat(y[o], H)
    out["ma3ets"] = (out["ma3"] + out["ets"]) / 2
    return out

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Backtest de origen móvil
# MAGIC Para cada origen **o** (2022-12 … 2026-01) se pronostica o+1 … o+6 usando solo datos ≤ o, y se compara contra lo **observado** (sin ajustar).
# MAGIC Se descartan del puntaje los meses anómalos de Bogotá/Nacional (2025-11, 2025-12).

# COMMAND ----------

ORIG = list(range(list(MESES).index(pd.Timestamp("2022-12-01")), list(MESES).index(pd.Timestamp("2026-01-01")) + 1))
filas = []
for s in SERIES:
    y, yo = adj[s].values.astype(float), obs[s].values.astype(float)
    for o in ORIG:
        P = pron_simples(y, o)
        for h in range(1, H + 1):
            t = o + h
            if t >= T: continue
            tm = MESES[t]
            if s in ("NACIONAL", "11001") and tm in ANOM_BOG: continue   # no se puntúa
            r = dict(serie=s, grupo=GRUPO[s], origen=MESES[o], h=h, mes_objetivo=tm, real=yo[t])
            r.update({m: P[m][h - 1] for m in P})
            filas.append(r)
BT = pd.DataFrame(filas)
MODELOS = ["naive", "ma3", "ma12", "snaive", "ets", "ma3ets"]
print("Filas de backtest:", len(BT), "| orígenes:", len(ORIG))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resultados del backtest (WAPE %, menor = mejor)

# COMMAND ----------

def wape(g, m): return (g[m] - g["real"]).abs().sum() / g["real"].sum() * 100

res = []
for gr in ["nacional", "top10", "11-50"]:
    g = BT[BT.grupo == gr]
    for h, x in list(g.groupby("h")) + [("todos", g)]:
        res.append(dict(grupo=gr, h=h, **{m: round(wape(x, m), 1) for m in MODELOS}))
RES = pd.DataFrame(res)
display(RES)

sesgo = {m: round((BT[BT.grupo == "nacional"][m].sum() - BT[BT.grupo == "nacional"]["real"].sum()) / BT[BT.grupo == "nacional"]["real"].sum() * 100, 1) for m in MODELOS}
print("Sesgo nacional (% pred − real). Positivo = sobreestima:", sesgo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Retador: LightGBM global (modelo único para todas las series)
# MAGIC Predicción directa multi-horizonte sobre series **normalizadas por su escala** (media de los últimos 12 meses): rezagos 0-11, medias 3 y 6 meses, variación anual, mes, efecto calendario y horizonte.
# MAGIC Es el tipo de modelo que “luce” en un portafolio; aquí se evalúa con el mismo backtest. Si no gana, se dice.

# COMMAND ----------

try:
    import lightgbm as lgb
    rows = []
    for s in SERIES:
        y, yo = adj[s].values.astype(float), obs[s].values.astype(float)
        for i in range(14, T - 1):
            sc = y[i - 11:i + 1].mean()
            if sc <= 0: continue
            lg = {f"l{k}": y[i - k] / sc for k in range(12)}
            r3, r6 = y[i - 2:i + 1].mean() / sc, y[i - 5:i + 1].mean() / sc
            yoy3 = y[i - 2:i + 1].mean() / max(y[i - 14:i - 11].mean(), 1e-9)
            for h in range(1, H + 1):
                t = i + h
                if t >= T: break
                tm = MESES[t]
                rows.append(dict(serie=s, i=i, h=h, t=t, scale=sc, lsc=np.log(sc), r3=r3, r6=r6, yoy3=yoy3, **lg,
                                 month=tm.month, cal=CAL[t], cal0=CAL[i], target=y[t] / sc,
                                 excl=(tm in COVID) or (s in ("NACIONAL", "11001") and tm in ANOM_BOG)))
    D = pd.DataFrame(rows)
    FEATS = [f"l{k}" for k in range(12)] + ["r3", "r6", "yoy3", "lsc", "month", "cal", "cal0", "h"]
    preds = []
    for o in ORIG:
        tr = D[(D.t <= o) & (~D.excl)]
        te = D[D.i == o]
        mdl = lgb.LGBMRegressor(objective="regression_l1", n_estimators=250, learning_rate=0.04, num_leaves=15,
                                min_child_samples=25, subsample=0.8, subsample_freq=1, colsample_bytree=0.8,
                                reg_lambda=1.0, verbose=-1, n_jobs=2, random_state=7)
        mdl.fit(tr[FEATS], tr["target"])
        preds.append(pd.DataFrame({"serie": te.serie.values, "origen": MESES[o], "h": te.h.values,
                                   "lgb": mdl.predict(te[FEATS]) * te.scale.values}))
    L = pd.concat(preds)
    BT = BT.merge(L, on=["serie", "origen", "h"], how="left")
    MODELOS_ALL = MODELOS + ["lgb"]
    r2 = []
    for gr in ["nacional", "top10", "11-50"]:
        g = BT[BT.grupo == gr]
        r2.append(dict(grupo=gr, **{m: round(wape(g, m), 1) for m in MODELOS_ALL}))
    print("WAPE % todos los horizontes (incluye LightGBM):")
    display(pd.DataFrame(r2))
except ImportError:
    MODELOS_ALL = MODELOS
    print("lightgbm no está instalado: ejecuta la celda 0 y reinicia. Se omite el retador.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Pronóstico final (origen = 2026-07) con intervalos empíricos
# MAGIC - **Modelo elegido: mezcla MM3+ETS** (empata con las mejores opciones, es simple y estable).
# MAGIC - **Intervalo 80%:** cuantiles 10/90 del error relativo `real / pronóstico − 1` del backtest, por grupo y horizonte.
# MAGIC - Ojo: las series tienen **deriva** (el nivel baja desde 2023), así que el error no es estacionario y los intervalos pueden quedarse cortos. Se verifica la cobertura abajo.

# COMMAND ----------

BT["rel"] = BT["real"] / BT["ma3ets"] - 1
Q = BT.groupby(["grupo", "h"])["rel"].quantile([0.1, 0.9]).unstack().rename(columns={0.1: "q10", 0.9: "q90"}).reset_index()
cobertura = BT.merge(Q, on=["grupo", "h"])
cobertura["dentro"] = (cobertura.rel >= cobertura.q10) & (cobertura.rel <= cobertura.q90)
print("Cobertura dentro de la muestra (nominal 80%):", round(cobertura.dentro.mean() * 100, 1), "%")

# Cobertura fuera de muestra (honesta): para cada origen, cuantiles calculados solo con orígenes ANTERIORES
oos = []
for (gr, h), x in BT.groupby(["grupo", "h"]):
    x = x.sort_values("origen")
    for ori in sorted(x.origen.unique())[12:]:
        pasado = x[x.origen < ori - pd.DateOffset(months=h)]["rel"]   # solo errores ya observables al origen
        act = x[x.origen == ori]["rel"]
        if len(pasado) < 30: continue
        lo, hi = pasado.quantile(0.1), pasado.quantile(0.9)
        oos += list(((act >= lo) & (act <= hi)).values)
print("Cobertura FUERA de muestra (nominal 80%):", round(np.mean(oos) * 100, 1), "% sobre", len(oos), "pronósticos")

o = T - 1
fut = pd.date_range(FIN + pd.offsets.MonthBegin(1), periods=H, freq="MS")
out = []
for s in SERIES:
    P = pron_simples(adj[s].values.astype(float), o)["ma3ets"]
    gq = Q[Q.grupo == GRUPO[s]].set_index("h")
    for h in range(1, H + 1):
        out.append(dict(serie=s, municipio=("NACIONAL" if s == "NACIONAL" else nombre.get(s, s)), grupo=GRUPO[s],
                        mes=fut[h - 1], h=h, pronostico=float(P[h - 1]),
                        p10=float(P[h - 1] * (1 + gq.loc[h, "q10"])), p90=float(P[h - 1] * (1 + gq.loc[h, "q90"]))))
FC = pd.DataFrame(out)
display(FC[FC.serie == "NACIONAL"].round(0))

# Chequeo de cordura: agosto 2026 observado (incompleto) vs pronóstico
ago = pm[pm.fecha_mes == "2026-08-01"]["victimas"].sum()
f_ago = FC[(FC.serie == "NACIONAL") & (FC.h == 1)]["pronostico"].iloc[0]
print(f"Agosto 2026 observado (provisional): {ago:,.0f} | pronóstico: {f_ago:,.0f} | observado/pronóstico = {ago / f_ago:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Gráfico: histórico + pronóstico nacional

# COMMAND ----------

n = FC[FC.serie == "NACIONAL"]
fig, ax = plt.subplots(figsize=(11, 4.5))
ax.plot(MESES, obs["NACIONAL"].values, color="#1f4e79", label="Observado")
ax.plot(fut, n.pronostico, color="#d9534f", marker="o", label="Pronóstico (MM3+ETS)")
ax.fill_between(fut, n.p10, n.p90, color="#d9534f", alpha=0.2, label="Intervalo 80% empírico")
ax.axvspan(COVID[0], COVID[-1], color="grey", alpha=0.15, label="COVID (enmascarado)")
ax.axvspan(ANOM_BOG[0], ANOM_BOG[-1], color="orange", alpha=0.2, label="Anomalía Bogotá")
ax.set_title("Hurto a personas · víctimas mensuales (municipios con ubicación válida)")
ax.legend(loc="lower left"); ax.grid(alpha=0.3); plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Guardar resultados en Gold

# COMMAND ----------

def guardar(df, tabla, comentario):
    d = df.copy()
    for c in d.columns:
        if str(d[c].dtype).startswith("datetime"): d[c] = d[c].dt.date
        elif d[c].dtype == object: d[c] = d[c].astype(str)     # evita columnas mixtas (p. ej. h = 1..6 y "todos")
    spark.createDataFrame(d).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{G}.{tabla}")
    spark.sql(f"COMMENT ON TABLE {G}.{tabla} IS '{comentario}'")
    print("OK", tabla, len(d))

guardar(FC, "gold_forecast_resultados", "Pronostico 6 meses (MM3+ETS) con intervalo empirico 80%, origen 2026-07")
guardar(BT[["serie", "grupo", "origen", "h", "mes_objetivo", "real"] + MODELOS_ALL], "gold_forecast_backtest",
        "Backtest rolling-origin: real vs pronostico por modelo, origen y horizonte")
guardar(RES, "gold_forecast_metricas", "WAPE por grupo, horizonte y modelo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Conclusiones (se actualizan con los resultados de tu ejecución)
# MAGIC 1. **No hay modelo mágico:** los métodos simples (último valor, media móvil 3, ETS amortiguado) empatan en las primeras posiciones. LightGBM, entrenado sobre el período 2017-2022 con nivel más alto, **sobreestima** y pierde.
# MAGIC 2. **La estacionalidad es débil**: el estacional ingenuo es el peor.
# MAGIC 3. **El error crece con el horizonte** y con lo pequeño del municipio (WAPE nacional ≈ 6-11 %, top10 ≈ 10-16 %, resto ≈ 17-28 %).
# MAGIC 4. **Deriva a la baja desde 2023:** todos los modelos sobreestiman en promedio (≈ 4-7 % a nivel nacional) y los intervalos empíricos calculados solo con el pasado cubren menos del 80 % nominal (revisa la cifra de la celda 8). Úsalos como orientación, no como garantía.
# MAGIC 5. Son delitos **denunciados**: los cambios de registro, no solo del delito real, mueven la serie.
