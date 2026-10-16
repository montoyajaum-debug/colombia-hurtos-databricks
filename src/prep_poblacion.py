"""Prepara la tabla de población municipal del DANE para el proyecto.

Entrada (en data/, descargados de la página de proyecciones de población del DANE):
  - DCD-area-proypoblacion-Mun-2005-2017_VP.xlsx
  - PPED-AreaMun-2018-2042_VP.xlsx
Salida: data/poblacion_municipal_dane.csv (cod_muni, municipio_dane, anio, poblacion, fuente)

Uso (desde la raíz del repositorio):  python src/prep_poblacion.py
"""
from pathlib import Path
import pandas as pd

DATA = Path(__file__).resolve().parent.parent / "data"


def total_por_municipio(df: pd.DataFrame, valor: str, fuente: str) -> pd.DataFrame:
    d = df[df["ÁREA GEOGRÁFICA"].astype(str).str.strip() == "Total"]
    return pd.DataFrame({
        "cod_muni": d["MPIO"].str.strip().str.zfill(5),
        "municipio_dane": d["DPMP"].str.strip(),
        "anio": d["AÑO"].astype(int),
        "poblacion": d[valor].astype(float).astype(int),
        "fuente": fuente,
    })


a = pd.read_excel(DATA / "DCD-area-proypoblacion-Mun-2005-2017_VP.xlsx", sheet_name="NuevaMpal", header=11, dtype=str)
b = pd.read_excel(DATA / "PPED-AreaMun-2018-2042_VP.xlsx", sheet_name="PobMunicipalxÁrea", header=7, dtype=str)
b = b[b["ÁREA GEOGRÁFICA"].notna()]

pob = pd.concat([total_por_municipio(a, "Población", "2005-2017"),
                 total_por_municipio(b, "TOTAL", "2018-2042")], ignore_index=True)

assert not pob.duplicated(["cod_muni", "anio"]).any(), "Hay municipios duplicados por año"
nac = pob.groupby("anio")["poblacion"].sum()
print(f"Filas: {len(pob):,} | municipios: {pob.cod_muni.nunique():,} | años: {pob.anio.min()}-{pob.anio.max()}")
print(f"Empalme 2017 -> 2018, población nacional: {nac[2018] / nac[2017] - 1:+.1%}")

pob.to_csv(DATA / "poblacion_municipal_dane.csv", index=False)
print("Escrito:", DATA / "poblacion_municipal_dane.csv")
