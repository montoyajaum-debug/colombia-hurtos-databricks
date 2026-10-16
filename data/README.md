# Datos

**Los datos no se versionan en este repositorio.** Se descargan de la fuente oficial.

## Fuente
| Campo | Valor |
|---|---|
| Dataset | HURTO PERSONAS |
| Publicador | Ministerio de Defensa Nacional, Observatorio MDN |
| Portal | datos.gov.co |
| ID | `4rxi-8m8d` |
| URL | https://www.datos.gov.co/d/4rxi-8m8d |
| Licencia | Creative Commons Atribución-CompartirIgual 4.0 (CC BY-SA 4.0) |
| Frecuencia de actualización | Mensual (según la ficha) |
| Fecha de extracción usada en este proyecto | 16/10/2026 |
| Archivo | `hurto_personas_20261016.csv` (35.062.496 bytes) |

## Estructura
Una fila por día y municipio. La variable `cantidad` mide **víctimas**, no denuncias.

| Columna | Descripción |
|---|---|
| `FECHA HECHO` | Fecha del hecho, formato dd/mm/aaaa |
| `COD_DEPTO` | Código DANE del departamento (texto) |
| `DEPARTAMENTO` | Nombre del departamento |
| `COD_MUNI` | Código DANE del municipio (texto, conserva ceros a la izquierda) |
| `MUNICIPIO` | Nombre del municipio |
| `CANTIDAD` | Número de víctimas ese día en ese municipio |

## Cómo reproducir
1. Entrar a https://www.datos.gov.co/d/4rxi-8m8d
2. **Exportar → CSV**.
3. Renombrar a `hurto_personas_AAAAMMDD.csv` con la fecha de descarga y colocarlo en la raíz del proyecto local (está excluido por `.gitignore`).
4. Subirlo al Volume `/Volumes/workspace/hurtos_co/raw/` de Databricks y ejecutar `notebooks/01_bronze_ingesta.py`.

Si descargas una fecha distinta, el número de filas y los totales cambiarán, porque el dataset se actualiza cada mes y las cifras están sujetas a variación.

## Atribución
Datos: Ministerio de Defensa Nacional, Observatorio MDN, vía datos.gov.co, bajo licencia CC BY-SA 4.0. Las tablas derivadas de estos datos deben conservar la atribución y compartirse bajo la misma licencia.

## Población (denominador para tasas por 100 mil habitantes)

- Fuente: DANE, *Proyecciones de población* (https://www.dane.gov.co/index.php/estadisticas-por-tema/demografia-y-poblacion/proyecciones-de-poblacion).
- Archivos (no se versionan, `*.xlsx` está en `.gitignore`): `DCD-area-proypoblacion-Mun-2005-2017_VP.xlsx` (serie 2005-2017) y `PPED-AreaMun-2018-2042_VP.xlsx` (serie 2018-2042, actualizada julio/agosto 2025). Se usa solo la fila `Total` por municipio y año.
- Resultado: `poblacion_municipal_dane.csv` (cod_muni, municipio_dane, anio, poblacion, fuente), 1.123 municipios, 2005-2042.
- Empalme 2017→2018: población nacional +1,8 % (crecimiento anual típico ≈ 1,2 %); salto mediano municipal +1,2 %. Hay un quiebre leve por el cambio de serie, aceptable para tasas.
