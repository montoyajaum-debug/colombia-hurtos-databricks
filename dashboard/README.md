# Dashboard

Dashboard interactivo construido sobre las tablas Gold del proyecto (`gold_tasa_anual_nacional`, `gold_tasa_anual_municipio`, `gold_forecast_resultados`, `gold_forecast_backtest` y el hurto mensual nacional).

Secciones:
- Cinco cifras clave: tasa nacional 2025, cambio frente al pico de 2023, víctimas 2025, pronóstico mensual y error del pronóstico.
- Serie mensual desde septiembre de 2017 con pronóstico a seis meses y su rango del 80 %.
- Tasa nacional por año.
- Error de cada modelo (WAPE), con filtro por grupo de series.
- Municipios: top 15 por tasa con selector de año, tabla de conteo frente a tasa y Ibagué destacada.
- Notas de calidad de los datos.

Los datos del dashboard son una copia fija de los resultados de la corrida del 16/10/2026: no se actualizan solos. Los gráficos estáticos equivalentes están en [`../images/`](../images/).

Vista del dashboard: [`../images/dashboard_completo.png`](../images/dashboard_completo.png) · Video: [`../images/integracion_databricks_dashboard.mp4`](../images/integracion_databricks_dashboard.mp4).

Nota: el dashboard interactivo no tiene enlace público (la plataforma exige iniciar sesión para ver los datos), por eso se documenta con captura y video.
