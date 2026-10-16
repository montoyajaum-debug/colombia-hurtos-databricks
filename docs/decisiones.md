# Registro de decisiones

| Fecha | Decisión | Motivo |
|---|---|---|
| 16/10/2026 | Dataset: HURTO PERSONAS (`4rxi-8m8d`) | El consolidado "Reporte Hurto por Modalidades" (`5y9k-32j7`) solo llega a octubre de 2020 |
| 16/10/2026 | Bronze: todas las columnas como texto | Evitar interpretaciones silenciosas del formato de fecha. El tipado se hace en Silver |
| 16/10/2026 | Códigos DANE como texto | Conservar ceros a la izquierda (`05001`) |
| 16/10/2026 | Sin eliminar filas en Silver | Cada exclusión se mide y se documenta antes de aplicarla |

## Hallazgos verificados sobre el CSV (extracción 16/10/2026)

| Verificación | Resultado |
|---|---|
| Filas | 651.200 (coincide con la ficha y con Bronze) |
| Fechas no parseables / cantidades no numéricas | 0 / 0 |
| Rango de fechas | 01/01/2003 a 31/08/2026 |
| `cantidad` mínima y máxima | 1 y 718 |
| Víctimas totales | 3.774.742 |
| Claves (fecha, municipio) repetidas | 0 |
| Departamentos distintos | 34 (33 reales + `SIN ESTABLECER`) |

### Calidad de 2008 (problema de datos)
- **2008 tiene solo 815 filas** frente a 17.000 a 25.000 en los años vecinos, pero 66.530 víctimas.
- Las 365 filas con `SIN ESTABLECER` (departamento y municipio) están **todas en 2008** y concentran 64.185 víctimas, el 96,5 % de ese año.
- Solo 77 municipios aparecen identificados en 2008.
- **Decisión provisional:** 2008 sirve para la serie nacional, pero **no para análisis por municipio**. Se excluye de los modelos municipales y se documenta en las limitaciones.

### Posible quiebre en la serie (2016 a 2019)
Víctimas por año: 94.427 (2014), 101.448 (2015), 146.864 (2016), 209.781 (2017), 257.069 (2018), 306.836 (2019). El salto puede ser real o deberse a cambios de registro. **No está explicado**. Hay que revisarlo en el EDA antes de fijar la ventana de entrenamiento.

### Otros
- 2020 baja a 208.037 (coherente con el efecto de la pandemia, por confirmar en el EDA).
- Los días sin casos por municipio **no aparecen** (cantidad mínima = 1): hay que completar el calendario con ceros en Gold.
- **Reconciliación externa:** la suma de enero a julio de 2026 da **160.998** víctimas. Un artículo externo, no verificado, cita 156.116 sobre una consulta anterior. La diferencia (+3,1 %) es compatible con registros tardíos o revisiones posteriores, pero no puedo confirmarlo.

## Pendientes
- Confirmar si el quiebre de 2016 a 2019 es metodológico o real.
- Población municipal DANE para tasas por 100.000 habitantes.
- Cambio de metodología de validación de denuncias con la Fiscalía: ¿afecta este dataset?

## Anomalías detectadas en la serie nacional (para el EDA)
- **Dos escalones, no crecimiento gradual:** el total mensual salta de ~8.400 (2015) a ~11.900 en **enero de 2016**, y de ~15.700 (julio de 2017) a ~20.000 en **agosto de 2017**. **Corrección tras el EDA:** el escalón de enero de 2016 es **nacional** (Bogotá +37,7 %, resto del país +47,4 %). El de agosto de 2017 es más fuerte en Bogotá (+51,1 % frente a +24,0 % en el resto). La participación de Bogotá pasa de 27 % (2015) a 37 % (2017) y 41 % (2018). Parecen cambios de registro, pero **la causa no está verificada**.
- **Bogotá cae a la mitad en noviembre y diciembre de 2025** (12.228 en octubre, 5.416 y 5.700, y 11.774 en enero de 2026). Parece un vacío de registro, no una caída real. Por confirmar.
- **Agosto de 2026 es bajo** (18.686; Bogotá 7.092) y es el último mes: puede estar incompleto por registros tardíos. La fuente indica que los datos son provisionales hasta marzo del año siguiente.
- **Ventana de entrenamiento (propuesta, por validar en el EDA):** desde septiembre de 2017, tratando nov-dic de 2025 como anomalía y excluyendo agosto de 2026 de la evaluación.
- **Ceros:** se completan desde el primer mes en que cada municipio aparece. 2008 queda fuera del panel.

## Resultados del EDA (calculados sobre el CSV del 16/10/2026; se contrastan con los del notebook 04)
- **Nov-dic de 2025 es una anomalía de Bogotá:** Bogotá cae a 0,48 y 0,51 de su media de ene-oct de 2025, mientras el resto del país queda en 1,12 y 1,05.
- **Agosto de 2026 es bajo en ambos grupos** (Bogotá 0,78 y resto 0,85 frente a may-jul). Es compatible con registro incompleto del último mes, pero no está demostrado.
- **Estacionalidad mensual débil:** fuerza de estacionalidad STL = 0,12; fuerza de tendencia = 0,69. La desviación entre años del índice mensual es grande (hasta 0,15). El nivel de la serie pesa mucho más que el calendario.
- **Estacionalidad semanal clara:** viernes 1,14 y sábado 1,10 frente a domingo 0,78 y lunes 0,91 (2018-2024, sin 2020).
- **Concentración:** Bogotá = 40,4 % de las víctimas de 2018-2024; 3 municipios suman el 50 %, 22 el 80 % y 55 el 90 %. Los 10 primeros concentran el 70 %.
- **Dispersión:** los 200 municipios con más víctimas casi no tienen meses en cero (mediana 0 %); los 912 restantes (3 % de las víctimas) tienen una mediana de 66,8 % de meses en cero.
- **Consecuencia para el modelo:** serie nacional más los ~50 municipios principales (89 % de las víctimas) por separado, y el resto agregado. La línea base no puede ser solo "mismo mes del año anterior": la estacionalidad es débil y hay cambios de nivel. Se probarán también el último valor y el promedio móvil.
