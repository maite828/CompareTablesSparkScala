# CompareTablesProject
![CI](https://github.com/maite828/CompareTablesSparkScala/actions/workflows/ci.yml/badge.svg)

---

# Guía rápida · Motor de comparación de tablas Spark

> **Objetivo ·** Explicar en un único documento el *qué*, *cómo* y *por qué* de la comparación de tablas. Incluye:
>
> 1. Configuración mínima en Scala **y** su equivalente JSON (legacy).
> 2. Ejemplo real con las tres tablas de resultado.
> 3. Interpretación de KPIs, códigos y duplicados.
> 4. Preguntas frecuentes & mantenimiento.
> 5. Rendimiento & buenas prácticas (Big Data)
> 6. Cheat‑sheet* de diagnóstico rápido
> 7. Flujo para investigar (3 tablas → 3 pasos)
> 8. Modo extendido vs ejecutivo
> 9. Preguntas frecuentes (FAQ)
> 10. Mantenimiento & CI

---

## 1. Configuración

### 1.1 Versión Scala (actual)

```scala
val cfg = CompareConfig(
  spark               = spark,
  refTable            = "default.ref_customers",
  newTable            = "default.new_customers",
  partitionSpec       = Some("date=\"2025-07-01\"/geo=\"ES\""),
  compositeKeyCols    = Seq("id"),
  ignoreCols          = Seq("last_update"),
  initiativeName      = "Swift",
  tablePrefix         = "result_",   // → result_differences / duplicates / summary
  checkDuplicates     = true,
  includeEqualsInDiff = true,          // registra también los MATCH
  autoCreateTables    = true,
  exportExcelPath     = Some("./output/summary.xlsx")
)
```

**Campos equivalentes**: `reportTable` → `tablePrefix+"summary"`, etc.  El JSON se mantuvo para PoCs; la API oficial es `CompareConfig`.

| Flag                  | Qué hace                                                       | Valor por defecto |
| --------------------- | -------------------------------------------------------------- | ----------------- |
| `includeEqualsInDiff` | Si `true`, guarda las filas **MATCH** en `result_differences`. | `false`           |
| `checkDuplicates`     | Detecta exactos y variaciones en `result_duplicates`.          | `false`           |
| `priorityCol`         | (Opcional) Qué columna elegir como "ganadora" en duplicados.   | *None*            |

---

## 2. Datos de ejemplo simplificados

> El dataset cubre **TODOS** los casos de negocio: coincidencias, diferencias, gaps y duplicados.

| id       | country | amount        | status  |   | id       | country | amount            | status   |
| -------- | ------- | ------------- | ------- | - | -------- | ------- | ----------------- | -------- |
| **REF**  |         |               |         |   | **NEW**  |         |                   |          |
| 1        | US      | 100.40        | active  |   | 1        | US      | 100.40            | active   |
| 2        | ES␠     | 1.000…001     | expired |   | 2        | ES      | 1.000…001         | expired  |
| 3        | MX      | 150.00        | active  |   | –        | –       | –                 | –        |
| 4        | FR/BR   | 200.00/201.00 | new     |   | 4        | BR      | 200.00×3 / 201.00 | new      |
| 5        | FR      | 300.00/300.50 | active  |   | –        | –       | –                 | –        |
| 6        | –       | –             | –       |   | 6        | DE      | 400.00×2 / 400.10 | new      |
| 7        | PT      | 300.50        | active  |   | 7        | ""      | 300.50            | active   |
| 8        | BR      | 100.50        | pending |   | 8        | BR      | **null**          | pending  |
| 9        | AN      | 80.00         | new     |   | 9        | AN      | 80.00             | **null** |
| 10       | GR      | 60.00         | new     |   | –        | –       | –                 | –        |
| **NULL** | GR      | 61.00 / 60.00 | new     |   | **NULL** | GR      | 60.00×3 / 61.00   | new      |

*Los múltiplos indican duplicados; el espacio en “ES␠” fuerza NO\_MATCH.*

---

## 3. Resultados clave

### 3.1 Tabla `result_differences`

| results                     | Significado                                                               |
| --------------------------- | ------------------------------------------------------------------------- |
| `MATCH`                     | Valor idéntico en REF y NEW *(visible en modo extendido)*.                |
| `NO_MATCH`                  | La clave existe en ambos lados, pero el **valor representativo** difiere. |
| `ONLY_IN_REF / ONLY_IN_NEW` | La clave sólo existe en uno de los lados.                                 |

**Cómo se calcula (visión funcional)**

1. **Normalización de claves vacías**\
   Valores vacíos en las columnas clave se tratan como **NULL**, de modo que todas las filas sin clave se agrupan bajo el id `"NULL"`. Esto afecta a la intersección y a los denominadores del resumen.

2. **Columnas constantes se omiten**\
   Si una columna tiene el **mismo valor en todo el dataset** (tanto en REF como en NEW), no se incluye en `result_differences`. Así evitas ruido en tablas anchas.

3. **Fila de mayor prioridad (opcional)**\
   Si defines una columna de prioridad, para cada clave se conserva **una única fila** (la de mayor prioridad) **antes** de comparar. Esto estabiliza el resultado cuando hay duplicados operativos. Los duplicados siguen viéndose en `result_duplicates`.

4. **Valor representativo por clave**\
   Cuando una clave aparece varias veces, se elige **un valor por columna** para compararlo entre REF y NEW. Reglas por defecto:

- **Numéricos/fechas/booleanos** → se toma el **máximo** (robusto ante ruido bajo).
- **Textos** → se usa el **orden natural** para elegir un valor estable.
- **Estructuras/arrays/mapas** → se comparan en formato JSON; en **mapas** el orden de pares no afecta, en **arrays** **sí** importa el orden.

> Esto explica por qué, en claves con varias filas (p. ej. id `NULL` con importes 60/61), el diff puede salir `MATCH`: si en ambos lados el valor representativo seleccionado coincide.

5. **Política de nulos en la comparación de claves**\
   Dos claves **NULL** pueden considerarse iguales (comportamiento por defecto). Si deseas tratarlas siempre como diferentes, cambia la política de emparejamiento de claves nulas.

6. **Formateo fiel de valores**

- Los decimales **conservan la escala** para mostrar (por ejemplo `1.000000000000000001`). La **comparación es numérica**, no por texto.
- Los nulos/vacíos en valores (no claves) se muestran como `-` para facilitar lectura.

**Reglas de comparación por tipo (resumen ejecutivo)**

| Tipo de dato        | Cómo se compara                              | Observación clave                       |
| ------------------- | -------------------------------------------- | --------------------------------------- |
| Numéricos/Decimal   | Igualdad por valor                           | `1.0 == 1.00` ; `100.50 ≠ 100.49`.      |
| String              | Igualdad exacta, *case‑sensitive*            | Los espacios cuentan: `"ES␠" ≠ `"ES"\`. |
| Date/Timestamp/Bool | Igualdad exacta                              | —                                       |
| Map                 | Se ordenan las entradas antes de comparar    | El orden de las claves no afecta.       |
| Array               | Igualdad por contenido **y orden**           | Si necesitas ignorar orden, normaliza.  |
| Struct              | Igualdad campo a campo (representación JSON) | —                                       |
| Binary              | Igualdad por contenido (codificado base64)   | —                                       |

**Casos borde típicos con el dataset de ejemplo**

- `id=2`, `country`: `"ES␠"` vs `"ES"` → `NO_MATCH` por espacio en blanco.
- `id=3` presente sólo en REF → varias filas `ONLY_IN_REF` (una por columna comparada).
- `id=6` presente sólo en NEW → varias filas `ONLY_IN_NEW`.
- `id=NULL` (claves vacías): al agregar por clave, ambos lados comparten un valor representativo común para `amount` → puede aparecer como `MATCH` en modo extendido; las **variaciones internas** se ven en `result_duplicates`.

**Cómo leerla eficazmente**

1. Para **saber dónde cambia** algo, filtra por `results != 'MATCH'` y ordena por `id, column`.
2. Si ves muchas `ONLY_IN_*`, comprueba si es por **particiones** mal filtradas o por **claves vacías** concentradas en `id="NULL"`.
3. Si una clave sale `MATCH` pero sospechas valores distintos internamente, abre `result_duplicates` para ver el **rango de variaciones**.

Ejemplo (extracto rápido):

```text
id=2 column=country ➜ NO_MATCH   ("ES␠" vs "ES")
id=3 column=country ➜ ONLY_IN_REF
id=6 column=amount  ➜ ONLY_IN_NEW (400.10 sólo en NEW)
```

### 3.2 Tabla `result_duplicates`

Mide la **calidad de unicidad** de cada identificador en ambos universos.

**Cómo se genera** (versión ejecutiva)

1. Para cada fila se crea una «huella» digital que resume todas sus columnas.
2. Se agrupa por *origen* (REF o NEW) y *id compuesto*.
3. Para cada grupo se calculan:
   - **Total de filas** (`occurrences`).
   - **Filas repetidas al 100 %** (`exact_duplicates`).\
     ⇒ mismo id + huella idéntica.
   - **Filas con al menos una diferencia** (`duplicates_w_variations`).
   - **Variaciones detectadas**: qué columnas cambian y sus valores.
4. Si un mismo id presenta duplicados **en los dos lados** verás **dos registros**, uno `ref` y otro `new` *(no existe un valor "both" en la salida actual)*.

| origin | Interpretación rápida                  |
| ------ | -------------------------------------- |
| `ref`  | Duplicados sólo en la tabla histórica. |
| `new`  | Duplicados sólo en la tabla candidata. |

> Para localizar rápidamente qué causa el problema:\
> *`exact_duplicates`* alto ⇒ copias exactas.\
> *`duplicates_w_variations`* alto ⇒ la clave se reescribe con valores distintos.

#### Ejemplo real (extracto)

| origin | id   | exact\_dup | var\_dup | occ | variations                                        |
| ------ | ---- | ---------- | -------- | --- | ------------------------------------------------- |
| ref    | 5    | 0          | 1        | 2   | `amount: [300.000…,300.500…]`                     |
| ref    | NULL | 0          | 1        | 2   | `amount: [60.000…,61.000…]`                       |
| new    | NULL | 2          | 1        | 4   | `amount: [60.000…,61.000…]`                       |
| new    | 6    | 1          | 1        | 3   | `amount: [400.000…,400.100…]`                     |
| ref    | 4    | 0          | 1        | 2   | `country: [BR,FR] \| amount: [200.000…,201.000…]` |
| new    | 4    | 2          | 1        | 4   | `amount: [200.000…,201.000…]`                     |

- **exact\_dup > 0** → existen *x* filas idénticas (hash repetido).
- **var\_dup > 0** → dentro de ese id hay al menos dos hashes distintos ⇒ alguna columna cambia.

#### Casos comunes

| Situación                                         | exact\_dup | var\_dup | Ejemplo                   |
| ------------------------------------------------- | ---------- | -------- | ------------------------- |
| 2 filas idénticas (id 4 en NEW)                   | 1          | 0        | `amount` todos iguales    |
| 2 filas idénticas **+** 1 variación (id 6 en NEW) | 1          | 1        | `amount` 400.00 vs 400.10 |
| 2 filas diferentes (id 5 en REF)                  | 0          | 1        | 300.00 vs 300.50          |
| 1 sola fila                                       | 0          | 0        | sin duplicados            |

—

#### Casos comunes

| Situación                                         | exact\_dup | var\_dup | Ejemplo                   |
| ------------------------------------------------- | ---------- | -------- | ------------------------- |
| 2 filas idénticas (id 4 en NEW)                   | 1          | 0        | `amount` todos iguales    |
| 2 filas idénticas **+** 1 variación (id 6 en NEW) | 1          | 1        | `amount` 400.00 vs 400.10 |
| 2 filas diferentes (id 5 en REF)                  | 0          | 1        | 300.00 vs 300.50          |
| 1 sola fila                                       | 0          | 0        | sin duplicados            |

### 3.3 Tabla `result_summary`

**Qué es.** Panel de KPIs a nivel de clave construido a partir de las tres salidas. Responde en segundos: tamaños, intersección, gaps, duplicados y *calidad global*.

**Columnas**

- **bloque**: familia de métrica (KPIS, MATCH, NO MATCH, GAP, DUPS).
- **metrica**: descripción legible de lo contado.
- **universo**: ámbito de cómputo (REF, NEW o BOTH).
- **numerador**: cantidad principal.
- **denominador**: referencia para el % (si aplica).
- **pct**: porcentaje formateado con 1 decimal (si `denominador=0` → "-").
- **ejemplos**: muestra de IDs para inspección rápida.

#### Cómo se calcula cada bloque

- **KPIS**
  - *IDs Uniques (REF/NEW)*: nº de claves **distintas** por lado. Todas las claves vacías se consolidan como `id="NULL"`.
  - *Total REF / Total NEW (ROWS)*: nº de **filas** (incluye duplicados).
  - *Total (NEW-REF)*: diferencia de filas (NEW − REF) y % respecto a **filas REF**.
- **MATCH / NO MATCH (BOTH)**
  - **Universo = intersección de claves** (claves presentes en ambos lados).
  - *1:1 (exact matches)*: nº de claves de la intersección cuyos **valores representativos** por columna son idénticos.
  - *1:1 (match not identical)*: nº de claves de la intersección con **al menos una diferencia** (`NO_MATCH` u `ONLY_IN_*`).
- **GAP**
  - *1:0 (only in reference)*: claves que sólo existen en REF.
  - *0:1 (only in new)*: claves que sólo existen en NEW.
- **DUPS**
  - *duplicates (both)*: claves con duplicados **en ambos lados**.
  - *duplicates (ref)*: claves con duplicados **sólo** en REF.
  - *duplicates (new)*: claves con duplicados **sólo** en NEW.
- **Quality global (REF)**
  - Fórmula: **(claves con match exacto y sin duplicados en ningún lado) / (IDs únicos REF)**.

#### Denominadores (qué significan)

- Filas con **universo REF o NEW** → el denominador se deja "-", salvo:
  - *Total (NEW-REF)* → referencia = **filas REF**.
  - *Quality global*  → referencia = **IDs únicos REF**.
- Filas con **universo BOTH** → denominador = **nº de claves en la intersección**.

#### Ejemplo con los datos de la sección 2

- **IDs únicos**: REF = 10, NEW = 8.
- **Filas totales**: REF = 13, NEW = 16 → *Total (NEW-REF) = 3* y % sobre REF = **23.1%**.
- **Intersección de claves**: 7
  - *1:1 exact matches*: 2 (id=1, id=NULL) → **28.6%**.
  - *1:1 con diferencias*: 5 (2,4,7,8,9) → **71.4%**.
- **GAPs**: sólo REF = 3 (10,3,5) · sólo NEW = 1 (6).
- **Duplicados**: both = 2 (4, NULL), ref = 1 (5), new = 1 (6).
- **Quality global**: 1 / 10 = **10.0%**.

#### Consejos de lectura

- Si el % de **NO MATCH** es alto en BOTH, revisa normalizaciones (espacios, mayúsculas/minúsculas) y las reglas de agregación por clave.
- Si una clave aparece con **variaciones** en `result_duplicates`, esa clave **no** suma en el numerador de *Quality global*.
- Para drill‑down: filtra `result_differences` por `results != 'MATCH'` y ordena por `id, column`.

## 4. Lectura rápida de la salida

1. **¿Puedo sustituir la tabla?** → mira `Quality global`. <100 % = todavía hay diferencias.
2. **¿Qué diferencias existen?** → filtra `result_differences` por `results!='MATCH'`.
3. **¿Hay duplicados problemáticos?** → `result_duplicates` donde `duplicates_w_variations>0`.

---

## 5. Rendimiento & buenas prácticas (Big Data)

**Escala objetivo.** El motor está probado para tablas anchas y millones de filas. Aun así, la comparación hace *full outer join* y agregaciones por clave: el *shuffle* puede ser costoso si no se filtra bien.

**Recomendaciones rápidas**

- **Filtra por partición** siempre que sea posible (fecha/país/canal, etc.).
- **Normaliza antes**: recorta espacios, homogeniza mayúsculas/minúscculas si negocio lo permite.
- **Controla el paralelismo**: ajusta `spark.sql.shuffle.partitions` al tamaño del cluster.
- **Skew de claves**: si hay ids muy calientes, considera *salting* o *skew join hints*.
- **Broadcast selectivo**: sólo para dimensiones pequeñas; aquí normalmente ambas tablas son grandes.
- **Evita columnas inútiles**: limita las columnas comparadas a lo relevante.
- **Excel export**: úsalo en muestras o resúmenes; para >1M filas, exporta Parquet/Delta.

**Tiempos típicos (orientativos)**

- 10–50 M filas por lado, 10–50 columnas: minutos en un cluster mediano.

**Señales de alerta**

- `only_in_*` masivo → filtros de partición incorrectos.
- `var_dup` alto → procesos upstream que reescriben claves.
- `% NO_MATCH` alto en BOTH → normalización/jerarquías de negocio mal definidas.

---



## 6. *Cheat‑sheet* de diagnóstico rápido

| Síntoma observado                               | Mira primero                           | Qué filtrar/ordenar                                 | Lectura/acción típica                                                                                                         |
| ----------------------------------------------- | -------------------------------------- | --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `% NO MATCH` alto en BOTH                       | `result_differences`                   | `results = 'NO_MATCH'` → ordena por `id, column`    | Suele ser **normalización** (espacios, mayúsculas) o **reglas de agregación por clave**. Revisa mapeos y prioridad operativa. |
| Muchos `ONLY_IN_REF` o `ONLY_IN_NEW`            | `result_differences`                   | `results LIKE 'ONLY_IN_%'`                          | **Particiones mal filtradas** o ventanas temporales distintas. Confirma `partitionSpec` y fechas de corte.                    |
| `duplicates_w_variations` alto                  | `result_duplicates`                    | `var_dup > 0`                                       | La clave se **reescribe** con valores distintos. Revisa procesos upstream, define reglas de consolidación.                    |
| `exact_duplicates` alto                         | `result_duplicates`                    | `exact_dup > 0`                                     | **Copias exactas** (reprocesos, loads duplicados). Dedup antes de comparar.                                                   |
| Duplicados en REF **y** NEW para el mismo id    | `result_duplicates`                    | filtra por ese `id` (verás fila `ref` y fila `new`) | El problema **existe en ambos universos**. Planifica corrección en los dos flujos.                                            |
| Aparece `id = "NULL"` en muchas filas           | `result_summary` + `result_duplicates` | busca `id = 'NULL'`                                 | Claves vacías concentradas. Acordar política de **NULL** y rellenar/descartar en ingestión.                                   |
| Valores `null` inesperados en NEW               | `result_differences`                   | filtra por columna afectada y `value_new = '-'`     | Campos no mapeados o casteos fallidos. Revisa ETL y *schemas*.                                                                |
| `Total (NEW-REF)` muy positivo                  | `result_summary`                       | —                                                   | **Suma de filas** en NEW por cargas repetidas o granularidad distinta. Confirma deduplicación y *joins*.                      |
| `Quality global` baja o inestable               | `result_summary`                       | —                                                   | Cambios de lógica/normalización. Usa CI y snapshots dorados para detectar regresiones.                                        |
| Log indica **“Excluyendo columnas constantes”** | Consola                                | —                                                   | Comportamiento esperado para evitar ruido. Si necesitas verlas, desactiva la exclusión en la configuración.                   |

> **Tip**: cuando una clave sale `MATCH` pero sospechas diferencias internas, abre `result_duplicates` para ver el **rango de valores** dentro de la clave.

## 7. Flujo para investigar (3 tablas → 3 pasos)

```
[1] result_summary (KPIs) 
   ├─ ¿GAP alto? → Revisa particiones / fechas / filtros
   ├─ ¿DUPS alto? → Ve a result_duplicates
   └─ ¿NO MATCH alto en BOTH? → Ve a result_differences

[2] result_differences (detalle de qué cambia)
   ├─ Filtra results != 'MATCH' (id, column)
   ├─ ¿Strings? mira espacios / mayúsculas
   ├─ ¿Nums/fechas? mira escala y agregación por clave
   └─ Si una clave parece estable pero hay ruido → ve a result_duplicates

[3] result_duplicates (por qué hay ruido)
   ├─ exact_dup > 0 → copias exactas → dedup
   └─ var_dup  > 0 → reescrituras → reglas de consolidación / prioridad
```
---

## 8. Modo extendido vs ejecutivo

| Modo          | Para quién | Qué muestra                                           |
| ------------- | ---------- | ----------------------------------------------------- |
| **Ejecutivo** | Negocio    | KPIs + primeras diferencias (>30 k filas se ocultan). |
| **Extendido** | Data Ops   | Todas las columnas + MATCH; exportable a Excel.       |

Actívalo con `includeEqualsInDiff=true` y consulta `summary.xlsx`.

---

## 9. Preguntas frecuentes (FAQ)

| Pregunta                                                | Resumen de respuesta                                              |
| ------------------------------------------------------- | ----------------------------------------------------------------- |
| *El espacio «ES␠» me genera NO\_MATCH, ¿cómo evitarlo?* | Normaliza valores (`trim/lower`) en `DiffGenerator.canonicalize`. |
| *¿Se pueden cambiar los códigos «ONLY\_IN*\*»?\_        | Sí, modifica `DiffGenerator.buildDiffStruct`.                     |
| *¿NULL se cuenta varias veces?*                         | No. Todos los NULLs de la clave se colapsan a `id="NULL"`.        |
| *¿Puedo comparar más columnas como clave?*              | Define `compositeKeyCols = Seq("id","country").`                  |

---

## 10. Mantenimiento & CI

- **Tests unitarios e integración**: 11 pruebas → `sbt test` (<15 s).
- **GitHub Actions**: `.github/workflows/ci.yml` ejecuta la batería en cada push.
- **Snapshots**: actualiza los Parquet dorados tras cambios de lógica.

---
© 2025 · Compare‑tables Spark 3.5.0 · MIT
---

