## **Sistema de Comparación( 2 tablas ) - Ejemplo Completo**

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

### 1.2 Equivalente JSON (legacy)

```json
{
  "refTable": "db.ref_customers",
  "newTable": "db.new_customers",
  "partitionSpec": "[partition_date=2025-07-25]",
  "compositeKeyCols": ["id"],
  "ignoreCols": ["last_update"],
  "reportTable": "customer_summary",
  "diffTable": "customer_differences",
  "duplicatesTable": "customer_duplicates",
  "checkDuplicates": true,
  "includeEqualsInDiff": false
}
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

| results                     | Significado                                                      |
| --------------------------- | ---------------------------------------------------------------- |
| `MATCH`                     | Valor idéntico en REF y NEW (solo si `includeEqualsInDiff=true`) |
| `NO_MATCH`                  | id presente en ambos, valor diferente                            |
| `ONLY_IN_REF / ONLY_IN_NEW` | id ausente en la tabla opuesta                                   |

Ejemplo (extracto):

```text
id=2 column=country ➜ NO_MATCH   ("ES␠" vs "ES")
id=3 column=country ➜ ONLY_IN_REF
id=6 column=amount  ➜ ONLY_IN_NEW (400.10 sólo en NEW)
```

### 3.2 Tabla `result_duplicates`

`origin` indica dónde se detecta el grupo de filas duplicadas:

| Valor  | Significado                                          |
| ------ | ---------------------------------------------------- |
| `ref`  | Sólo en la tabla de referencia.                      |
| `new`  | Sólo en la tabla nueva.                              |
| `both` | Existe al menos una fila con esa clave en cada lado. |

| Columna | Qué representa                                  | Cómo se calcula                                  |
| ------- | ----------------------------------------------- | ------------------------------------------------ |
| ``      | Nº total de filas con esa clave y ese origen.   | `count(*)`                                       |
| ``      | Filas 100 % iguales entre sí.                   | `occurrences - countDistinct(hash)`              |
| ``      | Variaciones dentro del mismo id (≠ exactos).    | `max(countDistinct(hash) - 1,0)`                 |
| ``      | Lista compacta de columnas con más de un valor. | `collect_set` por columna → texto "col: [v1,v2]" |

> **Hash interno** → Para cada fila se concatena clave + columnas no clave y se calcula `sha2(…,256)`. Dos filas son "exact duplicates" si comparten hash.

#### Ejemplo resumido

| origin | id | occ | exact\_dup | var\_dup | variations                |
| ------ | -- | --- | ---------- | -------- | ------------------------- |
| ref    | 5  | 2   | 1          | 1        | `amount: [300.00,300.50]` |
| new    | 6  | 3   | 2          | 1        | `amount: [400.00,400.10]` |
| both   | 4  | 4   | 2          | 1        | `country: [FR,BR]`        |

- **Filas exactas** de id 4 («200.00 / new») se cuentan en **ref** y **new**, por lo que `origin = both` y `exact_dup = 2`.
- El mismo id 4 presenta a la vez variaciones (`country`). Por eso `duplicates_w_variations = 1`.

#### Cuándo se cumple cada caso

1. **Exact duplicates** (`exact_dup > 0`):
   - Todas las columnas (clave + resto) coinciden.
   - El número es la cantidad de filas redundantes (p.ej. 2 filas idénticas ⇒ `exact_dup = 1`).
2. **Duplicates with variations** (`var_dup > 0`):
   - Mismo id, pero al menos una columna no clave tiene valores distintos.
   - Las columnas implicadas aparecen en `variations`.
3. **Ambos contadores = 0**: no hay duplicados; la fila ganadora se eligió (o `priorityCol`).

—

### 3.3 Tabla `result_summary` Tabla `result_summary`

| bloque/ métrica      | Comentario rápido                                                        |
| -------------------- | ------------------------------------------------------------------------ |
| **KPIS**             | Totales y IDs únicos por lado.                                           |
| **MATCH / NO MATCH** | Sólo se calculan sobre la **intersección** (`idsBoth`). 7 en el ejemplo. |
| **GAP**              | 1:0 (sólo ref) y 0:1 (sólo new).                                         |
| **DUPS**             | Duplicados globales.                                                     |
| **Quality global**   | (MATCH exactos sin duplicados) / IDs REF.                                |

> ⚠️ **Denominador de bloque BOTH** = `idsBoth`, no IDs NEW.

---

## 4. Lectura rápida de la salida

1. **¿Puedo sustituir la tabla?** → mira `Quality global`. <100 % = todavía hay diferencias.
2. **¿Qué diferencias existen?** → filtra `result_differences` por `results!='MATCH'`.
3. **¿Hay duplicados problemáticos?** → `result_duplicates` donde `duplicates_w_variations>0`.

---

## 5. Modo extendido vs ejecutivo

| Modo          | Para quién | Qué muestra                                           |
| ------------- | ---------- | ----------------------------------------------------- |
| **Ejecutivo** | Negocio    | KPIs + primeras diferencias (>30 k filas se ocultan). |
| **Extendido** | Data Ops   | Todas las columnas + MATCH; exportable a Excel.       |

Actívalo con `includeEqualsInDiff=true` y consulta `summary.xlsx`.

---

## 6. Preguntas frecuentes (FAQ)

| Pregunta                                                | Resumen de respuesta                                              |
| ------------------------------------------------------- | ----------------------------------------------------------------- |
| *El espacio «ES␠» me genera NO\_MATCH, ¿cómo evitarlo?* | Normaliza valores (`trim/lower`) en `DiffGenerator.canonicalize`. |
| *¿Se pueden cambiar los códigos «ONLY\_IN*\*»?\_        | Sí, modifica `DiffGenerator.buildDiffStruct`.                     |
| *¿NULL se cuenta varias veces?*                         | No. Todos los NULLs de la clave se colapsan a `id="NULL"`.        |
| *¿Puedo comparar más columnas como clave?*              | Define `compositeKeyCols = Seq("id","country").`                  |

---

## 7. Mantenimiento & CI

- **Tests unitarios e integración**: 11 pruebas → `sbt test` (<15 s).
- **GitHub Actions**: `.github/workflows/ci.yml` ejecuta la batería en cada push.
- **Snapshots**: actualiza los Parquet dorados tras cambios de lógica.

---

© 2025 · Compare‑tables Spark 3.5.0 · MIT

