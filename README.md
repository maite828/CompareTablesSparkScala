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

`origin` indica dónde se detecta el grupo duplicado:

| Valor  | Significado                                        |
| ------ | -------------------------------------------------- |
| `ref`  | Clave duplicada **solo** en tabla REF.             |
| `new`  | Clave duplicada **solo** en tabla NEW.             |
| `both` | Hay al menos una fila con esa clave en cada tabla. |

| Columna     | Qué representa                                                               |      |
| ----------- | ---------------------------------------------------------------------------- | ---- |
| ``          | Clave compuesta (NULL si todas sus columnas son nulas).                      |      |
| ``          | `occurrences - countDistinct(hash)` → filas 100 % iguales entre sí.          |      |
| ``          | `max(countDistinct(hash) - 1,0)` → mismo id pero al menos un valor distinto. |      |
| ``          | Total de filas con ese id y origen.                                          |      |
| ``          | Columnas con >1 valor: \`col: [v1,v2]                                        | …\`. |
| ``** / **`` | Particiones de salida añadidas por el controlador.                           |      |

> **Cómo decide el algoritmo**
>
> 1. Genera un *hash de fila* (`sha2`) con todas las columnas salvo `_src`.
> 2. Agrupa por `origin + id`.
> 3. Calcula contadores y set de valores.
> 4. Si se define `priorityCol`, primero se escoge la fila ganadora (máx. prioridad) y se eliminan las demás antes de contar.

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
- **origin = both** se da cuando REF y NEW presentan duplicados simultáneamente (no ocurre en este extracto, pero puede darse cuando los dos lados tienen ≥2 filas del mismo id).

#### Casos comunes

| Situación                                         | exact\_dup | var\_dup | Ejemplo                   |
| ------------------------------------------------- | ---------- | -------- | ------------------------- |
| 2 filas idénticas (id 4 en NEW)                   | 1          | 0        | `amount` todos iguales    |
| 2 filas idénticas **+** 1 variación (id 6 en NEW) | 1          | 1        | `amount` 400.00 vs 400.10 |
| 2 filas diferentes (id 5 en REF)                  | 0          | 1        | 300.00 vs 300.50          |
| 1 sola fila                                       | 0          | 0        | sin duplicados            |

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

