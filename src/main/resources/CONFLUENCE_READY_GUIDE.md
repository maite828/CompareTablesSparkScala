# AML Internal Tools - Guía de Referencia Rápida para Confluence

## 📖 Tabla de Contenidos

1. [Quick Start en 3 Pasos](#quick-start)
2. [Parámetros Esenciales](#parametros)
3. [Interpretación de Resultados](#resultados)
4. [Troubleshooting Top 5](#troubleshooting)
5. [Ejemplos de Configuración](#ejemplos)

---

## 🚀 QUICK START <a name="quick-start"></a>

### Flujo Básico de Ejecución

```
┌─────────────────┐
│ 1. PREPARAR     │ → Definir parámetros obligatorios
│    Comando      │   (refTable, newTable, keys, fecha)
└────────┬────────┘
         ↓
┌─────────────────┐
│ 2. EJECUTAR     │ → spark-submit con archivo JAR
│    Spark        │   Spark procesa y compara
└────────┬────────┘
         ↓
┌─────────────────┐
│ 3. VERIFICAR    │ → Query tabla summary
│    Resultados   │   Revisar Global Quality
└─────────────────┘
```

### Comando Mínimo

```bash
spark-submit \
  --class com.santander.cib.adhc.internal_aml_tools.Main \
  cib-adhc-internaltools.jar \
  refTable=default.table_ref \
  newTable=default.table_new \
  compositeKeyCols=id \
  initiativeName=test \
  tablePrefix=default.results_ \
  outputBucket=s3a://bucket/path \
  executionDate=2025-11-28
```

---

## 🎯 PARÁMETROS ESENCIALES <a name="parametros"></a>

### Tabla de Prioridades

| Nivel | Parámetros | Cuándo usar | Ejemplo |
|-------|------------|-------------|---------|
| 🔴 **CRÍTICO** | refTable<br/>newTable<br/>compositeKeyCols<br/>initiativeName<br/>executionDate | **SIEMPRE** | `refTable=prod.payments`<br/>`compositeKeyCols=id,date` |
| 🟠 **MUY IMPORTANTE** | partitionSpec<br/>tablePrefix<br/>outputBucket | **99% casos** | `partitionSpec="date=2025-11-28/"`<br/>`tablePrefix=default.results_` |
| 🟡 **RECOMENDADO** | checkDuplicates<br/>priorityCols | **Análisis completo** | `checkDuplicates=true`<br/>`priorityCols=timestamp` |
| 🟢 **OPCIONAL** | refFilter<br/>newFilter<br/>columnMapping<br/>enableDynamicPartitioning | **Casos especiales** | `refFilter="amount >= 1000"`<br/>`enableDynamicPartitioning=true` |

### Parámetros Detallados

#### 🔴 Obligatorios

| Parámetro | Descripción | Formato | Ejemplo |
|-----------|-------------|---------|---------|
| `refTable` | Tabla de referencia (histórica) | `db.table` | `prod.swift_v1` |
| `newTable` | Tabla nueva (candidata) | `db.table` | `test.swift_v2` |
| `compositeKeyCols` | Claves para join (CSV) | `col1,col2,col3` | `id,geo,type` |
| `initiativeName` | Etiqueta identificadora | Alfanumérico | `swift_migration` |
| `executionDate` | Fecha de ejecución | `YYYY-MM-DD` | `2025-11-28` |

#### 🟡 Opcionales Útiles

| Parámetro | Default | Descripción | Ejemplo |
|-----------|---------|-------------|---------|
| `partitionSpec` | - | Filtro de particiones | `geo=ES/date=2025-11-28/` |
| `checkDuplicates` | `false` | Detectar duplicados | `true` |
| `priorityCols` | - | Columnas de prioridad (CSV) | `timestamp,version` |
| `ignoreCols` | - | Columnas a excluir (CSV) | `audit_ts,ingestion_date` |
| `refFilter` | - | Filtro SQL para REF | `geo IN ('ES','FR')` |
| `newFilter` | - | Filtro SQL para NEW | `amount >= 1000` |
| `enableDynamicPartitioning` | `false` | Múltiples archivos output | `true` (solo >500MB) |

---

## 📊 INTERPRETACIÓN DE RESULTADOS <a name="resultados"></a>

### Estructura de la Tabla Summary

```
┌─────────────────────────────────────────────────────────┐
│ KPIS Block                                              │
├─────────────────────────────────────────────────────────┤
│ → Global Quality: 99.81%     [MÉTRICA PRINCIPAL]       │
│ → Total Rows REF: 1034                                  │
│ → Total Rows NEW: 1034                                  │
│ → Unique IDs REF: 1029                                  │
│ → Unique IDs NEW: 1029                                  │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ EXACT MATCH Block                                       │
├─────────────────────────────────────────────────────────┤
│ → 1:1 all columns: 1029/1029 (100%)   ✅ COINCIDEN     │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ PARTIAL MATCH Block                                     │
├─────────────────────────────────────────────────────────┤
│ → 1:1 with differences: 0/1029 (0%)   ⚠️ VARIACIONES  │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ GAP Block                                               │
├─────────────────────────────────────────────────────────┤
│ → 1:0 only in ref: 0/1029 (0%)        ❌ Falta en NEW │
│ → 0:1 only in new: 0/1029 (0%)        ❌ Falta en REF │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ DUPS Block                                              │
├─────────────────────────────────────────────────────────┤
│ → duplicates (both): 2/1029 (0.19%)   🔄 En ambos     │
│ → duplicates (only ref): 0/1029 (0%)  🔄 Solo REF     │
│ → duplicates (only new): 0/1029 (0%)  🔄 Solo NEW     │
└─────────────────────────────────────────────────────────┘
```

### Fórmula de Global Quality

```
┌──────────────────────────────────────────────────┐
│ Global Quality = qualityOk / nBothIds            │
│                                                  │
│ Donde:                                           │
│   qualityOk = Exact Match - Duplicates           │
│                                                  │
│ Ejemplo:                                         │
│   nBothIds = 1029                                │
│   Exact Match = 1029                             │
│   Duplicates (any) = 2                           │
│   qualityOk = 1029 - 2 = 1027                    │
│   Global Quality = 1027/1029 = 99.81%            │
└──────────────────────────────────────────────────┘
```

### Escala de Interpretación

| Global Quality | Interpretación | Acción |
|----------------|----------------|--------|
| **>99%** 🟢 | EXCELENTE - Migración lista | ✅ Proceder |
| **95-99%** 🟡 | BUENO - Revisar diferencias | ⚠️ Revisar samples en differences |
| **90-95%** 🟠 | ACEPTABLE - Investigar gaps | 🔍 Análisis profundo de gaps |
| **<90%** 🔴 | CRÍTICO - Acción requerida | ❌ NO proceder, corregir datos |

---

## 🔧 TROUBLESHOOTING TOP 5 <a name="troubleshooting"></a>

### Problema 1: Summary muestra números incorrectos (ej: 81 duplicados pero SQL muestra 2)

```
❌ CAUSA: dupRead lee TODA la tabla sin filtrar por initiative y fecha
          → Contamina con datos históricos de todas las ejecuciones

✅ SOLUCIÓN: Verificar que el código tenga este filtro en línea 158 de
             TableComparisonController.scala:

   val dupRead = config.spark.table(s"${config.tablePrefix}duplicates")
     .filter(col("initiative") === config.initiativeName 
       && col("data_date_part") === executionDate)

💡 VERIFICACIÓN: 
   SELECT COUNT(*) FROM duplicates 
   WHERE initiative='swift' AND data_date_part='2025-11-28';
   → Debe coincidir con el numerator del summary
```

### Problema 2: Task not serializable / HiveTableScan

```
❌ CAUSA: Tabla creada con Hive SerDe (no DataSource Parquet)

✅ SOLUCIÓN: Recrear tabla como DataSource:

   DROP TABLE IF EXISTS default.results_duplicates;
   
   CREATE TABLE default.results_duplicates (
     origin STRING,
     id STRING,
     category STRING,
     exact_duplicates STRING,
     dupes_w_variations STRING,
     occurrences STRING,
     variations STRING,
     initiative STRING,
     data_date_part STRING
   )
   USING parquet
   PARTITIONED BY (initiative, data_date_part)
   LOCATION 's3a://bucket/path/duplicates';
```

### Problema 3: Ejecución muy lenta (>2 horas para dataset pequeño)

```
❌ CAUSA: Sin partition pruning → Lee tabla completa

✅ SOLUCIÓN: Añadir partitionSpec para filtrar particiones:

   ANTES (lento):
   executionDate=2025-11-28
   
   DESPUÉS (rápido):
   partitionSpec="data_date_part=2025-11-28/"
   executionDate=2025-11-28
   
   Reducción: De 1000 particiones → 1 partición
```

### Problema 4: priorityCols no funciona

```
❌ CAUSA: Parámetro era "priorityCol" (singular) - ya no soportado

✅ SOLUCIÓN: Usar "priorityCols" (plural) con múltiples columnas:

   ANTES (no funciona):
   priorityCol=timestamp,version
   
   DESPUÉS (correcto):
   priorityCols=timestamp,version
```

### Problema 5: Muchos archivos pequeños en output (50+ archivos de KB)

```
❌ CAUSA: Versión antigua sin control de particionamiento

✅ SOLUCIÓN: Actualizar a versión reciente con modo default:

   enableDynamicPartitioning=false  (o no especificar)
   
   Resultado: 1 archivo por tabla (differences, duplicates, summary)
   
💡 NOTA: Solo usar enableDynamicPartitioning=true si output >500MB
```

---

## 💼 EJEMPLOS DE CONFIGURACIÓN <a name="ejemplos"></a>

### Ejemplo 1: Comparación Diaria Simple

**Escenario:** Comparar 2 tablas del mismo día, mismo schema, detectar duplicados

```bash
spark-submit \
  --class com.santander.cib.adhc.internal_aml_tools.Main \
  cib-adhc-internaltools.jar \
  refTable=prod.swift_transactions \
  newTable=test.swift_transactions_v2 \
  compositeKeyCols=id,geo,type \
  initiativeName=swift_daily \
  tablePrefix=default.results_ \
  outputBucket=s3a://bucket/comparisons \
  executionDate=2025-11-28 \
  partitionSpec="data_date_part=2025-11-28/" \
  checkDuplicates=true \
  priorityCols=timestamp,date
```

**Resultado esperado:**
- ✅ 3 archivos output (1 por tabla)
- ✅ Global Quality visible en summary
- ✅ Duplicados detectados y categorizados

---

### Ejemplo 2: Comparación con Filtros SQL

**Escenario:** Comparar solo España y Francia, con importes >= 1000, excluyendo columnas técnicas

```bash
spark-submit \
  --class com.santander.cib.adhc.internal_aml_tools.Main \
  cib-adhc-internaltools.jar \
  refTable=prod.payments \
  newTable=test.payments_v2 \
  compositeKeyCols=transaction_id \
  initiativeName=payments_filtered \
  tablePrefix=default.results_ \
  outputBucket=s3a://bucket/comparisons \
  executionDate=2025-11-28 \
  partitionSpec="data_date_part=2025-11-28/" \
  refFilter="geo IN ('ES','FR') AND amount >= 1000" \
  newFilter="geo IN ('ES','FR') AND amount >= 1000" \
  ignoreCols=audit_timestamp,ingestion_date,etl_version \
  checkDuplicates=true
```

**Optimización aplicada:**
```
Tabla completa: 10M filas
↓ partitionSpec (1 día): 500K filas (95% reducción)
↓ refFilter (geo + amount): 50K filas (90% reducción adicional)
→ Total: 99.5% reducción
```

---

### Ejemplo 3: Migración con Column Mapping

**Escenario:** Comparar tablas con columnas renombradas (legacy → nuevo)

```bash
spark-submit \
  --class com.santander.cib.adhc.internal_aml_tools.Main \
  cib-adhc-internaltools.jar \
  refTable=legacy.customer_data \
  newTable=prod.customer_data_v2 \
  compositeKeyCols=customer_id \
  initiativeName=customer_migration \
  tablePrefix=default.results_ \
  outputBucket=s3a://bucket/comparisons \
  executionDate=2025-11-28 \
  partitionSpec="year=2025/month=11/" \
  colMap.cust_id=customer_id \
  colMap.cust_name=customer_name \
  colMap.bal=balance \
  checkDuplicates=false
```

**Column Mapping aplicado:**
```
REF (legacy)         NEW (prod)           Mapping
─────────────────────────────────────────────────────
customer_id    →     customer_id         (igual)
cust_name      →     customer_name       colMap.cust_name
bal            →     balance             colMap.bal
```

---

### Ejemplo 4: Dataset Grande con Particionamiento Dinámico

**Escenario:** 50GB de datos, necesita múltiples archivos output

```bash
spark-submit \
  --class com.santander.cib.adhc.internal_aml_tools.Main \
  cib-adhc-internaltools.jar \
  refTable=hist.transactions \
  newTable=prod.transactions \
  compositeKeyCols=id \
  initiativeName=hist_comparison \
  tablePrefix=default.results_ \
  outputBucket=s3a://bucket/comparisons \
  executionDate=2025-11-28 \
  partitionSpec="year=2025/month=[10,11]/" \
  refFilter="status='ACTIVE'" \
  newFilter="status='ACTIVE'" \
  checkDuplicates=false \
  enableDynamicPartitioning=true
```

**Resultado esperado:**
```
differences: 7 archivos de ~128MB cada uno
duplicates: (skipped, checkDuplicates=false)
summary: 1 archivo
```

---

## 📋 CHECKLIST DE VALIDACIÓN

Usa este checklist antes de ejecutar en producción:

```
□ refTable y newTable existen y son accesibles
□ compositeKeyCols son claves únicas o casi únicas
□ partitionSpec filtra correctamente (no lee tabla completa)
□ ignoreCols excluye columnas técnicas no relevantes
□ Si hay timestamps → usar priorityCols
□ Si output >500MB → considerar enableDynamicPartitioning=true
□ initiativeName es único y descriptivo
□ outputBucket tiene permisos de escritura
□ Tablas de resultados creadas como USING parquet (no Hive SerDe)
```

---

## 🎯 MATRIZ DE DECISIÓN RÁPIDA

| Si tienes... | Entonces usa... | Ejemplo |
|-------------|-----------------|---------|
| Datos <10GB | partitionSpec específico | `date=2025-11-28/` |
| Datos >10GB | partitionSpec + refFilter | `date=[...]/` + `geo IN (...)` |
| Timestamps/versiones | priorityCols | `priorityCols=timestamp` |
| Columnas renombradas | columnMapping | `colMap.old=new` |
| Output >500MB | enableDynamicPartitioning | `true` |
| Migración crítica | checkDuplicates | `true` |
| Análisis rápido | checkDuplicates | `false` |

---

## 📞 SOPORTE Y ESCALAMIENTO

### Logs a revisar en caso de error:

1. **Spark Driver logs:** Errores de configuración
2. **Spark Executor logs:** Errores de procesamiento
3. **Console output:** Logs `[DEBUG]`, `[WRITE]`, `[FILTER]`

### Queries de diagnóstico:

```sql
-- Verificar datos escritos
SELECT COUNT(*) FROM default.results_summary 
WHERE initiative='swift' AND data_date_part='2025-11-28';

-- Ver duplicados reales
SELECT * FROM default.results_duplicates
WHERE initiative='swift' AND data_date_part='2025-11-28'
ORDER BY origin, id;

-- Ver diferencias específicas
SELECT id, column, value_ref, value_new 
FROM default.results_differences
WHERE initiative='swift' AND data_date_part='2025-11-28'
  AND results='NO_MATCH'
LIMIT 100;
```

---

**Versión del documento:** 1.0  
**Última actualización:** 2025-11-28  
**Repositorio:** github.alm.europe.cloudcenter.corp/cib-oasis-academy/cib-adhc-internaltools

