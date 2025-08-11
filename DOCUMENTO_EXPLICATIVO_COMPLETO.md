# CompareTablesProject - Documento Explicativo Completo

![CI](https://github.com/maite828/CompareTablesSparkScala/actions/workflows/ci.yml/badge.svg)

---

## 📋 Índice

1. [Visión General del Proyecto](#visión-general-del-proyecto)
2. [Arquitectura del Sistema](#arquitectura-del-sistema)
3. [Componentes Principales](#componentes-principales)
4. [Flujo de Comparación](#flujo-de-comparación)
5. [Análisis del Código Fuente](#análisis-del-código-fuente)
6. [Casos de Uso y Ejemplos](#casos-de-uso-y-ejemplos)
7. [Configuración y Personalización](#configuración-y-personalización)
8. [Rendimiento y Buenas Prácticas](#rendimiento-y-buenas-prácticas)
9. [Troubleshooting y Diagnóstico](#troubleshooting-y-diagnóstico)
10. [Mantenimiento y CI/CD](#mantenimiento-y-cicd)

---

## 🎯 Visión General del Proyecto

**CompareTablesProject** es un motor de comparación de tablas desarrollado en **Scala** y **Apache Spark 3.5**, diseñado para realizar análisis detallados de diferencias entre datasets de referencia y nuevos.

### Objetivos Principales

- **Comparación Fiel**: Mantener la integridad completa de los datos durante la comparación
- **Detección de Duplicados**: Identificar tanto duplicados exactos como variaciones
- **Análisis de Calidad**: Generar métricas y KPIs para evaluar la calidad de los datos
- **Escalabilidad**: Manejar datasets de Big Data de manera eficiente
- **Flexibilidad**: Configuración personalizable para diferentes casos de uso

### Tecnologías Utilizadas

- **Scala 2.13+**: Lenguaje de programación funcional
- **Apache Spark 3.5**: Motor de procesamiento distribuido
- **Parquet**: Formato de almacenamiento columnar
- **Hive**: Metadatos y gestión de tablas
- **Excel**: Exportación de resúmenes (opcional)

---

## 🏗️ Arquitectura del Sistema

### Componentes Principales

```
┌─────────────────────────────────────────────────────────────┐
│                    TableComparisonController                │
│                     (Orquestador Principal)                │
└─────────────────────┬───────────────────────────────────────┘
                      │
        ┌─────────────┼─────────────┐
        │             │             │
┌───────▼──────┐ ┌────▼────┐ ┌─────▼─────┐
│DiffGenerator │ │Duplicate│ │Summary    │
│              │ │Detector │ │Generator  │
│• Comparación │ │• Detección│ │• KPIs y   │
│• Diferencias │ │• Duplicados│ │• Métricas │
└──────────────┘ └─────────┘ └───────────┘
        │             │             │
        └─────────────┼─────────────┘
                      │
              ┌───────▼──────┐
              │   Output     │
              │   Tables     │
              │• differences │
              │• duplicates  │
              │• summary     │
              └──────────────┘
```

### Flujo de Datos

1. **Entrada**: Dos DataFrames (referencia y nuevo)
2. **Procesamiento**: Comparación paralela y detección de duplicados
3. **Salida**: Tres tablas de resultados + resumen ejecutivo
4. **Exportación**: Opcional a Excel para análisis posterior

---

## 🔧 Componentes Principales

### 1. DiffGenerator - Motor de Comparación

**Responsabilidad**: Generar la tabla de diferencias entre dos datasets.

#### Funcionalidades Clave

```scala
object DiffGenerator {
  // Normalización de claves vacías a NULL
  val norm: Column => Column = c => 
    when(trim(c.cast(StringType)) === "", lit(null)).otherwise(c)
  
  // Formateo fiel de valores respetando tipos de datos
  def formatValue(c: Column, dt: DataType): Column = dt match {
    case _: DecimalType =>
      val s = c.cast(StringType)
      when(s.isNull || trim(s) === "", lit("-")).otherwise(s)
    case _ =>
      val s = c.cast(StringType)
      when(s.isNull || trim(s) === "", lit("-")).otherwise(s)
  }
  
  // Canonicalización para comparaciones deterministas
  def canonicalize(c: Column, dt: DataType): Column = dt match {
    case _: NumericType   => c
    case _: StringType    => when(c.isNull, lit(null)).otherwise(c.cast(StringType))
    case mt: MapType      =>
      val sorted = array_sort(map_entries(c))
      to_json(map_from_entries(sorted))
    case _: ArrayType     => to_json(c)
    case _: StructType    => to_json(c)
    case BinaryType       => when(c.isNull, lit(null)).otherwise(encode(c.cast("binary"), "base64"))
    case _                => to_json(c)
  }
}
```

#### Proceso de Comparación

1. **Normalización de Claves**: Valores vacíos se convierten a NULL
2. **Filtrado de Columnas Constantes**: Se excluyen automáticamente
3. **Pre-ordenamiento**: Si hay columna de prioridad, se selecciona la fila de mayor prioridad
4. **Agregación por Clave**: Se elige un valor representativo por columna
5. **Full Outer Join**: Comparación completa entre ambos datasets
6. **Explosión de Diferencias**: Una fila por columna/clave con diferencias

### 2. DuplicateDetector - Detector de Duplicados

**Responsabilidad**: Identificar duplicados exactos y variaciones en ambos datasets.

#### Lógica de Detección

```scala
object DuplicateDetector {
  def detectDuplicatesTable(
    spark: SparkSession,
    refDf: DataFrame,
    newDf: DataFrame,
    compositeKeyCols: Seq[String],
    config: CompareConfig
  ): DataFrame = {
    
    // 1. Unir y etiquetar origen
    val withSrc = refDf.withColumn("_src", lit("ref"))
      .unionByName(newDf.withColumn("_src", lit("new")))
    
    // 2. Aplicar prioridad si está definida
    val base = config.priorityCol match {
      case Some(prio) if withSrc.columns.contains(prio) =>
        val w = Window.partitionBy(("_src" +: compositeKeyCols).map(col): _*)
                   .orderBy(col(prio).desc_nulls_last)
        withSrc.withColumn("_rn", row_number().over(w))
               .filter(col("_rn") === 1)
               .drop("_rn")
      case _ => withSrc
    }
    
    // 3. Generar hash de fila para identificación
    val hashCol = sha2(
      concat_ws("§", base.columns.filter(_ != "_src").map { c =>
        coalesce(col(c).cast(StringType), lit("__NULL__"))
      }: _*),
      256
    )
    
    // 4. Agregación y conteo
    val aggExprs = Seq(
      count(lit(1)).as("occurrences"),
      (count(lit(1)) - countDistinct("_row_hash")).as("exact_dup"),
      greatest(lit(0), countDistinct("_row_hash") - lit(1)).as("var_dup")
    )
  }
}
```

#### Tipos de Duplicados Detectados

- **Exact Duplicates**: Filas idénticas (mismo hash)
- **Duplicates with Variations**: Misma clave, valores diferentes
- **Occurrences**: Total de filas por grupo

### 3. SummaryGenerator - Generador de Resúmenes

**Responsabilidad**: Crear métricas ejecutivas y KPIs consolidados.

#### Métricas Generadas

```scala
case class SummaryRow(
  bloque: String,      // KPIS, MATCH, NO MATCH, GAP, DUPS
  metrica: String,     // Descripción de la métrica
  universo: String,    // REF, NEW, BOTH, ROWS
  numerador: String,   // Valor principal
  denominador: String, // Referencia para porcentajes
  pct: String,         // Porcentaje formateado
  ejemplos: String     // IDs de ejemplo para drill-down
)
```

#### Bloques de Métricas

1. **KPIS**: IDs únicos, totales de filas, calidad global
2. **MATCH**: Coincidencias exactas en la intersección
3. **NO MATCH**: Diferencias en la intersección
4. **GAP**: Claves presentes solo en un lado
5. **DUPS**: Duplicados por origen

---

## 🔄 Flujo de Comparación

### Paso 1: Preparación de Datos

```scala
// Configuración de la comparación
val cfg = CompareConfig(
  spark               = spark,
  refTable            = "default.ref_customers",
  newTable            = "default.new_customers",
  partitionSpec       = Some("date=\"2025-07-01\"/geo=\"ES\""),
  compositeKeyCols    = Seq("id"),
  ignoreCols          = Seq("last_update"),
  initiativeName      = "Swift",
  tablePrefix         = "result_",
  checkDuplicates     = true,
  includeEqualsInDiff = true,
  autoCreateTables    = true,
  exportExcelPath     = Some("./output/summary.xlsx")
)
```

### Paso 2: Generación de Diferencias

```scala
// DiffGenerator genera la tabla de diferencias
val diffDf = DiffGenerator.generateDifferencesTable(
  spark = spark,
  refDf = refDf,
  newDf = newDf,
  compositeKeyCols = cfg.compositeKeyCols,
  compareColsIn = compareCols,
  includeEquals = cfg.includeEqualsInDiff,
  config = cfg
)
```

### Paso 3: Detección de Duplicados

```scala
// DuplicateDetector identifica duplicados
val dupDf = DuplicateDetector.detectDuplicatesTable(
  spark = spark,
  refDf = refDf,
  newDf = newDf,
  compositeKeyCols = cfg.compositeKeyCols,
  config = cfg
)
```

### Paso 4: Generación del Resumen

```scala
// SummaryGenerator crea las métricas ejecutivas
val summaryDf = SummaryGenerator.generateSummaryTable(
  spark = spark,
  refDf = refDf,
  newDf = newDf,
  diffDf = diffDf,
  dupDf = dupDf,
  compositeKeyCols = cfg.compositeKeyCols,
  refDfRaw = refDfRaw,
  newDfRaw = newDfRaw,
  config = cfg
)
```

---

## 📊 Análisis del Código Fuente

### Estructura del Proyecto

```
src/main/scala/
├── Main.scala                    # Punto de entrada principal
├── TableComparisonController.scala # Controlador principal
├── CompareConfig.scala           # Configuración del sistema
├── DiffGenerator.scala           # Motor de comparación
├── DuplicateDetector.scala       # Detector de duplicados
├── SummaryGenerator.scala        # Generador de resúmenes
├── SourceSpec.scala              # Especificaciones de fuentes
└── ...
```

### Patrones de Diseño Utilizados

1. **Object Pattern**: Cada componente es un objeto singleton
2. **Case Classes**: Para estructuras de datos inmutables
3. **Functional Programming**: Uso extensivo de funciones de orden superior
4. **Builder Pattern**: Configuración flexible a través de CompareConfig

### Manejo de Tipos de Datos

#### Preservación de Fidelidad

```scala
// Los tipos numéricos mantienen su precisión
case _: DecimalType =>
  val s = c.cast(StringType)
  when(s.isNull || trim(s) === "", lit("-")).otherwise(s)

// Los arrays preservan el orden
case _: ArrayType => to_json(c)

// Los mapas se ordenan para comparación estable
case mt: MapType =>
  val sorted = array_sort(map_entries(c))
  to_json(map_from_entries(sorted))
```

#### Estrategias de Agregación

```scala
// Configuración personalizable de agregaciones
config.aggOverrides.get(c) match {
  case Some(MaxAgg)          => max(canon.cast(dt)).as(c)
  case Some(MinAgg)          => min(canon.cast(dt)).as(c)
  case Some(FirstNonNullAgg) => first(col(c), ignoreNulls = true).as(c)
  case None =>
    dt match {
      case _: NumericType | _: BooleanType | _: DateType | _: TimestampType =>
        max(canon.cast(dt)).as(c)
      case _ =>
        max(canon).as(c)
    }
}
```

---

## 🎯 Casos de Uso y Ejemplos

### Caso 1: Comparación Financiera

**Escenario**: Validar balances bancarios con precisión decimal crítica.

```scala
// Configuración para datos financieros
val financialConfig = CompareConfig(
  // ... configuración básica ...
  aggOverrides = Map(
    "balance" -> MaxAgg,           // Siempre tomar el balance más alto
    "last_update" -> MaxAgg,       // Fecha más reciente
    "currency" -> FirstNonNullAgg   // Primer valor no-null
  )
)
```

**Resultado**: El sistema detecta diferencias mínimas como `1000.000000000000000001` vs `1000.000000000000000000`.

### Caso 2: Validación de Códigos de Producto

**Escenario**: Verificar códigos donde `"ABC" ≠ "abc"`.

```scala
// El sistema es case-sensitive por defecto
// Para hacer case-insensitive, modificar canonicalize:
case _: StringType => 
  when(c.isNull, lit(null))
    .otherwise(lower(c.cast(StringType)))
```

### Caso 3: Auditoría de Transacciones

**Escenario**: Comparar timestamps con precisión de milisegundos.

```scala
// Los timestamps se comparan exactamente
case _: TimestampType => c
```

---

## ⚙️ Configuración y Personalización

### Parámetros de CompareConfig

```scala
final case class CompareConfig(
  spark: SparkSession,                    // Sesión de Spark
  refSource: SourceSpec,                  // Fuente de referencia
  newSource: SourceSpec,                  // Fuente nueva
  partitionSpec: Option[String],          // Filtro de particiones
  compositeKeyCols: Seq[String],          // Columnas de clave compuesta
  ignoreCols: Seq[String],                // Columnas a ignorar
  initiativeName: String,                 // Nombre de la iniciativa
  tablePrefix: String,                    // Prefijo de tablas de resultado
  checkDuplicates: Boolean,               // Verificar duplicados
  includeEqualsInDiff: Boolean,           // Incluir matches en diferencias
  autoCreateTables: Boolean,              // Crear tablas automáticamente
  nullKeyMatches: Boolean,                // NULLs en claves se consideran iguales
  includeDupInQuality: Boolean,           // Incluir duplicados en calidad
  priorityCol: Option[String],            // Columna de prioridad
  aggOverrides: Map[String, AggType],     // Overrides de agregación
  exportExcelPath: Option[String]         // Ruta de exportación a Excel
)
```

### Estrategias de Agregación Personalizables

```scala
sealed trait AggType
object AggType {
  case object MaxAgg          extends AggType
  case object MinAgg          extends AggType
  case object FirstNonNullAgg extends AggType
}

// Ejemplo de uso
val config = CompareConfig(
  // ... configuración básica ...
  aggOverrides = Map(
    "amount" -> MaxAgg,                    // Siempre máximo
    "status" -> FirstNonNullAgg,           // Primer valor no-null
    "country" -> MinAgg,                   // Siempre mínimo
    "last_update" -> MaxAgg                // Fecha más reciente
  )
)
```

---

## 🚀 Rendimiento y Buenas Prácticas

### Optimizaciones Implementadas

1. **Filtrado de Columnas Constantes**: Se excluyen automáticamente
2. **Pre-ordenamiento por Prioridad**: Reduce el procesamiento posterior
3. **Agregación Eficiente**: Una sola pasada por los datos
4. **Hash SHA-256**: Para identificación rápida de duplicados

### Recomendaciones de Rendimiento

```scala
// 1. Filtrar por particiones siempre que sea posible
partitionSpec = Some("date=\"2025-07-01\"/geo=\"ES\"")

// 2. Limitar columnas a comparar
ignoreCols = Seq("last_update", "created_by", "temp_field")

// 3. Usar columnas de prioridad para estabilizar resultados
priorityCol = Some("timestamp")

// 4. Configurar paralelismo de Spark
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

### Escalabilidad

- **Objetivo**: Tablas de 10-50M filas con 10-50 columnas
- **Tiempo típico**: Minutos en cluster mediano
- **Memoria**: Optimizado para evitar OOM en datasets grandes

---

## 🔍 Troubleshooting y Diagnóstico

### Cheat-Sheet de Diagnóstico Rápido

| Síntoma | Primera Revisión | Filtro/Ordenación | Acción Típica |
|---------|------------------|-------------------|---------------|
| `% NO MATCH` alto en BOTH | `result_differences` | `results = 'NO_MATCH'` → ordena por `id, column` | Revisar normalización y reglas de agregación |
| Muchos `ONLY_IN_REF` o `ONLY_IN_NEW` | `result_differences` | `results LIKE 'ONLY_IN_%'` | Verificar filtros de partición y fechas |
| `duplicates_w_variations` alto | `result_duplicates` | `var_dup > 0` | Revisar procesos upstream y reglas de consolidación |
| `exact_duplicates` alto | `result_duplicates` | `exact_dup > 0` | Identificar copias exactas para deduplicación |

### Flujo de Investigación (3 Pasos)

```
[1] result_summary (KPIs) 
   ├─ ¿GAP alto? → Revisar particiones/fechas/filtros
   ├─ ¿DUPS alto? → Ir a result_duplicates
   └─ ¿NO MATCH alto en BOTH? → Ir a result_differences

[2] result_differences (detalle de cambios)
   ├─ Filtrar results != 'MATCH' (id, column)
   ├─ ¿Strings? revisar espacios/mayúsculas
   ├─ ¿Nums/fechas? revisar escala y agregación
   └─ Si clave estable pero hay ruido → ir a result_duplicates

[3] result_duplicates (por qué hay ruido)
   ├─ exact_dup > 0 → copias exactas → deduplicar
   └─ var_dup > 0 → reescrituras → reglas de consolidación
```

### Señales de Alerta

- **`only_in_*` masivo**: Filtros de partición incorrectos
- **`var_dup` alto**: Procesos upstream que reescriben claves
- **`% NO_MATCH` alto en BOTH**: Normalización mal definida

---

## 🛠️ Mantenimiento y CI/CD

### Tests y Validación

```bash
# Ejecutar tests unitarios
sbt test

# Ejecutar comparación completa
./run_compare.sh

# Validar configuración
./scripts/bootstrap.sh
```

### GitHub Actions

```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up JDK 11
        uses: actions/setup-java@v3
        with:
          java-version: '11'
          distribution: 'temurin'
      - name: Run tests
        run: sbt test
```

### Snapshots y Validación

- **Tests unitarios**: 11 pruebas (<15s)
- **Tests de integración**: Validación end-to-end
- **Snapshots dorados**: Actualización tras cambios de lógica

---

## 📈 Métricas y KPIs

### Interpretación de Resultados

#### Tabla `result_differences`

| Resultado | Significado |
|-----------|-------------|
| `MATCH` | Valor idéntico en REF y NEW |
| `NO_MATCH` | Clave existe en ambos, pero valores difieren |
| `ONLY_IN_REF` | Clave solo existe en referencia |
| `ONLY_IN_NEW` | Clave solo existe en nuevo |

#### Tabla `result_duplicates`

| Métrica | Interpretación |
|---------|----------------|
| `exact_duplicates` | Filas idénticas (hash repetido) |
| `duplicates_w_variations` | Filas con al menos una diferencia |
| `occurrences` | Total de filas por grupo |

#### Tabla `result_summary`

| Bloque | Métricas |
|--------|----------|
| **KPIS** | IDs únicos, totales, calidad global |
| **MATCH** | Coincidencias exactas en intersección |
| **NO MATCH** | Diferencias en intersección |
| **GAP** | Claves presentes solo en un lado |
| **DUPS** | Duplicados por origen |

---

## 🔮 Futuras Mejoras

### Funcionalidades Planificadas

1. **Comparación Incremental**: Solo procesar cambios desde última ejecución
2. **Alertas Automáticas**: Notificaciones cuando calidad cae por debajo de umbrales
3. **Dashboard Web**: Interfaz gráfica para análisis de resultados
4. **Integración con Data Quality Tools**: Conectores con Great Expectations, Deequ
5. **Soporte para Streaming**: Comparación en tiempo real

### Optimizaciones Técnicas

1. **Broadcast Joins**: Para dimensiones pequeñas
2. **Skew Join Hints**: Manejo de claves muy calientes
3. **Compresión Avanzada**: Optimización de almacenamiento Parquet
4. **Cache Inteligente**: Reutilización de DataFrames intermedios

---

## 📚 Recursos Adicionales

### Documentación Técnica

- [README Ejecutivo](README_EJECUTIVO.md)
- [Flujo Funcional](FLUJO_FUNCIONAL.md)
- [README Original](README_ORIGINAL.md)

### Ejemplos de Uso

- [Scripts de Ejecución](scripts/)
- [Tests Unitarios](src/test/)
- [Configuraciones de Ejemplo](src/main/scala/)

### Comunidad y Soporte

- **GitHub**: [maite828/CompareTablesSparkScala](https://github.com/maite828/CompareTablesSparkScala)
- **Issues**: Reportar bugs y solicitar features
- **Discussions**: Preguntas y respuestas de la comunidad

---

## 🏁 Conclusión

**CompareTablesProject** representa una solución robusta y escalable para la comparación de datasets en entornos Big Data. Su arquitectura modular, configuración flexible y preservación de fidelidad de datos lo convierten en una herramienta esencial para:

- **Data Quality**: Validación y control de calidad de datos
- **Data Reconciliation**: Conciliación entre sistemas
- **Change Detection**: Detección de cambios en datasets
- **Audit Trail**: Trazabilidad de modificaciones

El proyecto demuestra las mejores prácticas de desarrollo en Scala y Spark, con un enfoque en mantenibilidad, rendimiento y usabilidad.

---

**© 2025 · CompareTables Spark 3.5.0 · MIT License**

*Desarrollado con ❤️ para la comunidad de Big Data*
