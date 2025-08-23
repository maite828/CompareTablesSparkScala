// src/main/scala/com/example/compare/CompareConfig.scala

import org.apache.spark.sql.SparkSession

/** Tipos de agregado soportados para overrides */
sealed trait AggType
object AggType {
  case object MaxAgg          extends AggType
  case object MinAgg          extends AggType
  case object FirstNonNullAgg extends AggType
}

/** Parámetros de configuración para toda la comparación */
final case class CompareConfig(
  spark: SparkSession,
  refTable: String,
  newTable: String,
  partitionSpec: Option[String],          // p.ej. Some("""geo="ES"/data_date_part="2025-08-14"""")
  compositeKeyCols: Seq[String],          // claves compuestas
  ignoreCols: Seq[String],                // columnas a ignorar
  initiativeName: String,                 // etiqueta
  tablePrefix: String,                    // p.ej. "default.result_"
  checkDuplicates: Boolean = false,
  includeEqualsInDiff: Boolean = false,
  autoCreateTables: Boolean = true,
  exportExcelPath: Option[String] = None, // (opcional) export a Excel

  // === NUEVOS CAMPOS para satisfacer DiffGenerator/DuplicateDetector ===
  priorityCol: Option[String] = None,     // columna para priorizar filas (desc_nulls_last)
  aggOverrides: Map[String, String] = Map.empty, // p.ej. Map("amount" -> "max")
  nullKeyMatches: Boolean = true,         // null-safe equality en claves

  // === Fecha que se escribirá en las tablas de salida ===
  outputDateISO: String                   // yyyy-MM-dd (viene de PartitionFormatTool)
)                // <-- NUEVO
  // exportExcelPath: Option[String] = Some("file:/.../summary.xlsx")
  // exportExcelPath: Option[String] = Some("s3a://mi-bucket/results/summary.xlsx")
