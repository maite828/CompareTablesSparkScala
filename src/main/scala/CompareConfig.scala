
import org.apache.spark.sql.SparkSession

import java.time.LocalDate

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
                                partitionSpec: Option[String],                 // p.ej. Some("""geo="ES"/data_date_part="2025-08-14"""")
                                compositeKeyCols: Seq[String],                 // claves compuestas
                                ignoreCols: Seq[String],                       // columnas a ignorar
                                initiativeName: String,                        // etiqueta
                                tablePrefix: String,                           // p.ej. "default.result_"
                                outputBucket: String,                          // p.ej. "s3a://mi-bucket/results/"
                                checkDuplicates: Boolean = false,
                                includeEqualsInDiff: Boolean = false,
                                autoCreateTables: Boolean = true,
                                exportExcelPath: Option[String] = None,        // Some("s3a://mi-bucket/results/summary.xlsx")

                                // Campos para Diff/Duplicates
                                priorityCol: Option[String] = None,            // columna para priorizar filas (desc_nulls_last)
                                aggOverrides: Map[String, String] = Map.empty, // p.ej. Map("amount" -> "max")
                                nullKeyMatches: Boolean = true,                // null-safe equality en claves

                                // Fecha a escribir en tablas de salida (por defecto hoy en ISO)
                                outputDateISO: String = LocalDate.now.toString // yyyy-MM-dd
                              )

