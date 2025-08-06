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
case class CompareConfig(
  spark: SparkSession,
  refTable: String,
  newTable: String,
  partitionSpec: Option[String],
  compositeKeyCols: Seq[String],
  ignoreCols: Seq[String],
  initiativeName: String,
  tablePrefix: String,
  checkDuplicates: Boolean,
  includeEqualsInDiff: Boolean = true,
  autoCreateTables: Boolean  = true,
  nullKeyMatches: Boolean    = true,
  includeDupInQuality: Boolean = false,
  priorityCol: Option[String]   = None,
  aggOverrides: Map[String, AggType] = Map.empty,
  exportExcelPath: Option[String] = None
    //exportExcelPath: Option[String] = Some("s3a://mi-bucket/resultados/summary.xlsx")


)
