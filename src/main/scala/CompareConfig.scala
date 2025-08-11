
import org.apache.spark.sql.SparkSession

/** Tipos de agregado soportados para overrides */
sealed trait AggType
object AggType {
  case object MaxAgg          extends AggType
  case object MinAgg          extends AggType
  case object FirstNonNullAgg extends AggType
}

/** Parámetros de configuración para toda la comparación (agnóstico de fuente) */
final case class CompareConfig(
  spark: SparkSession,
  refSource: SourceSpec,                  // Origen referencia: HiveTable o FileSource
  newSource: SourceSpec,                  // Origen nuevo: HiveTable o FileSource
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
  // exportExcelPath: Option[String] = Some("file:/Users/maite828/CompareTablesSparkScala/output/summary.xlsx")
  exportExcelPath: Option[String] = None
  // exportExcelPath: Option[String] = Some("s3a://mi-bucket/resultados/summary.xlsx")
)

object CompareConfig {

  /**
    * Compatibilidad hacia atrás: construir el config pasando nombres de tablas Hive.
    * (Sin defaults aquí para no colisionar con el apply del case class.)
    */
  def apply(
    spark: SparkSession,
    refTable: String,
    newTable: String,
    partitionSpec: Option[String],
    compositeKeyCols: Seq[String],
    ignoreCols: Seq[String],
    initiativeName: String,
    tablePrefix: String,
    checkDuplicates: Boolean,
    includeEqualsInDiff: Boolean,
    autoCreateTables: Boolean,
    exportExcelPath: Option[String]
  ): CompareConfig = {
    CompareConfig(
      spark               = spark,
      refSource           = HiveTable(refTable),
      newSource           = HiveTable(newTable),
      partitionSpec       = partitionSpec,
      compositeKeyCols    = compositeKeyCols,
      ignoreCols          = ignoreCols,
      initiativeName      = initiativeName,
      tablePrefix         = tablePrefix,
      checkDuplicates     = checkDuplicates,
      includeEqualsInDiff = includeEqualsInDiff,
      autoCreateTables    = autoCreateTables,
      nullKeyMatches      = true,
      includeDupInQuality = false,
      priorityCol         = None,
      aggOverrides        = Map.empty,
      exportExcelPath     = exportExcelPath
    )
  }
}
