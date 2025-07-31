import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import DiffGenerator._
import SummaryGenerator._
import DuplicateDetector._

object CompareTablesEnhancedStrict {

  def run(
    spark: SparkSession,
    refTable: String,
    newTable: String,
    partitionSpec: Option[String],
    compositeKeyCols: Seq[String],
    ignoreCols: Seq[String],
    reportTable: String,
    diffTable: String,
    duplicatesTable: String,
    checkDuplicates: Boolean,
    includeEqualsInDiff: Boolean,
    partitionHour: String
  ): Unit = {

    import spark.implicits._

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.insertInto.partitionOverwriteMode", "dynamic")

    def applyPartition(df: DataFrame): DataFrame = {
      partitionSpec match {
        case Some(spec) =>
          val (partitionCol, partitionVal) = spec.stripPrefix("[").stripSuffix("]").split("=") match {
            case Array(col, value) => col.trim -> value.trim
            case _ => throw new IllegalArgumentException(s"Error en formato de partición: '$spec'")
          }
          if (!df.columns.contains(partitionCol)) {
            throw new IllegalArgumentException(s"La columna '$partitionCol' no existe en la tabla para aplicar la partición.")
          }
          df.filter(col(partitionCol) === lit(partitionVal))

        case None =>
          throw new IllegalArgumentException("Se requiere una partición explícita para procesar los datos.")
      }
    }

    val refDfBase = applyPartition(spark.table(refTable))
    val newDfBase = applyPartition(spark.table(newTable))

    val allCols = refDfBase.columns.filterNot(ignoreCols.contains).filterNot(_ == "partition_date")
    val allColsFiltered = allCols.filterNot(compositeKeyCols.contains) // evita duplicar claves

    // Importante: mantener orden y claves correctas sin .distinct
    val refDf = refDfBase.select((compositeKeyCols ++ allColsFiltered).map(col): _*)
    val newDf = newDfBase.select((compositeKeyCols ++ allColsFiltered).map(col): _*)

    val diffDf = DiffGenerator.generateDifferencesTable(
      spark, refDf, newDf, compositeKeyCols, allCols, partitionHour, includeEqualsInDiff
    )
    diffDf.write.mode("overwrite").insertInto(diffTable)

    val dupDf = DuplicateDetector.detectDuplicatesTable(
      spark, refDf, newDf, compositeKeyCols, partitionHour
    )
    if (checkDuplicates && !dupDf.isEmpty) {
      dupDf.write.mode("overwrite").insertInto(duplicatesTable)
    }

    val summary = SummaryGenerator.generateSummaryTable(
      spark, refDf, newDf, diffDf, dupDf, compositeKeyCols, partitionHour, refDfBase, newDfBase
    )
    summary.write.mode("overwrite").insertInto(reportTable)
  }
}
