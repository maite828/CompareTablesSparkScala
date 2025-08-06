import java.time.LocalDate
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TableComparisonController {

  def run(config: CompareConfig): Unit = {
    import config._
    val session = spark

    // Overwrite dinámico de particiones
    session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    session.conf.set("hive.exec.dynamic.partition", "true")
    session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    // 1) Extraer executionDate de partitionSpec o usar la fecha de hoy
    val executionDate: String = partitionSpec.flatMap { spec =>
      """(?i)data_date_part\s*=\s*"?([0-9]{4}-[0-9]{2}-[0-9]{2})"?""".r
        .findFirstMatchIn(spec)
        .map(_.group(1))
    }.getOrElse {
      LocalDate.now().toString
    }

    // 2) Nombres de tablas de salida
    val diffTable       = s"${tablePrefix}differences"
    val summaryTable    = s"${tablePrefix}summary"
    val duplicatesTable = s"${tablePrefix}duplicates"

    // 3) Crear tablas si es necesario
    if (autoCreateTables) {
      ensureResultTables(session, diffTable, summaryTable, duplicatesTable)
    }

    // 4) Cargar ref y new (si no hay partitionSpec, carga toda la tabla)
    val refDf = loadWithPartition(session, refTable, partitionSpec)
    val newDf = loadWithPartition(session, newTable, partitionSpec)

    // 5) Columnas a comparar (excluir compositeKeyCols, ignoreCols, partición)
    val partitionKeys = partitionSpec
      .map(_.split("/").map(_.split("=")(0).trim).toSet)
      .getOrElse(Set.empty)
    val colsToCompare = refDf.columns.toSeq
      .filterNot(ignoreCols.contains)
      .filterNot(partitionKeys.contains)
      .filterNot(compositeKeyCols.contains)

    // 6) Diferencias
    val diffDf = DiffGenerator.generateDifferencesTable(
      session, refDf, newDf, compositeKeyCols, colsToCompare, includeEqualsInDiff, config
    )
    writeResult(session, diffTable, diffDf,
      Seq("id","column","value_ref","value_new","results"),
      initiativeName, executionDate
    )

    // 7) Duplicados
    if (checkDuplicates) {
      val dups = DuplicateDetector.detectDuplicatesTable(
        session, refDf, newDf, compositeKeyCols, config
      )
      writeResult(session, duplicatesTable, dups,
        Seq("origin","id","exact_duplicates","duplicates_w_variations","occurrences","variations"),
        initiativeName, executionDate
      )
    }

    // 8) Resumen
    val dupDf = if (checkDuplicates) session.table(duplicatesTable) else session.emptyDataFrame
    val summaryDf = SummaryGenerator.generateSummaryTable(
      session, refDf, newDf, diffDf, dupDf, compositeKeyCols, refDf, newDf, config
    )
    writeResult(session, summaryTable, summaryDf,
      Seq("bloque","metrica","universo","numerador","denominador","pct","ejemplos"),
      initiativeName, executionDate
    )

    // 9) Export opcional a Excel
    config.exportExcelPath.foreach { path =>
      SummaryGenerator.exportToExcel(summaryDf, path)
    }
  }

  // -------------------------------------------------------------------
  // Helpers para crear tablas si no existen
  // -------------------------------------------------------------------
  private def ensureResultTables(
      spark: SparkSession,
      diffTable: String,
      summaryTable: String,
      duplicatesTable: String
  ): Unit = {
    val diffDDL =
      s"""
         |CREATE TABLE IF NOT EXISTS $diffTable (
         |  id STRING,
         |  `column` STRING,
         |  value_ref STRING,
         |  value_new STRING,
         |  results STRING
         |)
         |PARTITIONED BY (initiative STRING, data_date_part STRING)
         |STORED AS PARQUET
       """.stripMargin

    val summaryDDL =
      s"""
         |CREATE TABLE IF NOT EXISTS $summaryTable (
         |  bloque STRING,
         |  metrica STRING,
         |  universo STRING,
         |  numerador STRING,
         |  denominador STRING,
         |  pct STRING,
         |  ejemplos STRING
         |)
         |PARTITIONED BY (initiative STRING, data_date_part STRING)
         |STORED AS PARQUET
       """.stripMargin

    val duplicatesDDL =
      s"""
         |CREATE TABLE IF NOT EXISTS $duplicatesTable (
         |  origin STRING,
         |  id STRING,
         |  exact_duplicates STRING,
         |  duplicates_w_variations STRING,
         |  occurrences STRING,
         |  variations STRING
         |)
         |PARTITIONED BY (initiative STRING, data_date_part STRING)
         |STORED AS PARQUET
       """.stripMargin

    spark.sql(diffDDL)
    spark.sql(summaryDDL)
    spark.sql(duplicatesDDL)
  }

  // -------------------------------------------------------------------
  // Carga tabla entera o filtrada según partitionSpec
  // -------------------------------------------------------------------
  private def loadWithPartition(
      spark: SparkSession,
      tableName: String,
      partitionSpec: Option[String]
  ): DataFrame = {
    val base = spark.table(tableName)
    partitionSpec match {
      case Some(spec) =>
        spec.split("/").foldLeft(base) { (df, kv) =>
          val Array(k, vRaw) = kv.split("=", 2)
          val v = vRaw.replaceAll("\"", "")
          if (df.columns.contains(k)) df.filter(col(k) === lit(v)) else df
        }
      case None =>
        // Sin partición: devolvemos toda la tabla
        base
    }
  }

  // -------------------------------------------------------------------
  // Escritura de resultados con overwrite dinámico de partición
  // -------------------------------------------------------------------
  private def writeResult(
      spark: SparkSession,
      tableName: String,
      df: DataFrame,
      columns: Seq[String],
      initiative: String,
      executionDate: String
  ): Unit = {
    df
      .withColumn("initiative", lit(initiative))
      .withColumn("data_date_part", lit(executionDate))
      .repartition(col("initiative"), col("data_date_part"))
      .write
      .mode(SaveMode.Overwrite)
      .insertInto(tableName)
  }
}