/**
 * TableComparisonController is the main orchestrator for comparing two tables in Spark.
 * It handles loading, filtering, comparing, and summarizing differences and duplicates between tables.
 *
 * Main workflow:
 *  1. Configures Spark performance settings.
 *  2. Optionally creates output tables for differences, summary, and duplicates.
 *  3. Extracts execution date from partition specification or uses current date.
 *  4. Loads reference and new tables, prunes columns, and persists DataFrames.
 *  5. Generates and writes differences between tables.
 *  6. Optionally detects and writes duplicate records.
 *  7. Generates and writes summary metrics.
 *  8. Optionally exports summary to Excel.
 *  9. Releases cached DataFrames.
 *
 * Helper methods:
 *  - ensureResultTables: Creates output tables if they do not exist.
 *  - loadWithPartition: Loads a table, optionally filtering by partition specification.
 *  - writeResult: Writes results to a table, repartitioned by initiative and execution date.
 *
 * @param config CompareConfig containing Spark session, table names, columns, partition specs, and options.
 */
// src/main/scala/com/example/compare/TableComparisonController.scala

import java.time.LocalDate
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types._

object TableComparisonController {

  def run(config: CompareConfig): Unit = {
    import config._
    val session = spark

    // ── 0) CONFIGURACIÓN DE RENDIMIENTO ──
    session.conf.set("spark.sql.shuffle.partitions", "100")
    session.sparkContext.setCheckpointDir("/tmp/checkpoints")
    session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    session.conf.set("hive.exec.dynamic.partition", "true")
    session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    // ── 1) CREAR TABLAS DE SALIDA SI ES NECESARIO ──
    if (autoCreateTables) {
      val diffTable       = s"$tablePrefix" + "differences"
      val summaryTable    = s"$tablePrefix" + "summary"
      val duplicatesTable = s"$tablePrefix" + "duplicates"
      ensureResultTables(session, diffTable, summaryTable, duplicatesTable)
    }

    // ── 2) EXTRAER FECHA DE EJECUCIÓN ──
    val isoRegex    = "[A-Za-z0-9_]+\\s*=\\s*\"([0-9]{4}-[0-9]{2}-[0-9]{2})\"".r
    val tripleRegex = "[A-Za-z0-9_]+\\s*=\\s*\"([0-9]{2})\"\\s*/\\s*[A-Za-z0-9_]+\\s*=\\s*\"([0-9]{2})\"\\s*/\\s*[A-Za-z0-9_]+\\s*=\\s*\"([0-9]{4})\"".r

    val executionDate: String = partitionSpec
      .flatMap { spec =>
        isoRegex.findFirstMatchIn(spec).map(_.group(1))
          .orElse(tripleRegex.findFirstMatchIn(spec).map(m => s"${m.group(3)}-${m.group(2)}-${m.group(1)}"))
      }
      .getOrElse(LocalDate.now().toString)

    // ── 3) CARGAR TABLAS ORIGEN Y PODAR COLUMNAS ──
    val rawRef = loadWithPartition(session, refTable, partitionSpec)
    val rawNew = loadWithPartition(session, newTable, partitionSpec)

    // Determinar columnas a comparar
    val partitionKeys = partitionSpec
      .map(_.split("/").map(_.split("=")(0).trim).toSet)
      .getOrElse(Set.empty[String])

    val colsToCompare = rawRef.columns.toSeq
      .filterNot(ignoreCols.contains)
      .filterNot(partitionKeys.contains)
      .filterNot(compositeKeyCols.contains)

    // Solo necesitamos clave + colsToCompare
    val neededCols = compositeKeyCols ++ colsToCompare

    // Reparticiono y persisto para evitar relecturas
    val refDf = rawRef
      .select(neededCols.map(col): _*)
      .repartition(100, compositeKeyCols.map(col): _*)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val newDf = rawNew
      .select(neededCols.map(col): _*)
      .repartition(100, compositeKeyCols.map(col): _*)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // ── 4) GENERAR Y ESCRIBIR DIFERENCIAS ──
    val diffDf = DiffGenerator.generateDifferencesTable(
      session, refDf, newDf,
      compositeKeyCols, colsToCompare,
      includeEqualsInDiff, config
    )
    writeResult(session, s"${tablePrefix}differences", diffDf,
      Seq("id","column","value_ref","value_new","results"),
      initiativeName, executionDate
    )

    // ── 5) GENERAR Y ESCRIBIR DUPLICADOS ──
    if (checkDuplicates) {
      val dupDf = DuplicateDetector.detectDuplicatesTable(
        session, refDf, newDf, compositeKeyCols, config
      )
      writeResult(session, s"${tablePrefix}duplicates", dupDf,
        Seq("origin","id","exact_duplicates","dups_w_variations","occurrences","variations"),
        initiativeName, executionDate
      )
    }

    // ── 6) GENERAR Y ESCRIBIR RESUMEN ──
    val dupRead = if (checkDuplicates)
      session.table(s"${tablePrefix}duplicates").persist(StorageLevel.MEMORY_AND_DISK)
    else
      session.emptyDataFrame

    val summaryDf = SummaryGenerator.generateSummaryTable(
      session, refDf, newDf, diffDf, dupRead,
      compositeKeyCols, rawRef, rawNew, config
    )
    writeResult(session, s"${tablePrefix}summary", summaryDf,
      Seq("bloque","metrica","universo","numerador","denominador","pct","ejemplos"),
      initiativeName, executionDate
    )

    // ── 7) EXPORTAR A EXCEL OPCIONAL ──
    exportExcelPath.foreach(path => SummaryGenerator.exportToExcel(summaryDf, path))

    // ── 8) LIBERAR CACHE ──
    refDf.unpersist()
    newDf.unpersist()
    if (checkDuplicates) dupRead.unpersist()
  }

  // -------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------

  /** Crea las tablas de resultados si no existen */
   def ensureResultTables(
      spark: SparkSession,
      diffTable: String,
      summaryTable: String,
      duplicatesTable: String
  ): Unit = {
    spark.sql(
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
       """.stripMargin)
    spark.sql(
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
       """.stripMargin)
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS $duplicatesTable (
         |  origin STRING,
         |  id STRING,
         |  exact_duplicates STRING,
         |  dups_w_variations STRING,
         |  occurrences STRING,
         |  variations STRING
         |)
         |PARTITIONED BY (initiative STRING, data_date_part STRING)
         |STORED AS PARQUET
       """.stripMargin)
  }

  /** Carga toda la tabla o filtra por cada clave=valor de partitionSpec */
   def loadWithPartition(
      spark: SparkSession,
      tableName: String,
      partitionSpec: Option[String]
  ): DataFrame = {
    val base = spark.table(tableName)
    partitionSpec
      .getOrElse("")
      .split("/")
      .foldLeft(base) { (df, kv) =>
        val parts = kv.split("=", 2)
        if (parts.length == 2) {
          val k = parts(0).trim
          val v = parts(1).replaceAll("\"", "")
          if (df.columns.contains(k)) df.filter(col(k) === lit(v)) else df
        } else df
      }
  }

  /** Inserta el resultado siempre reparticionado por initiative + data_date_part */
   def writeResult(
      spark: SparkSession,
      tableName: String,
      df: DataFrame,
      columns: Seq[String],
      initiative: String,
      executionDate: String
  ): Unit = {
    df.withColumn("initiative", lit(initiative))
      .withColumn("data_date_part", lit(executionDate))
      .repartition(col("initiative"), col("data_date_part"))
      .write.mode(SaveMode.Overwrite)
      .insertInto(tableName)
  }
}
