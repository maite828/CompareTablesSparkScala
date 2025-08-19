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
 * CompareConfig containing Spark session, table names, columns, partition specs, and options.
 */

import java.time.LocalDate
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object TableComparisonController {

  /** Minimal holder for the two aligned DataFrames under comparison. */
  private final case class Frames(ref: DataFrame, neu: DataFrame)

  /** Minimal holder for summary dependencies to avoid >8 params. */
  private final case class SummaryDeps(
                                        diff: DataFrame,
                                        dups: DataFrame,
                                        rawRef: DataFrame,
                                        rawNew: DataFrame
                                      )

  // --------------------------------------------------------------------------------------

  def run(config: CompareConfig): Unit = {
    import config._
    val session = spark

    configurePerformance(session)
    if (autoCreateTables) ensureResultTables(session, s"$tablePrefix" + "differences", s"$tablePrefix" + "summary", s"$tablePrefix" + "duplicates")

    val executionDate = extractExecutionDate(partitionSpec)
    val prep = loadAndPrepare(session, refTable, newTable, partitionSpec, compositeKeyCols, ignoreCols)

    val diffDf = computeDifferences(session, prep.refDf, prep.newDf, compositeKeyCols, prep.colsToCompare, includeEqualsInDiff, config)
    writeResult(s"${tablePrefix}differences", diffDf, Seq("id","column","value_ref","value_new","results"), initiativeName, executionDate)

    val dupRead = handleDuplicates(session, checkDuplicates, tablePrefix, compositeKeyCols, prep.refDf, prep.newDf, config)

    // --- CHANGED: build holders and call the new computeSummary (<= 8 params) -----------
    val frames = Frames(prep.refDf, prep.newDf)
    val deps   = SummaryDeps(diffDf, dupRead, prep.rawRef, prep.rawNew)

    val summaryDf = computeSummary(frames, deps, config)
    // ------------------------------------------------------------------------------------

    writeResult(
      s"${tablePrefix}summary", summaryDf, Seq("bloque","metrica","universo","numerador","denominador","pct","ejemplos"),
      initiativeName, executionDate)

    exportExcelPath.foreach(path => SummaryGenerator.exportToExcel(summaryDf, path))

    prep.refDf.unpersist(); prep.newDf.unpersist()
    if (checkDuplicates) dupRead.unpersist()
    diffDf.unpersist(); summaryDf.unpersist()
  }

  // ─────────────────────────── Helpers de orquestación (sin cambiar semántica) ───────────────────────────

  /** Keep the original performance settings exactly the same. */
  private def configurePerformance(session: SparkSession): Unit = {
    session.conf.set("spark.sql.shuffle.partitions", "100")
    session.sparkContext.setCheckpointDir("/tmp/checkpoints")
    session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    session.conf.set("hive.exec.dynamic.partition", "true")
    session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  }

  /** Extract execution date from the same regex rules as before. */
  private def extractExecutionDate(partitionSpec: Option[String]): String = {
    val isoRegex    = "[A-Za-z0-9_]+\\s*=\\s*\"([0-9]{4}-[0-9]{2}-[0-9]{2})\"".r
    val tripleRegex = "[A-Za-z0-9_]+\\s*=\\s*\"([0-9]{2})\"\\s*/\\s*[A-Za-z0-9_]+\\s*=\\s*\"([0-9]{2})\"\\s*/\\s*[A-Za-z0-9_]+\\s*=\\s*\"([0-9]{4})\"".r
    partitionSpec
      .flatMap { spec =>
        isoRegex.findFirstMatchIn(spec).map(_.group(1))
          .orElse(tripleRegex.findFirstMatchIn(spec).map(m => s"${m.group(3)}-${m.group(2)}-${m.group(1)}"))
      }
      .getOrElse(LocalDate.now().toString)
  }

  private final case class Prep(
                                 rawRef: DataFrame,
                                 rawNew: DataFrame,
                                 refDf: DataFrame,
                                 newDf: DataFrame,
                                 colsToCompare: Seq[String]
                               )

  /** Load tables, derive colsToCompare, select needed columns, repartition and persist exactly like before. */
  private def loadAndPrepare(
                              spark: SparkSession,
                              refTable: String,
                              newTable: String,
                              partitionSpec: Option[String],
                              compositeKeyCols: Seq[String],
                              ignoreCols: Seq[String]
                            ): Prep = {
    val rawRef = loadWithPartition(spark, refTable, partitionSpec)
    val rawNew = loadWithPartition(spark, newTable, partitionSpec)

    val partitionKeys = partitionSpec.map(_.split("/").map(_.split("=")(0).trim).toSet).getOrElse(Set.empty[String])

    val colsToCompare = rawRef.columns.toSeq
      .filterNot(ignoreCols.contains)
      .filterNot(partitionKeys.contains)
      .filterNot(compositeKeyCols.contains)

    val neededCols = compositeKeyCols ++ colsToCompare

    val refDf = rawRef.select(neededCols.map(col): _*).repartition(100, compositeKeyCols.map(col): _*)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val newDf = rawNew.select(neededCols.map(col): _*).repartition(100, compositeKeyCols.map(col): _*)
      .persist(StorageLevel.MEMORY_AND_DISK)

    Prep(rawRef, rawNew, refDf, newDf, colsToCompare)
  }

  /** Duplicate stage handling with identical semantics: compute+write, then read table and persist. */
  private def handleDuplicates(
                                spark: SparkSession,
                                enabled: Boolean,
                                tablePrefix: String,
                                compositeKeyCols: Seq[String],
                                refDf: DataFrame,
                                newDf: DataFrame,
                                config: CompareConfig
                              ): DataFrame = {
    if (!enabled) spark.emptyDataFrame
    else {
      val dupDf = computeDuplicates(spark, refDf, newDf, compositeKeyCols, config)
      writeResult(s"${tablePrefix}duplicates", dupDf,
        Seq("origin","id","exact_duplicates","dups_w_variations","occurrences","variations"),
        config.initiativeName, extractExecutionDate(config.partitionSpec)
      )
      spark.table(s"${tablePrefix}duplicates").persist(StorageLevel.MEMORY_AND_DISK)
    }
  }

  // ─────────────────────────── Helpers de cálculo (idéntico a antes) ───────────────────────────

  /** Compute differences exactly as before and persist it to avoid recomputation downstream. */
  private def computeDifferences(
                                  spark: SparkSession,
                                  refDf: DataFrame,
                                  newDf: DataFrame,
                                  compositeKeyCols: Seq[String],
                                  colsToCompare: Seq[String],
                                  includeEqualsInDiff: Boolean,
                                  config: CompareConfig,
                                  persistLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
                                ): DataFrame = {
    val df = DiffGenerator.generateDifferencesTable(
      spark, refDf, newDf,
      compositeKeyCols, colsToCompare,
      includeEqualsInDiff, config
    )
    df.persist(persistLevel)
  }

  /** Compute duplicates exactly as before and persist it to avoid recomputation downstream. */
  private def computeDuplicates(
                                 spark: SparkSession,
                                 refDf: DataFrame,
                                 newDf: DataFrame,
                                 compositeKeyCols: Seq[String],
                                 config: CompareConfig,
                                 persistLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
                               ): DataFrame = {
    val df = DuplicateDetector.detectDuplicatesTable(
      spark, refDf, newDf, compositeKeyCols, config
    )
    df.persist(persistLevel)
  }

  /** Compute summary with identical semantics*/
  private def computeSummary(
                              frames: Frames,
                              deps: SummaryDeps,
                              config: CompareConfig
                            ): DataFrame = {
    // Use spark/compositeKeyCols from config to avoid passing them as parameters.
    val spark = config.spark
    val df = SummaryGenerator.generateSummaryTable(
      spark,
      frames.ref, frames.neu,
      deps.diff, deps.dups,
      config.compositeKeyCols,
      deps.rawRef, deps.rawNew,
      config
    )
    // Keep the same persistence behavior as before.
    df.persist(StorageLevel.MEMORY_AND_DISK)
  }

  // ─────────────────────────── Helpers existentes (sin cambios) ───────────────────────────

  private def ensureResultTables(
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

  private def loadWithPartition(
                                 spark: SparkSession,
                                 tableName: String,
                                 partitionSpec: Option[String]
                               ): DataFrame = {
    val base = spark.table(tableName)
    partitionSpec.getOrElse("")
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

  private def writeResult(
                           tableName: String,
                           df: DataFrame,
                           columns: Seq[String],
                           initiative: String,
                           executionDate: String
                         ): Unit = {
    df.select(columns.map(col): _*)
      .withColumn("initiative", lit(initiative))
      .withColumn("data_date_part", lit(executionDate))
      .repartition(col("initiative"), col("data_date_part"))
      .write.mode(SaveMode.Overwrite)
      .insertInto(tableName)
  }
}
