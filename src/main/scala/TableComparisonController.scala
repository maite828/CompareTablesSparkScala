// src/main/scala/TableComparisonController.scala
// (sin package)

import java.time.LocalDate
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object TableComparisonController {

  /** DataFrames alineados para comparar. */
  private final case class Frames(ref: DataFrame, neu: DataFrame)

  /** Dependencias necesarias para el resumen. */
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

    if (autoCreateTables)
      ensureResultTables(
        session,
        s"$tablePrefix" + "differences",
        s"$tablePrefix" + "summary",
        s"$tablePrefix" + "duplicates"
      )

    // 1) Fecha a sellar en outputs: primero config.outputDateISO, si no derive de partitionSpec
    val executionDate: String =
      Option(outputDateISO).map(_.trim).filter(_.nonEmpty)
        .getOrElse(extractExecutionDate(partitionSpec))
    println(s"[DEBUG] Fecha para outputs (data_date_part): $executionDate")

    // 2) Carga y preparación
    val prep = loadAndPrepare(
      session, refTable, newTable, partitionSpec, compositeKeyCols, ignoreCols
    )

    // 3) Diferencias
    val diffDf = computeDifferences(
      session, prep.refDf, prep.newDf,
      compositeKeyCols, prep.colsToCompare, includeEqualsInDiff, config
    )
    writeResult(
      s"${tablePrefix}differences",
      diffDf,
      Seq("id","column","value_ref","value_new","results"),
      initiativeName,
      executionDate
    )

    // 4) Duplicados (ahora recibe executionDate)
    val dupRead = handleDuplicates(
      session, checkDuplicates, tablePrefix, compositeKeyCols,
      prep.refDf, prep.newDf, config, executionDate
    )

    // 5) Resumen
    val frames = Frames(prep.refDf, prep.newDf)
    val deps   = SummaryDeps(diffDf, dupRead, prep.rawRef, prep.rawNew)
    val summaryDf = computeSummary(frames, deps, config)

    writeResult(
      s"${tablePrefix}summary",
      summaryDf,
      Seq("bloque","metrica","universo","numerador","denominador","pct","ejemplos"),
      initiativeName,
      executionDate
    )

    // 6) Export opcional
    exportExcelPath.foreach(path => SummaryGenerator.exportToExcel(summaryDf, path))

    // 7) Limpieza de cachés
    prep.refDf.unpersist(); prep.newDf.unpersist()
    if (checkDuplicates) dupRead.unpersist()
    diffDf.unpersist(); summaryDf.unpersist()
  }

  // ─────────────────────────── Helpers de orquestación ───────────────────────────

  private def configurePerformance(session: SparkSession): Unit = {
    session.conf.set("spark.sql.shuffle.partitions", "100")
    session.sparkContext.setCheckpointDir("/tmp/checkpoints")
    session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    session.conf.set("hive.exec.dynamic.partition", "true")
    session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  }

  /** Fallback si no viene outputDateISO en config. */
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

  /** Carga tablas (filtrando por partitionSpec si aplica) y prepara columnas a comparar. */
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

    val partitionKeys =
      partitionSpec.map(_.split("/").map(_.split("=", 2)(0).trim).toSet).getOrElse(Set.empty[String])

    val colsToCompare = rawRef.columns.toSeq
      .filterNot(ignoreCols.contains)
      .filterNot(partitionKeys.contains)
      .filterNot(compositeKeyCols.contains)

    val neededCols = compositeKeyCols ++ colsToCompare

    val refDf = rawRef
      .select(neededCols.map(col): _*)
      .repartition(100, compositeKeyCols.map(col): _*)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val newDf = rawNew
      .select(neededCols.map(col): _*)
      .repartition(100, compositeKeyCols.map(col): _*)
      .persist(StorageLevel.MEMORY_AND_DISK)

    Prep(rawRef, rawNew, refDf, newDf, colsToCompare)
  }

  /** Manejo de duplicados (compute + write + read persistido), con fecha explícita. */
  private def handleDuplicates(
    spark: SparkSession,
    enabled: Boolean,
    tablePrefix: String,
    compositeKeyCols: Seq[String],
    refDf: DataFrame,
    newDf: DataFrame,
    config: CompareConfig,
    executionDate: String
  ): DataFrame = {
    if (!enabled) spark.emptyDataFrame
    else {
      val dupDf = computeDuplicates(spark, refDf, newDf, compositeKeyCols, config)
      writeResult(
        s"${tablePrefix}duplicates",
        dupDf,
        Seq("origin","id","exact_duplicates","dups_w_variations","occurrences","variations"),
        config.initiativeName,
        executionDate
      )
      spark.table(s"${tablePrefix}duplicates").persist(StorageLevel.MEMORY_AND_DISK)
    }
  }

  // ─────────────────────────── Cálculos ───────────────────────────

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

  private def computeSummary(
    frames: Frames,
    deps: SummaryDeps,
    config: CompareConfig
  ): DataFrame = {
    val spark = config.spark
    val df = SummaryGenerator.generateSummaryTable(
      spark,
      frames.ref, frames.neu,
      deps.diff, deps.dups,
      config.compositeKeyCols,
      deps.rawRef, deps.rawNew,
      config
    )
    df.persist(StorageLevel.MEMORY_AND_DISK)
  }

  // ─────────────────────────── Utilidades ───────────────────────────

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
          val v = parts(1).replace("\"", "")
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
    val out = df.select(columns.map(col): _*)
      .withColumn("initiative", lit(initiative))
      .withColumn("data_date_part", lit(executionDate))
      .repartition(col("initiative"), col("data_date_part"))

    // Con partitionOverwriteMode=dynamic, overwrite sólo las particiones afectadas.
    out.write
      .mode(SaveMode.Overwrite)
      .insertInto(tableName)
  }
}
