
import java.time.LocalDate
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types._

// Nuevo: fuentes agnósticas

import CompareConfig._

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
    val partitionEntries = partitionSpec.map(parsePartitionSpec).getOrElse(Seq.empty)

    val executionDate: String = resolveExecutionDate(partitionEntries, executionDateOverride)

    // ── 3) CARGAR ORÍGENES Y PODAR COLUMNAS ──
    val rawRef = loadSource(session, refSource, partitionEntries)
    val rawNew = loadSource(session, newSource, partitionEntries)

    // Determinar columnas a comparar
    val partitionKeys = partitionEntries.map(_._1).toSet

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

    // ── 4) DIFERENCIAS ──
    val diffDf = DiffGenerator.generateDifferencesTable(
      session, refDf, newDf,
      compositeKeyCols, colsToCompare,
      includeEqualsInDiff, config
    )
    writeResult(session, s"${tablePrefix}differences", diffDf,
      Seq("id","column","value_ref","value_new","results"),
      initiativeName, executionDate
    )

    // ── 5) DUPLICADOS ──
    if (checkDuplicates) {
      val dupDf = DuplicateDetector.detectDuplicatesTable(
        session, refDf, newDf, compositeKeyCols, config
      )
      writeResult(session, s"${tablePrefix}duplicates", dupDf,
        Seq("origin","id","exact_duplicates","dups_w_variations","occurrences","variations"),
        initiativeName, executionDate
      )
    }

    // ── 6) RESUMEN ──
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

    // ── 7) EXPORT EXCEL ──
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

  // ───────────────────────────────────────────────────────────────────
  // NUEVO: carga agnóstica de fuente + filtro por partitionSpec
  // ───────────────────────────────────────────────────────────────────
  private def loadSource(
      spark: SparkSession,
      source: SourceSpec,
      partitionEntries: Seq[(String, String)]
  ): DataFrame = {
    val base: DataFrame = source match {
      case HiveTable(tableName, _) =>
        spark.table(tableName)

      case FileSource(path, fmt, opts, maybeSchema) =>
        val reader0 = spark.read.format(fmt).options(opts)
        val reader  = maybeSchema.map(reader0.schema).getOrElse(reader0)
        reader.load(path)
    }

    partitionEntries.foldLeft(base) {
      case (df, (key, value)) if df.columns.contains(key) && shouldApplyPartitionFilter(value) =>
        df.filter(col(key) === lit(value))
      case (df, _) => df
    }
  }

  private def parsePartitionSpec(spec: String): Seq[(String, String)] = {
    spec.split("/").toSeq.flatMap { token =>
      val parts = token.split("=", 2)
      if (parts.length == 2) {
        val key   = parts(0).trim
        val value = parts(1).trim.stripPrefix("\"").stripSuffix("\"")
        if (key.nonEmpty) Some(key -> value) else None
      } else None
    }
  }

  private val isoDateRegex     = "(\\d{4}-\\d{2}-\\d{2})".r
  private val tripleDateRegex  = "(\\d{2})/(\\d{2})/(\\d{4})".r
  private val placeholderRegex = "(?i)^(Y{2,4}|M{2}|D{2})([-_/]?(Y{2,4}|M{2}|D{2}))*$".r

  private def resolveExecutionDate(
      partitionEntries: Seq[(String, String)],
      overrideDate: Option[String]
  ): String = {
    overrideDate
      .orElse {
        partitionEntries.collectFirst { case (_, isoDateRegex(date)) => date }
      }
      .orElse {
        partitionEntries.collectFirst {
          case (_, tripleDateRegex(day, month, year)) => s"$year-$month-$day"
        }
      }
      .getOrElse(LocalDate.now().toString)
  }

  private def shouldApplyPartitionFilter(value: String): Boolean = {
    val trimmed = value.trim
    if (trimmed.isEmpty) {
      false
    } else {
      val containsTemplate   = trimmed.contains("{{") || trimmed.contains("}}")
      val looksLikePlaceholder = placeholderRegex.pattern.matcher(trimmed).matches()
      val hasWildcard        = trimmed.contains("*") || trimmed.equalsIgnoreCase("ALL")
      val startsWithVar      = trimmed.startsWith("$")

      !(hasWildcard || containsTemplate || looksLikePlaceholder || startsWithVar)
    }
  }
}
