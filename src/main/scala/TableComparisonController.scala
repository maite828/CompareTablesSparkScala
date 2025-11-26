import org.apache.spark.internal.Logging

import java.time.LocalDate
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * Orchestrates the end-to-end comparison workflow (lean controller).
 * Notes:
 *  - We force Metastore Parquet → DataSource scans to avoid HiveTableReader serialization issues.
 *  - We assert physical plans are FileScan (not HiveTableScan) before heavy ops and before writing to sinks.
 */
object TableComparisonController extends Logging {

  // lightweight double-logging
  private def logInfo(msg: String): Unit = { log.info(msg); println(msg) }
  private def logWarn(msg: String): Unit = { log.warn(msg); println(msg) }

  // Minimal DTO used by this file
  private final case class Prep(
                                 rawRef: DataFrame,
                                 rawNew: DataFrame,
                                 refDf: DataFrame,
                                 newDf: DataFrame,
                                 colsToCompare: Seq[String]
                               )

  // ─────────────────────────── Entry point ───────────────────────────
  def run(config: CompareConfig): Unit = {
    val spark = config.spark

    // Make sure DataSource readers are enabled before any table() is materialized
    enableFileSourceReaders(spark)

    // Clear any cached plan that could contain stale Hive scans
    spark.catalog.clearCache()

    ensureTablesIfNeeded(config)
    // Verify the three sink tables are DataSource Parquet (or missing, in which case we will create)
    verifySinksAreDatasource(config)

    val executionDate = resolveExecutionDate(config.partitionSpec, config.outputDateISO)
    logInfo(s"[DEBUG] Date for outputs (data_date_part): $executionDate")

    val prep      = loadAndPrepare(
      spark,
      config.refTable,
      config.newTable,
      config.partitionSpec,
      config.compositeKeyCols,
      config.ignoreCols,
      // overrides por lado (nuevos)
      config.refPartitionSpecOverride,
      config.newPartitionSpecOverride,
      // filtros SQL personalizados
      config.refFilter,
      config.newFilter,
      config.columnMapping // NEW
    )
    val diffDf    = computeAndWriteDifferences(config, prep, executionDate)
    val dupRead   = computeWriteAndReadDuplicates(config, prep, executionDate)
    val summaryDf = computeAndWriteSummary(config, prep, diffDf, dupRead, executionDate)

    exportExcelIfRequested(summaryDf, config.exportExcelPath)
    cleanup(prep, diffDf, dupRead, summaryDf, config.checkDuplicates)
  }

  // ─────────────────────────── Control plane ───────────────────────────
  private def ensureTablesIfNeeded(config: CompareConfig): Unit = {
    if (config.autoCreateTables) {
      val p = config.tablePrefix
      TableIO.ensureResultTables(
        config.spark,
        s"${p}differences",
        s"${p}summary",
        s"${p}duplicates",
        config
      )
    }
  }

  /** Best-effort check that sink tables resolve to DataSource scans. */
  private def verifySinksAreDatasource(config: CompareConfig): Unit = {
    val spark = config.spark
    val sinks = Seq(
      s"${config.tablePrefix}differences",
      s"${config.tablePrefix}summary",
      s"${config.tablePrefix}duplicates"
    )
    sinks.foreach { t =>
      if (spark.catalog.tableExists(t)) {
        // Will throw with a clear message if it resolves to HiveTableScan/HadoopTableReader
        assertDatasourceTable(spark, t)
      } else {
        logWarn(s"[CONF] Sink table '$t' does not exist yet; it will be created on first write.")
      }
    }
  }

  private def resolveExecutionDate(partitionSpec: Option[String], configuredISO: String): String =
    Option(configuredISO).map(_.trim).filter(_.nonEmpty).getOrElse(extractExecutionDate(partitionSpec))

  private def extractExecutionDate(partitionSpec: Option[String]): String = {
    val isoRx =
      "[A-Za-z0-9_]+\\s*=\\s*\\\"([0-9]{4}-[0-9]{2}-[0-9]{2})\\\"".r
    val tripleRx =
      "[A-Za-z0-9_]+\\s*=\\s*\\\"([0-9]{2})\\\"\\s*/\\s*[A-Za-z0-9_]+\\s*=\\s*\\\"([0-9]{2})\\\"\\s*/\\s*[A-Za-z0-9_]+\\s*=\\s*\\\"([0-9]{4})\\\"".r
    partitionSpec
      .flatMap { spec =>
        isoRx.findFirstMatchIn(spec).map(_.group(1))
          .orElse(tripleRx.findFirstMatchIn(spec).map(m => s"${m.group(3)}-${m.group(2)}-${m.group(1)}"))
      }
      .getOrElse(LocalDate.now().toString)
  }

  // ─────────────────────────── Main stages ───────────────────────────
  private def computeAndWriteDifferences(config: CompareConfig, prep: Prep, executionDate: String): DataFrame = {
    val diffDf = ComparisonBuilders.computeDifferences(
      config.spark,
      prep.refDf,
      prep.newDf,
      config.compositeKeyCols,
      prep.colsToCompare,
      config.includeEqualsInDiff,
      config
    )
    writeResult(
      s"${config.tablePrefix}differences",
      diffDf,
      Seq("id", "column", "value_ref", "value_new", "results"),
      config.initiativeName,
      executionDate
    )
    diffDf
  }

  private def computeWriteAndReadDuplicates(config: CompareConfig, prep: Prep, executionDate: String): DataFrame = {
    if (config.checkDuplicates) {
      val dupDf = ComparisonBuilders.computeDuplicates(
        config.spark,
        prep.refDf,
        prep.newDf,
        config.compositeKeyCols,
        config
      )
      writeResult(
        s"${config.tablePrefix}duplicates",
        dupDf,
        Seq("origin","id","category","exact_duplicates","dupes_w_variations","occurrences","variations"),
        config.initiativeName,
        executionDate
      )
      // Read back and assert the plan is also a FileScan (sink must be DataSource)
      val dupRead = config.spark.table(s"${config.tablePrefix}duplicates")
      assertFileSource(dupRead, s"[sink-read] ${config.tablePrefix}duplicates")
      dupRead
    } else {
      config.spark.emptyDataFrame
    }
  }

  private def computeAndWriteSummary(
                                      config: CompareConfig,
                                      prep: Prep,
                                      diffDf: DataFrame,
                                      dupRead: DataFrame,
                                      executionDate: String
                                    ): DataFrame = {
    val inputs = SummaryInputs(
      config.spark,
      prep.refDf,
      prep.newDf,
      diffDf,
      dupRead,
      config.compositeKeyCols
    )

    val summaryDf = SummaryGenerator.generateSummaryTable(inputs)
    writeResult(
      s"${config.tablePrefix}summary",
      summaryDf,
      Seq("block","metric","universe","numerator","denominator","pct","samples"),
      config.initiativeName,
      executionDate
    )
    summaryDf
  }

  private def exportExcelIfRequested(summaryDf: DataFrame, target: Option[String]): Unit =
    target.foreach(path => SummaryGenerator.exportToExcel(summaryDf, path))

  private def cleanup(
                       prep: Prep,
                       diffDf: DataFrame,
                       dupRead: DataFrame,
                       summaryDf: DataFrame,
                       hadDup: Boolean
                     ): Unit = {
    // unpersist is no-op if not cached
    prep.rawRef.unpersist(blocking = true)
    prep.rawNew.unpersist(blocking = true)
    prep.refDf.unpersist(blocking = true)
    prep.newDf.unpersist(blocking = true)
    if (hadDup) dupRead.unpersist(blocking = true)
    diffDf.unpersist(blocking = true)
    summaryDf.unpersist(blocking = true)
  }

  // ─────────────────────────── Load & prep ───────────────────────────
  private def loadAndPrepare(
                              spark: SparkSession,
                              refTable: String,
                              newTable: String,
                              partitionSpec: Option[String],
                              compositeKeyCols: Seq[String],
                              ignoreCols: Seq[String],
                              refPartitionSpecOverride: Option[String],   // NEW
                              newPartitionSpecOverride: Option[String],   // NEW
                              refFilter: Option[String] = None,           // NEW
                              newFilter: Option[String] = None,           // NEW
                              columnMapping: Map[String, String] = Map.empty // NEW
                            ): Prep = {

    // Decidir spec por lado (precedencia: override > global)
    val refSpec = refPartitionSpecOverride.orElse(partitionSpec)

    // Para newSpec, si hay mapeo, debemos "des-mapear" las columnas de partición
    // (usar el nombre físico en newTable en lugar del nombre lógico de refTable)
    val rawNewSpec = newPartitionSpecOverride.orElse(partitionSpec)
    val newSpec = rawNewSpec.map { spec =>
      columnMapping.foldLeft(spec) { case (currentSpec, (refCol, newCol)) =>
        // Reemplaza "refCol=" por "newCol=" en la spec
        // Usamos regex para asegurar que reemplazamos nombres de columna completos
        currentSpec.replaceAll(s"\\b$refCol=", s"$newCol=")
      }
    }

    // Read with partition pruning
    val rawRef = PartitionPruning.loadWithPartition(spark, refTable, refSpec)
    val rawNew = PartitionPruning.loadWithPartition(spark, newTable, newSpec)

    // Optional per-side filters before any further processing
    val filteredRef = applyOptionalFilter(rawRef, refFilter, "ref")
    val filteredNewRaw = applyOptionalFilter(rawNew, newFilter, "new")

    // NEW: Apply column mapping (rename NEW columns to match REF)
    val filteredNew = applyColumnMapping(filteredNewRaw, columnMapping)

    // Defensive: ensure FileScan (datasource) and not HiveTableScan
    assertFileSource(filteredRef, s"ref=$refTable")
    assertFileSource(filteredNew, s"new=$newTable")

    // Log row counts before/after filtering
    if (refFilter.isDefined || newFilter.isDefined) {
      logInfo(s"[FILTER] Applying custom SQL filters:")
      if (refFilter.isDefined) logInfo(s"[FILTER]   REF: ${refFilter.get}")
      if (newFilter.isDefined) logInfo(s"[FILTER]   NEW: ${newFilter.get}")
    }

    PrepUtils.logFilteredInputFiles(filteredRef, newDf = filteredNew, info = logInfo)

    val schemaReport = SchemaChecker.analyze(filteredRef, filteredNew)
    SchemaChecker.log(schemaReport, logInfo, logWarn)

    // Compute columns to compare from REF (for backward compatibility)
    val colsToCompareRef = PrepUtils.computeColsToCompare(filteredRef, refSpec, ignoreCols, compositeKeyCols)

    // Get actual available columns from each DataFrame (only select columns that exist)
    val refAvailableCols = filteredRef.columns.toSet
    val newAvailableCols = filteredNew.columns.toSet

    // For each table, select: keys + columns that exist in that table
    val neededColsRef = (compositeKeyCols ++ colsToCompareRef).filter(refAvailableCols.contains).distinct
    val neededColsNew = (compositeKeyCols ++ colsToCompareRef).filter(newAvailableCols.contains).distinct

    logInfo(s"[COLUMNS] ✓ REF selecting ${neededColsRef.length} cols (${compositeKeyCols.length} keys + ${neededColsRef.length - compositeKeyCols.length} data)")
    logInfo(s"[COLUMNS] ✓ NEW selecting ${neededColsNew.length} cols (${compositeKeyCols.length} keys + ${neededColsNew.length - compositeKeyCols.length} data)")

    // Columns to actually compare: intersection of both after filtering
    val colsToCompare = colsToCompareRef.filter(c => refAvailableCols.contains(c) && newAvailableCols.contains(c))
    val excludedCols = compositeKeyCols.length + ignoreCols.length +
      (if (refSpec.isDefined) refSpec.get.split("/").length else 0)
    logInfo(s"[COLUMNS] → Comparing ${colsToCompare.length} common columns")
    logInfo(s"[COLUMNS] → Excluded: ${excludedCols} total (${compositeKeyCols.length} keys, ${ignoreCols.length} ignored, partition cols)")
    logInfo(s"[COLUMNS] → Comparison scope: Only columns present in BOTH tables will be compared")

    val nParts = PrepUtils.pickTargetPartitions(spark)
    logInfo(s"[DEBUG] repartition(nParts=$nParts, keys=${compositeKeyCols.mkString(",")})")

    // PERF OPTIMIZATION: Use SER for 30-50% less memory usage (serialized + compressed)
    val refDf = PrepUtils
      .selectAndRepartition(filteredRef, neededColsRef, compositeKeyCols, nParts)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val newDf = PrepUtils
      .selectAndRepartition(filteredNew, neededColsNew, compositeKeyCols, nParts)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    Prep(filteredRef, filteredNew, refDf, newDf, colsToCompare)
  }

  // Apply an optional SQL filter clause to a DF, logging the expression.
  // PERF OPTIMIZATION: Removed expensive count() operations (2 full scans eliminated)
  // Use Spark UI → SQL tab to monitor actual row counts if needed
  private def applyOptionalFilter(df: DataFrame, filterExprOpt: Option[String], label: String): DataFrame =
    filterExprOpt match {
      case Some(expr) if expr.nonEmpty =>
        val filtered = df.filter(expr)
        logInfo(s"[FILTER] ✓ Applied filter on '$label': $expr")
        logInfo(s"[FILTER]   (Row counts skipped for performance - use Spark UI to monitor)")
        filtered
      case _ => df
    }

  // NEW: Rename columns in NEW table based on mapping (refName -> newName)
  // We want NEW table to have REF names. So if mapping is "id" -> "id_v2", we rename "id_v2" to "id".
  def applyColumnMapping(df: DataFrame, mapping: Map[String, String]): DataFrame = {
    if (mapping.isEmpty) df
    else {
      logInfo(s"[MAPPING] Applying column mapping: ${mapping.mkString(", ")}")
      mapping.foldLeft(df) { case (acc, (refName, newName)) =>
        if (acc.columns.contains(newName)) {
          logInfo(s"[MAPPING] Renaming '$newName' -> '$refName'")
          acc.withColumnRenamed(newName, refName)
        } else {
          logWarn(s"[MAPPING] [WARN] Target column '$newName' not found in NEW table. Skipping rename to '$refName'.")
          acc
        }
      }
    }
  }

  // ─────────────────────────── Write helpers ───────────────────────────
  private def writeResult(
                           tableName: String,
                           df: DataFrame,
                           columns: Seq[String],
                           initiative: String,
                           executionDate: String
                         ): Unit = {
    val spark = df.sparkSession

    // Pre-flight: if the sink table exists, ensure it resolves to a FileScan
    if (spark.catalog.tableExists(tableName)) {
      assertDatasourceTable(spark, tableName)
    } else {
      logWarn(s"[CONF] Sink '$tableName' does not exist yet; it will be created (IF NOT EXISTS already attempted).")
    }

    val out = df
      .select(columns.map(col): _*)
      .withColumn("initiative", lit(initiative))
      .withColumn("data_date_part", lit(executionDate))

    logInfo(s"[DEBUG] writeResult: insertInto($tableName) | hasData=${!out.isEmpty}")
    // Control output file size to prevent huge partition files (e.g., >1GB)
    // Target: ~128MB per file (671K rows assuming 200 bytes/row average)
    // Works in Databricks shared clusters where we can't modify global Spark config
    out.write
      .option("maxRecordsPerFile", ComparatorDefaults.MaxRecordsPerFile)
      .mode(SaveMode.Overwrite)
      .insertInto(tableName)
  }

  // ─────────────────────────── Source enforcement ───────────────────────────
  /** Make sure Spark uses DataSource (FileScan) for metastore Parquet/ORC tables. */
  private def enableFileSourceReaders(spark: SparkSession): Unit = {
    // Do this before any spark.table(...) is materialized
    spark.conf.set("spark.sql.hive.convertMetastoreParquet", "true")
    spark.conf.set("spark.sql.hive.convertMetastoreOrc", "true")
    // Nice-to-have for inserts into partitioned result tables
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    val convParquet = spark.conf.get("spark.sql.hive.convertMetastoreParquet")
    val convOrc     = spark.conf.get("spark.sql.hive.convertMetastoreOrc")
    logInfo(s"[CONF] convertMetastoreParquet=$convParquet, convertMetastoreOrc=$convOrc")
    logInfo(s"[CONF] Output file size control: maxRecordsPerFile=${ComparatorDefaults.MaxRecordsPerFile} (~128MB per file)")
  }

  /**
   * Asserts the physical plan is a FileScan (datasource), not a HiveTableScan/HadoopTableReader.
   * If Hive scan is detected, we fail fast with a clear message.
   */
  private def assertFileSource(df: DataFrame, label: String): Unit = {
    val plan = df.queryExecution.executedPlan.toString()
    val isHive = plan.contains("HiveTableScanExec") || plan.contains("HadoopTableReader")
    if (isHive) {
      val msg =
        s"""[ERROR] '$label' is being read through Hive SerDe reader (HiveTableScan/HadoopTableReader).
           |This can trigger 'Task not serializable' in PRE.
           |Ensure:
           |  - spark.sql.hive.convertMetastoreParquet=true (we set it at start)
           |  - Table is created as 'USING parquet' (drop & recreate if needed).
           |Physical plan:
           |$plan
           |""".stripMargin
      throw new IllegalStateException(msg)
    }
  }

  /** Assert an existing catalog table resolves to a FileScan (not HiveTableScan). */
  private def assertDatasourceTable(spark: SparkSession, tableName: String): Unit = {
    val df = spark.table(tableName).limit(1) // cheap read; enough to inspect executedPlan
    assertFileSource(df, s"[sink-check] $tableName")
  }
}