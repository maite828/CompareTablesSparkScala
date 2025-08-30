import org.apache.spark.sql.SparkSession

class TableComparatorApp()(implicit spark: SparkSession) extends Logging {
  // ─────────────────────────── logger + println (doble log) ───────────────────────────
  private def info(msg: String): Unit = { logger.info(msg); println(msg) }
  private def warn(msg: String): Unit = { logger.warn(msg); println(msg) }
  private def err (msg: String): Unit = { logger.error(msg); println(msg) }

  // ---------------------------- helpers KV ----------------------------
  private def isBlank(s: String): Boolean = (s == null) || s.trim.isEmpty
  private def parseKvArg(s: String): Option[(String, String)] = {
    if (s == null) None
    else {
      val i = s.indexOf('='); if (i <= 0) None
      else {
        val k = s.substring(0, i).trim
        val v = s.substring(i + 1) // no trim para permitir espacios si se quisieran
        if (k.isEmpty) None else Some(k -> v)
      }
    }
  }
  private def csvToSeq(s: String): Seq[String] =
    if (isBlank(s)) Seq.empty else s.split(",").toSeq.map(_.trim).filter(_.nonEmpty)

  // ---------------------------- Entry point ----------------------------
  def execute(args: Array[String]): Unit = {
    info(s"[Driver] Arrancando TableComparatorApp en modo KV-only")
    info(s"[DEBUG] KV-mode args count: ${if (args == null) -1 else args.length}")
    if (args == null || args.isEmpty) {
      val msg = "[Driver] Missing KV args"
      err(msg); throw new IllegalArgumentException(msg)
    }

    val kv: Map[String, String] = args.flatMap(parseKvArg).toMap
    info(s"[DEBUG] KV received keys: ${kv.keys.toSeq.sorted.mkString(",")}")

    def req(k: String): String = {
      val value = kv.getOrElse(k, "")
      if (value == null || value.isEmpty) {
        val msg = s"""Missing required arg: $k"""
        err(msg); throw new IllegalArgumentException(msg)
      }
      value
    }

    val refTable       = req("refTable")
    val newTable       = req("newTable")
    val initiativeName = req("initiativeName")
    val tablePrefixRaw = req("tablePrefix")
    val outputBucket   = req("outputBucket")
    val executionDate  = req("executionDate") // yyyy-MM-dd

    val tablePrefix =
      if (tablePrefixRaw.endsWith("_")) tablePrefixRaw else s"${tablePrefixRaw}_"

    val compositeKeyCols = csvToSeq(kv.getOrElse("compositeKeyCols", ""))
    val ignoreCols       = csvToSeq(kv.getOrElse("ignoreCols", ""))

    val checkDuplicates  = kv.get("checkDuplicates").exists(_.trim.equalsIgnoreCase("true"))
    val includeEquals    = kv.get("includeEqualsInDiff").exists(_.trim.equalsIgnoreCase("true"))

    val rawSpecOpt: Option[String] = kv.get("partitionSpec")
    val partitionSpecOpt: Option[String] =
      PartitionFormatTool.normalizeKvPartitionSpec(rawSpecOpt, executionDate)

    info(s"[DEBUG] partitionSpec (normalized KV): ${partitionSpecOpt.getOrElse("<none>")}")

    val outputDateISO =
      PartitionFormatTool.extractDateOr(partitionSpecOpt, executionDate)
    info(s"[DEBUG] outputDateISO (para data_date_part): $outputDateISO")

    val cfg = CompareConfig(
      spark = spark,
      refTable = refTable,
      newTable = newTable,
      partitionSpec = partitionSpecOpt,
      compositeKeyCols = compositeKeyCols,
      ignoreCols = ignoreCols,
      initiativeName = initiativeName,
      tablePrefix = tablePrefix,
      outputBucket = outputBucket,
      checkDuplicates = checkDuplicates,
      includeEqualsInDiff = includeEquals,
      priorityCol = None,
      aggOverrides = Map.empty,
      nullKeyMatches = true,
      outputDateISO = outputDateISO
    )

    info("[Driver] Llamando a TableComparisonController.run(cfg)")
    TableComparisonController.run(cfg)

    showComparisonResults(spark, cfg.tablePrefix, cfg.initiativeName, outputDateISO)
    info(s"[DEBUG] execDateForFilter: $outputDateISO")
    info("[Driver] KV-mode comparison finished.")
  }

  private def showComparisonResults(
                                     spark: SparkSession,
                                     prefix: String,
                                     initiative: String,
                                     datePart: String
                                   ): Unit = {
    def q(table: String) =
      s"""
         |SELECT *
         |FROM $table
         |WHERE initiative = '$initiative'
         |  AND data_date_part = '$datePart'
         |""".stripMargin

    info("\n-- Available Hive tables --")
    spark.sql("SHOW TABLES").show(false)

    info(s"\n-- Differences (${prefix}differences) --")
    spark.sql(q(prefix + "differences")).show(100, false)

    info(s"\n-- Summary (${prefix}summary) --")
    spark.sql(q(prefix + "summary")).show(100, false)

    info(s"\n-- Duplicates (${prefix}duplicates) --")
    spark.sql(q(prefix + "duplicates")).show(100, false)
  }
}

object TableComparatorApp {
  def execute(args: Array[String])(implicit spark: SparkSession): Unit = {
    val app = new TableComparatorApp()
    app.execute(args)
  }
}
