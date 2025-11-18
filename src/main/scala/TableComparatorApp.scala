import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.time.LocalDate

class TableComparatorApp()(implicit spark: SparkSession) extends Logging {

  // ─────────────────────────── log + println (double logging) ───────────────────────────
  private def info(msg: String): Unit = { log.info(msg); println(msg) }
  private def warn(msg: String): Unit = { log.warn(msg); println(msg) }
  private def err (msg: String): Unit = { log.error(msg); println(msg) }

  // ---------------------------- KV helpers ----------------------------
  private def isBlank(s: String): Boolean = (s == null) || s.trim.isEmpty

  private val KvPattern = """^\s*([^=]+)\s*=\s*(.*)\s*$""".r
  private def stripQuotes(s: String): String =
    s.replaceAll("""^\s*['"]|['"]\s*$""", "")

  // Parse "k=v" keeping inner quotes; removes only outer quotes.
  private def parseKvArg(s: String): Option[(String, String)] = s match {
    case null => None
    case KvPattern(k, vRaw) =>
      val k1 = Option(k).map(_.trim).getOrElse("")
      if (k1.isEmpty) { None } else { Some(k1 -> stripQuotes(vRaw)) }
    case _ => None
  }

  private def csvToSeq(s: String): Seq[String] =
    if (isBlank(s)) { Seq.empty }
    else { s.split(",").iterator.map(_.trim).filter(_.nonEmpty).toSeq }

  private def requireArg(kv: Map[String, String], k: String): String = {
    val value = kv.getOrElse(k, "")
    if (value == null || value.isEmpty) {
      val msg = s"Missing required arg: $k"
      err(msg); throw new IllegalArgumentException(msg)
    }
    value
  }

  private def normalizeTablePrefix(raw: String): String =
    if (raw.endsWith("_")) raw else s"${raw}_"

  private def parseBooleans(kv: Map[String, String]): (Boolean, Boolean) = {
    val checkDuplicates = kv.get("checkDuplicates").exists(_.trim.equalsIgnoreCase("true"))
    val includeEquals = kv.get("includeEqualsInDiff").exists(_.trim.equalsIgnoreCase("true"))
    (checkDuplicates, includeEquals)
  }

  private def normalizePartitionSpec(rawSpecOpt: Option[String], executionDate: String): Option[String] = {
    val norm = PartitionFormatTool.normalizeKvPartitionSpec(rawSpecOpt, executionDate)
    info(s"[DEBUG] partitionSpec (normalized KV): ${norm.getOrElse("<none>")}")
    norm
  }

  private def extractOutputDate(partitionSpec: Option[String], defaultExecDate: String): String = {
    val iso = PartitionFormatTool.extractDateOr(partitionSpec, defaultExecDate)
    info(s"[DEBUG] outputDateISO (for data_date_part): $iso")
    iso
  }

  // ─────────────────────────── Nuevos helpers: ventanas/overrides por lado ───────────────────────────
  private val WindowRx = """^\s*([+-]?\d+)\s*\.\.\s*([+-]?\d+)\s*$""".r

  private def parseWindow(s: String): Option[(Int, Int)] = s match {
    case null => None
    case WindowRx(a, b) =>
      try Some(a.toInt -> b.toInt) catch { case _: Throwable => None }
    case _ => None
  }

  private def expandDates(centerISO: String, start: Int, end: Int): Seq[String] = {
    val base = LocalDate.parse(centerISO)
    (start to end).map(delta => base.plusDays(delta.toLong).toString)
  }

  /** Inserta o sustituye data_date_part con lista [d1,d2,...] en una spec existente. */
  private def upsertDates(specOpt: Option[String], dates: Seq[String]): Option[String] = {
    if (dates == null || dates.isEmpty) specOpt
    else {
      val list = s"[${dates.mkString(",")}]"
      specOpt match {
        case None => Some(s"data_date_part=$list")
        case Some(specRaw) =>
          val spec = specRaw.trim
          if (spec.isEmpty) Some(s"data_date_part=$list")
          else {
            val rx = "data_date_part\\s*=\\s*[^/]+"
            if (spec.matches(s".*${rx}.*"))
              Some(spec.replaceAll(rx, s"data_date_part=$list"))
            else
              Some(if (spec.endsWith("/")) s"${spec}data_date_part=$list" else s"$spec/data_date_part=$list")
          }
      }
    }
  }

  private def buildConfig(kv: Map[String, String]): CompareConfig = {
    val refTable = requireArg(kv, "refTable")
    val newTable = requireArg(kv, "newTable")
    val initiativeName = requireArg(kv, "initiativeName")
    val tablePrefixRaw = requireArg(kv, "tablePrefix")
    val outputBucket = requireArg(kv, "outputBucket")
    val executionDate = requireArg(kv, "executionDate") // yyyy-MM-dd

    val tablePrefix = normalizeTablePrefix(tablePrefixRaw)
    val compositeKeyCols = csvToSeq(kv.getOrElse("compositeKeyCols", ""))
    val ignoreCols = csvToSeq(kv.getOrElse("ignoreCols", ""))

    val (checkDuplicates, includeEquals) = parseBooleans(kv)

    val rawSpecOpt = kv.get("partitionSpec")
    val partitionSpec = normalizePartitionSpec(rawSpecOpt, executionDate)
    val outputDateISO = extractOutputDate(partitionSpec, executionDate)

    // ---- overrides por lado (precedencia: direct > window > partitionSpec) ----
    // 1) Overrides directos
    val refSpecDirect = kv.get("refPartitionSpec")
      .flatMap(s => PartitionFormatTool.normalizeKvPartitionSpec(Option(s), executionDate))
    val newSpecDirect = kv.get("newPartitionSpec")
      .flatMap(s => PartitionFormatTool.normalizeKvPartitionSpec(Option(s), executionDate))

    // 2) Ventanas compactas por lado: "refWindowDays=-2..+3", "newWindowDays=0..+1"
    val refSpecWindow = kv.get("refWindowDays").flatMap(parseWindow).flatMap {
      case (start, end) =>
        val dates = expandDates(outputDateISO, start, end)
        upsertDates(partitionSpec, dates)
    }

    val newSpecWindow = kv.get("newWindowDays").flatMap(parseWindow).flatMap {
      case (start, end) =>
        val dates = expandDates(outputDateISO, start, end)
        upsertDates(partitionSpec, dates)
    }

    val refSpecOverride = refSpecDirect.orElse(refSpecWindow)
    val newSpecOverride = newSpecDirect.orElse(newSpecWindow)

    CompareConfig(
      spark = spark,
      refTable = refTable,
      newTable = newTable,
      partitionSpec = partitionSpec,
      compositeKeyCols = compositeKeyCols,
      ignoreCols = ignoreCols,
      initiativeName = initiativeName,
      tablePrefix = tablePrefix,
      outputBucket = outputBucket,
      checkDuplicates = checkDuplicates,
      includeEqualsInDiff = includeEquals,
      priorityCol = None,
      aggOverrides = Map.empty,
      outputDateISO = outputDateISO,
      // nuevos campos:
      refPartitionSpecOverride = refSpecOverride,
      newPartitionSpecOverride = newSpecOverride
    )
  }

  // ---------------------------- Entry point ----------------------------
  def execute(args: Array[String]): Unit = {
    info(s"[Driver] Starting TableComparatorApp in KV-only mode")
    info(s"[DEBUG] KV-mode args count: ${if (args == null) -1 else args.length}")

    if (args == null || args.isEmpty) {
      val msg = "[Driver] Missing KV args"
      err(msg); throw new IllegalArgumentException(msg)
    }

    val kv: Map[String, String] = args.flatMap(parseKvArg).toMap
    info(s"[DEBUG] KV received keys: ${kv.keys.toSeq.sorted.mkString(",")}")

    val cfg = buildConfig(kv)

    info("[Driver] Calling TableComparisonController.run(cfg)")
    TableComparisonController.run(cfg)

    // showComparisonResults(spark, cfg.tablePrefix, cfg.initiativeName, cfg.outputDateISO)
    info(s"[DEBUG] execDateForFilter: ${cfg.outputDateISO}")
    info("[Driver] KV-mode comparison finished.")
  }

  // helper for manual inspection On demand test
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

    info(s"\n-- Differences (${prefix}differences) --")
    spark.sql(q(prefix + "differences")).show(false)

    info(s"\n-- Summary (${prefix}summary) --")
    spark.sql(q(prefix + "summary")).show(false)

    info(s"\n-- Duplicates (${prefix}duplicates) --")
    spark.sql(q(prefix + "duplicates")).show(false)
  }
}

object TableComparatorApp {
  def execute(args: Array[String])(implicit spark: SparkSession): Unit = {
    val app = new TableComparatorApp()
    app.execute(args)
  }
}