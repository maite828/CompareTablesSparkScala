import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * Partition pruning and spec parsing/resolution.
 * Safe for PRE/DEV: resolves geos via metastore only (SHOW PARTITIONS) and parses on the driver.
 */
object PartitionPruning {

  private final case class Values(values: Seq[String]) {
    def nonEmpty: Boolean = values.nonEmpty
    def isEmptyOrWildcard: Boolean = values.isEmpty || values.exists(v => v == "*" || v.isEmpty)
  }

  def loadWithPartition(
                         spark: SparkSession,
                         tableName: String,
                         partitionSpec: Option[String]
                       ): DataFrame = {
    val baseDf = spark.table(tableName)
    val spec   = partitionSpec.map(_.trim).getOrElse("")

    if (spec.isEmpty) {
      println(s"[DEBUG] loadWithPartition: $tableName without spec -> no filter")
      baseDf
    } else {
      val specMap     = parsePartitionSpecToMap(spec)
      val geoValsOpt  = specMap.get("geo")
      val dateValsOpt = specMap.get("data_date_part")

      // If date is set but geo is * or empty, resolve geos via metastore only
      val needResolve = dateValsOpt.exists(_.nonEmpty) && geoValsOpt.forall(_.isEmptyOrWildcard)

      val (finalGeoVals, finalDateVals) =
        if (needResolve) {
          val dvals = dateValsOpt.get.values
          val geos  = resolveGeosForDates(spark, tableName, dvals)
          println(s"[DEBUG] Table: $tableName resolved geos for dates ${dvals.mkString(",")} => ${geos.size} values")
          (Some(Values(geos)), Some(Values(dvals)))
        } else {
          (geoValsOpt.filter(_.nonEmpty), dateValsOpt.filter(_.nonEmpty))
        }

      val preds = buildPredicates(finalGeoVals, finalDateVals)
      if (preds.nonEmpty) { baseDf.where(preds.reduce(_ && _)) }
      else { baseDf.where(buildCompositeFilter(spec)) }
    }
  }

  private def buildPredicates(finalGeoVals: Option[Values], finalDateVals: Option[Values]): Seq[Column] =
    Seq(
      finalGeoVals.map(vs => col("geo").isin(vs.values: _*)),
      finalDateVals.map(vs => col("data_date_part").isin(vs.values: _*))
    ).flatten

  private def parsePartitionSpecToMap(spec: String): Map[String, Values] = {
    val toks = spec.split("/").iterator.map(_.trim).filter(_.nonEmpty).toSeq
    def unq(s: String): String = s.stripPrefix("\"").stripPrefix("'").stripSuffix("\"").stripSuffix("'").trim
    def splitList(s: String, or: Boolean): Seq[String] = {
      val sep = if (or) "\\|" else "[,|]"
      s.split(sep).toSeq.map(unq).filter(_.nonEmpty)
    }
    val InList1 = "(?i)^in\\s*\\[(.*)\\]$".r
    val InList2 = "(?i)^in\\s*\\((.*)\\)$".r
    val EqList1 = "^\\[(.*)\\]$".r
    val EqList2 = "^\\((.*)\\)$".r
    toks.flatMap { kv =>
      kv.split("=", 2) match {
        case Array(k, rhsRaw) =>
          val k1  = unq(k)
          val rhs = rhsRaw.trim
          val vals: Seq[String] =
            if (rhs == "*" || rhs.isEmpty) { Nil }
            else {
              rhs match {
                case InList1(inner) => splitList(inner, or = false)
                case InList2(inner) => splitList(inner, or = false)
                case EqList1(inner) => splitList(inner, or = false)
                case EqList2(inner) => splitList(inner, or = true)
                case other => Seq(unq(other))
              }
            }
          Some(k1 -> Values(vals))
        case _ => None
      }
    }.toMap
  }

  /**
   * Resolve available geo values for given dates using ONLY metastore metadata.
   * Uses `SHOW PARTITIONS table`, then filters on the driver and parses strings like:
   *   "geo=ES/data_date_part=2025-08-27"
   */
  private def resolveGeosForDates(spark: SparkSession, table: String, dateVals: Seq[String]): Seq[String] = {
    if (dateVals == null || dateVals.isEmpty) { Seq.empty[String] }
    else {
      val maxDates = 256
      val dates    = dateVals.take(maxDates)
      if (dateVals.size > maxDates) {
        println(s"[WARN] resolveGeosForDates: trimming date list from ${dateVals.size} to $maxDates")
      }
      val wanted = dates.map(d => s"data_date_part=$d").toSet
      try {
        val parts: Array[String] = spark.sql(s"SHOW PARTITIONS $table").collect().map(_.getString(0))
        parts.iterator
          .filter(p => wanted.exists(p.contains))
          .flatMap(_.split("/").find(_.startsWith("geo=")).map(_.substring(4)))
          .toSet
          .toSeq
          .sorted
      } catch {
        case _: Throwable =>
          println(s"[WARN] resolveGeosForDates: SHOW PARTITIONS failed, returning empty list")
          Seq.empty[String]
      }
    }
  }

  // Fallback composite filter builder (supports IN/[]/() and (a|b) forms)
  private def buildCompositeFilter(spec: String): Column = {
    val filters = spec.split("/").flatMap { kvRaw =>
      val kv = kvRaw.trim
      if (kv.isEmpty) { None }
      else {
        val parts = kv.split("=", 2)
        if (parts.length < 2) { None }
        else {
          val k   = parts(0).trim
          val rhs = parts(1).trim
          if (rhs == "*" || rhs.isEmpty) { None }
          else {
            parseValueList(rhs) match {
              case Some(vs) if vs.nonEmpty => Some(col(k).isin(vs: _*))
              case _ => Some(col(k) === lit(unquote(rhs)))
            }
          }
        }
      }
    }
    if (filters.isEmpty) lit(true) else filters.reduce(_ && _)
  }

  private def parseValueList(rhs: String): Option[Seq[String]] = {
    val InList1 = "(?i)^in\\s*\\[(.*)\\]$".r
    val InList2 = "(?i)^in\\s*\\((.*)\\)$".r
    val EqList1 = "^\\[(.*)\\]$".r
    val EqList2 = "^\\((.*)\\)$".r
    val OrGroup = "^\\(([^()]+\\|[^()]+)+\\)$".r
    rhs match {
      case InList1(inner) => Some(splitList(inner))
      case InList2(inner) => Some(splitList(inner))
      case EqList1(inner) => Some(splitList(inner))
      case EqList2(inner) => Some(splitList(inner))
      case OrGroup(inner) => Some(splitList(inner, or = true))
      case _              => None
    }
  }
  private def splitList(s: String, or: Boolean = false): Seq[String] = {
    val sep = if (or) "\\|" else "[,|]"
    s.split(sep).toSeq.map(unquote).filter(_.nonEmpty)
  }
  private def unquote(s: String): String =
    s.stripPrefix("\"").stripPrefix("'").stripSuffix("\"").stripSuffix("'").trim
}
