import ComparatorDefaults.{DayMax, DayMin, MonthMax, MonthMin, YearMax, YearMin}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale

object PartitionFormatTool {

  // Date formatters
  private val ISO   = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT)
  private val DMY   = DateTimeFormatter.ofPattern("dd/MM/yyyy", Locale.ROOT)
  private val BASIC = DateTimeFormatter.BASIC_ISO_DATE // yyyyMMdd

  // Normalizes a partitionSpec received in KV-mode:
  // - Replaces date tokens with executionDate (yyyy-MM-dd)
  // - Strips outer quotes around values
  // - Drops keys with wildcard value (*) so filters stay selective
  // - Keeps lists like IN[...] or (a|b) intact (agnostic)
  def normalizeKvPartitionSpec(rawSpec: Option[String], executionDateISO: String): Option[String] = {
    rawSpec.map(_.trim).filter(_.nonEmpty).map { s0 =>
      val exec  = LocalDate.parse(executionDateISO, ISO)
      val iso   = exec.format(ISO)
      val dmy   = exec.format(DMY)
      val basic = exec.format(BASIC)

      // Replace known date tokens (pure textual replace)
      val s1 = s0
        .replace("YYYY-MM-dd", iso)
        .replace("yyyy-MM-dd", iso)
        .replace("DD/MM/YYYY", dmy)
        .replace("dd/MM/yyyy", dmy)
        .replace("YYYYMMdd",   basic)
        .replace("yyyyMMdd",   basic)
        .replace("$EXEC_DATE", iso)
        .replace("${EXEC_DATE}", iso)
        .replace("{{ds}}", iso)

      // Split by '/', strip outer quotes in RHS, drop k=* entries, rebuild
      val cleanedTokens = s1.split("/").toSeq
        .map(_.trim)
        .filter(_.nonEmpty)
        .flatMap { kv =>
          val parts = kv.split("=", 2)
          if (parts.length != 2) { None }
          else {
            val k  = parts(0).trim
            val v0 = parts(1).trim
            val v1 = stripOuterQuotes(v0)
            if (isWildcard(v1)) { None } else { Some(s"$k=$v1") }
          }
        }

      cleanedTokens.mkString("/")
    }.filter(_.nonEmpty)
  }

  // Extracts an ISO date (yyyy-MM-dd) if present; otherwise returns defaultISO.
  def extractDateOr(partitionSpec: Option[String], defaultISO: String): String = {
    val spec = partitionSpec.getOrElse("")
    if (!spec.exists(_.isDigit)) { defaultISO }
    else {
      val extracted = extractDateFromPartitionSpec(partitionSpec)
      if (extracted == null || extracted.isEmpty) defaultISO else extracted
    }
  }

  // Attempts to extract an ISO date from a spec. Returns empty string if not found.
  private def extractDateFromPartitionSpec(partitionSpec: Option[String]): String = {
    partitionSpec match {
      case None => ""
      case Some(spec) =>
        // Regex with escaped quotes to avoid parser confusion in Scala 2.12
        val isoQuoted = "[A-Za-z0-9_]+\\s*=\\s*\\\"([0-9]{4}-[0-9]{2}-[0-9]{2})\\\"".r
          .findFirstMatchIn(spec).map(_.group(1))
        val isoBare = "\\b([0-9]{4}-[0-9]{2}-[0-9]{2})\\b".r
          .findFirstMatchIn(spec).map(_.group(1))

        val dmyQuoted = "[A-Za-z0-9_]+\\s*=\\s*\\\"([0-9]{2}/[0-9]{2}/[0-9]{4})\\\"".r
          .findFirstMatchIn(spec).flatMap(m => safeParse(m.group(1), DMY))
        val dmyBare = "\\b([0-9]{2}/[0-9]{2}/[0-9]{4})\\b".r
          .findFirstMatchIn(spec).flatMap(m => safeParse(m.group(1), DMY))

        val quotedNums = "=\\s*\\\"([0-9]{2,4})\\\"".r.findAllMatchIn(spec).map(_.group(1)).toList
        val bareNums = "\\b([0-9]{2,4})\\b".r.findAllMatchIn(spec).map(_.group(1)).toList
        val merged = (quotedNums ++ bareNums).distinct.take(3)
        val triple = tripleToIso(merged)

        isoQuoted
          .orElse(isoBare)
          .orElse(dmyQuoted)
          .orElse(dmyBare)
          .orElse(triple)
          .getOrElse("")
    }
  }

  // -------- helpers --------

  // Remove only outer quotes (single or double)
  private def stripOuterQuotes(s: String): String =
    s.stripPrefix("\"").stripPrefix("'").stripSuffix("\"").stripSuffix("'").trim

  // True if value is a wildcard (*), with or without quotes
  private def isWildcard(v: String): Boolean = {
    val w = v.trim
    w == "*" || w == "'*'" || w == "\"*\""
  }

  // Safe parse using a given formatter; returns ISO string on success
  private def safeParse(s: String, fmt: DateTimeFormatter): Option[String] =
    try Some(LocalDate.parse(s, fmt).format(ISO)) catch { case _: Throwable => None }

  // Safe toInt; None on failure
  private def toInt(s: String): Option[Int] =
    try Some(s.toInt) catch { case _: Throwable => None }

  // Try to assemble an ISO date from up to 3 numeric tokens:
  // - One must look like a plausible year [YearMin..YearMax].
  // - The other two must be in [0..99] and will be interpreted as (m,d) or (d,m).
  private def tripleToIso(tokens: List[String]): Option[String] = {
    val nums = tokens.flatMap(toInt)
    if (nums.length < 3) { None }
    else {
      val maybeYear = nums.find(n => n >= YearMin && n <= YearMax)
      def fmt(y: Int, m: Int, d: Int): Option[String] = {
        val monthOk = m >= MonthMin && m <= MonthMax
        val dayOk = d >= DayMin && d <= DayMax
        if (monthOk && dayOk) Some(f"$y-$m%02d-$d%02d") else None
      }
      maybeYear.flatMap { year =>
        val twos = nums.filter(n => n >= 0 && n <= 99).take(2)
        twos match {
          case m1 :: d1 :: _ => fmt(year, m1, d1).orElse(fmt(year, d1, m1))
          case _             => None
        }
      }
    }
  }
}
