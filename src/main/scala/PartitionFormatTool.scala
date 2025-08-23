// src/main/scala/PartitionFormatTool.scala
// (no package to match your current project layout)

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.collection.JavaConverters._
import scala.util.matching.Regex

/** Utilities to normalize partition specs and inject execution dates in a fully agnostic way. */
object PartitionFormatTool {

  // Jackson mapper reused across methods
  private val mapper = new ObjectMapper()

  // Date formatters
  private val ISO = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT)
  private val DMY = DateTimeFormatter.ofPattern("dd/MM/yyyy", Locale.ROOT)

  /** Parse a JSON string into a JsonNode. */
  def parseJson(json: String): JsonNode =
    mapper.readTree(json)

  /** Replace date tokens, placeholders (YYYY/MM/dd, dd-MM-YYYY, yyyyMMdd, …)
    *  and date-like substrings in a string, keeping the intended format. */
  private def replaceDateInString(s: String, execDate: LocalDate): String = {
    def fmt(p: String) = DateTimeFormatter.ofPattern(p, Locale.ROOT)

    // 1) Token replacements first (extend if needed)
    val withTokens = s
      .replace("$dataDatePart", execDate.format(ISO))
      .replace("$EXEC_DATE",    execDate.format(ISO))
      .replace("${EXEC_DATE}",  execDate.format(ISO))
      .replace("{{ds}}",        execDate.format(ISO))
      .replace("{{ ds }}",      execDate.format(ISO))
      .replace("{{ds_nodash}}", execDate.format(fmt("yyyyMMdd")))
      .replace("{{ ds_nodash }}", execDate.format(fmt("yyyyMMdd")))

    // 2) Placeholder patterns (case-insensitive) -> render with the matching formatter
    //    Soportamos varias formas frecuentes con/ sin separadores.
    val placeholderFormats: Seq[(Regex, DateTimeFormatter)] = Seq(
      // ISO-like con separadores
      ("(?i)\\byyyy[-/]MM[-/]dd\\b".r, fmt("yyyy-MM-dd")),
      ("(?i)\\bYYYY[-/]MM[-/]dd\\b".r, fmt("yyyy-MM-dd")),
      // DMY con separadores
      ("(?i)\\bdd[-/]MM[-/]yyyy\\b".r, fmt("dd/MM/yyyy")),
      ("(?i)\\bdd[-/]MM[-/]YYYY\\b".r, fmt("dd/MM/yyyy")),
      // Otras variantes separadas
      ("(?i)\\byyyy[/]MM[/]dd\\b".r,   fmt("yyyy/MM/dd")),
      ("(?i)\\bdd[-]MM[-]yyyy\\b".r,   fmt("dd-MM-yyyy")),
      ("(?i)\\bdd[_]MM[_]yyyy\\b".r,   fmt("dd_MM_yyyy")),
      ("(?i)\\byyyy[_]MM[_]dd\\b".r,   fmt("yyyy_MM_dd")),
      // Compactas
      ("(?i)\\byyyyMMdd\\b".r,         fmt("yyyyMMdd")),
      ("(?i)\\bYYYYMMDD\\b".r,         fmt("yyyyMMdd")),
      ("(?i)\\bddMMyyyy\\b".r,         fmt("ddMMyyyy"))
    )

    val afterPlaceholders = placeholderFormats.foldLeft(withTokens){ case (acc,(rx,df)) =>
      rx.replaceAllIn(acc, _ => execDate.format(df))
    }

    // 3) Helper para validar date strings
    def isValidDate(dateStr: String, formatter: DateTimeFormatter): Boolean =
      try { LocalDate.parse(dateStr, formatter); true } catch { case _: Exception => false }

    // 4) Reemplazo de fechas reales ISO -> a execDate (misma forma ISO)
    val isoRegex  = raw"\b([0-9]{4}-[0-9]{2}-[0-9]{2})\b".r
    val afterIso  = isoRegex.replaceAllIn(afterPlaceholders, m => {
      val dateStr = m.group(1)
      if (isValidDate(dateStr, ISO)) execDate.format(ISO) else dateStr
    })

    // 5) Reemplazo de fechas reales DMY -> a execDate (misma forma DMY)
    val dmyRegex  = raw"\b([0-9]{2})/([0-9]{2})/([0-9]{4})\b".r
    val afterDmy  = dmyRegex.replaceAllIn(afterIso, m => {
      val dateStr = m.group(0)
      if (isValidDate(dateStr, DMY)) execDate.format(DMY) else dateStr
    })

    afterDmy
  }

  /** Deeply rewrite any string fields in the JSON tree, injecting execution date where appropriate. */
  private def rewriteDatesDeep(node: JsonNode, execDate: LocalDate): JsonNode = node match {
    case t: TextNode =>
      new TextNode(replaceDateInString(t.asText(), execDate))
    case obj: ObjectNode =>
      val copy = obj.deepCopy[ObjectNode]()
      val fields = obj.fields().asScala.toList
      fields.foreach { e =>
        copy.set[JsonNode](e.getKey, rewriteDatesDeep(e.getValue, execDate))
      }
      copy
    case arr: ArrayNode =>
      val copy = arr.deepCopy[ArrayNode]()
      var i = 0
      while (i < arr.size()) {
        copy.set(i, rewriteDatesDeep(arr.get(i), execDate))
        i += 1
      }
      copy
    case other => other // numbers, booleans, null
  }

  /** Escape double quotes to keep a valid key="value" partitionSpec. */
  private def escapeQuotes(s: String): String =
    s.replace("\"", "\\\"")

  /** Build a partitionSpec string from an object node: key="val"/key2="val2". */
  private def partitionsNodeToSpec(node: JsonNode): String = {
    val it = node.fields().asScala.toSeq // preserve insertion order
    it.map { e =>
      val k = e.getKey
      val v = escapeQuotes(e.getValue.asText())
      s"""$k="$v""""
    }.mkString("/")
  }

  /** Compute the final partitionSpec as Option[String] from either string or object. */
  private def computePartitionSpec(params: JsonNode): Option[String] = {
    val direct = Option(params.get("partitionSpec"))
      .filter(n => n != null && !n.isNull)
      .map(_.asText())
      .filter(_.nonEmpty)
    val fromObj = Option(params.get("partitions"))
      .filter(n => n != null && n.isObject)
      .map(partitionsNodeToSpec)
    direct.orElse(fromObj)
  }

  /** Normalize parameters:
    *  - Requires executionDate (ISO yyyy-MM-dd).
    *  - Rewrites strings (tokens/placeholders/fechas reales) across the tree.
    *  - Returns (normalizedParams, normalizedPartitionSpec).
    */
  def normalizeParameters(paramsNode: JsonNode, executionDateISO: String): (JsonNode, Option[String]) = {
    val execDate   = LocalDate.parse(executionDateISO, ISO)
    val normalized = rewriteDatesDeep(paramsNode, execDate)
    val finalSpec  = computePartitionSpec(normalized)
    (normalized, finalSpec)
  }

  /** Extract yyyy-MM-dd from a partitionSpec.
    * Supports:
    *  - key="2025-08-14" (ISO quoted)
    *  - bare 2025-08-14 anywhere (ISO)
    *  - key="14/08/2025" (DMY quoted)  -> converted to ISO
    *  - bare 14/08/2025 anywhere (DMY) -> converted to ISO
    *  - three numeric tokens (2/4 digits) in any order (e.g., year/month/day or month/year/day)
    * If not found, returns today's date in ISO.
    */
  def extractDateFromPartitionSpec(partitionSpec: Option[String]): String = {
    def fromTripleTokens(tokens: List[String]): Option[String] = {
      val (four, rest) = tokens.partition(_.length == 4)
      val yearOpt = four.map(_.toInt).find(y => y >= 1900 && y <= 2100)
        .orElse(four.headOption.map(_.toInt))
      val twos = rest.filter(_.length == 2).map(_.toInt)

      (yearOpt, twos) match {
        case (Some(y), m1 :: d1 :: _) =>
          def fmt(y: Int, m: Int, d: Int): Option[String] =
            if (m >= 1 && m <= 12 && d >= 1 && d <= 31) Some(f"$y-$m%02d-$d%02d") else None
          // Try month=m1/day=d1, then swap if invalid
          fmt(y, m1, d1).orElse(fmt(y, d1, m1))
        case _ => None
      }
    }

    partitionSpec.flatMap { spec =>
      // 1) ISO quoted key="yyyy-MM-dd"
      val isoQuoted = """[A-Za-z0-9_]+\s*=\s*"([0-9]{4}-[0-9]{2}-[0-9]{2})"""".r
        .findFirstMatchIn(spec).map(_.group(1))
      // 2) ISO bare anywhere
      val isoBare   = """\b([0-9]{4}-[0-9]{2}-[0-9]{2})\b""".r
        .findFirstMatchIn(spec).map(_.group(1))
      // 3) DMY quoted key="dd/MM/yyyy" -> ISO
      val dmyQuoted = """[A-Za-z0-9_]+\s*=\s*"([0-9]{2}/[0-9]{2}/[0-9]{4})"""".r
        .findFirstMatchIn(spec).map(m => LocalDate.parse(m.group(1), DMY).format(ISO))
      // 4) DMY bare anywhere -> ISO
      val dmyBare   = """\b([0-9]{2}/[0-9]{2}/[0-9]{4})\b""".r
        .findFirstMatchIn(spec).map(m => LocalDate.parse(m.group(1), DMY).format(ISO))
      // 5) Three quoted numeric tokens (2/4 digits) separated by '/', in any order
      val quotedNums = """=\s*"([0-9]{2,4})"""".r.findAllMatchIn(spec).map(_.group(1)).toList
      val tripleGenericQuoted = if (quotedNums.size >= 3) fromTripleTokens(quotedNums) else None
      // 6) Fallback: three numeric tokens (2/4 digits) anywhere (first 3 distinct)
      val bareNums = """\b([0-9]{2,4})\b""".r.findAllMatchIn(spec).map(_.group(1)).toList
      val merged   = (quotedNums ++ bareNums).distinct.take(3)
      val tripleGenericBare = if (merged.size == 3) fromTripleTokens(merged) else None

      isoQuoted
        .orElse(isoBare)
        .orElse(dmyQuoted)
        .orElse(dmyBare)
        .orElse(tripleGenericQuoted)
        .orElse(tripleGenericBare)
    }.getOrElse(LocalDate.now().format(ISO))
  }
}
