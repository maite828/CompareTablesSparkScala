import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.collection.JavaConverters._

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

  /** Replace date tokens and date-like substrings in a string, keeping the original format. */
  private def replaceDateInString(s: String, execDate: LocalDate): String = {
    // Reemplazo de tokens primero (extender si es necesario)
    val withTokens = s
      .replace("$dataDatePart", execDate.format(ISO))
      .replace("$EXEC_DATE",    execDate.format(ISO))
      .replace("${EXEC_DATE}",  execDate.format(ISO))
      .replace("{{ds}}",        execDate.format(ISO))

    // Función helper para validar y parsear fechas
    def isValidDate(dateStr: String, formatter: DateTimeFormatter): Boolean = {
      try {
        LocalDate.parse(dateStr, formatter)
        true
      } catch {
        case _: Exception => false
      }
    }

    // Reemplazar solo fechas ISO válidas
    val isoRegex = raw"\b([0-9]{4}-[0-9]{2}-[0-9]{2})\b".r
    val afterIso = isoRegex.replaceAllIn(withTokens, m => {
      val dateStr = m.group(1)
      if (isValidDate(dateStr, ISO)) execDate.format(ISO) else dateStr
    })

    // Reemplazar solo fechas DMY válidas
    val dmyRegex = raw"\b([0-9]{2})/([0-9]{2})/([0-9]{4})\b".r
    val result = dmyRegex.replaceAllIn(afterIso, m => {
      val dateStr = m.group(0)
      if (isValidDate(dateStr, DMY)) execDate.format(DMY) else dateStr
    })

    result
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

  /** Build a partitionSpec string from an object node: key="val"/key2="val2". */
  private def partitionsNodeToSpec(node: JsonNode): String = {
    val it = node.fields().asScala.toSeq // preserve insertion order
    it.map { e =>
      val k = e.getKey
      val v = e.getValue.asText()
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
   *  - Rewrites strings (tokens and date-like substrings) across the tree.
   *  - Returns (normalizedParams, normalizedPartitionSpec).
   */
  def normalizeParameters(paramsNode: JsonNode, executionDateISO: String): (JsonNode, Option[String]) = {
    val execDate = LocalDate.parse(executionDateISO, ISO)
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
      // tokens: preserve order of appearance; pick first 4-digit year in [1900,2100]
      val (years, rest) = tokens.partition(_.length == 4)
      val yearOpt = years
        .map(_.toInt)
        .find(y => y >= 1900 && y <= 2100)
        .orElse(years.headOption.map(_.toInt)) // fallback to any 4-digit
      val twos = rest.filter(_.length == 2).map(_.toInt)
      (yearOpt, twos) match {
        case (Some(y), m1 :: d1 :: _) =>
          def fmt(y: Int, m: Int, d: Int): Option[String] =
            if (m >= 1 && m <= 12 && d >= 1 && d <= 31) Some(f"$y-$m%02d-$d%02d") else None
          // try month=m1, day=d1; then swap if invalid
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
      // 6) Fallback: three numeric tokens (2/4 digits) anywhere, keep first 3 distinct
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