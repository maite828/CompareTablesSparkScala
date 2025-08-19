package com.santander.cib.adhc.internal_aml_tools.app.table_comparator
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import scala.collection.JavaConverters._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale

/** Utilities to normalize partition specs and inject execution dates in a fully agnostic way. */
object PartitionSpecUtils {

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
    // Token replacements first (add more tokens here if needed)
    val withTokens = s
      .replace("$dataDatePart", execDate.format(ISO))
      .replace("$EXEC_DATE",    execDate.format(ISO))
      .replace("${EXEC_DATE}",  execDate.format(ISO))
      .replace("{{ds}}",        execDate.format(ISO))
    // Regex: ISO yyyy-MM-dd and DMY dd/MM/yyyy
    val isoRegex = raw"\b[0-9]{4}-[0-9]{2}-[0-9]{2}\b".r
    val dmyRegex = raw"\b([0-9]{2})/([0-9]{2})/([0-9]{4})\b".r
    val afterIso = isoRegex.replaceAllIn(withTokens, _ => execDate.format(ISO))
    val result   = dmyRegex.replaceAllIn(afterIso, _ => execDate.format(DMY))
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
  /** Extract yyyy-MM-dd from a partitionSpec like: date="2025-08-14"/... or dd/MM/yyyy variants. */
  def extractDateFromPartitionSpec(partitionSpec: Option[String]): String = {
    val isoRegex    = """[A-Za-z0-9_]+\s*=\s*"([0-9]{4}-[0-9]{2}-[0-9]{2})"""".r
    val tripleRegex = """[A-Za-z0-9_]+\s*=\s*"([0-9]{2})"\s*/\s*[A-Za-z0-9_]+\s*=\s*"([0-9]{2})"\s*/\s*[A-Za-z0-9_]+\s*=\s*"([0-9]{4})"""".r
    partitionSpec.flatMap { spec =>
      isoRegex.findFirstMatchIn(spec).map(_.group(1))
        .orElse(tripleRegex.findFirstMatchIn(spec).map(m => s"${m.group(3)}-${m.group(2)}-${m.group(1)}"))
    }.getOrElse(LocalDate.now().format(ISO))
  }
}