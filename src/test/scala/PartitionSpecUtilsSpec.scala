import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.OptionValues.convertOptionToValuable

import scala.collection.JavaConverters._

/** Tests for PartitionSpecUtils focusing on agnostic date injection and partitionSpec normalization. */
class PartitionSpecUtilsSpec extends AnyFunSuite with Matchers {

  private val mapper = new ObjectMapper()
  private def J(s: String): JsonNode = mapper.readTree(s)

  test("normalizeParameters should inject ISO executionDate into tokens and ISO date strings") {
    val params =
      """
        |{
        |  "refTable": "db.ref",
        |  "newTable": "db.new",
        |  "initiativeName": "Swift",
        |  "tablePrefix": "default.result_",
        |  "partitionSpec": "date=\"$dataDatePart\"/geo=\"ES\"",
        |  "checkDuplicates": true,
        |  "includeEqualsInDiff": false
        |}
      """.stripMargin

    val (normalized, specOpt) =
      PartitionSpecUtils.normalizeParameters(J(params), "2025-08-14")
    specOpt shouldBe Some("""date="2025-08-14"/geo="ES"""")
    normalized.get("partitionSpec").asText() should include ("2025-08-14")
  }

  test("normalizeParameters should replace dd/MM/yyyy substrings preserving format") {
    val params =
      """
        |{
        |  "refTable": "db.r",
        |  "newTable": "db.n",
        |  "initiativeName": "Swift",
        |  "tablePrefix": "pfx",
        |  "partitionSpec": "fecha=\"14/08/2024\"/country=\"ES\""
        |}
      """.stripMargin
    val (normalized, specOpt) =
      PartitionSpecUtils.normalizeParameters(J(params), "2025-08-14")
    specOpt shouldBe Some("""fecha="14/08/2025"/country="ES"""")
  }
  test("normalizeParameters should build partitionSpec from partitions object (order-preserving)") {
    val params =
      """
        |{
        |  "refTable": "db.r",
        |  "newTable": "db.n",
        |  "initiativeName": "Swift",
        |  "tablePrefix": "pfx",
        |  "partitions": { "layer": "silver", "ds": "2025-08-14", "region": "ES" }
        |}
      """.stripMargin
    val (normalized, specOpt) =
      PartitionSpecUtils.normalizeParameters(J(params), "2025-08-14")
    // Order should be as inserted: layer/ds/region
    specOpt shouldBe Some("""layer="silver"/ds="2025-08-14"/region="ES"""")
    normalized.get("partitions").get("ds").asText() shouldBe "2025-08-14"
  }

  test("normalizeParameters should deeply rewrite string values in nested structures") {
    val params =
      """
        |{
        |  "refTable": "db.r",
        |  "newTable": "db.n",
        |  "initiativeName": "Swift",
        |  "tablePrefix": "pfx",
        |  "extra": {
        |    "notes": "run for $dataDatePart",
        |    "array": ["value on 2024-01-01", "plain"]
        |  },
        |  "partitions": { "dt": "2024-12-31" }
        |}
      """.stripMargin
    val (normalized, specOpt) =
      PartitionSpecUtils.normalizeParameters(J(params), "2025-08-14")
    // partitionSpec from partitions, with dt replaced
    specOpt shouldBe Some("""dt="2025-08-14"""")
    normalized.get("extra").get("notes").asText() shouldBe "run for 2025-08-14"
    val arr = normalized.get("extra").get("array").elements().asScala.toSeq.map(_.asText())
    arr should contain ("value on 2025-08-14")
    arr should contain ("plain")
  }

  test("normalizeParameters respects explicit partitionSpec over partitions object when both provided") {
    val params =
      """
        |{
        |  "refTable": "db.r",
        |  "newTable": "db.n",
        |  "initiativeName": "Swift",
        |  "tablePrefix": "pfx",
        |  "partitionSpec": "d=\"2024-07-01\"/r=\"EU\"",
        |  "partitions": { "d": "SHOULD_NOT_BE_USED", "r": "NO" }
        |}
      """.stripMargin
    val (normalized, specOpt) =
      PartitionSpecUtils.normalizeParameters(J(params), "2025-08-14")
    specOpt shouldBe Some("""d="2025-08-14"/r="EU"""")
  }

  test("extractDateFromPartitionSpec should capture ISO or dd/MM/yyyy regardless of key names") {
    val iso  = Some("""foo="x"/any_key="2025-08-14"/bar="y"""")
    val dmy  = Some("""dia="14/08/2025"/otro="zz"""")
    val none = Some("""no_date_here="x"/still="y"""")
    val sep1 = Some("""no_date_here="2025"/still="08"/and_year="14"""") // separate day/month/year
    val sep2 = Some("""still="08"/no_date_here="2025"/and_year="14"""") // separate day/year/month

    val nullv: Option[String] = None
    PartitionSpecUtils.extractDateFromPartitionSpec(iso) shouldBe "2025-08-14"
    PartitionSpecUtils.extractDateFromPartitionSpec(dmy) shouldBe "2025-08-14"  // DMY converted to ISO
    // When no date found, it returns "today" in ISO; we just assert it matches yyyy-MM-dd
    val today = PartitionSpecUtils.extractDateFromPartitionSpec(none)
    today should fullyMatch regex """\d{4}-\d{2}-\d{2}"""
    PartitionSpecUtils.extractDateFromPartitionSpec(nullv) should fullyMatch regex """\d{4}-\d{2}-\d{2}"""
    // Test nuevo
    PartitionSpecUtils.extractDateFromPartitionSpec(sep1) shouldBe "2025-08-14"
    PartitionSpecUtils.extractDateFromPartitionSpec(sep2) shouldBe "2025-08-14"

  }

  test("normalizeParameters leaves non-string primitives untouched") {
    val params =
      """
        |{
        |  "refTable": "db.r",
        |  "newTable": "db.n",
        |  "initiativeName": "Swift",
        |  "tablePrefix": "pfx",
        |  "checkDuplicates": true,
        |  "includeEqualsInDiff": false,
        |  "numeric": 123
        |}
      """.stripMargin
    val (normalized, specOpt) =
      PartitionSpecUtils.normalizeParameters(J(params), "2025-08-14")
    specOpt shouldBe None
    normalized.get("checkDuplicates").asBoolean() shouldBe true
    normalized.get("includeEqualsInDiff").asBoolean() shouldBe false
    normalized.get("numeric").asInt() shouldBe 123
  }

  // Añade estos casos a src/test/scala/.../PartitionSpecUtilsSpec.scala

  test("parseJson should parse valid JSON and fail on invalid JSON") {
    val good = """{ "a": 1, "b": "x" }"""
    val bad  = """{ "a": 1, "b": }"""

    val node = PartitionSpecUtils.parseJson(good)
    node.get("a").asInt() shouldBe 1
    node.get("b").asText() shouldBe "x"

    // Para invalid JSON esperamos excepción del parser
    assertThrows[com.fasterxml.jackson.core.JsonParseException] {
      PartitionSpecUtils.parseJson(bad)
    }
  }

  test("normalizeParameters should replace all supported date tokens with executionDate (ISO)") {
    val params =
      """
        |{
        |  "partitionSpec": "d1=\"$dataDatePart\"/d2=\"$EXEC_DATE\"/d3=\"${EXEC_DATE}\"/d4=\"{{ds}}\"",
        |  "extra": "inside text: date=$dataDatePart and also {{ds}}"
        |}
    """.stripMargin

    val (norm, specOpt) = PartitionSpecUtils.normalizeParameters(J(params), "2025-08-14")
    specOpt.value shouldBe """d1="2025-08-14"/d2="2025-08-14"/d3="2025-08-14"/d4="2025-08-14""""
    norm.get("extra").asText() shouldBe "inside text: date=2025-08-14 and also 2025-08-14"
  }

  test("normalizeParameters should replace ISO and DMY substrings inside arbitrary strings preserving format") {
    val params =
      """
        |{
        |  "notes": "A: 2024-01-02 | B: 03/02/2024 | C end",
        |  "partitionSpec": "p=\"x\""
        |}
    """.stripMargin

    val (norm, _) = PartitionSpecUtils.normalizeParameters(J(params), "2025-08-14")
    norm.get("notes").asText() shouldBe "A: 2025-08-14 | B: 14/08/2025 | C end"
  }

  test("normalizeParameters should not touch non-date substrings and should not alter numbers embedded in words") {
    val params =
      """
        |{
        |  "text": "ref-2024-01-01x not_a_date 99/99/9999",
        |  "partitionSpec": "k=\"v\""
        |}
    """.stripMargin

    val (norm, _) = PartitionSpecUtils.normalizeParameters(J(params), "2025-08-14")
    // "2024-01-01x" no casa con el patrón completo yyyy-MM-dd por la 'x' final, así que no se reemplaza
    norm.get("text").asText() shouldBe "ref-2024-01-01x not_a_date 99/99/9999"
  }

  test("normalizeParameters should replace multiple occurrences within the same field") {
    val params =
      """
        |{
        |  "text": "two dates: 2024-01-01 and 01/01/2024 and again 2024-01-01",
        |  "partitionSpec": "k=\"v\""
        |}
    """.stripMargin

    val (norm, _) = PartitionSpecUtils.normalizeParameters(J(params), "2025-08-14")
    norm.get("text").asText() shouldBe "two dates: 2025-08-14 and 14/08/2025 and again 2025-08-14"
  }

  test("normalizeParameters should leave arrays and non-string primitives untouched while rewriting string elements") {
    val params =
      """
        |{
        |  "arr": ["on 2024-05-05", 123, true, "and 06/05/2024"],
        |  "num": 7,
        |  "bool": false,
        |  "partitionSpec": "k=\"v\""
        |}
    """.stripMargin

    val (norm, _) = PartitionSpecUtils.normalizeParameters(J(params), "2025-08-14")
    val arr = norm.get("arr")
    arr.get(0).asText() shouldBe "on 2025-08-14"
    arr.get(1).asInt()  shouldBe 123
    arr.get(2).asBoolean() shouldBe true
    arr.get(3).asText() shouldBe "and 14/08/2025"
    norm.get("num").asInt() shouldBe 7
    norm.get("bool").asBoolean() shouldBe false
  }
}