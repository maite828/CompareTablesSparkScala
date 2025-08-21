import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.fasterxml.jackson.databind.ObjectMapper

/** Ensures the exact JSON shape we expect from Airflow is consumable and normalized. */
class PayloadIntegrationSpec extends AnyFunSuite with Matchers {
  private val mapper = new ObjectMapper()
  test("Airflow-style payload is normalized and partitionSpec is computed from partitions") {
    val payloadJson =
      """
        |{
        |  "parameters": {
        |    "refTable": "db.ref_customers",
        |    "newTable": "db.new_customers",
        |    "partitions": { "region": "ES", "ds": "2024-01-01" },
        |    "compositeKeyCols": ["id"],
        |    "ignoreCols": ["last_update"],
        |    "initiativeName": "Swift",
        |    "tablePrefix": "default.result_",
        |    "checkDuplicates": true,
        |    "includeEqualsInDiff": false,
        |    "executionDate": "2025-08-14"
        |  }
        |}
      """.stripMargin
    val root   = mapper.readTree(payloadJson)
    val params = root.get("parameters")
    val (normalized, specOpt) =
      PartitionFormatTool.normalizeParameters(params, executionDateISO = "2025-08-14")
    specOpt shouldBe Some("""region="ES"/ds="2025-08-14"""")
    normalized.get("tablePrefix").asText() shouldBe "default.result_"
  }
}
