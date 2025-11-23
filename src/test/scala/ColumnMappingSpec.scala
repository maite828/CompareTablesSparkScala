
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ColumnMappingSpec extends AnyFlatSpec with Matchers with SparkSessionTestWrapper {

  import spark.implicits._

  "TableComparisonController.applyColumnMapping" should "rename columns correctly" in {
    // 1. Prepare DataFrame with "NEW" names
    val df = Seq(
      (1, "Alice", 100.0),
      (2, "Bob", 200.0)
    ).toDF("id_v2", "nombre_cliente", "monto")

    // 2. Define mapping: REF name -> NEW name
    // We want "id_v2" -> "id", "nombre_cliente" -> "name"
    val mapping = Map(
      "id" -> "id_v2",
      "name" -> "nombre_cliente"
    )

    // 3. Apply mapping
    val result = TableComparisonController.applyColumnMapping(df, mapping)

    // 4. Verify schema
    result.columns should contain("id")
    result.columns should contain("name")
    result.columns should contain("monto") // Unchanged
    result.columns should not contain("id_v2")
    result.columns should not contain("nombre_cliente")

    // 5. Verify data
    val row1 = result.filter($"id" === 1).head()
    row1.getAs[String]("name") shouldBe "Alice"
    row1.getAs[Double]("monto") shouldBe 100.0
  }

  it should "handle missing columns gracefully (warn but not fail)" in {
    val df = Seq((1, "Alice")).toDF("id", "name")
    val mapping = Map("missing_col" -> "non_existent_col")

    val result = TableComparisonController.applyColumnMapping(df, mapping)

    result.columns should contain("id")
    result.columns should contain("name")
    // Should just return same DF structure effectively
  }

  it should "work with empty mapping" in {
    val df = Seq((1, "Alice")).toDF("id", "name")
    val result = TableComparisonController.applyColumnMapping(df, Map.empty)
    result.columns should contain theSameElementsAs Seq("id", "name")
  }
}
