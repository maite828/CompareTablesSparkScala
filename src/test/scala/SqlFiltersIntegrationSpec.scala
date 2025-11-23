import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SaveMode

/**
 * Integration tests for SQL filters combined with column mapping.
 * These tests verify the end-to-end behavior of filtering data before comparison.
 */
class SqlFiltersIntegrationSpec extends AnyFlatSpec with Matchers with SparkSessionTestWrapper {

  import spark.implicits._

  "SQL filters with column mapping" should "filter to exact match only (ID=1)" in {
    // Setup: Create test tables with different column names
    val refData = Seq(
      (1, "Alice", 100.0, "2023-11-22", "ES"),
      (2, "Bob", 200.0, "2023-11-23", "ES"),
      (2, "Bob", 200.0, "2023-11-22", "ES"), // Duplicate
      (3, "Charlie", 300.0, "2023-11-22", "FR")
    ).toDF("id", "name", "balance", "data_date_part", "geo")

    val newData = Seq(
      (1, "Alice", 100.0, "2023-11-22", "ES"), // Exact match with ID=1
      (2, "Bob", 250.0, "2023-11-23", "ES"),   // Different balance
      (4, "David", 400.0, "2023-11-22", "FR")  // New ID
    ).toDF("id_v2", "nombre_cliente", "saldo", "fecha_proceso", "pais")

    // Column mapping
    val columnMapping = Map(
      "id" -> "id_v2",
      "name" -> "nombre_cliente",
      "balance" -> "saldo",
      "data_date_part" -> "fecha_proceso",
      "geo" -> "pais"
    )

    // Apply filters to get only ID=1
    val refFiltered = refData.filter("id = 1")
    val newFiltered = newData.filter("id_v2 = 1")

    // Apply column mapping
    val newMapped = TableComparisonController.applyColumnMapping(newFiltered, columnMapping)

    // Verify: Both should have exactly 1 row with ID=1
    refFiltered.count() shouldBe 1
    newMapped.count() shouldBe 1

    // Verify: Columns are aligned
    newMapped.columns should contain("id")
    newMapped.columns should contain("name")
    newMapped.columns should contain("balance")

    // Verify: Data matches exactly
    val refRow = refFiltered.head()
    val newRow = newMapped.head()
    
    refRow.getAs[Int]("id") shouldBe newRow.getAs[Int]("id")
    refRow.getAs[String]("name") shouldBe newRow.getAs[String]("name")
    refRow.getAs[Double]("balance") shouldBe newRow.getAs[Double]("balance")
  }

  it should "filter to partial match (IDs 2,3 vs 2,4)" in {
    val refData = Seq(
      (1, "Alice", 100.0),
      (2, "Bob", 200.0),
      (3, "Charlie", 300.0)
    ).toDF("id", "name", "balance")

    val newData = Seq(
      (1, "Alice", 100.0),
      (2, "Bob", 250.0), // Different balance
      (4, "David", 400.0)
    ).toDF("id_v2", "nombre_cliente", "saldo")

    val columnMapping = Map(
      "id" -> "id_v2",
      "name" -> "nombre_cliente",
      "balance" -> "saldo"
    )

    // Filter: REF gets IDs 2,3; NEW gets IDs 2,4
    val refFiltered = refData.filter("id IN (2, 3)")
    val newFiltered = newData.filter("id_v2 IN (2, 4)")
    val newMapped = TableComparisonController.applyColumnMapping(newFiltered, columnMapping)

    // Verify counts
    refFiltered.count() shouldBe 2 // IDs: 2, 3
    newMapped.count() shouldBe 2   // IDs: 2, 4

    // Verify ID=2 exists in both but with different balance
    val refId2 = refFiltered.filter($"id" === 2).head()
    val newId2 = newMapped.filter($"id" === 2).head()
    
    refId2.getAs[Double]("balance") shouldBe 200.0
    newId2.getAs[Double]("balance") shouldBe 250.0

    // Verify ID=3 only in REF
    refFiltered.filter($"id" === 3).count() shouldBe 1
    newMapped.filter($"id" === 3).count() shouldBe 0

    // Verify ID=4 only in NEW
    refFiltered.filter($"id" === 4).count() shouldBe 0
    newMapped.filter($"id" === 4).count() shouldBe 1
  }

  it should "handle empty result after filtering" in {
    val refData = Seq(
      (1, "Alice", 100.0),
      (2, "Bob", 200.0)
    ).toDF("id", "name", "balance")

    val newData = Seq(
      (3, "Charlie", 300.0),
      (4, "David", 400.0)
    ).toDF("id_v2", "nombre_cliente", "saldo")

    // Filter to non-existent IDs
    val refFiltered = refData.filter("id = 999")
    val newFiltered = newData.filter("id_v2 = 999")

    refFiltered.count() shouldBe 0
    newFiltered.count() shouldBe 0
  }

  it should "filter with complex SQL expressions" in {
    val refData = Seq(
      (1, "Alice", 100.0, "2023-11-22"),
      (2, "Bob", 200.0, "2023-11-23"),
      (3, "Charlie", 300.0, "2023-11-22")
    ).toDF("id", "name", "balance", "date")

    // Complex filter: balance > 150 AND date = '2023-11-22'
    val filtered = refData.filter("balance > 150 AND date = '2023-11-22'")

    filtered.count() shouldBe 1 // Only Charlie (300.0, 2023-11-22)
    filtered.head().getAs[String]("name") shouldBe "Charlie"
  }

  it should "preserve partition columns after filtering" in {
    val refData = Seq(
      (1, "Alice", "2023-11-22", "ES"),
      (2, "Bob", "2023-11-23", "ES"),
      (3, "Charlie", "2023-11-22", "FR")
    ).toDF("id", "name", "data_date_part", "geo")

    val filtered = refData.filter("data_date_part = '2023-11-22'")

    filtered.count() shouldBe 2 // Alice and Charlie
    filtered.columns should contain("data_date_part")
    filtered.columns should contain("geo")
    
    // Verify all rows have the filtered date
    filtered.select("data_date_part").distinct().count() shouldBe 1
    filtered.select("data_date_part").head().getString(0) shouldBe "2023-11-22"
  }
}
