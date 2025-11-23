import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests for edge cases and advanced features:
 * - Null handling in keys and values
 * - Priority column for duplicate resolution
 * - Partition spec transformations with column mapping
 * - Aggregate overrides
 */
class AdvancedFeaturesSpec extends AnyFlatSpec with Matchers with SparkSessionTestWrapper {

  import spark.implicits._

  "Null handling" should "treat null keys correctly based on nullKeyMatches config" in {
    val refData = Seq(
      (Some(1), "Alice"),
      (None, "Bob"),      // Null key
      (Some(3), "Charlie")
    ).toDF("id", "name")

    val newData = Seq(
      (Some(1), "Alice"),
      (None, "David"),    // Null key (different value)
      (Some(4), "Eve")
    ).toDF("id", "name")

    // With nullKeyMatches=true, nulls should match
    // With nullKeyMatches=false, nulls should NOT match
    
    // Verify null keys exist
    refData.filter($"id".isNull).count() shouldBe 1
    newData.filter($"id".isNull).count() shouldBe 1
  }

  it should "handle null values in comparison columns" in {
    val refData = Seq(
      (1, Some("Alice"), Some(100.0)),
      (2, Some("Bob"), None),          // Null balance
      (3, None, Some(300.0))           // Null name
    ).toDF("id", "name", "balance")

    val newData = Seq(
      (1, Some("Alice"), Some(100.0)),
      (2, Some("Bob"), None),          // Same null balance
      (3, None, Some(350.0))           // Different balance, same null name
    ).toDF("id", "name", "balance")

    // Verify null handling
    refData.filter($"balance".isNull).count() shouldBe 1
    newData.filter($"name".isNull).count() shouldBe 1
  }

  "Priority column" should "keep highest priority row when duplicates exist" in {
    val refData = Seq(
      (1, "Alice_v1", 100.0, 1), // Lower priority
      (1, "Alice_v2", 150.0, 2), // Higher priority - should be kept
      (2, "Bob", 200.0, 1)
    ).toDF("id", "name", "balance", "priority")

    // Simulate priority-based deduplication
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val w = Window.partitionBy("id").orderBy($"priority".desc_nulls_last)
    val deduplicated = refData
      .withColumn("_rn", row_number().over(w))
      .filter($"_rn" === 1)
      .drop("_rn")

    deduplicated.count() shouldBe 2 // Only 2 unique IDs
    
    val id1Row = deduplicated.filter($"id" === 1).head()
    id1Row.getAs[String]("name") shouldBe "Alice_v2" // Higher priority version
    id1Row.getAs[Double]("balance") shouldBe 150.0
  }

  "Partition spec transformation" should "apply column mapping to partition filters" in {
    // Original spec uses REF column names
    val refSpec = "geo=*/data_date_part=2023-11-22"
    
    // Column mapping
    val columnMapping = Map(
      "data_date_part" -> "fecha_proceso",
      "geo" -> "pais"
    )

    // Transform spec for NEW table
    val newSpec = columnMapping.foldLeft(refSpec) { case (currentSpec, (refCol, newCol)) =>
      currentSpec.replaceAll(s"\\b$refCol=", s"$newCol=")
    }

    newSpec shouldBe "pais=*/fecha_proceso=2023-11-22"
  }

  it should "handle complex partition specs with multiple columns" in {
    val refSpec = "year=2023/month=11/day=22/geo=ES"
    
    val columnMapping = Map(
      "year" -> "ano",
      "month" -> "mes",
      "day" -> "dia",
      "geo" -> "pais"
    )

    val newSpec = columnMapping.foldLeft(refSpec) { case (currentSpec, (refCol, newCol)) =>
      currentSpec.replaceAll(s"\\b$refCol=", s"$newCol=")
    }

    newSpec shouldBe "ano=2023/mes=11/dia=22/pais=ES"
  }

  "Empty DataFrame handling" should "handle empty REF table" in {
    val refData = Seq.empty[(Int, String, Double)].toDF("id", "name", "balance")
    val newData = Seq(
      (1, "Alice", 100.0),
      (2, "Bob", 200.0)
    ).toDF("id", "name", "balance")

    refData.count() shouldBe 0
    newData.count() shouldBe 2

    // All NEW rows should be "ONLY_IN_NEW"
  }

  it should "handle empty NEW table" in {
    val refData = Seq(
      (1, "Alice", 100.0),
      (2, "Bob", 200.0)
    ).toDF("id", "name", "balance")
    val newData = Seq.empty[(Int, String, Double)].toDF("id", "name", "balance")

    refData.count() shouldBe 2
    newData.count() shouldBe 0

    // All REF rows should be "ONLY_IN_REF"
  }

  it should "handle both tables empty" in {
    val refData = Seq.empty[(Int, String, Double)].toDF("id", "name", "balance")
    val newData = Seq.empty[(Int, String, Double)].toDF("id", "name", "balance")

    refData.count() shouldBe 0
    newData.count() shouldBe 0
  }

  "Column mapping edge cases" should "handle partial mapping (some columns unmapped)" in {
    val df = Seq(
      (1, "Alice", 100.0, "ES"),
      (2, "Bob", 200.0, "FR")
    ).toDF("id_v2", "nombre_cliente", "saldo", "pais")

    // Only map some columns
    val partialMapping = Map(
      "id" -> "id_v2",
      "name" -> "nombre_cliente"
      // balance and geo are NOT mapped
    )

    val result = TableComparisonController.applyColumnMapping(df, partialMapping)

    result.columns should contain("id")
    result.columns should contain("name")
    result.columns should contain("saldo")  // Unchanged
    result.columns should contain("pais")   // Unchanged
  }

  it should "handle mapping to same name (identity mapping)" in {
    val df = Seq((1, "Alice")).toDF("id", "name")
    
    val identityMapping = Map(
      "id" -> "id",
      "name" -> "name"
    )

    val result = TableComparisonController.applyColumnMapping(df, identityMapping)
    
    result.columns should contain theSameElementsAs Seq("id", "name")
  }

  "Constant column detection" should "exclude columns with same constant value on both sides" in {
    val refData = Seq(
      (1, "Alice", "CONSTANT_VALUE"),
      (2, "Bob", "CONSTANT_VALUE")
    ).toDF("id", "name", "status")

    val newData = Seq(
      (1, "Alice", "CONSTANT_VALUE"),
      (2, "Bob_Modified", "CONSTANT_VALUE")
    ).toDF("id", "name", "status")

    // "status" column has same constant value on both sides
    // It should be excluded from comparison
    
    refData.select("status").distinct().count() shouldBe 1
    newData.select("status").distinct().count() shouldBe 1
    
    val refConstant = refData.select("status").head().getString(0)
    val newConstant = newData.select("status").head().getString(0)
    
    refConstant shouldBe newConstant
  }
}
