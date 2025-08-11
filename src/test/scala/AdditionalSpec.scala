import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import CompareConfig._

/**
  * Additional tests to cover configuration switches and utilities.
  */
class AdditionalSpec extends AnyFlatSpec with Matchers with SparkSessionTestWrapper {
  import spark.implicits._

  // --------------------------------------------------------------------------
  // A. includeEqualsInDiff = false ⇒ never should have "MATCH"
  // --------------------------------------------------------------------------
  "DiffGenerator" should "excluir filas MATCH cuando includeEqualsInDiff = false" in {
    val ref = Seq((1, "X"), (2, "A")).toDF("id","val")
    val newDf = Seq((1, "X"), (2, "B")).toDF("id","val")

  val cfg = CompareConfig(
    spark               = spark,
    refSource           = HiveTable("dummy.ref"),
    newSource           = HiveTable("dummy.new"),
    partitionSpec       = None,
    compositeKeyCols    = Seq("id"),
    ignoreCols          = Seq.empty,             // antes: Seq[Nothing]
    initiativeName      = "Swift",
    tablePrefix         = "unit_",
    checkDuplicates     = true,
    includeEqualsInDiff = false,
    autoCreateTables    = false,
    nullKeyMatches      = true,
    includeDupInQuality = false,
    priorityCol         = Some("status"),        // <-- pon la col real que usa tu test
    aggOverrides        = Map.empty,
    exportExcelPath     = None
  )

    val diffs = DiffGenerator.generateDifferencesTable(spark, ref, newDf, Seq("id"), Seq("val"), includeEquals = false, cfg)
    diffs.select("results").as[String].collect() should not contain ("MATCH")
  }

  // --------------------------------------------------------------------------
  // B. DuplicateDetector con priorityCol
  // --------------------------------------------------------------------------
  "DuplicateDetector" should "eliminar duplicados cuando se aplica priorityCol" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType, true),
      StructField("ts", IntegerType, true),
      StructField("v",  StringType,  true)
    ))

    val refRows = Seq(Row(1, 10, "A"), Row(1, 20, "A"), Row(2, 10, "B"))
    val refDF = spark.createDataFrame(spark.sparkContext.parallelize(refRows), schema)
    val newDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

  val cfg = CompareConfig(
    spark               = spark,
    refSource           = HiveTable("dummy.ref"),
    newSource           = HiveTable("dummy.new"),
    partitionSpec       = None,
    compositeKeyCols    = Seq("id"),
    ignoreCols          = Seq.empty,             // antes: Seq[Nothing]
    initiativeName      = "Swift",
    tablePrefix         = "unit_",
    checkDuplicates     = true,
    includeEqualsInDiff = false,
    autoCreateTables    = false,
    nullKeyMatches      = true,
    includeDupInQuality = false,
    priorityCol         = Some("ts"),            // <-- usar la columna "ts" que sí existe
    aggOverrides        = Map.empty,
    exportExcelPath     = None
  )

    val dups = DuplicateDetector.detectDuplicatesTable(spark, refDF, newDF, Seq("id"), cfg)
    // Con priorityCol, la fila de mayor ts queda y no hay duplicados
    dups.count() shouldEqual 0L
  }

  // --------------------------------------------------------------------------

  // --------------------------------------------------------------------------
  // C. pctStr denominador = 0
  // --------------------------------------------------------------------------
  "SummaryGenerator.pctStr" should "devolver '-' cuando el denominador es 0" in {
    SummaryGenerator.pctStr(5, 0) shouldEqual "-"
  }
}
