import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.lit

/**
  * Test unitario: SummaryGenerator con DataFrames vacíos.
  */
class SummaryGeneratorSpec extends AnyFlatSpec with Matchers with SparkSessionTestWrapper {
  import spark.implicits._

  "generateSummaryTable" should "producir KPIs en cero para entradas vacías" in {
    val schema  = StructType(Seq(StructField("id", IntegerType, true)))
    val emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    // diff necesita columnas id y results
    val diffDf = emptyDf.withColumn("results", lit(null: String))
    // dup necesita columna origin (y ya tiene id)
    val dupDf  = emptyDf.withColumn("origin",  lit(null: String))

    val cfg = CompareConfig(
      spark               = spark,
      refTable            = "dummy.ref",
      newTable            = "dummy.new",
      partitionSpec       = None,
      compositeKeyCols    = Seq("id"),
      ignoreCols          = Seq.empty,
      initiativeName      = "unit",
      tablePrefix         = "unit_",
      outputBucket        = "file:///tmp/compare-tests",
      checkDuplicates     = false,
      includeEqualsInDiff = false,
      autoCreateTables    = false,
      exportExcelPath     = None
    )

    val summary = SummaryGenerator.generateSummaryTable(
      SummaryInputs(spark, emptyDf, emptyDf, diffDf, dupDf, Seq("id"))
    )

    summary.filter($"block" === "KPIS" && $"metric" === "Total rows REF")
      .select("numerator").as[String].collect().head
  }
}

