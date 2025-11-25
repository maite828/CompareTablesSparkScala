import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
  * Pruebas complementarias para cubrir switches de configuración y utilidades.
  */
class AdditionalSpec extends AnyFlatSpec with Matchers with SparkSessionTestWrapper {
  import spark.implicits._

  // --------------------------------------------------------------------------
  // A. includeEqualsInDiff = false ⇒ nunca debe haber "MATCH"
  // --------------------------------------------------------------------------
  "DiffGenerator" should "excluir filas MATCH cuando includeEqualsInDiff = false" in {
    val ref = Seq((1, "X"), (2, "A")).toDF("id","val")
    val newDf = Seq((1, "X"), (2, "B")).toDF("id","val")

    val cfg = CompareConfig(
      spark               = spark,
      refTable            = "d.ref",
      newTable            = "d.new",
      partitionSpec       = None,
      compositeKeyCols    = Seq("id"),
      ignoreCols          = Seq.empty,
      initiativeName      = "t",
      tablePrefix         = "u_",
      outputBucket        = "file:///tmp/compare-tests",
      checkDuplicates     = false,
      includeEqualsInDiff = false,
      autoCreateTables    = false,
      priorityCols        = Seq.empty,
      aggOverrides        = Map.empty,
      exportExcelPath     = None,
      outputDateISO       = "2025-01-01"
    )

    val diffs = DiffGenerator.generateDifferencesTable(
      spark,
      ref,
      newDf,
      Seq("id"),
      Seq("val"),
      includeEquals = false,
      cfg
    )
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
      refTable            = "x.ref",
      newTable            = "x.new",
      partitionSpec       = None,
      compositeKeyCols    = Seq("id"),
      ignoreCols          = Seq.empty,
      initiativeName      = "t",
      tablePrefix         = "u_",
      outputBucket        = "file:///tmp/compare-tests",
      checkDuplicates     = true,
      includeEqualsInDiff = false,
      autoCreateTables    = false,
      priorityCols        = Seq("ts"),
      aggOverrides        = Map.empty,
      exportExcelPath     = None,
      outputDateISO       = "2025-01-01"
    )

    val dups = DuplicateDetector.detectDuplicatesTable(spark, refDF, newDF, Seq("id"), cfg)
    // Con priorityCol, la fila de mayor ts queda y no hay duplicados
    dups.count() shouldEqual 0L
  }

  // --------------------------------------------------------------------------
  // C. pctStr denominador = 0
  // --------------------------------------------------------------------------
  "SummaryGenerator.pctStr" should "devolver '-' cuando el denominador es 0" in {
    SummaryGenerator.pctStr(5, 0) shouldEqual "-"
  }
}
