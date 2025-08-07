import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import java.math.BigDecimal

/**
  * Test de flujo completo (end‑to‑end) con el dataset sintético.
  */
class CompareTablesSpec extends AnyFlatSpec with Matchers with SparkSessionTestWrapper {
  import spark.implicits._

  // --------------------------------------------------------------------------
  // Datos base
  // --------------------------------------------------------------------------
  private val schema = StructType(Seq(
    StructField("id",      IntegerType,         true),
    StructField("country", StringType,          true),
    StructField("amount",  DecimalType(38,18),  true),
    StructField("status",  StringType,          true)
  ))

  private val refRows = Seq(
    Row(1: java.lang.Integer, "US", new BigDecimal("100.40"), "active"),
    Row(2: java.lang.Integer, "ES ", new BigDecimal("1.000000000000000001"), "expired"),
    Row(3: java.lang.Integer, "MX", new BigDecimal("150.00"), "active"),
    Row(4: java.lang.Integer, "FR", new BigDecimal("200.00"), "new"),
    Row(4: java.lang.Integer, "BR", new BigDecimal("201.00"), "new"),
    Row(5: java.lang.Integer, "FR", new BigDecimal("300.00"), "active"),
    Row(5: java.lang.Integer, "FR", new BigDecimal("300.50"), "active"),
    Row(7: java.lang.Integer, "PT", new BigDecimal("300.50"), "active"),
    Row(8: java.lang.Integer, "BR", new BigDecimal("100.50"), "pending"),
    Row(9: java.lang.Integer, "AN", new BigDecimal("80.00"), "new"),
    Row(10: java.lang.Integer, "GR", new BigDecimal("60.00"), "new"),
    Row(null                 , "GR", new BigDecimal("61.00"), "new"),
    Row(null                 , "GR", new BigDecimal("60.00"), "new")
  )

  private val newRows = Seq(
    Row(1: java.lang.Integer, "US", new BigDecimal("100.40"), "active"),
    Row(2: java.lang.Integer, "ES", new BigDecimal("1.000000000000000001"), "expired"),
    Row(4: java.lang.Integer, "BR", new BigDecimal("201.00"), "new"),
    Row(4: java.lang.Integer, "BR", new BigDecimal("200.00"), "new"),
    Row(4: java.lang.Integer, "BR", new BigDecimal("200.00"), "new"),
    Row(4: java.lang.Integer, "BR", new BigDecimal("200.00"), "new"),
    Row(6: java.lang.Integer, "DE", new BigDecimal("400.00"), "new"),
    Row(6: java.lang.Integer, "DE", new BigDecimal("400.00"), "new"),
    Row(6: java.lang.Integer, "DE", new BigDecimal("400.10"), "new"),
    Row(7: java.lang.Integer, "",   new BigDecimal("300.50"), "active"),
    Row(8: java.lang.Integer, "BR", null                    ,  "pending"),
    Row(9: java.lang.Integer, "AN", new BigDecimal("80.00"),  null),
    Row(null                , "GR", new BigDecimal("60.00"), "new"),
    Row(null                , "GR", new BigDecimal("60.00"), "new"),
    Row(null                , "GR", new BigDecimal("60.00"), "new"),
    Row(null                , "GR", new BigDecimal("61.00"), "new")
  )

  private val refDF = spark.createDataFrame(spark.sparkContext.parallelize(refRows), schema)
  private val newDF = spark.createDataFrame(spark.sparkContext.parallelize(newRows), schema)

  private val keyCols     = Seq("id")
  private val compareCols = Seq("country", "amount", "status")

  private val cfg = CompareConfig(
    spark               = spark,
    refTable            = "dummy.ref",
    newTable            = "dummy.new",
    partitionSpec       = None,
    compositeKeyCols    = keyCols,
    ignoreCols          = Seq("last_update"),
    initiativeName      = "Swift",
    tablePrefix         = "unit_",
    checkDuplicates     = true,
    includeEqualsInDiff = false,
    autoCreateTables    = false,
    exportExcelPath     = None
  )

  // DataFrames resultado in‑memory
  private val diffDF = DiffGenerator.generateDifferencesTable(spark, refDF, newDF, keyCols, compareCols, includeEquals = false, cfg)
  private val dupDF  = DuplicateDetector.detectDuplicatesTable(spark, refDF, newDF, keyCols, cfg)
  private val sumDF  = SummaryGenerator.generateSummaryTable(spark, refDF, newDF, diffDF, dupDF, keyCols, refDF, newDF, cfg)

  behavior of "Comparación End‑to‑End"

  it should "marcar NO_MATCH para id 2 / country" in {
    diffDF.filter($"id" === "2" && $"column" === "country").select("results").as[String].head() shouldEqual "NO_MATCH"
  }

  it should "contener 17 diferencias" in {
    diffDF.count() shouldEqual 17L
  }

  it should "detectar duplicados" in {
    dupDF.count() should be > 0L
  }

  it should "calcular métricas clave" in {
    def num(m: String, u: String): Long = sumDF.filter($"metrica"===m && $"universo"===u).select($"numerador").as[String].head().toLong
    num("IDs Uniques", "REF")  shouldEqual 10L
    num("IDs Uniques", "NEW")  shouldEqual 8L
    num("Total REF",   "ROWS") shouldEqual 13L
    num("Total NEW",   "ROWS") shouldEqual 16L
  }

  it should "mantener las 7 columnas del resumen" in {
    sumDF.columns.toSeq.sorted shouldEqual Seq("bloque","denominador","ejemplos","metrica","numerador","pct","universo").sorted
  }
}
