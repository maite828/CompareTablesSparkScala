// src/test/scala/TestData.scala
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.math.BigDecimal
import CompareConfig._


object TestData {
  private val schema = StructType(Seq(
    StructField("id",      IntegerType,        true),
    StructField("country", StringType,         true),
    StructField("amount",  DecimalType(38,18), true),
    StructField("status",  StringType,         true)
  ))

  private val refRows = Seq(
    Row(1: java.lang.Integer, "US", new BigDecimal("100.40"), "active"),
    Row(1: java.lang.Integer, "US", new BigDecimal("100.40"), "active"), // dup extra
    Row(2: java.lang.Integer, "ES ", new BigDecimal("1.000000000000000001"), "expired"),
    Row(3: java.lang.Integer, "MX", new BigDecimal("150.00"), "active"),
    Row(4: java.lang.Integer, "FR", new BigDecimal("200.00"), "new"),
    Row(4: java.lang.Integer, "BR", new BigDecimal("201.00"), "new"),
    Row(5: java.lang.Integer, "FR", new BigDecimal("300.00"), "active"),
    Row(5: java.lang.Integer, "FR", new BigDecimal("300.50"), "active"),
    Row(7: java.lang.Integer, "PT", new BigDecimal("300.50"), "active"),
    Row(8: java.lang.Integer, "BR", new BigDecimal("100.50"), "pending"),
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
    Row(8: java.lang.Integer, "BR", null                    , "pending"),
    Row(null                , "GR", new BigDecimal("60.00"), "new"),
    Row(null                , "GR", new BigDecimal("60.00"), "new"),
    Row(null                , "GR", new BigDecimal("60.00"), "new"),
    Row(null                , "GR", new BigDecimal("61.00"), "new")
  )

  def frames(spark: SparkSession, date: String, geo: String): (DataFrame, DataFrame) = {
    val refDF = spark.createDataFrame(spark.sparkContext.parallelize(refRows), schema)
      .withColumn("date", lit(date)).withColumn("geo", lit(geo))
    val newDF = spark.createDataFrame(spark.sparkContext.parallelize(newRows), schema)
      .withColumn("date", lit(date)).withColumn("geo", lit(geo))
    (refDF, newDF)
  }
}
