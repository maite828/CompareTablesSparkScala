// src/test/scala/DiffGeneratorSpec.scala

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import DiffGenerator._
import CompareConfig._

class DiffGeneratorSpec extends AnyFlatSpec with Matchers with SparkSessionTestWrapper {

  import spark.implicits._

  "generateDifferencesTable" should "detect NO_MATCH and ONLY_IN_REF correctly" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("col", StringType,  nullable = true)
    ))

    val refData = Seq(Row(1, "a"), Row(2, "x"))
    val newData = Seq(Row(1, "b"))

    val refDf = spark.createDataFrame(spark.sparkContext.parallelize(refData), schema)
    val newDf = spark.createDataFrame(spark.sparkContext.parallelize(newData), schema)

    val config = CompareConfig(
      spark,
      "dummy.ref",
      "dummy.new",
      None,                 // partitionSpec
      Seq("id"),            // compositeKeyCols
      Seq.empty,            // ignoreCols
      "Swift",              // initiativeName
      "unit_",              // tablePrefix
      checkDuplicates     = true,
      includeEqualsInDiff = false,   // <--- FALTABA
      autoCreateTables    = false,   // <--- FALTABA
      exportExcelPath     = None     // <--- FALTABA
    )

    val diffs = DiffGenerator
      .generateDifferencesTable(
        spark, refDf, newDf,
        compositeKeyCols = Seq("id"),
        compareColsIn    = Seq("col"),
        includeEquals    = true,
        config
      )
      .collect()

    // Debe haber exactamente dos filas: una NO_MATCH y una ONLY_IN_REF
    diffs.length shouldBe 2

    // Verificamos los contenidos
    diffs should contain (
      Row("1", "col", "a", "b", "NO_MATCH")
    )
    diffs should contain (
      Row("2", "col", "x", "-", "ONLY_IN_REF")
    )
  }
}
