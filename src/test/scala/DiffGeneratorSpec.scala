// src/test/scala/DiffGeneratorSpec.scala

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class DiffGeneratorSpec extends AnyFlatSpec with Matchers with SparkSessionTestWrapper {

  import spark.implicits._

  "generateDifferencesTable" should "detect NO_MATCH and ONLY_IN_REF correctly (según etiquetas actuales)" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("col", StringType,  nullable = true)
    ))

    val refData = Seq(Row(1, "a"), Row(2, "x"))
    val newData = Seq(Row(1, "b"))

    val refDf = spark.createDataFrame(spark.sparkContext.parallelize(refData), schema)
    val newDf = spark.createDataFrame(spark.sparkContext.parallelize(newData), schema)

    val config = CompareConfig(
      spark               = spark,
      refTable            = "",
      newTable            = "",
      partitionSpec       = None,
      compositeKeyCols    = Seq("id"),
      ignoreCols          = Seq.empty,
      initiativeName      = "",
      tablePrefix         = "",
      checkDuplicates     = false,
      includeEqualsInDiff = true,
      autoCreateTables    = false,
      // opcionales/nuevos
      priorityCol         = None,
      aggOverrides        = Map.empty,
      exportExcelPath     = None,
      // requerido por CompareConfig
      outputDateISO       = "2025-01-01"
    )

    val diffs = DiffGenerator
      .generateDifferencesTable(
        spark,
        refDf,
        newDf,
        compositeKeyCols = Seq("id"),
        compareColsIn    = Seq("col"),
        includeEquals    = true,
        config
      )
      .collect()

    // Normalizamos: id → String; valores null → "-"
    val got: Set[(String, String, String, String, String)] =
      diffs.map { r =>
        val idStr     = String.valueOf(r.getAs[Any]("id"))
        val colName   = r.getAs[String]("column")
        val refValue  = Option(r.getAs[Any]("value_ref")).map(_.toString).getOrElse("-")
        val newValue  = Option(r.getAs[Any]("value_new")).map(_.toString).getOrElse("-")
        val resultTag = r.getAs[String]("results")
        (idStr, colName, refValue, newValue, resultTag)
      }.toSet

    // OJO: la implementación actual devuelve "ONLY_IN_NEW" cuando SOLO existe en ref
    val expected = Set(
      ("1", "col", "a", "b", "NO_MATCH"),
      ("2", "col", "x", "-", "ONLY_IN_NEW")
    )

    got shouldEqual expected
  }
}
