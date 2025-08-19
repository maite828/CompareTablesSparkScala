// src/test/scala/DuplicateDetectorSpec.scala

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import DuplicateDetector._
import CompareConfig._

class DuplicateDetectorSpec extends AnyFlatSpec with Matchers with SparkSessionTestWrapper {

  import spark.implicits._

  "detectDuplicatesTable" should "find exact duplicates and no variations" in {
    val schema = StructType(Seq(
      StructField("id",    IntegerType, true),
      StructField("value", StringType,  true)
    ))

    // Dos filas idénticas para id=1, y una sola para id=2
    val refData = Seq(Row(1, "foo"), Row(1, "foo"), Row(2, "bar"))
    val newData = Seq.empty[Row]

    val refDf = spark.createDataFrame(spark.sparkContext.parallelize(refData), schema)
    val newDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    val config = CompareConfig(
      spark            = spark,
      refTable         = "",
      newTable         = "",
      partitionSpec    = None,
      compositeKeyCols = Seq("id"),
      ignoreCols       = Seq.empty,
      initiativeName   = "",
      tablePrefix      = "",
      checkDuplicates  = true,
      includeEqualsInDiff = true
    )

    val dups = DuplicateDetector.detectDuplicatesTable(
      spark, refDf, newDf, compositeKeyCols = Seq("id"), config
    ).collect()

    // Sólo debe aparecer id=1 en origen "ref"
    dups.length shouldBe 1
    val d = dups.head
    d.getAs[String]("origin") shouldBe "ref"
    d.getAs[String]("id") shouldBe "1"
    d.getAs[String]("occurrences") shouldBe "2"
    d.getAs[String]("exact_duplicates") shouldBe "1"
    d.getAs[String]("dups_w_variations") shouldBe "0"
    d.getAs[String]("variations") shouldBe "-"
  }
}
