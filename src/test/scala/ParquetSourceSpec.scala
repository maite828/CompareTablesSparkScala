// src/test/scala/ParquetSourceSpec.scala
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.nio.file.Files

// >>> Imports del proyecto
import FileSource._
import CompareConfig._

class ParquetSourceSpec extends AnyFlatSpec with Matchers with SparkSessionTestWrapper {

  "Controller" should "comparar correctamente leyendo Parquet particionado (pushdown)" in {
    val date = "2025-07-01"
    val geo  = "ES"
    val (refDF, newDF) = TestData.frames(spark, date, geo)

    val base   = Files.createTempDirectory("pq-io-").toFile.getAbsolutePath
    val refDir = s"$base/ref"
    val newDir = s"$base/new"

    refDF.write.mode("overwrite").partitionBy("date","geo").parquet(refDir)
    newDF.write.mode("overwrite").partitionBy("date","geo").parquet(newDir)

    val prefix = "default.result_"
    TableComparisonController.ensureResultTables(
      spark, s"${prefix}differences", s"${prefix}summary", s"${prefix}duplicates"
    )

    val cfg = CompareConfig(
      spark              = spark,
      refSource          = FileSource(refDir, "parquet"),
      newSource          = FileSource(newDir, "parquet"),
      partitionSpec      = Some(s"""date="$date"/geo="$geo""""),
      compositeKeyCols   = Seq("id"),
      ignoreCols         = Seq("last_update"),
      initiativeName     = "Swift",
      tablePrefix        = prefix,
      checkDuplicates    = true,
      includeEqualsInDiff= true,
      autoCreateTables   = false,
      exportExcelPath    = None
    )

    TableComparisonController.run(cfg)

    val where = s"initiative = 'Swift' AND data_date_part = '$date'"
    val diffCnt = spark.sql(s"SELECT COUNT(*) FROM ${prefix}differences WHERE $where").count()
    val sumCnt  = spark.sql(s"SELECT COUNT(*) FROM ${prefix}summary    WHERE $where").count()
    val dupCnt  = spark.sql(s"SELECT COUNT(*) FROM ${prefix}duplicates WHERE $where").count()

    diffCnt should be > 0L
    sumCnt  should be > 0L
    dupCnt  should be > 0L

    // (Opcional) Validación concreta ejemplo NO_MATCH en country para id=2
    val hasNoMatchId2 =
      spark.sql(s"SELECT 1 FROM ${prefix}differences WHERE $where AND id='2' AND column='country' AND results='NO_MATCH'")
        .limit(1).count() == 1L
    hasNoMatchId2 shouldBe true
  }
}
