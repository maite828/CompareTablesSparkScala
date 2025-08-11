// src/test/scala/CSVSourceSpec.scala
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.nio.file.Files

// >>> Imports del proyecto
import FileSource._
import org.apache.spark.sql.types._ 

import org.apache.spark.sql.types._

class CSVSourceSpec extends AnyFlatSpec with Matchers with SparkSessionTestWrapper {

  "Controller" should "comparar correctamente leyendo CSV con DecimalType(38,18)" in {
    val date = "2025-07-01"
    val geo  = "ES"
    val (refDF, newDF) = TestData.frames(spark, date, geo)

    val base   = Files.createTempDirectory("csv-io-").toFile.getAbsolutePath
    val refDir = s"$base/ref"
    val newDir = s"$base/new"

    // escribir CSV con cabecera
    refDF.coalesce(1).write.mode("overwrite").option("header","true").csv(refDir)
    newDF.coalesce(1).write.mode("overwrite").option("header","true").csv(newDir)

    // esquema CSV para preservar DecimalType
    val csvSchema = StructType(Seq(
      StructField("id", IntegerType,            true),
      StructField("country", StringType,        true),
      StructField("amount", DecimalType(38,18), true),
      StructField("status", StringType,         true),
      StructField("date", StringType,           true),
      StructField("geo",  StringType,           true)
    ))
    val csvOpts = Map(
      "header" -> "true",
      "mode" -> "PERMISSIVE",
      "enforceSchema" -> "true",
      "ignoreLeadingWhiteSpace" -> "true",
      "ignoreTrailingWhiteSpace" -> "true",
      "nullValue" -> ""
    )

    // preparar tablas resultado en Hive
    val prefix = "default.result_"
    TableComparisonController.ensureResultTables(
      spark, s"${prefix}differences", s"${prefix}summary", s"${prefix}duplicates"
    )

    val cfg = CompareConfig(
      spark              = spark,
      refSource          = FileSource(refDir, "csv", csvOpts, Some(csvSchema)),
      newSource          = FileSource(newDir, "csv", csvOpts, Some(csvSchema)),
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

    // (Opcional) comprobación concreta como en el test in-memory:
    // val hasNoMatchId2 =
    //   spark.sql(s"SELECT 1 FROM ${prefix}differences WHERE $where AND id='2' AND column='country' AND results='NO_MATCH'")
    //     .limit(1).count() == 1L
    // hasNoMatchId2 shouldBe true
  }
}
