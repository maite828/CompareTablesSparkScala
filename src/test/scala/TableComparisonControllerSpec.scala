import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.functions._

import CompareConfig._

class TableComparisonControllerSpec
    extends AnyFlatSpec
    with Matchers
    with SparkSessionTestWrapper {

  import spark.implicits._

  "TableComparisonController" should "respect executionDate override and ignore wildcard partitions" in {
    val spark = this.spark

    val refData = Seq(
      (1, "A", "EU", "2025-08-13"),
      (2, "B", "EU", "2025-08-13")
    ).toDF("id", "status", "geo", "data_date_part")

    val newData = Seq(
      (1, "A", "EU", "2025-08-13"),
      (2, "C", "EU", "2025-08-13")
    ).toDF("id", "status", "geo", "data_date_part")

    val refTable = "default.ref_airflow_spec"
    val newTable = "default.new_airflow_spec"
    val prefix   = "default.airflow_results_"

    Seq(
      refTable,
      newTable,
      s"${prefix}differences",
      s"${prefix}summary",
      s"${prefix}duplicates"
    ).foreach(tbl => spark.sql(s"DROP TABLE IF EXISTS $tbl PURGE"))

    refData.write.mode("overwrite").saveAsTable(refTable)
    newData.write.mode("overwrite").saveAsTable(newTable)

    val cfg = CompareConfig(
      spark               = spark,
      refSource           = HiveTable(refTable),
      newSource           = HiveTable(newTable),
      partitionSpec       = Some("geo=\"*\"/data_date_part=\"YYYY-MM-dd\""),
      compositeKeyCols    = Seq("id"),
      ignoreCols          = Seq.empty,
      initiativeName      = "swift",
      tablePrefix         = prefix,
      executionDateOverride = Some("2025-08-14"),
      checkDuplicates     = false,
      includeEqualsInDiff = false,
      autoCreateTables    = true,
      exportExcelPath     = None
    )

    TableComparisonController.run(cfg)

    val diffDates = spark.table(s"${prefix}differences")
      .where($"initiative" === "swift")
      .select("data_date_part")
      .distinct()
      .collect()
      .map(_.getString(0))

    diffDates should contain only "2025-08-14"

    val summaryCount = spark.table(s"${prefix}summary")
      .where($"initiative" === "swift" && $"data_date_part" === "2025-08-14")
      .count()

    summaryCount should be > 0L

    Seq(
      refTable,
      newTable,
      s"${prefix}differences",
      s"${prefix}summary",
      s"${prefix}duplicates"
    ).foreach(tbl => spark.sql(s"DROP TABLE IF EXISTS $tbl PURGE"))
  }
}
