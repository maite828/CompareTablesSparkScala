import org.apache.spark.sql.SparkSession

/** Table DDL helpers to keep the controller small. */
object TableIO {

  def ensureResultTables(spark: SparkSession, diffTable: String, summaryTable: String, duplicatesTable: String, config: CompareConfig): Unit = {
    createDiffTable(spark, diffTable, config)
    createSummaryTable(spark, summaryTable, config)
    createDuplicatesTable(spark, duplicatesTable, config)
  }

  private def createDiffTable(spark: SparkSession, table: String, config: CompareConfig): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS $table (
         |  id STRING,
         |  `column` STRING,
         |  value_ref STRING,
         |  value_new STRING,
         |  results STRING,
         |  initiative STRING,
         |  data_date_part STRING
         |)
         |USING parquet
         |PARTITIONED BY (initiative, data_date_part)
         |LOCATION '${config.outputBucket}/differences/'
         |""".stripMargin)
  }

  private def createSummaryTable(spark: SparkSession, table: String, config: CompareConfig): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS $table (
         |  block STRING,
         |  metric STRING,
         |  universe STRING,
         |  numerator STRING,
         |  denominator STRING,
         |  pct STRING,
         |  samples STRING,
         |  initiative STRING,
         |  data_date_part STRING
         |)
         |USING parquet
         |PARTITIONED BY (initiative, data_date_part)
         |LOCATION '${config.outputBucket}/summary/'
         |""".stripMargin)
  }

  private def createDuplicatesTable(spark: SparkSession, table: String, config: CompareConfig): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS $table (
         |  origin STRING,
         |  id STRING,
         |  exact_duplicates STRING,
         |  dupes_w_variations STRING,
         |  occurrences STRING,
         |  variations STRING,
         |  initiative STRING,
         |  data_date_part STRING
         |)
         |USING parquet
         |PARTITIONED BY (initiative, data_date_part)
         |LOCATION '${config.outputBucket}/duplicates/'
         |""".stripMargin)
  }
}
