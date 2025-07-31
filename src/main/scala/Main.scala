import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CompareTablesMain")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val partitionDate = "2025-07-25"
    val partitionHour = java.time.LocalDateTime.now().format(
      java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
    )

    val refData = Seq(
      (1, "US", 100.50, "active"),
      (2, "ES", 75.20, "pending"),
      (3, "MX", 150.00, "active"),
      (4, "BR", 200.00, "new"),
      (4, "BR", 200.00, "new"),
      (5, "FR", 300.00, "active"),
      (5, "FR", 300.50, "active")
    ).toDF("id", "country", "amount", "status")
     .withColumn("partition_date", lit(partitionDate))

    spark.sql("DROP TABLE IF EXISTS default.ref_customers")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS default.ref_customers (
        |  id INT,
        |  country STRING,
        |  amount DOUBLE,
        |  status STRING
        |)
        |PARTITIONED BY (partition_date STRING)
        |STORED AS PARQUET
      """.stripMargin)

    refData.write.mode("overwrite").insertInto("default.ref_customers")

    val newData = Seq(
      (1, "US", 100.49, "active"),
      (2, "ES", 75.20, "expired"),
      (4, "BR", 200.00, "new"),
      (4, "BR", 200.00, "new"),
      (6, "DE", 400.00, "new"),
      (6, "DE", 400.00, "new"),
      (6, "DE", 400.10, "new")
    ).toDF("id", "country", "amount", "status")
     .withColumn("partition_date", lit(partitionDate))

    spark.sql("DROP TABLE IF EXISTS default.new_customers")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS default.new_customers (
        |  id INT,
        |  country STRING,
        |  amount DOUBLE,
        |  status STRING
        |)
        |PARTITIONED BY (partition_date STRING)
        |STORED AS PARQUET
      """.stripMargin)

    newData.write.mode("overwrite").insertInto("default.new_customers")

    spark.sql("DROP TABLE IF EXISTS default.customer_differences")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS default.customer_differences (
        |  id STRING,
        |  Columna STRING,
        |  Value_ref STRING,
        |  Value_new STRING,
        |  Results STRING
        |)
        |PARTITIONED BY (partition_hour STRING)
        |STORED AS PARQUET
      """.stripMargin)

    spark.sql("DROP TABLE IF EXISTS default.customer_summary")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS default.customer_summary (
        |  Metrica STRING,
        |  total_Ref STRING,
        |  total_New STRING,
        |  pct_Ref STRING,
        |  Status STRING,
        |  Examples STRING
        |)
        |PARTITIONED BY (partition_hour STRING)
        |STORED AS PARQUET
      """.stripMargin)

    spark.sql("DROP TABLE IF EXISTS default.customer_duplicates")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS default.customer_duplicates (
        |  origin STRING,
        |  id STRING,
        |  exact_duplicates STRING,
        |  duplicates_w_variations STRING,
        |  occurrences STRING,
        |  variations STRING
        |)
        |PARTITIONED BY (partition_hour STRING)
        |STORED AS PARQUET
      """.stripMargin)

    println("✅ Tablas ref_customers y new_customers con partición creadas")
    println("✅ Tablas destino creadas: customer_differences, customer_summary y customer_duplicates")

    val config = Map(
      "refTable" -> "default.ref_customers",
      "newTable" -> "default.new_customers",
      "partitionSpec" -> s"[partition_date=$partitionDate]",
      "compositeKeyCols" -> Seq("id"),
      "ignoreCols" -> Seq("last_update"),
      "reportTable" -> "default.customer_summary",
      "diffTable" -> "default.customer_differences",
      "duplicatesTable" -> "default.customer_duplicates",
      "checkDuplicates" -> true,
      "includeEqualsInDiff" -> false
    )

    CompareTablesEnhancedStrict.run(
      spark = spark,
      refTable = config("refTable").asInstanceOf[String],
      newTable = config("newTable").asInstanceOf[String],
      partitionSpec = Some(config("partitionSpec").asInstanceOf[String]),
      compositeKeyCols = config("compositeKeyCols").asInstanceOf[Seq[String]],
      ignoreCols = config("ignoreCols").asInstanceOf[Seq[String]],
      reportTable = config("reportTable").asInstanceOf[String],
      diffTable = config("diffTable").asInstanceOf[String],
      duplicatesTable = config("duplicatesTable").asInstanceOf[String],
      checkDuplicates = config("checkDuplicates").asInstanceOf[Boolean],
      includeEqualsInDiff = config("includeEqualsInDiff").asInstanceOf[Boolean],
      partitionHour = partitionHour
    )

    println("==== Tablas Hive disponibles ====")
    spark.sql("SHOW TABLES").show(false)

    println("==== customer_differences ====")
    spark.sql("SELECT * FROM customer_differences").show(false)

    println("==== customer_summary ====")
    spark.sql("SELECT * FROM customer_summary").show(false)

    println("==== customer_duplicates ====")
    spark.sql("SELECT * FROM customer_duplicates").show(false)

    spark.stop()
  }
}
