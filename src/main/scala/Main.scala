import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.lit

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

//-----------------------------------------------------------------------
    // 1. Definir el esquema
    val customSchema0 = StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("country", StringType, nullable = true),
      StructField("amount", DoubleType, nullable = true),
      StructField("status", StringType, nullable = true)
    ))

    val refData = Seq(
      Row(1, "US", 100.50, "active"),
      Row(2, "ES", 75.20, "pending"),
      Row(3, "MX", 150.00, "active"),
      Row(4, "BR", 200.00, "new"),
      Row(4, "BR", 200.00, "new"),
      Row(5, "FR", 300.00, "active"),
      Row(5, "FR", 300.50, "active"),
      Row(7, "PT", 300.50, "active"),
      Row(8, "BR", 100.50, "pending"),
      Row(9, "AN", 80.00, "new"),
      Row(10, "GR",60.00, "new"),
      Row(null, "GR",61.00, "new")
    )

    // 3. Crear DataFrame
    val newDataFrame0 = spark.createDataFrame(
      spark.sparkContext.parallelize(refData),
      customSchema0
    ).withColumn("partition_date", lit(partitionDate))  


    // 4. Configuración para particiones dinámicas (opcional)
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    // 5. Crear tabla (si no existe)
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

    // 6. Escribir datos
    newDataFrame0.write
      .mode("overwrite")
      .insertInto("default.ref_customers")   


//-----------------------------------------------------------------------
    // 2. Preparar los datos
    val rowData = Seq(
      Row(1, "US", 100.49, "active"),
      Row(2, "ES", 75.20, "expired"),
      Row(4, "BR", 200.00, "new"),
      Row(4, "BR", 200.00, "new"),
      Row(6, "DE", 400.00, "new"),
      Row(6, "DE", 400.00, "new"),
      Row(6, "DE", 400.10, "new"),
      Row(7, "",   300.50, "active"),
      Row(8, "BR", null,   "pending"),
      Row(9, "AN", 80.00,  null),
      Row(null, "GR",60.00, "new"),
      Row(null, "GR",60.00, "new"),
      Row(null, "GR",60.00, "new")
    )

    // 3. Crear DataFrame
    val newDataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(rowData),
      customSchema0
    ).withColumn("partition_date", lit(partitionDate))

    // 4. Crear tabla (si no existe)
    spark.sql("DROP TABLE IF EXISTS default.new_customers")
    spark.sql(
      """
        |CREATE TABLE default.new_customers (
        |  id INT,
        |  country STRING,
        |  amount DOUBLE,
        |  status STRING
        |)
        |PARTITIONED BY (partition_date STRING)
        |STORED AS PARQUET
      """.stripMargin)

    // 6. Escribir datos
    newDataFrame.write
      .mode("overwrite")
      .insertInto("default.new_customers")

//-----------------------------------------------------------------------
    spark.sql("DROP TABLE IF EXISTS default.customer_differences")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS default.customer_differences (
        |  id STRING,
        |  column STRING,
        |  value_ref STRING,
        |  value_new STRING,
        |  results STRING
        |)
        |PARTITIONED BY (partition_hour STRING)
        |STORED AS PARQUET
      """.stripMargin)

    spark.sql("DROP TABLE IF EXISTS default.customer_summary")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS default.customer_summary (
        | metrica        STRING,
        | total_ref      STRING,
        | total_new      STRING,
        | pct_ref        STRING,
        | status         STRING,
        | examples       STRING,
        |table          STRING  
        |)
        |PARTITIONED BY (partition_hour STRING)
        |STORED AS PARQUET
      """.stripMargin)


//-----------------------------------------------------------------------
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
//-----------------------------------------------------------------------


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
      "includeEqualsInDiff" -> true
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
    spark.sql("SELECT * FROM customer_differences").show(100,false)

    println("==== customer_summary ====")
    spark.sql("SELECT * FROM customer_summary").show(false)

    println("==== customer_duplicates ====")
    spark.sql("SELECT * FROM customer_duplicates").show(false)

    spark.stop()
  }
}
