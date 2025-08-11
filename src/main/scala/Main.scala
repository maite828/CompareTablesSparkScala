// src/main/scala/com/example/compare/Main.scala

import java.math.BigDecimal
import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.TableIdentifier

// Importa el CompareConfig nuevo (con compatibilidad Hive)
import CompareConfig._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CompareTablesMain")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val dataDatePart    = "2025-07-01"
    val geoPart         = "ES"
    val partitionSpec   = Some(s"""date="$dataDatePart"/geo="$geoPart"""")
    val initiativeName  = "Swift"
    val tablePrefix     = "default.result_"

    // 0) Generar DataFrames de prueba con DecimalType(38,18)
    val (refDF, newDF) = createTestDataFrames(spark, dataDatePart, geoPart)
    createAndLoadSourceTables(spark, refDF, newDF)

    println(s"✅ Tablas ref_customers y new_customers creadas (particiones: date, geo)")
    println(s"✅ Iniciativa: $initiativeName")
    println(s"✅ PartitionSpec: ${partitionSpec.getOrElse("-")}")

    // 1) Preparar tablas de resultados (limpieza fuerte)
    val diffTableName       = s"${tablePrefix}differences"
    val summaryTableName    = s"${tablePrefix}summary"
    val duplicatesTableName = s"${tablePrefix}duplicates"
    cleanAndPrepareTables(spark, diffTableName, summaryTableName, duplicatesTableName)

    // 2) Construir CompareConfig (modo compat: Hive → Hive)
    val config = CompareConfig(
      spark              = spark,
      refTable           = "default.ref_customers",
      newTable           = "default.new_customers",
      partitionSpec      = partitionSpec,
      compositeKeyCols   = Seq("id"),
      ignoreCols         = Seq("last_update"),
      initiativeName     = initiativeName,
      tablePrefix        = tablePrefix,
      checkDuplicates    = true,
      includeEqualsInDiff= true,
      autoCreateTables   = true,
      // el resto usa defaults (nullKeyMatches=true, etc.)
      exportExcelPath    = Some("./output/summary.xlsx")
    )

    // 3) Ejecutar comparación
    TableComparisonController.run(config)

    // 4) Mostrar resultados
    showComparisonResults(spark, tablePrefix, initiativeName, dataDatePart)

    spark.stop()
  }

  private def createTestDataFrames(
      spark: SparkSession,
      dataDatePart: String,
      geoPart: String
  ): (DataFrame, DataFrame) = {
    val schema = StructType(Seq(
      StructField("id", IntegerType,            nullable = true),
      StructField("country", StringType,        nullable = true),
      StructField("amount", DecimalType(38,18), nullable = true),
      StructField("status", StringType,         nullable = true)
    ))

   val ref = Seq(
      Row(1: java.lang.Integer, "US", new BigDecimal("100.40"), "active"),
      Row(2: java.lang.Integer, "ES ", new BigDecimal("1.000000000000000001"), "expired"),
      Row(3: java.lang.Integer, "MX", new BigDecimal("150.00"), "active"),
      Row(4: java.lang.Integer, "FR", new BigDecimal("200.00"), "new"),
      Row(4: java.lang.Integer, "BR", new BigDecimal("201.00"), "new"),
      Row(5: java.lang.Integer, "FR", new BigDecimal("300.00"), "active"),
      Row(5: java.lang.Integer, "FR", new BigDecimal("300.50"), "active"),
      Row(7: java.lang.Integer, "PT", new BigDecimal("300.50"), "active"),
      Row(8: java.lang.Integer, "BR", new BigDecimal("100.50"), "pending"),
      Row(9: java.lang.Integer, "AN", new BigDecimal("80.00"), "new"),
      Row(10: java.lang.Integer, "GR", new BigDecimal("60.00"), "new"),
      Row(null                 , "GR", new BigDecimal("61.00"), "new"),
      Row(null                 , "GR", new BigDecimal("60.00"), "new")
    )

    val nw = Seq(
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
      Row(9: java.lang.Integer, "AN", new BigDecimal("80.00"),  null),
      Row(null                , "GR", new BigDecimal("60.00"), "new"),
      Row(null                , "GR", new BigDecimal("60.00"), "new"),
      Row(null                , "GR", new BigDecimal("60.00"), "new"),
      Row(null                , "GR", new BigDecimal("61.00"), "new")
    )

    val refDF = spark.createDataFrame(spark.sparkContext.parallelize(ref.asInstanceOf[Seq[Row]]), schema)
      .withColumn("date", lit(dataDatePart))
      .withColumn("geo",  lit(geoPart))

    val newDF = spark.createDataFrame(spark.sparkContext.parallelize(nw.asInstanceOf[Seq[Row]]), schema)
      .withColumn("date", lit(dataDatePart))
      .withColumn("geo",  lit(geoPart))

    (refDF, newDF)
  }

  private def createAndLoadSourceTables(
      spark: SparkSession,
      refDF: DataFrame,
      newDF: DataFrame
  ): Unit = {
    // ref
    spark.sql("DROP TABLE IF EXISTS default.ref_customers")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS default.ref_customers (
        |  id INT,
        |  country STRING,
        |  amount DECIMAL(38,18),
        |  status STRING
        |)
        |PARTITIONED BY (date STRING, geo STRING)
        |STORED AS PARQUET
      """.stripMargin)
    refDF.write.mode("overwrite").insertInto("default.ref_customers")

    // new
    spark.sql("DROP TABLE IF EXISTS default.new_customers")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS default.new_customers (
        |  id INT,
        |  country STRING,
        |  amount DECIMAL(38,18),
        |  status STRING
        |)
        |PARTITIONED BY (date STRING, geo STRING)
        |STORED AS PARQUET
      """.stripMargin)
    newDF.write.mode("overwrite").insertInto("default.new_customers")
  }

  private def cleanAndPrepareTables(
      spark: SparkSession,
      tableNames: String*
  ): Unit = {
    tableNames.foreach { fullTableName =>
      try {
        val parts = fullTableName.split('.')
        val (db, table) = if (parts.length > 1) (parts(0), parts(1)) else ("default", parts(0))
        val tabId = TableIdentifier(table, Some(db))

        spark.sql(s"DROP TABLE IF EXISTS $fullTableName PURGE")

        val fs      = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val catalog = spark.sessionState.catalog
        val loc     = if (catalog.tableExists(tabId))
          catalog.getTableMetadata(tabId).location
        else
          catalog.defaultTablePath(tabId)

        val path = new Path(loc.toString)
        if (fs.exists(path)) fs.delete(path, true)
      } catch {
        case e: Exception =>
          println(s"[WARN] Error cleaning $fullTableName: ${e.getMessage}")
      }
    }
  }

  private def showComparisonResults(
      spark: SparkSession,
      prefix: String,
      initiative: String,
      datePart: String
  ): Unit = {
    def q(table: String) =
      s"""
         |SELECT *
         |FROM $table
         |WHERE initiative = '$initiative'
         |  AND data_date_part = '$datePart'
         """.stripMargin

    println("\n-- Tablas Hive disponibles --")
    spark.sql("SHOW TABLES").show(false)

    println(s"\n-- Differences (${prefix}differences) --")
    spark.sql(q(prefix + "differences")).show(100, false)

    println(s"\n-- Summary (${prefix}summary) --")
    spark.sql(q(prefix + "summary")).show(100, false)

    println(s"\n-- Duplicates (${prefix}duplicates) --")
    spark.sql(q(prefix + "duplicates")).show(100, false)
  }
}
