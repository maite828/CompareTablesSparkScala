// src/main/scala/com/example/compare/Main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.TableIdentifier

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CompareTablesMain")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val dataDatePart = "2025-07-01"
    val geoPart      = "ES"
    val partitionSpec = Some(s"""data_date_part="$dataDatePart"/geo="$geoPart"""")
    val initiativeName = "Swift"                  // Nombre de la iniciativa
    val tablePrefix    = "default.result_"        // Prefijo para tablas de resultados


    // 0) Generar datos de prueba y crear tablas Hive
    val (refDF, newDF) = createTestDataFrames(spark, dataDatePart, geoPart)
    createAndLoadSourceTables(spark, refDF, newDF)

    // 1) Crear y cargar tablas de origen (particionadas por data_date_part y geo)
    createAndLoadSourceTables(spark, refDF, newDF)

    println(s"✅ Tablas ref_customers y new_customers (particiones: data_date_part, geo) creadas")
    println(s"✅ Iniciativa: $initiativeName")
    println(s"✅ PartitionSpec usado: ${partitionSpec.getOrElse("-")}")
    println(s"✅ (data_date_part esperado en resultados): $dataDatePart")

    // 2) Nombres de tablas de salida
    val diffTableName       = s"${tablePrefix}differences"
    val summaryTableName    = s"${tablePrefix}summary"
    val duplicatesTableName = s"${tablePrefix}duplicates"

    // 3) Eliminar y recrear tablas de resultados (si procede)
    cleanAndPrepareTables(spark, diffTableName, summaryTableName, duplicatesTableName)

    // 4)Construir configuración
    val config = CompareConfig(
      spark           = spark,
      refTable        = "default.ref_customers",
      newTable        = "default.new_customers",
      partitionSpec   = Some("""data_date_part="2025-07-01"/geo="ES""""),
      compositeKeyCols= Seq("id","country"),
      ignoreCols      = Seq("last_update"),
      initiativeName  = "Swift",
      tablePrefix     = "default.result_",
      checkDuplicates = true,
      includeEqualsInDiff = false,
      autoCreateTables = true,
      exportExcelPath     = Some("./output/summary.xlsx")

    )

    // 5) Ejecutar comparación
    TableComparisonController.run(config)

    // 6) Mostrar resultados (data_date_part = dataDatePart)
    showComparisonResults(spark, tablePrefix, initiativeName, dataDatePart)


    spark.stop()
  }

  private def createTestDataFrames(
      spark: SparkSession,
      dataDatePart: String,
      geoPart: String
  ) = {
    // Esquema de prueba
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("country", StringType, nullable = true),
      StructField("amount", DoubleType, nullable = true),
      StructField("status", StringType, nullable = true)
    ))

    val ref= Seq(
      Row(1, "US", 100.50, "active"),
      Row(2, "ES", 75.20, "pending"),
      Row(3, "MX", 150.00, "active"),
      Row(4, "FR", 200.00, "new"),
      Row(4, "BR", 201.00, "new"),
      Row(5, "FR", 300.00, "active"),
      Row(5, "FR", 300.50, "active"),
      Row(7, "PT", 300.50, "active"),
      Row(8, "BR", 100.50, "pending"),
      Row(9, "AN", 80.00, "new"),
      Row(10, "GR", 60.00, "new"),
      Row(null, "GR", 61.00, "new"),
      Row(null, "GR", 60.00, "new"),
    )

    val nw = Seq(
      Row(1, "US", 100.49, "active"),
      Row(2, "ES", 75.20, "expired"),
      Row(4, "BR", 201.00, "new"),
      Row(4, "BR", 200.00, "new"),
      Row(4, "BR", 200.00, "new"),
      Row(4, "BR", 200.00, "new"),
      Row(6, "DE", 400.00, "new"),
      Row(6, "DE", 400.00, "new"),
      Row(6, "DE", 400.10, "new"),
      Row(7, "",   300.50, "active"),
      Row(8, "BR", null,   "pending"),
      Row(9, "AN", 80.00,  null),
      Row(null, "GR", 60.00, "new"),
      Row(null, "GR", 60.00, "new"),
      Row(null, "GR", 60.00, "new"),
      Row(null, "GR", 61.00, "new")
    )

    val refDF = spark.createDataFrame(spark.sparkContext.parallelize(ref), schema)
      .withColumn("data_date_part", lit(dataDatePart))
      .withColumn("geo", lit(geoPart))
    val newDF = spark.createDataFrame(spark.sparkContext.parallelize(nw), schema)
      .withColumn("data_date_part", lit(dataDatePart))
      .withColumn("geo", lit(geoPart))
    (refDF, newDF)
  }

  private def createAndLoadSourceTables(spark: SparkSession, refDF: org.apache.spark.sql.DataFrame, newDF: org.apache.spark.sql.DataFrame): Unit = {
    spark.sql("DROP TABLE IF EXISTS default.ref_customers")
    spark.sql(
      """CREATE TABLE default.ref_customers (
        | id INT, country STRING, amount DOUBLE, status STRING
        |) PARTITIONED BY (data_date_part STRING, geo STRING) STORED AS PARQUET""".stripMargin)
    refDF.write.mode("overwrite").insertInto("default.ref_customers")

    spark.sql("DROP TABLE IF EXISTS default.new_customers")
    spark.sql(
      """CREATE TABLE default.new_customers (
        | id INT, country STRING, amount DOUBLE, status STRING
        |) PARTITIONED BY (data_date_part STRING, geo STRING) STORED AS PARQUET""".stripMargin)
    newDF.write.mode("overwrite").insertInto("default.new_customers")
  }

      /**
   * Elimina tablas de salida y sus ubicaciones físicas (si existen).
   */
  private def cleanAndPrepareTables(
      spark: SparkSession,
      tableNames: String*
  ): Unit = {
    tableNames.foreach { fullTableName =>
      try {
        val parts = fullTableName.split('.')
        val (db, table) = if (parts.length > 1) (parts(0), parts(1)) else ("default", parts(0))
        val tableIdentifier = TableIdentifier(table, Some(db))

        // 1) Borrar tabla si existe
        spark.sql(s"DROP TABLE IF EXISTS $fullTableName PURGE")

        // 2) Borrar ubicación física si existe
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val catalog = spark.sessionState.catalog

        val tableLocation =
          if (catalog.tableExists(tableIdentifier)) catalog.getTableMetadata(tableIdentifier).location
          else catalog.defaultTablePath(tableIdentifier)

        val path = new Path(tableLocation.toString)
        if (fs.exists(path)) {
          fs.delete(path, true)
          println(s"Eliminada ubicación física: $path")
        }
      } catch {
        case e: Exception =>
          println(s"Advertencia: Error al limpiar tabla $fullTableName - ${e.getMessage}")
      }
    }
  }

    /** Muestra resultados filtrando por initiative y data_date_part (que será dataDatePart). */
  private def showComparisonResults(
      spark: SparkSession,
      tablePrefix: String,
      initiativeName: String,
      executionDate: String
  ): Unit = {
    val diffTable       = s"${tablePrefix}differences"
    val summaryTable    = s"${tablePrefix}summary"
    val duplicatesTable = s"${tablePrefix}duplicates"

    def queryWithPartition(table: String) =
      s"SELECT * FROM $table WHERE initiative = '$initiativeName' AND data_date_part = '$executionDate'"

    println("\n==== Tablas Hive disponibles ====")
    spark.sql("SHOW TABLES").show(false)

    println(s"\n==== Diferencias ($diffTable) ====")
    spark.sql(queryWithPartition(diffTable)).show(100, false)

    println(s"\n==== Resumen comparativo ($summaryTable) ====")
    spark.sql(queryWithPartition(summaryTable)).show(100, false)

    println(s"\n==== Duplicados ($duplicatesTable) ====")
    spark.sql(queryWithPartition(duplicatesTable)).show(100, false)
  }
}
