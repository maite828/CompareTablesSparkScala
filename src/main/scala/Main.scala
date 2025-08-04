import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    // Configuración mínima para entorno local y metastore
    System.setProperty("hadoop.home.dir", "/tmp/hadoop-dummy")
    System.setProperty("spark.hadoop.validateOutputSpecs", "false")
    System.setProperty("spark.sql.warehouse.dir", "/tmp/spark-warehouse")

    // (Opcional) Desactivar hooks de Hadoop si molestan en local
    try {
      val manager = org.apache.hadoop.util.ShutdownHookManager.get()
      val hooksField = manager.getClass.getDeclaredField("hooks")
      hooksField.setAccessible(true)
      hooksField.set(manager, new java.util.HashSet[Runnable]())
    } catch {
      case e: Exception =>
        println(s"Advertencia: No se pudieron deshabilitar los hooks de Hadoop: ${e.getMessage}")
    }

    val spark = SparkSession.builder()
      .appName("CompareTablesMain")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    try {
      import spark.implicits._

      // ==== NUEVOS PARÁMETROS DE PARTICIÓN ====
      val dataDatePart = "2025-07-01"
      val geoPart      = "ES"
      // Ejemplo de partitionSpec con múltiples claves (el parser del Controller soporta orden libre)
      val partitionSpec = Some(s"""data_date_part="$dataDatePart"/geo="$geoPart" """.trim)

      val initiativeName = "Swift"                  // Nombre de la iniciativa
      val tablePrefix    = "default.result_"        // Prefijo para tablas de resultados

      // 1) Esquema de datos
      val customSchema = StructType(Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("country", StringType, nullable = true),
        StructField("amount", DoubleType, nullable = true),
        StructField("status", StringType, nullable = true)
      ))

      // 2) Crear DataFrames de prueba (con columnas de partición nuevas)
      val (refDF, newDF) = createTestDataFrames(spark, customSchema, dataDatePart, geoPart)

      // 3) Configuración para particiones dinámicas
      spark.conf.set("hive.exec.dynamic.partition", "true")
      spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

      // 4) Crear y cargar tablas de origen (particionadas por data_date_part y geo)
      createAndLoadSourceTables(spark, refDF, newDF)

      println(s"✅ Tablas ref_customers y new_customers (particiones: data_date_part, geo) creadas")
      println(s"✅ Iniciativa: $initiativeName")
      println(s"✅ PartitionSpec usado: ${partitionSpec.getOrElse("-")}")
      println(s"✅ (execution_date esperado en resultados): $dataDatePart")

      // 5) Ejecutar comparación (el Controller extraerá execution_date = dataDatePart desde partitionSpec)
      TableComparisonController.run(
        spark = spark,
        refTable = "default.ref_customers",
        newTable = "default.new_customers",
        partitionSpec = partitionSpec,
        compositeKeyCols = Seq("id"),
        ignoreCols = Seq("last_update"),
        initiativeName = initiativeName,
        tablePrefix = tablePrefix,
        checkDuplicates = true,
        includeEqualsInDiff = true
        // executionDateOpt = None  // opcional; si no lo pasas, el Controller lo infiere del partitionSpec
      )

      // 6) Mostrar resultados (execution_date = dataDatePart)
      showComparisonResults(spark, tablePrefix, initiativeName, dataDatePart)

    } finally {
      spark.stop()
    }
  }

  /** Crea los DataFrames de prueba e incluye las columnas de partición (data_date_part, geo). */
  private def createTestDataFrames(
      spark: SparkSession,
      schema: StructType,
      dataDatePart: String,
      geoPart: String
  ): (DataFrame, DataFrame) = {
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
      Row(10, "GR", 60.00, "new"),
      Row(null, "GR", 61.00, "new")
    )

    val newData = Seq(
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
      Row(null, "GR", 60.00, "new"),
      Row(null, "GR", 60.00, "new"),
      Row(null, "GR", 60.00, "new")
    )

    val refDF = spark.createDataFrame(spark.sparkContext.parallelize(refData), schema)
      .withColumn("data_date_part", lit(dataDatePart))
      .withColumn("geo", lit(geoPart))

    val newDF = spark.createDataFrame(spark.sparkContext.parallelize(newData), schema)
      .withColumn("data_date_part", lit(dataDatePart))
      .withColumn("geo", lit(geoPart))

    (refDF, newDF)
  }

  /** Crea las tablas de origen con particiones (data_date_part, geo) y escribe los datos. */
  private def createAndLoadSourceTables(
      spark: SparkSession,
      refDF: DataFrame,
      newDF: DataFrame
  ): Unit = {
    // Tabla de referencia
    spark.sql("DROP TABLE IF EXISTS default.ref_customers")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS default.ref_customers (
        |  id INT,
        |  country STRING,
        |  amount DOUBLE,
        |  status STRING
        |)
        |PARTITIONED BY (data_date_part STRING, geo STRING)
        |STORED AS PARQUET
      """.stripMargin)
    refDF.write.mode("overwrite").insertInto("default.ref_customers")

    // Tabla nueva
    spark.sql("DROP TABLE IF EXISTS default.new_customers")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS default.new_customers (
        |  id INT,
        |  country STRING,
        |  amount DOUBLE,
        |  status STRING
        |)
        |PARTITIONED BY (data_date_part STRING, geo STRING)
        |STORED AS PARQUET
      """.stripMargin)
    newDF.write.mode("overwrite").insertInto("default.new_customers")
  }

  /** Muestra resultados filtrando por initiative y execution_date (que será dataDatePart). */
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
      s"SELECT * FROM $table WHERE initiative = '$initiativeName' AND execution_date = '$executionDate'"

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
