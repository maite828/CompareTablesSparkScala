import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Main {
  def main(args: Array[String]): Unit = {
    // Configuración para evitar errores de Hadoop
    System.setProperty("hadoop.home.dir", "/tmp/hadoop-dummy")
    System.setProperty("spark.hadoop.validateOutputSpecs", "false")
    System.setProperty("spark.sql.warehouse.dir", "/tmp/spark-warehouse")

    // Solución alternativa compatible para ShutdownHookManager
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

      val partitionDate = "2025-07-25"
      val executionDate = LocalDate.now().toString
      val initiativeName = "Swift"  // Nombre de la iniciativa
      val tablePrefix = "default.result_"  // Prefijo para las tablas de resultados

      // 1. Definir el esquema
      val customSchema = StructType(Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("country", StringType, nullable = true),
        StructField("amount", DoubleType, nullable = true),
        StructField("status", StringType, nullable = true)
      ))

      // 2. Crear DataFrames de prueba
      val (refDF, newDF) = createTestDataFrames(spark, customSchema, partitionDate)

      // 3. Configuración para particiones dinámicas
      spark.conf.set("hive.exec.dynamic.partition", "true")
      spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

      // 4. Crear y cargar tablas de origen
      createAndLoadSourceTables(spark, refDF, newDF, partitionDate)

      println(s"✅ Tablas ref_customers y new_customers con partición creadas")
      println(s"✅ Iniciativa: $initiativeName")
      println(s"✅ Fecha de ejecución: $executionDate")

      // 5. Ejecutar comparación
      TableComparisonController.run(
        spark = spark,
        refTable = "default.ref_customers",
        newTable = "default.new_customers",
        partitionSpec = Some(s"[partition_date=$partitionDate]"),
        compositeKeyCols = Seq("id"),
        ignoreCols = Seq("last_update"),
        initiativeName = initiativeName,
        tablePrefix = tablePrefix,
        executionDate = executionDate,
        checkDuplicates = true,
        includeEqualsInDiff = true
      )

      // 6. Mostrar resultados
      showComparisonResults(spark, tablePrefix, initiativeName, executionDate)

    } finally {
      spark.stop()
    }
  }

  private def createTestDataFrames(
      spark: SparkSession,
      schema: StructType,
      partitionDate: String
  ): (DataFrame, DataFrame) = {
    // Datos de referencia
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

    // Datos nuevos
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

    // Crear DataFrames con partición
    val refDF = spark.createDataFrame(spark.sparkContext.parallelize(refData), schema)
      .withColumn("partition_date", lit(partitionDate))
      
    val newDF = spark.createDataFrame(spark.sparkContext.parallelize(newData), schema)
      .withColumn("partition_date", lit(partitionDate))

    (refDF, newDF)
  }

  private def createAndLoadSourceTables(
      spark: SparkSession,
      refDF: DataFrame,
      newDF: DataFrame,
      partitionDate: String
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
        |PARTITIONED BY (partition_date STRING)
        |STORED AS PARQUET
      """.stripMargin)
    refDF.write.mode("overwrite").insertInto("default.ref_customers")

    // Tabla nueva
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
    newDF.write.mode("overwrite").insertInto("default.new_customers")
  }

  private def showComparisonResults(
      spark: SparkSession,
      tablePrefix: String,
      initiativeName: String,
      executionDate: String
  ): Unit = {
    // Nombres de las tablas de resultados
    val diffTable = s"${tablePrefix}differences"
    val summaryTable = s"${tablePrefix}summary"
    val duplicatesTable = s"${tablePrefix}duplicates"

    // Consulta con filtro por iniciativa y fecha
    def queryWithPartition(table: String) = 
      s"SELECT * FROM $table WHERE initiative = '$initiativeName' AND execution_date = '$executionDate'"

    println("\n==== Tablas Hive disponibles ====")
    spark.sql("SHOW TABLES").show(false)

    println(s"\n==== Diferencias ($diffTable) ====")
    spark.sql(queryWithPartition(diffTable)).show(20, false)

    println(s"\n==== Resumen comparativo ($summaryTable) ====")
    spark.sql(queryWithPartition(summaryTable)).show(false)

    println(s"\n==== Duplicados ($duplicatesTable) ====")
    spark.sql(queryWithPartition(duplicatesTable)).show(false)
  }
}