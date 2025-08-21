/**
 * Main object for comparing two Spark tables with partitioning and decimal support using a single JSON argument.
 *
 * Flow:
 *  1) Build sample DataFrames (DecimalType) and create Hive tables partitioned by (date, geo).
 *  2) Clean result tables (differences/summary/duplicates).
 *  3) Build a JSON payload with "parameters" (including executionDate and partitions).
 *  4) Route that JSON to TableComparatorApp via AppSelection (same contract as Airflow).
 *  5) Print results filtered by initiative and execution date.
 *
 * Notes:
 *  - Single-entry JSON mode only (no legacy branches).
 *  - Uses DecimalType(38,18) for precise numeric comparisons.
 *  - Keeps your previous test data & schemas intact.
 */

// src/main/scala/Main.scala  (sin paquete para compilar fácil en tu proyecto)
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import java.math.BigDecimal
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.TableIdentifier

import scala.collection.JavaConverters.asScalaIteratorConverter


object Main {


  def main(args: Array[String]): Unit = {
    // 0) Spark con Hive habilitado
    val spark = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder()
        .appName("AML-Internal-Tools")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )


    // ---------- Configuración de la simulación ----------
    val jexecutionDateISO = "2025-07-01" // <- fecha de ejecución (equivale a dag_run.conf.args[0])
    val jgeoPart = "ES" // <- ejemplo de otra partición
    val jinitiativeName = "Swift"
    val jtablePrefix = "default.result_" // mantener "_" final para {differences,summary,duplicates}

    // 1) Generar DataFrames de prueba y crear tablas fuente
    val (refDF, newDF) = createTestDataFrames(spark, jexecutionDateISO, jgeoPart)
    createAndLoadSourceTables(spark, refDF, newDF)

    println(s"Ref/New tables created with partitions: (date, geo)")
    println(s"Initiative: $jinitiativeName")

    // 2) Limpiar tablas de resultados
    val diffTableName = s"${jtablePrefix}differences"
    val summaryTableName = s"${jtablePrefix}summary"
    val duplicatesTableName = s"${jtablePrefix}duplicates"
    cleanOnlyTables(spark, diffTableName, summaryTableName, duplicatesTableName)

    // 3) Construir el payload JSON de "parameters" (tal y como lo enviaría Airflow)
    //    - partitions: mapa agnóstico (aquí usamos date y geo)
    //    - executionDate: imprescindible para inyección agnóstica en el JAR
    val jsonPayload =
      s"""
         |{
         |  "parameters": {
         |    "refTable": "default.ref_customers",
         |    "newTable": "default.new_customers",
         |    "partitions": { "date": "$jexecutionDateISO", "geo": "$jgeoPart" },
         |    "compositeKeyCols": ["id"],
         |    "ignoreCols": ["last_update"],
         |    "initiativeName": "$jinitiativeName",
         |    "tablePrefix": "$jtablePrefix",
         |    "checkDuplicates": true,
         |    "includeEqualsInDiff": false,
         |    "executionDate": "$jexecutionDateISO"
         |  }
         |}
         |""".stripMargin

    // 4) Enviar el JSON a tu app (contrato de una sola entrada, como HybridOperator)
    val mapper = new ObjectMapper()

    /** Read required text field with explicit error if missing. */
    def reqText(node: JsonNode, field: String): String = {
      val n = node.get(field)
      if (n == null || n.isNull || n.asText().trim.isEmpty)
        throw new IllegalArgumentException(s"""Missing or empty required field: "$field" in parameters""")
      n.asText()
    }

    def arrOfStrings(node: JsonNode, field: String): Seq[String] = {
      val n = node.get(field)
      if (n == null || n.isNull) Seq.empty
      else n.elements().asScala.toSeq.map(_.asText()).toList
    }

    def bool(node: JsonNode, field: String, dflt: Boolean): Boolean =
      Option(node.get(field)).exists(_.asBoolean(dflt))


    // ---------- JSON mode (new, backward-compatible) ----------
    val root = mapper.readTree(jsonPayload.trim)// args[0] is the JSON payload
    val params0 = Option(root.get("parameters"))
      .getOrElse(throw new IllegalArgumentException("""Missing "parameters" object"""))

    // executionDate is required to perform agnostic replacement
    val executionDateISO = Option(params0.get("executionDate"))
      .map(_.asText()).filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException("""Missing "executionDate" in parameters"""))

    // Normalize parameters (deep date/token replacement) and compute final partitionSpec
    val (params, partitionSpecOpt) = PartitionFormatTool.normalizeParameters(params0, executionDateISO)

    // Read the rest of fields from normalized params
    val refTable = reqText(params, "refTable")
    val newTable = reqText(params, "newTable")
    val initiativeName = reqText(params, "initiativeName")
    val tablePrefixRaw = reqText(params, "tablePrefix")
    val tablePrefix = if (tablePrefixRaw.endsWith("_")) tablePrefixRaw else s"${tablePrefixRaw}_"
    val compositeKeyCols = arrOfStrings(params, "compositeKeyCols")
    val ignoreCols = arrOfStrings(params, "ignoreCols")
    val checkDuplicates = bool(params, "checkDuplicates", false)
    val includeEquals = bool(params, "includeEqualsInDiff", false)

    val cfg = CompareConfig(
      spark = spark,
      refTable = refTable,
      newTable = newTable,
      partitionSpec = partitionSpecOpt, // <- already normalized Option[String]
      compositeKeyCols = compositeKeyCols,
      ignoreCols = ignoreCols,
      initiativeName = initiativeName,
      tablePrefix = tablePrefix,
      checkDuplicates = checkDuplicates,
      includeEqualsInDiff = includeEquals
    )

    TableComparisonController.run(cfg)
    // Use normalized partitionSpec to derive date for result filtering
    val execDateForFilter = PartitionFormatTool.extractDateFromPartitionSpec(partitionSpecOpt)


    // 5) Mostrar resultados (TableComparatorApp ya los muestra, pero dejamos confirmación aquí)
    showComparisonResults(spark, cfg.tablePrefix, cfg.initiativeName, execDateForFilter)

    println("[Driver] JSON-mode comparison finished.")
    // spark.stop() // Descomenta si quieres cerrar Spark al terminar
  }

  // ---------- Helpers de la simulación (idénticos a los tuyos, con pequeños ajustes de limpieza) ----------
  def createTestDataFrames(
                            spark: SparkSession,
                            dataDatePart: String,
                            geoPart: String
                          ): (DataFrame, DataFrame) = {
    // Schema with DecimalType(38,18) for 'amount'
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("country", StringType, nullable = true),
      StructField("amount", DecimalType(38, 18), nullable = true),
      StructField("status", StringType, nullable = true)
    ))

    // Reference data (BigDecimal instead of Double)
    val ref = Seq(
      Row(1: java.lang.Integer, "US", new BigDecimal("100.40"), "active"),
      Row(1: java.lang.Integer, "US", new BigDecimal("100.40"), "active"),
      Row(2: java.lang.Integer, "ES ", new BigDecimal("1.000000000000000001"), "expired"),
      Row(3: java.lang.Integer, "MX", new BigDecimal("150.00"), "active"), // Not in new
      Row(4: java.lang.Integer, "FR", new BigDecimal("200.00"), "new"), // Repeated
      Row(4: java.lang.Integer, "BR", new BigDecimal("201.00"), "new"), // Repeated w/ different country
      Row(5: java.lang.Integer, "FR", new BigDecimal("300.00"), "active"), // Repeated w/ different amount and Not in new
      Row(5: java.lang.Integer, "FR", new BigDecimal("300.50"), "active"), // Repeated w/ different amount and Not in new
      Row(7: java.lang.Integer, "PT", new BigDecimal("300.50"), "active"),
      Row(8: java.lang.Integer, "BR", new BigDecimal("100.50"), "pending"),
      Row(10: java.lang.Integer, "GR", new BigDecimal("60.00"), "new"), // Not in new
      Row(null, "GR", new BigDecimal("61.00"), "new"),
      Row(null, "GR", new BigDecimal("60.00"), "new")
    )

    val nw = Seq(
      Row(1: java.lang.Integer, "US", new BigDecimal("100.40"), "active"),
      Row(2: java.lang.Integer, "ES", new BigDecimal("1.000000000000000001"), "expired"),
      Row(4: java.lang.Integer, "BR", new BigDecimal("201.00"), "new"), // Repeated w/ different amount
      Row(4: java.lang.Integer, "BR", new BigDecimal("200.00"), "new"), // Identical Repeated
      Row(4: java.lang.Integer, "BR", new BigDecimal("200.00"), "new"),
      Row(4: java.lang.Integer, "BR", new BigDecimal("200.00"), "new"),
      Row(6: java.lang.Integer, "DE", new BigDecimal("400.00"), "new"), // Repeated and Not in ref
      Row(6: java.lang.Integer, "DE", new BigDecimal("400.00"), "new"),
      Row(6: java.lang.Integer, "DE", new BigDecimal("400.10"), "new"), // Repeated and Not in ref w/ different amount
      Row(7: java.lang.Integer, "", new BigDecimal("300.50"), "active"),
      Row(8: java.lang.Integer, "BR", null, "pending"),
      Row(null, "GR", new BigDecimal("60.00"), "new"),
      Row(null, "GR", new BigDecimal("60.00"), "new"), // Identical Repeated
      Row(null, "GR", new BigDecimal("60.00"), "new"), // Identical Repeated
      Row(null, "GR", new BigDecimal("61.00"), "new") // Repeated w/ different amount
    )

    val refDF = spark.createDataFrame(
        spark.sparkContext.parallelize(ref),
        schema
      ).withColumn("date", lit(dataDatePart))
      .withColumn("geo", lit(geoPart))

    val newDF = spark.createDataFrame(
        spark.sparkContext.parallelize(nw),
        schema
      ).withColumn("date", lit(dataDatePart))
      .withColumn("geo", lit(geoPart))

    (refDF, newDF)
  }

  def createAndLoadSourceTables(
                                 spark: SparkSession,
                                 refDF: DataFrame,
                                 newDF: DataFrame
                               ): Unit = {
    // Reference table
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

    // New table
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

  def cleanOnlyTables(spark: SparkSession, tableNames: String*): Unit = {
    tableNames.foreach { fullTableName =>
      try {
        val parts = fullTableName.split('.')
        val (db, table) = if (parts.length > 1) (parts(0), parts(1)) else ("default", parts(0))
        val tabId = TableIdentifier(table, Some(db))

        // Drop table
        spark.sql(s"DROP TABLE IF EXISTS $fullTableName PURGE")

        // Remove data files
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val catalog = spark.sessionState.catalog
        val loc = if (catalog.tableExists(tabId))
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

  def showComparisonResults(
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

    println("\n-- Available Hive tables --")
    spark.sql("SHOW TABLES").show(false)

    println(s"\n-- Differences (${prefix}differences) --")
    spark.sql(q(prefix + "differences")).show(100, false)

    println(s"\n-- Summary (${prefix}summary) --")
    spark.sql(q(prefix + "summary")).show(100, false)

    println(s"\n-- Duplicates (${prefix}duplicates) --")
    spark.sql(q(prefix + "duplicates")).show(100, false)
  }


}
