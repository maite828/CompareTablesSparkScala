import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import java.math.BigDecimal
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.TableIdentifier

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

object Main {

  private val mapper = new ObjectMapper()

  // ---------------------------- utils JSON ----------------------------
  private def reqText(node: JsonNode, field: String): String = {
    val n = node.get(field)
    if (n == null || n.isNull || n.asText().trim.isEmpty)
      throw new IllegalArgumentException(s"""Missing or empty required field: "$field" in parameters""")
    n.asText()
  }
  private def arrOfStrings(node: JsonNode, field: String): Seq[String] = {
    val n = node.get(field)
    if (n == null || n.isNull) Seq.empty[String]
    else n.elements().asScala.map(_.asText()).toList
  }
  private def bool(node: JsonNode, field: String, dflt: Boolean): Boolean =
    Option(node.get(field)).map(_.asBoolean(dflt)).getOrElse(dflt)

  /** Extrae pares key -> value de un partitionSpec normalizado (key="val"/k2="v2"). */
  private def parsePartitionsFromSpec(partitionSpecOpt: Option[String]): Map[String, String] = {
    partitionSpecOpt.map { spec =>
      spec.split("/").toSeq.flatMap { kv =>
        val p = kv.split("=", 2)
        if (p.length == 2) {
          val k = p(0).trim
          val v = p(1).trim.stripPrefix("\"").stripSuffix("\"")
          Some(k -> v)
        } else None
      }.toMap
    }.getOrElse(Map.empty)
  }

  // ---------------------------- main ----------------------------
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder()
        .appName("AML-Internal-Tools")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )

    // Si el JSON de Airflow está presente, lo usamos, sino usamos datos hardcode
    val inboundJson: String =
      if (args != null && args.length >= 1 && args(0) != null && args(0).trim.startsWith("{")) 
        args(0).trim
      else {
        // Datos hardcode en modo local
        val jexecutionDateISO = "2025-07-01"
        s"""
           |{
           |  "parameters": {
           |    "refTable": "default.ref_customers",
           |    "newTable": "default.new_customers",
           |    "partitionSpec": "geo=BJG/data_date_part=YYYY-MM-dd/",
           |    "compositeKeyCols": ["id"],
           |    "ignoreCols": ["last_update"],
           |    "initiativeName": "Swift",
           |    "tablePrefix": "default.result_",
           |    "checkDuplicates": true,
           |    "includeEqualsInDiff": false,
           |    "executionDate": "$jexecutionDateISO"
           |  }
           |}
           |""".stripMargin.trim
     }

    println(s"[Main] JSON recibido/embebido:\n$inboundJson")

    // ------------------ parseo y normalización con PartitionFormatTool ------------------
    val root    = mapper.readTree(inboundJson)
    val params0 = Option(root.get("parameters"))
      .getOrElse(throw new IllegalArgumentException("""Missing "parameters" object"""))

    val executionDateISO = Option(params0.get("executionDate"))
      .map(_.asText()).filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException("""Missing "executionDate" in parameters"""))

    // Reescribe tokens/placeholder de fecha y construye partitionSpec final si aplica
    val (params, partitionSpecOpt) = PartitionFormatTool.normalizeParameters(params0, executionDateISO)
    println(s"[DEBUG] partitionSpec (normalized): ${partitionSpecOpt.getOrElse("<none>")}")

    // Campos normalizados
    val refTable         = reqText(params, "refTable")
    val newTable         = reqText(params, "newTable")
    val initiativeName   = reqText(params, "initiativeName")
    val tablePrefixRaw   = reqText(params, "tablePrefix")
    val tablePrefix      = if (tablePrefixRaw.endsWith("_")) tablePrefixRaw else s"${tablePrefixRaw}_"
    val compositeKeyCols = arrOfStrings(params, "compositeKeyCols")
    val ignoreCols       = arrOfStrings(params, "ignoreCols")
    val checkDuplicates  = bool(params, "checkDuplicates", false)
    val includeEquals    = bool(params, "includeEqualsInDiff", false)

    // ------------------ preparar datos de prueba con las MISMAS particiones que vienen por JSON ------------------
    val partsMap: Map[String, String] = parsePartitionsFromSpec(partitionSpecOpt)
    println(s"[DEBUG] partitions map: $partsMap")

    // 1) Crear DFs base (SIN columnas de partición)
    val (refDFBase, newDFBase) = createTestDataFrames(spark)

    // 2) Crear tablas de origen con las particiones EXACTAS de partsMap y cargar datos
    createAndLoadSourceTables(spark, refDFBase, newDFBase, refTable, newTable, partsMap)

    // Verificación inmediata de fuentes
    showCreateAndCounts(spark, refTable, partsMap)
    showCreateAndCounts(spark, newTable, partsMap)

    // 3) Limpiar SOLO las tablas de salida (prefijo del JSON)
    val diffTableName       = s"${tablePrefix}differences"
    val summaryTableName    = s"${tablePrefix}summary"
    val duplicatesTableName = s"${tablePrefix}duplicates"
    cleanOnlyTables(spark, diffTableName, summaryTableName, duplicatesTableName)

    // 4) Mostrar resultados filtrados por iniciativa y fecha (extraída del spec)
    // (después de normalizar parámetros)
    val outputDateISO = PartitionFormatTool.extractDateFromPartitionSpec(partitionSpecOpt)
    println(s"[DEBUG] outputDateISO (para data_date_part): $outputDateISO")

    val cfg = CompareConfig(
      spark               = spark,
      refTable            = refTable,
      newTable            = newTable,
      partitionSpec       = partitionSpecOpt,
      compositeKeyCols    = compositeKeyCols,
      ignoreCols          = ignoreCols,
      initiativeName      = initiativeName,
      tablePrefix         = tablePrefix,
      checkDuplicates     = checkDuplicates,
      includeEqualsInDiff = includeEquals,

      // nuevos opcionales (ajusta si quieres):
      priorityCol         = None,
      aggOverrides        = Map.empty,
      nullKeyMatches      = true,

      // fecha que se escribirá en outputs
      outputDateISO       = outputDateISO
    )

    TableComparisonController.run(cfg)

    // (si quieres ver resultados por consola usando la misma fecha)
    showComparisonResults(spark, cfg.tablePrefix, cfg.initiativeName, outputDateISO)


    // 5) Mostrar resultados filtrados por iniciativa y fecha (extraída del spec)
    val execDateForFilter = PartitionFormatTool.extractDateFromPartitionSpec(partitionSpecOpt)
    println(s"[DEBUG] execDateForFilter: $execDateForFilter")

    println("[Driver] JSON-mode comparison finished.")
    // spark.stop() // opcional
  }

  // ---------------------------- helpers de simulación ----------------------------

  /** DFs de demo (sin columnas de partición). */
  def createTestDataFrames(spark: SparkSession): (DataFrame, DataFrame) = {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("country", StringType, nullable = true),
      StructField("amount", DecimalType(38, 18), nullable = true),
      StructField("status", StringType, nullable = true)
    ))

    val ref = Seq(
      Row(1: java.lang.Integer, "US", new BigDecimal("100.40"), "active"),
      Row(1: java.lang.Integer, "US", new BigDecimal("100.40"), "active"),
      Row(2: java.lang.Integer, "ES ", new BigDecimal("1.000000000000000001"), "expired"),
      Row(3: java.lang.Integer, "MX", new BigDecimal("150.00"), "active"),
      Row(4: java.lang.Integer, "FR", new BigDecimal("200.00"), "new"),
      Row(4: java.lang.Integer, "BR", new BigDecimal("201.00"), "new"),
      Row(5: java.lang.Integer, "FR", new BigDecimal("300.00"), "active"),
      Row(5: java.lang.Integer, "FR", new BigDecimal("300.50"), "active"),
      Row(7: java.lang.Integer, "PT", new BigDecimal("300.50"), "active"),
      Row(8: java.lang.Integer, "BR", new BigDecimal("100.50"), "pending"),
      Row(10: java.lang.Integer, "GR", new BigDecimal("60.00"), "new"),
      Row(null, "GR", new BigDecimal("61.00"), "new"),
      Row(null, "GR", new BigDecimal("60.00"), "new")
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
      Row(7: java.lang.Integer, "", new BigDecimal("300.50"), "active"),
      Row(8: java.lang.Integer, "BR", null, "pending"),
      Row(null, "GR", new BigDecimal("60.00"), "new"),
      Row(null, "GR", new BigDecimal("60.00"), "new"),
      Row(null, "GR", new BigDecimal("60.00"), "new"),
      Row(null, "GR", new BigDecimal("61.00"), "new")
    )

    val refDF = spark.createDataFrame(spark.sparkContext.parallelize(ref), schema)
    val newDF = spark.createDataFrame(spark.sparkContext.parallelize(nw), schema)
    (refDF, newDF)
  }

  /** Crea tablas particionadas con las claves de partsMap y carga datos en esa partición. */
  def createAndLoadSourceTables(
      spark: SparkSession,
      refDFBase: DataFrame,
      newDFBase: DataFrame,
      refTableName: String,
      newTableName: String,
      partsMap: Map[String, String]
  ): Unit = {
    val partKeys = partsMap.keys.toSeq

    def withParts(df: DataFrame): DataFrame =
      partKeys.foldLeft(df){ case (acc, k) => acc.withColumn(k, lit(partsMap(k))) }

    // Drop y (re)creación con saveAsTable + partitionBy para que el esquema de particiones coincida 1:1
    spark.sql(s"DROP TABLE IF EXISTS $refTableName")
    val refDF = withParts(refDFBase)
    if (partKeys.nonEmpty)
      refDF.write.mode("overwrite").partitionBy(partKeys:_*).format("parquet").saveAsTable(refTableName)
    else
      refDF.write.mode("overwrite").format("parquet").saveAsTable(refTableName)

    spark.sql(s"DROP TABLE IF EXISTS $newTableName")
    val newDF = withParts(newDFBase)
    if (partKeys.nonEmpty)
      newDF.write.mode("overwrite").partitionBy(partKeys:_*).format("parquet").saveAsTable(newTableName)
    else
      newDF.write.mode("overwrite").format("parquet").saveAsTable(newTableName)

    println(s"[DEBUG] Tablas cargadas: $refTableName / $newTableName con particiones $partsMap")
  }

  /** Limpia solo tablas de salida (differences/summary/duplicates). */
  def cleanOnlyTables(spark: SparkSession, tableNames: String*): Unit = {
    tableNames.foreach { fullTableName =>
      try {
        val parts = fullTableName.split('.')
        val (db, table) = if (parts.length > 1) (parts(0), parts(1)) else ("default", parts(0))
        val tabId = TableIdentifier(table, Some(db))

        spark.sql(s"DROP TABLE IF EXISTS $fullTableName PURGE")

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

  /** Muestra resultados filtrados por initiative y fecha (columna data_date_part en outputs). */
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

