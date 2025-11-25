import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import java.math.BigDecimal
import java.io.File
import java.time.LocalDate
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.TableIdentifier

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq


object MainAirflow extends Logging {

  private val mapper = new ObjectMapper()

  // ─────────────────────────── log + println (double logging) ───────────────────────────
  private def info(msg: String): Unit = { log.info(msg); println(msg) }
  private def warn(msg: String): Unit = { log.warn(msg); println(msg) }
  private def err(msg: String): Unit = { log.error(msg); println(msg) }

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

  private def optText(node: JsonNode, field: String): Option[String] = {
    val n = node.get(field)
    if (n == null || n.isNull || n.asText().trim.isEmpty) None
    else Some(n.asText())
  }

  private def mapOfStrings(node: JsonNode, field: String): Map[String, String] = {
    val n = node.get(field)
    if (n == null || n.isNull) Map.empty[String, String]
    else {
      n.fields().asScala.map { entry =>
        entry.getKey -> entry.getValue.asText()
      }.toMap
    }
  }

  // ─────────────────────────── Ventanas de fechas (refWindowDays/newWindowDays) ───────────────────────────
  private val WindowRx = """^\s*([+-]?\d+)\s*\.\.\s*([+-]?\d+)\s*$""".r

  private def parseWindow(s: String): Option[(Int, Int)] = s match {
    case null => None
    case WindowRx(a, b) =>
      try Some(a.toInt -> b.toInt) catch { case _: Throwable => None }
    case _ => None
  }

  private def expandDates(centerISO: String, start: Int, end: Int): Seq[String] = {
    val base = LocalDate.parse(centerISO)
    (start to end).map(delta => base.plusDays(delta.toLong).toString)
  }

  /** Inserta o sustituye data_date_part con lista [d1,d2,...] en una spec existente. */
  private def upsertDates(specOpt: Option[String], dates: Seq[String]): Option[String] = {
    if (dates == null || dates.isEmpty) specOpt
    else {
      val list = s"[${dates.mkString(",")}]"
      specOpt match {
        case None => Some(s"data_date_part=$list")
        case Some(specRaw) =>
          val spec = specRaw.trim
          if (spec.isEmpty) Some(s"data_date_part=$list")
          else {
            val rx = "data_date_part\\s*=\\s*[^/]+"
            if (spec.matches(s".*${rx}.*"))
              Some(spec.replaceAll(rx, s"data_date_part=$list"))
            else
              Some(if (spec.endsWith("/")) s"${spec}data_date_part=$list" else s"$spec/data_date_part=$list")
          }
      }
    }
  }

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

  // ---------------------------- helpers de debug ----------------------------
  private def showCreateAndCounts(
      spark: SparkSession,
      table: String,
      parts: Map[String,String]
  ): Unit = {
    println(s"\n[DEBUG] SHOW CREATE TABLE $table")
    try spark.sql(s"SHOW CREATE TABLE $table").show(200, false)
    catch { case e: Throwable => println(s"  (no existe aún) -> ${e.getMessage}") }

    println(s"[DEBUG] COUNT(*) en $table")
    try spark.sql(s"SELECT COUNT(*) AS cnt FROM $table").show(false)
    catch { case e: Throwable => println(s"  (no existe) -> ${e.getMessage}") }

    if (parts.nonEmpty) {
      val where = parts.map{ case (k,v) => s"$k='${v.replace("'", "\\'")}'" }.mkString(" AND ")
      println(s"[DEBUG] COUNT(*) en $table WHERE $where")
      try spark.sql(s"SELECT COUNT(*) AS cnt FROM $table WHERE $where").show(false)
      catch { case e: Throwable => println(s"  (no existe/partición) -> ${e.getMessage}") }

      println(s"[DEBUG] 5 filas de $table WHERE $where")
      try spark.sql(s"SELECT * FROM $table WHERE $where LIMIT 5").show(false)
      catch { case _: Throwable => () }
    }
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

    println(s"Arguments received: ${args.zipWithIndex.map { case (arg, i) => s"  Arg[$i]: $arg" }.mkString("\n")}")

    // Determinar formato de entrada: JSON vs argumentos individuales
    val inboundJson: String = if (args != null && args.length >= 1 && args(0) != null && args(0).trim.startsWith("{")) {
      // Formato JSON
      args(0).trim
    } else if (args != null && args.length > 1) {
      // Formato de argumentos individuales (producción)
      // Convertir args a JSON
      val argsMap = args.map { arg =>
        val parts = arg.split("=", 2)
        if (parts.length == 2) parts(0) -> parts(1) else null
      }.filter(_ != null).toMap

      info(s"[Main] Parsed arguments map: $argsMap")

      // Convertir a JSON
      val compositeKeysJson = argsMap.getOrElse("compositeKeyCols", "").split(",").filter(_.nonEmpty).map(k => s""""$k"""").mkString("[", ",", "]")
      val ignoreColsJson = argsMap.getOrElse("ignoreCols", "").split(",").filter(_.nonEmpty).map(k => s""""$k"""").mkString("[", ",", "]")

      s"""{
         |  "parameters": {
         |    "refTable": "${argsMap.getOrElse("refTable", "")}",
         |    "newTable": "${argsMap.getOrElse("newTable", "")}",
         |    "partitionSpec": "${argsMap.getOrElse("partitionSpec", "")}",
         |    "compositeKeyCols": $compositeKeysJson,
         |    "ignoreCols": $ignoreColsJson,
         |    "initiativeName": "${argsMap.getOrElse("initiativeName", "")}",
         |    "tablePrefix": "${argsMap.getOrElse("tablePrefix", "")}",
         |    "outputBucket": "${argsMap.getOrElse("outputBucket", "")}",
         |    "checkDuplicates": ${argsMap.getOrElse("checkDuplicates", "false")},
         |    "includeEqualsInDiff": ${argsMap.getOrElse("includeEqualsInDiff", "false")},
         |    "autoCreateTables": ${argsMap.getOrElse("autoCreateTables", "true")},
         |    "exportExcelPath": null,
         |    "priorityCol": null,
         |    "aggOverrides": {},
         |    "nullKeyMatches": true,
         |    "executionDate": "${argsMap.getOrElse("executionDate", "")}",
         |    "refWindowDays": "${argsMap.getOrElse("refWindowDays", "")}",
         |    "newWindowDays": "${argsMap.getOrElse("newWindowDays", "")}",
         |    "refPartitionSpec": "${argsMap.getOrElse("refPartitionSpec", "")}",
         |    "newPartitionSpec": "${argsMap.getOrElse("newPartitionSpec", "")}",
         |    "refPartitionSpecOverride": null,
         |    "newPartitionSpecOverride": null
         |  }
         |}""".stripMargin
    } else {
      // Datos hardcode en modo local
      val jexecutionDateISO = "2025-07-01"
      val localOutputBucket = new File("spark-warehouse/local-output").toURI.toString.stripSuffix("/")
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
         |    "outputBucket": "$localOutputBucket",
         |    "checkDuplicates": true,
         |    "includeEqualsInDiff": false,
         |    "autoCreateTables": true,
         |    "exportExcelPath": null,
         |    "priorityCol": null,
         |    "aggOverrides": {},
         |    "nullKeyMatches": true,
         |    "executionDate": "$jexecutionDateISO",
         |    "refPartitionSpecOverride": null,
         |    "newPartitionSpecOverride": null
         |  }
         |}
         |""".stripMargin.trim
    }

    println(s"[MainOld] JSON recibido/generado:\n$inboundJson")

    // ------------------ parseo y normalización con PartitionFormatTool ------------------
    val root    = mapper.readTree(inboundJson)
    val params0 = Option(root.get("parameters"))
      .getOrElse(throw new IllegalArgumentException("""Missing "parameters" object"""))

    val executionDateISO = Option(params0.get("executionDate"))
      .map(_.asText()).filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException("""Missing "executionDate" in parameters"""))

    val defaultOutputBucket = new File("spark-warehouse/local-output").toURI.toString.stripSuffix("/")
    val outputBucket = Option(params0.get("outputBucket")).map(_.asText()).filter(_.nonEmpty).getOrElse(defaultOutputBucket)

    // Reescribe tokens/placeholder de fecha y construye partitionSpec final si aplica
    val (params: JsonNode, partitionSpecOpt: Option[String]) =
      PartitionFormatTool.normalizeParameters(params0, executionDateISO)
    println(s"[DEBUG] partitionSpec (normalized): ${partitionSpecOpt.getOrElse("<none>")}")

    // Campos normalizados
    val refTable                  = reqText(params, "refTable")
    val newTable                  = reqText(params, "newTable")
    val initiativeName            = reqText(params, "initiativeName")
    val tablePrefixRaw            = reqText(params, "tablePrefix")
    val tablePrefix               = if (tablePrefixRaw.endsWith("_")) tablePrefixRaw else s"${tablePrefixRaw}_"
    val compositeKeyCols          = arrOfStrings(params, "compositeKeyCols")
    val ignoreCols                = arrOfStrings(params, "ignoreCols")
    val checkDuplicates           = bool(params, "checkDuplicates", false)
    val includeEquals             = bool(params, "includeEqualsInDiff", false)
    val autoCreateTables          = bool(params, "autoCreateTables", true)
    val exportExcelPath           = optText(params, "exportExcelPath")
    val priorityCol               = optText(params, "priorityCol")
    val aggOverrides              = mapOfStrings(params, "aggOverrides")
    val nullKeyMatches            = bool(params, "nullKeyMatches", true)
    val refPartitionSpecOverride  = optText(params, "refPartitionSpecOverride")
    val newPartitionSpecOverride  = optText(params, "newPartitionSpecOverride")
    val refFilter                 = optText(params, "refFilter")
    val newFilter                 = optText(params, "newFilter")

    // ------------------ Ventanas de fechas (refWindowDays/newWindowDays) ------------------
    val refWindowDaysStr = optText(params, "refWindowDays")
    val newWindowDaysStr = optText(params, "newWindowDays")
    val refPartitionSpecStr = optText(params, "refPartitionSpec")
    val newPartitionSpecStr = optText(params, "newPartitionSpec")

    // Fecha base para expandir ventanas (extraída del partitionSpec)
    val outputDateISO = PartitionFormatTool.extractDateFromPartitionSpec(partitionSpecOpt)
    info(s"[DEBUG] outputDateISO (para data_date_part): $outputDateISO")

    // Calcular overrides finales con precedencia: direct > window > base partitionSpec
    // 1) Overrides directos (refPartitionSpec/newPartitionSpec)
    val refSpecDirect = refPartitionSpecStr.flatMap(s =>
      PartitionFormatTool.normalizeKvPartitionSpec(Option(s), executionDateISO))
    val newSpecDirect = newPartitionSpecStr.flatMap(s =>
      PartitionFormatTool.normalizeKvPartitionSpec(Option(s), executionDateISO))

    // 2) Ventanas compactas por lado: "refWindowDays=-2..+3", "newWindowDays=0..+1"
    val refSpecWindow = refWindowDaysStr.flatMap(parseWindow).flatMap {
      case (start, end) =>
        val dates = expandDates(outputDateISO, start, end)
        upsertDates(partitionSpecOpt, dates)
    }

    val newSpecWindow = newWindowDaysStr.flatMap(parseWindow).flatMap {
      case (start, end) =>
        val dates = expandDates(outputDateISO, start, end)
        upsertDates(partitionSpecOpt, dates)
    }

    // Precedencia: direct override > window > parámetro básico refPartitionSpecOverride
    val finalRefOverride = refSpecDirect.orElse(refSpecWindow).orElse(refPartitionSpecOverride)
    val finalNewOverride = newSpecDirect.orElse(newSpecWindow).orElse(newPartitionSpecOverride)

    if (finalRefOverride.isDefined) {
      info(s"[DEBUG] refPartitionSpecOverride final: ${finalRefOverride.get}")
    }
    if (finalNewOverride.isDefined) {
      info(s"[DEBUG] newPartitionSpecOverride final: ${finalNewOverride.get}")
    }

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

    // 4) Crear configuración y ejecutar comparación
    val cfg = CompareConfig(
      spark                        = spark,
      refTable                     = refTable,
      newTable                     = newTable,
      partitionSpec                = partitionSpecOpt,
      compositeKeyCols             = compositeKeyCols,
      ignoreCols                   = ignoreCols,
      initiativeName               = initiativeName,
      tablePrefix                  = tablePrefix,
      outputBucket                 = outputBucket,
      checkDuplicates              = checkDuplicates,
      includeEqualsInDiff          = includeEquals,
      autoCreateTables             = autoCreateTables,
      exportExcelPath              = exportExcelPath,
      priorityCols                 = priorityCol.map(Seq(_)).getOrElse(Seq.empty),
      aggOverrides                 = aggOverrides,
      nullKeyMatches               = nullKeyMatches,
      outputDateISO                = outputDateISO,
      refPartitionSpecOverride     = finalRefOverride,
      newPartitionSpecOverride     = finalNewOverride,
      refFilter                    = refFilter,
      newFilter                    = newFilter
    )

    TableComparisonController.run(cfg)

    // 5) Mostrar resultados filtrados por iniciativa y fecha
    showComparisonResults(spark, cfg.tablePrefix, cfg.initiativeName, outputDateISO)

    info("[Driver] Comparison finished.")
    // spark.stop() // opcional
  }

  // ---------------------------- helpers de simulación ----------------------------

  /** DFs de demo (sin columnas de partición). */
  def createTestDataFrames(spark: SparkSession): (DataFrame, DataFrame) = {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("geo", StringType, nullable = true),
      StructField("amount", DecimalType(38, 18), nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("data_date_part", StringType, nullable = true)
    ))

    val ref = Seq(
      Row(1: java.lang.Integer, "US", new BigDecimal("100.40"), "active","2025-11-14"),
      Row(1: java.lang.Integer, "US", new BigDecimal("100.40"), "active","2025-11-14"),
      Row(2: java.lang.Integer, "ES ", new BigDecimal("1.000000000000000001"), "expired","2025-11-15"),
      Row(3: java.lang.Integer, "MX", new BigDecimal("150.00"), "active","2025-11-16"),
      Row(4: java.lang.Integer, "FR", new BigDecimal("200.00"), "new","2025-11-17"),
      Row(4: java.lang.Integer, "BR", new BigDecimal("201.00"), "new","2025-11-17"),
      Row(5: java.lang.Integer, "FR", new BigDecimal("300.00"), "active","2025-11-15"),
      Row(5: java.lang.Integer, "FR", new BigDecimal("300.50"), "active","2025-11-15"),
      Row(7: java.lang.Integer, "PT", new BigDecimal("300.50"), "active","2025-11-14"),
      Row(8: java.lang.Integer, "BR", new BigDecimal("100.50"), "pending","2025-11-18"),
      Row(10: java.lang.Integer, "GR", new BigDecimal("60.00"), "new","2025-11-17"),
      Row(null, "GR", new BigDecimal("61.00"), "new","2025-11-13"),
      Row(null, "GR", new BigDecimal("60.00"), "new","2025-11-13")
    )

    val nw = Seq(
      Row(1: java.lang.Integer, "US", new BigDecimal("100.40"), "active","2025-11-14"),
      Row(2: java.lang.Integer, "ES", new BigDecimal("1.000000000000000001"), "expired","2025-11-15"),
      Row(4: java.lang.Integer, "BR", new BigDecimal("201.00"), "new","2025-11-16"),
      Row(4: java.lang.Integer, "BR", new BigDecimal("200.00"), "new","2025-11-16"),
      Row(4: java.lang.Integer, "BR", new BigDecimal("200.00"), "new","2025-11-17"),
      Row(4: java.lang.Integer, "BR", new BigDecimal("200.00"), "new","2025-11-17"),
      Row(6: java.lang.Integer, "DE", new BigDecimal("400.00"), "new","2025-11-18"),
      Row(6: java.lang.Integer, "DE", new BigDecimal("400.00"), "new","2025-11-18"),
      Row(6: java.lang.Integer, "DE", new BigDecimal("400.10"), "new","2025-11-14"),
      Row(7: java.lang.Integer, "", new BigDecimal("300.50"), "active","2025-11-15"),
      Row(8: java.lang.Integer, "BR", null, "pending","2025-11-13"),
      Row(null, "GR", new BigDecimal("60.00"), "new","2025-11-18"),
      Row(null, "GR", new BigDecimal("60.00"), "new","2025-11-18"),
      Row(null, "GR", new BigDecimal("60.00"), "new","2025-11-18"),
      Row(null, "GR", new BigDecimal("61.00"), "new","2025-11-18")
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

    info("\n-- Available Hive tables --")
    spark.sql("SHOW TABLES").show(false)

    info(s"\n-- Differences (${prefix}differences) --")
    spark.sql(q(prefix + "differences")).show(100, false)

    info(s"\n-- Summary (${prefix}summary) --")
    spark.sql(q(prefix + "summary")).show(100, false)

    info(s"\n-- Duplicates (${prefix}duplicates) --")
    spark.sql(q(prefix + "duplicates")).show(100, false)
  }
}
