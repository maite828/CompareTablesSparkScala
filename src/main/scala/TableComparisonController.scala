import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.functions._

object TableComparisonController {

  /**
   * Ejecuta la comparación de tablas.
   *
   * Requisitos cubiertos:
   *  - Crea tablas de resultados si no existen (sin borrar histórico).
   *  - Overwrite dinámico: sólo sustituye la partición (initiative, data_date_part)
   *    de la ejecución actual; el resto del histórico se mantiene.
   *  - Aplica todos los filtros presentes en partitionSpec que existan en la tabla.
   *
   * @param partitionSpec  Cadena con pares clave=valor (cualquier orden),
   *                       p.ej: data_date_part="2025-07-01"/geo="ES"/...
   */
  def run(
      spark: SparkSession,
      refTable: String,
      newTable: String,
      partitionSpec: Option[String],
      compositeKeyCols: Seq[String],
      ignoreCols: Seq[String],
      initiativeName: String,
      tablePrefix: String = "default.result_",
      checkDuplicates: Boolean = true,
      includeEqualsInDiff: Boolean = true,
      autoCreateTables: Boolean = true
  ): Unit = {

    // === Configuración segura para overwrite por partición ===
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    // 0) Derivar executionDate desde partitionSpec
    val executionDate: String = extractExecutionDate(partitionSpec).getOrElse {
      throw new IllegalArgumentException(
        "No se pudo inferir executionDate: falta data_date_part=\"YYYY-MM-DD\" " +
        "o data_timestamp_part=\"YYYYMMDD...\" en partitionSpec."
      )
    }

    // 1) Nombres de tablas de salida
    val diffTableName       = s"${tablePrefix}differences"
    val summaryTableName    = s"${tablePrefix}summary"
    val duplicatesTableName = s"${tablePrefix}duplicates"

    // 2) Crear tablas de resultados si no existen (NO borra histórico)
    if (autoCreateTables) {
      ensureResultTables(spark, diffTableName, summaryTableName, duplicatesTableName)
    }

    // 3) Cargar datos aplicando TODAS las claves del partitionSpec que existan en la tabla
    val refDf = loadDataWithPartition(spark, refTable, partitionSpec)
    val newDf = loadDataWithPartition(spark, newTable, partitionSpec)

    // 4) Diferencias
    val colsToCompare = refDf.columns.filterNot(ignoreCols.contains)
    val diffDf = DiffGenerator.generateDifferencesTable(
      spark, refDf, newDf, compositeKeyCols, colsToCompare, includeEqualsInDiff
    )

    writeResultTable(
      spark, diffTableName, diffDf,
      Seq("id", "column", "value_ref", "value_new", "results"),
      initiativeName, executionDate
    )

    // 5) Duplicados
    if (checkDuplicates) {
      val dups = DuplicateDetector.detectDuplicatesTable(
        spark, refDf, newDf, compositeKeyCols
      )
      writeResultTable(
        spark, duplicatesTableName, dups,
        Seq("origin", "id", "exact_duplicates", "duplicates_w_variations", "occurrences", "variations"),
        initiativeName, executionDate
      )
    }

    // 6) Resumen
    val summaryDf = SummaryGenerator.generateSummaryTable(
      spark, refDf, newDf, diffDf,
      if (checkDuplicates) spark.table(duplicatesTableName) else spark.emptyDataFrame,
      compositeKeyCols, refDf, newDf
    )

    writeResultTable(
      spark, summaryTableName, summaryDf,
      Seq("metrica", "total_ref", "total_new", "pct_ref", "status", "examples", "table"),
      initiativeName, executionDate
    )
  }

  // -------------------------------------------------------------------
  // Crea tablas de resultados si no existen (sin borrar nada)
  // -------------------------------------------------------------------
  private def ensureResultTables(
      spark: SparkSession,
      diffTableName: String,
      summaryTableName: String,
      duplicatesTableName: String
  ): Unit = {

    // Backticks para nombres potencialmente reservados (`column`, `table`)
    val diffDDL =
      s"""
         |CREATE TABLE IF NOT EXISTS $diffTableName (
         |  id STRING,
         |  `column` STRING,
         |  value_ref STRING,
         |  value_new STRING,
         |  results STRING
         |)
         |PARTITIONED BY (initiative STRING, data_date_part STRING)
         |STORED AS PARQUET
       """.stripMargin

    val summaryDDL =
      s"""
         |CREATE TABLE IF NOT EXISTS $summaryTableName (
         |  metrica   STRING,
         |  total_ref STRING,
         |  total_new STRING,
         |  pct_ref   STRING,
         |  status    STRING,
         |  examples  STRING,
         |  `table`   STRING
         |)
         |PARTITIONED BY (initiative STRING, data_date_part STRING)
         |STORED AS PARQUET
       """.stripMargin

    val duplicatesDDL =
      s"""
         |CREATE TABLE IF NOT EXISTS $duplicatesTableName (
         |  origin STRING,
         |  id STRING,
         |  exact_duplicates STRING,
         |  duplicates_w_variations STRING,
         |  occurrences STRING,
         |  variations STRING
         |)
         |PARTITIONED BY (initiative STRING, data_date_part STRING)
         |STORED AS PARQUET
       """.stripMargin

    spark.sql(diffDDL)
    spark.sql(summaryDDL)
    spark.sql(duplicatesDDL)
  }

  // -------------------------------------------------------------------
  // ESCRITURA: overwrite dinámico SOLO de la partición (initiative, data_date_part)
  // -------------------------------------------------------------------
  private def writeResultTable(
      spark: SparkSession,
      tableName: String,
      df: DataFrame,
      columns: Seq[String],
      initiativeName: String,
      executionDate: String
  ): DataFrame = {

    val resultDf = df
      .withColumn("initiative", lit(initiativeName))
      .withColumn("data_date_part", lit(executionDate))

    // Overwrite dinámico: sustituye sólo la partición escrita
    resultDf
      .repartition(col("initiative"), col("data_date_part"))
      .write
      .mode(SaveMode.Overwrite)   // con partitionOverwriteMode=dynamic
      .insertInto(tableName)      // requiere que la tabla exista (ensureResultTables)

    resultDf
  }

  // -------------------------------------------------------------------
  // Helpers de partición
  // -------------------------------------------------------------------

  /**
   * Extrae executionDate en formato "YYYY-MM-DD" desde partitionSpec.
   * - Prioriza data_date_part="YYYY-MM-DD".
   * - Si no está, intenta data_timestamp_part="YYYYMMDD..." → YYYY-MM-DD.
   * - Soporta separadores '/', comillas, y formato "[col=val]".
   */
  private def extractExecutionDate(partitionSpecOpt: Option[String]): Option[String] = {
    partitionSpecOpt.flatMap { specRaw =>
      val spec = specRaw.trim

      // 1) data_date_part="YYYY-MM-DD"
      val dateR = """(?i)data_date_part\s*=\s*"?([0-9]{4}-[0-9]{2}-[0-9]{2})"?""".r
      dateR.findFirstMatchIn(spec).map(_.group(1)).orElse {
        // 2) data_timestamp_part="YYYYMMDD..." → YYYY-MM-DD
        val tsR = """(?i)data_timestamp_part\s*=\s*"?([0-9]{8,})"?""".r
        tsR.findFirstMatchIn(spec).map { m =>
          val yyyymmdd = m.group(1).take(8)
          s"${yyyymmdd.substring(0,4)}-${yyyymmdd.substring(4,6)}-${yyyymmdd.substring(6,8)}"
        }
      }
    }
  }

  /**
   * Parsea un partitionSpec con múltiples pares clave=valor y devuelve un Map.
   * Acepta comillas y/o valores sin comillas; separadores como '/', espacios, etc.
   * Ejemplos válidos:
   *   data_date_part="2025-07-01"/geo="ES"/process_group="pseudo"
   *   [partition_date=2025-07-25]
   */
  private def parseKeyValuePairs(specRaw: String): Map[String, String] = {
    val spec = specRaw.trim.stripPrefix("[").stripSuffix("]")

    // Regex: key = "value" | value_sin_comillas (hasta '/', ']', espacio)
    val kv = """([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?:"([^"]*)"|([^/\]\s]+))""".r

    kv.findAllMatchIn(spec).map { m =>
      val key = m.group(1)
      val v1  = m.group(2) // con comillas
      val v2  = m.group(3) // sin comillas
      val value = if (v1 != null) v1 else v2
      key -> value
    }.toMap
  }

  /**
   * Carga una tabla y aplica TODOS los filtros presentes en partitionSpec
   * (solo para columnas existentes en la tabla; el resto se ignora con aviso).
   */
  private def loadDataWithPartition(
      spark: SparkSession,
      tableName: String,
      partitionSpec: Option[String]
  ): DataFrame = {
    val base = spark.table(tableName)

    partitionSpec match {
      case Some(spec) =>
        val kvs = parseKeyValuePairs(spec)
        if (kvs.isEmpty) {
          throw new IllegalArgumentException(s"partitionSpec vacío o inválido: '$spec'")
        }

        val existing = base.columns.toSet
        val (applied, skipped) = kvs.partition { case (k, _) => existing.contains(k) }

        val filtered = applied.foldLeft(base) {
          case (df, (k, v)) => df.filter(col(k) === lit(v))
        }

        if (skipped.nonEmpty) {
          val msg = skipped.keys.mkString(",")
          println(s"[AVISO] Claves de partición no presentes en '$tableName' ignoradas: $msg")
        }
        filtered

      case None =>
        throw new IllegalArgumentException("Se requiere especificación de partición")
    }
  }
}
