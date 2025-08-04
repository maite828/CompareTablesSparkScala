import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDate
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.TableIdentifier

object TableComparisonController {

  /**
   * Ejecuta la comparación de tablas.
   *
   * @param partitionSpec  cadena con uno o varios pares clave=valor (cualquier orden),
   *                       p.ej: -data_date_part="2025-07-01"/geo="ES"/process_group="pseudo"
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

    // 0) Derivar executionDate
    val executionDate: String = extractExecutionDate(partitionSpec)
      .getOrElse {
        throw new IllegalArgumentException(
          "No se pudo inferir executionDate: falta data_date_part=\"YYYY-MM-DD\" " +
          "o data_timestamp_part=\"YYYYMMDD...\" en partitionSpec."
        )
      }

    // 1) Nombres de tablas de salida
    val diffTableName       = s"${tablePrefix}differences"
    val summaryTableName    = s"${tablePrefix}summary"
    val duplicatesTableName = s"${tablePrefix}duplicates"

    // 2) Eliminar y recrear tablas de resultados (si procede)
    if (autoCreateTables) {
      cleanAndPrepareTables(spark, diffTableName, summaryTableName, duplicatesTableName)
    }

    // 3) Cargar datos aplicando TODAS las claves de la partición (si existen en la tabla)
    val refDf = loadDataWithPartition(spark, refTable, partitionSpec)
    val newDf = loadDataWithPartition(spark, newTable, partitionSpec)

    // 4) Generar tabla de diferencias
    val colsToCompare = refDf.columns.filterNot(ignoreCols.contains)
    val diffDf = DiffGenerator.generateDifferencesTable(
      spark, refDf, newDf, compositeKeyCols, colsToCompare, includeEqualsInDiff
    )

    writeResultTable(
      spark, diffTableName, diffDf,
      Seq("id", "column", "value_ref", "value_new", "results"),
      initiativeName, executionDate
    )

    // 5) Duplicados internos
    val dupDf =
      if (checkDuplicates) {
        val d = DuplicateDetector.detectDuplicatesTable(
          spark, refDf, newDf, compositeKeyCols
        )
        writeResultTable(
          spark, duplicatesTableName, d,
          Seq("origin", "id", "exact_duplicates", "duplicates_w_variations", "occurrences", "variations"),
          initiativeName, executionDate
        )
      } else spark.emptyDataFrame

    // 6) Resumen (usa diffDf y, si procede, los duplicados)
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
  // Helpers
  // -------------------------------------------------------------------

  /**
   * Extrae executionDate en formato "YYYY-MM-DD" desde partitionSpec.
   * - Prioriza data_date_part="YYYY-MM-DD".
   * - Si no está, intenta data_timestamp_part="YYYYMMDD..." → formatea YYYY-MM-DD con los 8 primeros dígitos.
   * - Soporta separadores '/', comillas, guiones iniciales, y también formato "[col=val]".
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
   *   -data_date_part="2025-07-01"/geo="ES"/process_group="pseudo"
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
   * (solo para columnas existentes en la tabla, el resto se ignora con aviso).
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

        // Aplicar únicamente filtros para columnas existentes
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

  /**
   * Escribe una tabla de resultados, añadiendo particiones (initiative, execution_date).
   */
  private def writeResultTable(
      spark: SparkSession,
      tableName: String,
      df: DataFrame,
      columns: Seq[String],              // columnas núcleo a conservar (se ignora si df ya las trae)
      initiativeName: String,
      executionDate: String
  ): DataFrame = {
    val resultDf = df
      .withColumn("initiative", lit(initiativeName))
      .withColumn("execution_date", lit(executionDate))

    resultDf.write
      .partitionBy("initiative", "execution_date")
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName)

    resultDf
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
}
