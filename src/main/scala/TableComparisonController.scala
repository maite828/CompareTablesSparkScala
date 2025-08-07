// src/main/scala/com/example/compare/TableComparisonController.scala

import java.time.LocalDate
import scala.util.matching.Regex

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TableComparisonController {

  def run(config: CompareConfig): Unit = {
    import config._
    val session = spark

    // Overwrite dinámico de particiones
    session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    session.conf.set("hive.exec.dynamic.partition", "true")
    session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    // ── 1) Extraer columna y valor de partición (o fallback a hoy / data_date_part) ──
    val isoRegex: Regex =
      """([A-Za-z0-9_]+)\s*=\s*"([0-9]{4}-[0-9]{2}-[0-9]{2})"""".r
    val tripleRegex: Regex =
      """([A-Za-z0-9_]+)\s*=\s*"([0-9]{2})"\s*/\s*([A-Za-z0-9_]+)\s*=\s*"([0-9]{2})"\s*/\s*([A-Za-z0-9_]+)\s*=\s*"([0-9]{4})"""".r

    val (partCol, executionDate) = partitionSpec
      .flatMap { spec =>
        isoRegex.findFirstMatchIn(spec).map(m => (m.group(1), m.group(2)))
          .orElse {
            tripleRegex.findFirstMatchIn(spec).map { m =>
              val key1 = m.group(1)
              val dd   = m.group(2)
              val mm   = m.group(4)
              val yyyy = m.group(6)
              (key1, s"$yyyy-$mm-$dd")
            }
          }
      }
      .getOrElse(("data_date_part", LocalDate.now().toString))

    // ── 2) Nombres de tablas de salida ──
    val diffTable       = s"$tablePrefix" + "differences"
    val summaryTable    = s"$tablePrefix" + "summary"
    val duplicatesTable = s"$tablePrefix" + "duplicates"

    // ── 3) Crear tablas de resultados si falta ──
    if (autoCreateTables) {
      ensureResultTables(session, diffTable, summaryTable, duplicatesTable)
    }

    // ── 4) Cargar DataFrames (filtrado o completo) ──
    val refDf = loadWithPartition(session, refTable, partitionSpec)
    val newDf = loadWithPartition(session, newTable, partitionSpec)

    // ── 5) Determinar columnas a comparar ──
    val partitionKeys = partitionSpec
      .map(_.split("/").map(_.split("=")(0).trim).toSet)
      .getOrElse(Set.empty)
    val colsToCompare = refDf.columns.toSeq
      .filterNot(ignoreCols.contains)
      .filterNot(partitionKeys.contains)
      .filterNot(compositeKeyCols.contains)

    // ── 6) Generar y escribir diferencias ──
    val diffDf = DiffGenerator.generateDifferencesTable(
      session, refDf, newDf, compositeKeyCols, colsToCompare, includeEqualsInDiff, config
    )
    writeResult(
      session, diffTable, diffDf,
      Seq("id","column","value_ref","value_new","results"),
      initiativeName, partCol, executionDate
    )

    // ── 7) Generar y escribir duplicados ──
    if (checkDuplicates) {
      val dups = DuplicateDetector.detectDuplicatesTable(
        session, refDf, newDf, compositeKeyCols, config
      )
      writeResult(
        session, duplicatesTable, dups,
        Seq("origin","id","exact_duplicates","duplicates_w_variations","occurrences","variations"),
        initiativeName, partCol, executionDate
      )
    }

    // ── 8) Generar y escribir resumen ──
    val dupDf    = if (checkDuplicates) session.table(duplicatesTable) else session.emptyDataFrame
    val summaryDf = SummaryGenerator.generateSummaryTable(
      session, refDf, newDf, diffDf, dupDf,
      compositeKeyCols, refDf, newDf, config
    )
    writeResult(
      session, summaryTable, summaryDf,
      Seq("bloque","metrica","universo","numerador","denominador","pct","ejemplos"),
      initiativeName, partCol, executionDate
    )

    // ── 9) Export opcional a Excel ──
    config.exportExcelPath.foreach { path =>
      SummaryGenerator.exportToExcel(summaryDf, path)
    }
  }

  // -------------------------------------------------------------------
  // Crea tablas de resultados si no existen (sin alterar histórico)
  // -------------------------------------------------------------------
  private def ensureResultTables(
      spark: SparkSession,
      diffTable: String,
      summaryTable: String,
      duplicatesTable: String
  ): Unit = {
    val diffDDL =
      s"""
         |CREATE TABLE IF NOT EXISTS $diffTable (
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
         |CREATE TABLE IF NOT EXISTS $summaryTable (
         |  bloque STRING,
         |  metrica STRING,
         |  universo STRING,
         |  numerador STRING,
         |  denominador STRING,
         |  pct STRING,
         |  ejemplos STRING
         |)
         |PARTITIONED BY (initiative STRING, data_date_part STRING)
         |STORED AS PARQUET
       """.stripMargin

    val duplicatesDDL =
      s"""
         |CREATE TABLE IF NOT EXISTS $duplicatesTable (
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
  // Carga tabla entera o filtrada según partitionSpec
  // -------------------------------------------------------------------
  private def loadWithPartition(
      spark: SparkSession,
      tableName: String,
      partitionSpec: Option[String]
  ): DataFrame = {
    val base = spark.table(tableName)
    partitionSpec match {
      case Some(spec) =>
        spec.split("/").foldLeft(base) { (df, kv) =>
          val Array(k, vRaw) = kv.split("=", 2)
          val v = vRaw.replaceAll("\"", "")
          if (df.columns.contains(k)) df.filter(col(k) === lit(v)) else df
        }
      case None =>
        base
    }
  }

  // -------------------------------------------------------------------
  // Escritura de resultados con overwrite dinámico de la partición indicada
  // -------------------------------------------------------------------
  private def writeResult(
      spark: SparkSession,
      tableName: String,
      df: DataFrame,
      columns: Seq[String],
      initiative: String,
      partitionColumn: String,
      executionDate: String
  ): Unit = {
    df
      .withColumn("initiative", lit(initiative))
      .withColumn(partitionColumn, lit(executionDate))
      .repartition(col("initiative"), col(partitionColumn))
      .write
      .mode(SaveMode.Overwrite)
      .insertInto(tableName)
  }
}
