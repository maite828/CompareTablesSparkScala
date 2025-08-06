// src/main/scala/com/example/compare/TableComparisonController.scala

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

    // Extraer executionDate de partitionSpec
    val executionDate = partitionSpec.flatMap { spec =>
      """(?i)data_date_part\s*=\s*"?([0-9]{4}-[0-9]{2}-[0-9]{2})"?""".r.findFirstMatchIn(spec).map(_.group(1))
    }.getOrElse {
      throw new IllegalArgumentException("Falta data_date_part en partitionSpec")
    }

     // Nombres de tablas de salida
    val diffTable       = s"$tablePrefix" + "differences"
    val summaryTable    = s"$tablePrefix" + "summary"
    val duplicatesTable = s"$tablePrefix" + "duplicates"

    // 0) Crear tablas si es necesario
    if (autoCreateTables) 
      ensureResultTables(session, diffTable, summaryTable, duplicatesTable)

    // 1) Cargar ref y new aplicando filtros de partición
    val refDf = loadWithPartition(session, refTable, partitionSpec)
    val newDf = loadWithPartition(session, newTable, partitionSpec)

    // 2) Columnas a comparar (excluir compositeKeyCols, ignoreCols, partición)
    val partitionKeys = partitionSpec.map(_.split("/").map(_.split("=")(0).trim).toSet).getOrElse(Set.empty)
    val colsToCompare = refDf.columns.toSeq
      .filterNot(ignoreCols.contains)
      .filterNot(partitionKeys.contains)
      .filterNot(compositeKeyCols.contains)

    // 3) Diferencias
    val diffDf = DiffGenerator.generateDifferencesTable(
      session, refDf, newDf, compositeKeyCols, colsToCompare, includeEqualsInDiff, config
    )
    writeResult(session, diffTable, diffDf, Seq("id","column","value_ref","value_new","results"), initiativeName, executionDate)

    // 4) Duplicados
    if (checkDuplicates) {
      val dups = DuplicateDetector.detectDuplicatesTable(session, refDf, newDf, compositeKeyCols, config)
      writeResult(session, duplicatesTable, dups, Seq("origin","id","exact_duplicates","duplicates_w_variations","occurrences","variations"), initiativeName, executionDate)
    }

    // 5) Resumen
    val dupDf = if (checkDuplicates) session.table(duplicatesTable) else session.emptyDataFrame
    val summaryDf = SummaryGenerator.generateSummaryTable(
      session, refDf, newDf, diffDf, dupDf, compositeKeyCols, refDf, newDf, config
    )
    writeResult(session, summaryTable, summaryDf, Seq("bloque","metrica","universo","numerador","denominador","pct","ejemplos"), initiativeName, executionDate)

    // 6) Export opcional a Excel en S3
    config.exportExcelPath.foreach { path =>
      SummaryGenerator.exportToExcel(summaryDf, path)
    }

  }

  // -- Helpers para DDL y particiones (sin cambios) --
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

  private def loadWithPartition(
      spark: SparkSession,
      tableName: String,
      partitionSpec: Option[String]
  ): DataFrame = {
    val base = spark.table(tableName)
    partitionSpec match {
      case Some(spec) =>
        spec.split("/").foldLeft(base) { (df, kv) =>
          val Array(k, vRaw) = kv.split("=",2)
          val v = vRaw.replaceAll("\"","")
          if (df.columns.contains(k)) df.filter(col(k) === lit(v)) else df
        }
      case None => throw new IllegalArgumentException("Se requiere partitionSpec")
    }
  }

  private def writeResult(
      spark: SparkSession,
      tableName: String,
      df: DataFrame,
      columns: Seq[String],
      initiative: String,
      executionDate: String
  ): Unit = {
    df.withColumn("initiative", lit(initiative))
      .withColumn("data_date_part", lit(executionDate))
      .repartition(col("initiative"), col("data_date_part"))
      .write.mode(SaveMode.Overwrite)
      .insertInto(tableName)
  }
}
