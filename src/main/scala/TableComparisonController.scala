import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDate

object TableComparisonController {

  def run(
    spark: SparkSession,
    refTable: String,
    newTable: String,
    partitionSpec: Option[String],
    compositeKeyCols: Seq[String],
    ignoreCols: Seq[String],
    initiativeName: String,  // Nombre de la iniciativa (ej: "Swift")
    tablePrefix: String = "default.result_",
    checkDuplicates: Boolean = true,
    includeEqualsInDiff: Boolean = true,
    executionDate: String = LocalDate.now().toString, // Fecha de ejecución (ej: "2025-08-02")
    autoCreateTables: Boolean = true
  ): Unit = {

    // Nombres de tablas con prefijo (sin "customer_" para simplificar)
    val diffTableName = s"${tablePrefix}differences"
    val summaryTableName = s"${tablePrefix}summary"
    val duplicatesTableName = s"${tablePrefix}duplicates"

    // Creación automática de tablas si está habilitado
    if (autoCreateTables) {
      createResultTables(spark, diffTableName, summaryTableName, duplicatesTableName)
    }

    // Cargar datos con partición
    val refDf = loadDataWithPartition(spark, refTable, partitionSpec)
    val newDf = loadDataWithPartition(spark, newTable, partitionSpec)

    // Generación de resultados con columnas adicionales para particionamiento
    val diffDf = DiffGenerator.generateDifferencesTable(
      spark, refDf, newDf, compositeKeyCols, 
      refDf.columns.filterNot(ignoreCols.contains), 
      executionDate, includeEqualsInDiff
    )
    .withColumn("initiative", lit(initiativeName))
    .withColumn("execution_date", lit(executionDate))

    // Escribir resultados con particionamiento compuesto
    diffDf.write
      .partitionBy("initiative", "execution_date")
      .mode("append")
      .saveAsTable(diffTableName)

    // Procesamiento de duplicados
    if (checkDuplicates) {
      val dupDf = DuplicateDetector.detectDuplicatesTable(
        spark, refDf, newDf, compositeKeyCols, executionDate
      )
      .withColumn("initiative", lit(initiativeName))
      .withColumn("execution_date", lit(executionDate))

      if (!dupDf.isEmpty) {
        dupDf.write
          .partitionBy("initiative", "execution_date")
          .mode("append")
          .saveAsTable(duplicatesTableName)
      }
    }

    // Generación de resumen
    val summaryDf = SummaryGenerator.generateSummaryTable(
      spark, refDf, newDf, diffDf, 
      if (checkDuplicates) spark.table(duplicatesTableName) else spark.emptyDataFrame,
      compositeKeyCols, executionDate, refDf, newDf
    )
    .withColumn("initiative", lit(initiativeName))
    .withColumn("execution_date", lit(executionDate))

    summaryDf.write
      .partitionBy("initiative", "execution_date")
      .mode("append")
      .saveAsTable(summaryTableName)
  }

  private def createResultTables(
      spark: SparkSession,
      diffTableName: String,
      summaryTableName: String,
      duplicatesTableName: String
  ): Unit = {
    // Tabla de diferencias
    spark.sql(s"DROP TABLE IF EXISTS $diffTableName")
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $diffTableName (
        id STRING,
        column STRING,
        value_ref STRING,
        value_new STRING,
        results STRING
      )
      PARTITIONED BY (initiative STRING, execution_date STRING)
      STORED AS PARQUET
    """)

    // Tabla de resumen
    spark.sql(s"DROP TABLE IF EXISTS $summaryTableName")
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $summaryTableName (
        metrica STRING,
        total_ref STRING,
        total_new STRING,
        pct_ref STRING,
        status STRING,
        examples STRING,
        table STRING
      )
      PARTITIONED BY (initiative STRING, execution_date STRING)
      STORED AS PARQUET
    """)

    // Tabla de duplicados
    spark.sql(s"DROP TABLE IF EXISTS $duplicatesTableName")
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $duplicatesTableName (
        origin STRING,
        id STRING,
        exact_duplicates STRING,
        duplicates_w_variations STRING,
        occurrences STRING,
        variations STRING
      )
      PARTITIONED BY (initiative STRING, execution_date STRING)
      STORED AS PARQUET
    """)
  }

  private def loadDataWithPartition(
      spark: SparkSession,
      tableName: String,
      partitionSpec: Option[String]
  ): DataFrame = {
    partitionSpec match {
      case Some(spec) =>
        val (partitionCol, partitionVal) = parsePartitionSpec(spec)
        spark.table(tableName).filter(col(partitionCol) === lit(partitionVal))
      case None =>
        throw new IllegalArgumentException("Se requiere especificación de partición")
    }
  }

  private def parsePartitionSpec(spec: String): (String, String) = {
    val cleanSpec = spec.stripPrefix("[").stripSuffix("]")
    cleanSpec.split("=") match {
      case Array(col, value) => (col.trim, value.trim)
      case _ => throw new IllegalArgumentException(s"Formato de partición inválido: '$spec'")
    }
  }
}