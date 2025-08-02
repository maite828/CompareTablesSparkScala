import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDate
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.TableIdentifier

object TableComparisonController {

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
    executionDate: String = LocalDate.now().toString,
    autoCreateTables: Boolean = true
  ): Unit = {

    // Nombres de tablas
    val diffTableName = s"${tablePrefix}differences"
    val summaryTableName = s"${tablePrefix}summary"
    val duplicatesTableName = s"${tablePrefix}duplicates"

    // Eliminar y recrear tablas de resultados
    cleanAndPrepareTables(spark, diffTableName, summaryTableName, duplicatesTableName)

    // Cargar datos con partición
    val refDf = loadDataWithPartition(spark, refTable, partitionSpec)
    val newDf = loadDataWithPartition(spark, newTable, partitionSpec)

    // Generar tabla de diferencias
    val diffDf = DiffGenerator.generateDifferencesTable(
      spark, refDf, newDf, compositeKeyCols, 
      refDf.columns.filterNot(ignoreCols.contains), 
      executionDate, includeEqualsInDiff
    )
    
    // Escribir tabla de diferencias con columnas adicionales
    writeResultTable(
      spark, diffTableName, diffDf,
      Seq("id", "column", "value_ref", "value_new", "results"),
      initiativeName, executionDate
    )

    // Procesar duplicados si está habilitado
    if (checkDuplicates) {
      val dupDf = DuplicateDetector.detectDuplicatesTable(
        spark, refDf, newDf, compositeKeyCols, executionDate
      )
      
      writeResultTable(
        spark, duplicatesTableName, dupDf,
        Seq("origin", "id", "exact_duplicates", "duplicates_w_variations", "occurrences", "variations"),
        initiativeName, executionDate
      )
    }

    // Generar tabla de resumen
    val summaryDf = SummaryGenerator.generateSummaryTable(
      spark, refDf, newDf, diffDf, 
      if (checkDuplicates) spark.table(duplicatesTableName) else spark.emptyDataFrame,
      compositeKeyCols, executionDate, refDf, newDf
    )
    
    writeResultTable(
      spark, summaryTableName, summaryDf,
      Seq("metrica", "total_ref", "total_new", "pct_ref", "status", "examples", "table"),
      initiativeName, executionDate
    )
  }

  private def cleanAndPrepareTables(
      spark: SparkSession,
      tableNames: String*
  ): Unit = {
    tableNames.foreach { fullTableName =>
      try {
        val parts = fullTableName.split('.')
        val (db, table) = if (parts.length > 1) (parts(0), parts(1)) else ("default", parts(0))
        val tableIdentifier = TableIdentifier(table, Some(db))
        
        // 1. Eliminar la tabla si existe
        spark.sql(s"DROP TABLE IF EXISTS $fullTableName PURGE")
        
        // 2. Eliminar la ubicación física si existe
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val catalog = spark.sessionState.catalog
        
        val tableLocation = if (catalog.tableExists(tableIdentifier)) {
          catalog.getTableMetadata(tableIdentifier).location
        } else {
          catalog.defaultTablePath(tableIdentifier)
        }
        
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

  private def writeResultTable(
      spark: SparkSession,
      tableName: String,
      df: DataFrame,
      columns: Seq[String],
      initiativeName: String,
      executionDate: String
  ): DataFrame = {
    // Añadir columnas de particionamiento al DataFrame
    val resultDf = df
      .withColumn("initiative", lit(initiativeName))
      .withColumn("execution_date", lit(executionDate))

    // Escribir la tabla
    resultDf.write
      .partitionBy("initiative", "execution_date")
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName)
    
    resultDf
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