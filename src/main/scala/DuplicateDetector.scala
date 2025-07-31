import org.apache.spark.sql.{DataFrame, Row, SparkSession, Dataset, Encoders}
import org.apache.spark.sql.functions._

case class DuplicateOut(
  origin: String,
  id: String,
  exact_duplicates: String,          // total - numDistinct (exceso de filas idénticas)
  duplicates_w_variations: String,   // max(numDistinct - 1, 0) (exceso de versiones)
  occurrences: String,               // total
  variations: String,                // campo: [v1,v2,...] | ...
  partition_hour: String
)

object DuplicateDetector {

  def detectDuplicatesTable(
    spark: SparkSession,
    refDf: DataFrame,
    newDf: DataFrame,
    compositeKeyCols: Seq[String],
    partitionHour: String
  ): DataFrame = {

    import spark.implicits._

    // Añadimos marca de origen
    val withSourceRef = refDf.withColumn("_source", lit("ref"))
    val withSourceNew = newDf.withColumn("_source", lit("new"))
    val unioned = withSourceRef.unionByName(withSourceNew)

    // Agrupamos por origen + clave y acumulamos los registros del grupo
    val grouped = unioned
      .groupBy((Seq(col("_source")) ++ compositeKeyCols.map(col)): _*)
      .agg(
        collect_list(
          struct(unioned.columns.filterNot(_ == "_source").map(col): _*)
        ).as("records")
      )
      // Solo grupos con más de una fila
      .filter(size(col("records")) > 1)

    // Calculamos métricas por grupo y emitimos como Strings (DDL exige STRING)
    val ds: Dataset[DuplicateOut] = grouped.map { row =>
      val sourceTag = row.getAs[String]("_source")
      val origin = if (sourceTag == "ref") "Reference" else "New"

      val keyValues = compositeKeyCols.map(k => row.getAs[Any](k))
      val idStr = keyValues.headOption.map(_.toString).getOrElse("")

      val records = row.getAs[Seq[Row]]("records")
      val total = records.size

      // Distintos por igualdad de struct
      val distinctRecords = records.distinct
      val numDistinct = distinctRecords.size

      val exactDupExcess  = math.max(total - numDistinct, 0) // exceso de filas idénticas
      val variedDupExcess = math.max(numDistinct - 1, 0)     // exceso de versiones

      // Campos con variaciones (excluimos claves)
      val allFieldNames =
        if (records.nonEmpty) records.head.schema.fieldNames.toSeq else Seq.empty[String]
      val fieldsToCheck = allFieldNames.filterNot(f => compositeKeyCols.contains(f))

      val variationsParts = fieldsToCheck.flatMap { f =>
        val vals = distinctRecords.map(r => r.getAs[Any](f)).filter(_ != null).distinct
        if (vals.size > 1) Some(s"$f: [${vals.mkString(",")}]") else None
      }
      val variationsStr = if (variationsParts.nonEmpty) variationsParts.mkString(" | ") else "-"

      DuplicateOut(
        origin = origin,
        id = idStr,
        exact_duplicates = exactDupExcess.toString,
        duplicates_w_variations = variedDupExcess.toString,
        occurrences = total.toString,
        variations = variationsStr,
        partition_hour = partitionHour
      )
    }(Encoders.product[DuplicateOut])

    ds.toDF()
  }
}
