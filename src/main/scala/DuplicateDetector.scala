import org.apache.spark.sql.{DataFrame, Row, SparkSession, Dataset, Encoders}
import org.apache.spark.sql.functions._

case class DuplicateRow(
  origin: String,
  id: Int,
  exact_duplicates: Int,
  varied_duplicates: Int,
  total: Int,
  variations: String,
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

    val withSourceRef = refDf.withColumn("_source", lit("ref"))
    val withSourceNew = newDf.withColumn("_source", lit("new"))
    val unioned = withSourceRef.unionByName(withSourceNew)

    val grouped = unioned
      .groupBy((Seq(col("_source")) ++ compositeKeyCols.map(col)): _*)
      .agg(
        collect_list(struct(unioned.columns.filterNot(_ == "_source").map(col): _*)).as("records")
      )
      .filter(size(col("records")) > 1)

    val duplicatesRows: Dataset[DuplicateRow] = grouped.flatMap { row =>
      val origin = row.getAs[String]("_source")
      val keyValues = compositeKeyCols.map(k => row.getAs[Any](k))
      val records = row.getAs[Seq[Row]]("records")

      val exactos = records.groupBy(identity).count(_._2.size > 1)
      val distintos = records.distinct.size

      val variaciones: Seq[String] = if (distintos > 1) {
        records
          .flatMap(r => r.schema.fieldNames.map(f => f -> r.getAs[Any](f)))
          .groupBy(_._1)
          .mapValues(vals => vals.map(_._2).distinct.filter(_ != null))
          .filter { case (_, v) => v.size > 1 }
          .map { case (campo, valores) =>
            s"$campo: [${valores.mkString(",")}]"
          }.toSeq
      } else Seq.empty[String]

      Some(DuplicateRow(
        origin = if (origin == "ref") "Referencia" else "Nuevos",
        id = keyValues.headOption.map(_.toString.toInt).getOrElse(-1),
        exact_duplicates = exactos,
        varied_duplicates = distintos - exactos,
        total = records.size,
        variations = if (variaciones.nonEmpty) variaciones.mkString(" | ") else "-",
        partition_hour = partitionHour
      ))
    }(Encoders.product[DuplicateRow])

    duplicatesRows.toDF()
  }
}
