import org.apache.spark.sql.{DataFrame, SparkSession, Row, Encoder, Encoders}
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

object CompareTablesEnhancedStrict {

  def run(
    spark: SparkSession,
    refTable: String,
    newTable: String,
    partitionSpec: Option[String],
    compositeKeyCols: Seq[String],
    ignoreCols: Seq[String],
    reportTable: String,
    diffTable: String,
    duplicatesTable: String,
    checkDuplicates: Boolean,
    includeEqualsInDiff: Boolean,
    partitionHour: String
  ): Unit = {

    import spark.implicits._

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.insertInto.partitionOverwriteMode", "dynamic")

    def applyPartition(df: DataFrame): DataFrame = {
      partitionSpec match {
        case Some(spec) =>
          val (partitionCol, partitionVal) = spec.stripPrefix("[").stripSuffix("]").split("=") match {
            case Array(col, value) => col.trim -> value.trim
            case _ => throw new IllegalArgumentException(s"❌ Error en formato de partición: '$spec'")
          }
          if (!df.columns.contains(partitionCol)) {
            throw new IllegalArgumentException(s"❌ La columna '$partitionCol' no existe en la tabla para aplicar la partición.")
          }
          df.filter(col(partitionCol) === lit(partitionVal))

        case None =>
          throw new IllegalArgumentException("❌ Se requiere una partición explícita para procesar los datos.")
      }
    }

    val refDfBase = applyPartition(spark.table(refTable))
    val newDfBase = applyPartition(spark.table(newTable))

    val allCols = refDfBase.columns.filterNot(ignoreCols.contains).filterNot(_ == "partition_date")

    val refDf = refDfBase.select((compositeKeyCols ++ allCols).distinct.map(col): _*)
    val newDf = newDfBase.select((compositeKeyCols ++ allCols).distinct.map(col): _*)

    val refKeys = refDf.select(compositeKeyCols.map(col): _*).distinct().withColumn("ref", lit(1))
    val newKeys = newDf.select(compositeKeyCols.map(col): _*).distinct().withColumn("new", lit(1))

    val summaryJoin = refKeys.join(newKeys, compositeKeyCols, "fullouter")
      .withColumn("Resultado",
        when(col("ref").isNotNull && col("new").isNotNull, "EN_AMBAS")
          .when(col("ref").isNotNull, "ONLY_IN_REF")
          .otherwise("ONLY_IN_NEW")
      )

    val resumen = summaryJoin.groupBy("Resultado")
      .agg(
        count("*").as("count"),
        format_number(count("*") * 100.0 / refKeys.count(), 2).as("% Ref")
      )
      .withColumn("partition_hour", lit(partitionHour))

    val joined = refDf.as("ref").join(newDf.as("new"), compositeKeyCols, "fullouter")

    val diffsExtended = allCols.map { colName =>
      struct(
        compositeKeyCols.map(c => coalesce(col(s"ref.$c"), col(s"new.$c")).as(c)) ++ Seq(
          lit(colName).as("Columna"),
          col(s"ref.$colName").cast("string").as("Valor_ref"),
          col(s"new.$colName").cast("string").as("Valor_new"),
          when(col(s"ref.$colName").isNull && col(s"new.$colName").isNotNull, "ONLY_IN_NEW")
            .when(col(s"new.$colName").isNull && col(s"ref.$colName").isNotNull, "ONLY_IN_REF")
            .when(col(s"ref.$colName") =!= col(s"new.$colName"), "DIFERENCIA")
            .otherwise("COINCIDE").as("Resultado")
        ): _*
      )
    }

    val diffRaw = joined.select(array(diffsExtended: _*).as("diferencias"))
      .withColumn("diferencia", explode(col("diferencias")))
      .select("diferencia.*")
      .withColumn("partition_hour", lit(partitionHour))

    val diffDf = if (includeEqualsInDiff) diffRaw else diffRaw.filter($"Resultado" =!= "COINCIDE")

    val withSourceRef = refDf.withColumn("_source", lit("ref"))
    val withSourceNew = newDf.withColumn("_source", lit("new"))

    val unioned = withSourceRef.unionByName(withSourceNew)

    val duplicates = if (checkDuplicates) {
     val grouped = unioned
  	.groupBy((Seq(col("_source")) ++ compositeKeyCols.map(col)): _*)
  	.agg(
    	  collect_list(struct(unioned.columns.filterNot(_ == "_source").map(col): _*)).as("records")
  	)
  	.filter(size(col("records")) > 1)
 

    val duplicatesRows = grouped.flatMap { row =>
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
} else {
  spark.emptyDataFrame
}

    diffDf.write.mode("overwrite").insertInto(diffTable)
    resumen.write.mode("overwrite").insertInto(reportTable)
    if (checkDuplicates && !duplicates.isEmpty) {
      duplicates.write.mode("overwrite").insertInto(duplicatesTable)
    }
  }
}   
