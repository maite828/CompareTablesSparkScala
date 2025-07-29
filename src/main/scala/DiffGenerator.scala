import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DiffGenerator {

  def generateDifferencesTable(
    spark: SparkSession,
    refDf: DataFrame,
    newDf: DataFrame,
    compositeKeyCols: Seq[String],
    compareCols: Seq[String],
    partitionHour: String,
    includeEquals: Boolean
  ): DataFrame = {

    import spark.implicits._

    val joined = refDf.as("ref").join(newDf.as("new"), compositeKeyCols, "fullouter")

    val diffsExtended = compareCols.map { colName =>
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

    val diffRaw = joined
      .select(array(diffsExtended: _*).as("diferencias"))
      .withColumn("diferencia", explode(col("diferencias")))
      .select("diferencia.*")
      .withColumn("partition_hour", lit(partitionHour))

    val diffDf = if (includeEquals) diffRaw else diffRaw.filter($"Resultado" =!= "COINCIDE")

    diffDf
  }
}
