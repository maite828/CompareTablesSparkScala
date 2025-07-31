import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._

object DiffGenerator {

  def generateDifferencesTable(
    spark: SparkSession,
    refDf: DataFrame,
    newDf: DataFrame,
    compositeKeyCols: Seq[String],
    compareColsIn: Seq[String],
    partitionHour: String,
    includeEquals: Boolean
  ): DataFrame = {

    import spark.implicits._

    // 1) Asegurar que no comparamos las claves
    val compareCols: Seq[String] = compareColsIn.filterNot(compositeKeyCols.contains)

    // 2) Construir agregaciones por clave para evitar duplicados (tomamos un representante con first)
    def aggPerKey(df: DataFrame, alias: String): DataFrame = {
      val aggs: Seq[Column] = compareCols.map(c => first(col(c), ignoreNulls = true).as(c))
      df.groupBy(compositeKeyCols.map(col): _*).agg(aggs.head, aggs.tail: _*).as(alias)
    }

    val refAgg = aggPerKey(refDf, "ref")
    val newAgg = aggPerKey(newDf, "new")

    // 3) Full outer join por clave
    val joined = refAgg.join(newAgg, compositeKeyCols, "fullouter")

    // 4) Para cada columna comparada, armamos un struct con el resultado
    val diffsPerCol: Seq[Column] = compareCols.map { c =>
      val refCol = col(s"ref.$c")
      val newCol = col(s"new.$c")
      val result = when(refCol.isNull && newCol.isNotNull, lit("ONLY_IN_NEW"))
        .when(newCol.isNull && refCol.isNotNull, lit("ONLY_IN_REF"))
        .when(refCol =!= newCol, lit("NO_MATCH"))
        .otherwise(lit("MATCH"))

      // id de salida = coalesce de las claves (como string)
      val idOut: Column =
        if (compositeKeyCols.length == 1)
          coalesce(col(s"ref.${compositeKeyCols.head}"), col(s"new.${compositeKeyCols.head}")).cast("string").as("id")
        else {
          // Si la clave es compuesta, concatenamos sus valores con '|'
          val refKey = concat_ws("|", compositeKeyCols.map(k => col(s"ref.$k").cast("string")): _*)
          val newKey = concat_ws("|", compositeKeyCols.map(k => col(s"new.$k").cast("string")): _*)
          coalesce(refKey, newKey).as("id")
        }

      struct(
        idOut,
        lit(c).as("Column"),
        coalesce(refCol.cast("string"), lit("-")).as("value_ref"),
        coalesce(newCol.cast("string"), lit("-")).as("value_new"),
        result.as("Results")
      )
    }

    val exploded = joined
      .select(array(diffsPerCol: _*).as("diffs"))
      .withColumn("d", explode(col("diffs")))
      .select("d.*")
      .withColumn("partition_hour", lit(partitionHour))

    if (includeEquals) {
      // Modo extendido: devolvemos todo (MATCH, NO_MATCH, ONLY_IN_*)
      exploded
    } else {
      // Modo compacto:
      // - Mantener NO_MATCH
      val noMatch = exploded.filter($"Results" === "NO_MATCH")

      // - Para ONLY_IN_REF / ONLY_IN_NEW: dejar una sola fila por id usando la "primera" columna de compare
      val pickCol = compareCols.headOption.getOrElse(compareColsIn.headOption.getOrElse(""))

      val onlyInRef = exploded
        .filter($"Results" === "ONLY_IN_REF" && $"Column" === lit(pickCol))
      val onlyInNew = exploded
        .filter($"Results" === "ONLY_IN_NEW" && $"Column" === lit(pickCol))

      noMatch.unionByName(onlyInRef).unionByName(onlyInNew)
    }
  }
}
