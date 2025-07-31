import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Locale

object SummaryGenerator {

  private def has(df: DataFrame, c: String): Boolean = df.columns.contains(c)
  private def pick(df: DataFrame, en: String, es: String): String =
    if (has(df, en)) en else es

  def generateSummaryTable(
      spark: SparkSession,
      refDf: DataFrame,
      newDf: DataFrame,
      diffDf: DataFrame,
      dupDf: DataFrame,
      compositeKeyCols: Seq[String],
      partitionHour: String,
      refDfRaw: DataFrame,
      newDfRaw: DataFrame
  ): DataFrame = {

    import spark.implicits._

    val totalRef = refDfRaw.count()
    val totalNew = newDfRaw.count()

    // ---------------------------
    // NORMALIZACIÓN: DIFERENCIAS (diffDf)
    // Acepta español (Resultado, Valor_ref, Valor_new) o inglés (Results, Value_ref, Value_new)
    // ---------------------------
    val diffResultCol  = pick(diffDf, "Results", "Resultado")
    val diffIdCol      = if (has(diffDf, "id")) "id" else compositeKeyCols.head

    val diffNorm = diffDf
      .withColumnRenamed(diffResultCol, "result_norm")
      .withColumnRenamed(diffIdCol, "id_norm")
      .withColumn("result_norm_lc", lower(col("result_norm")))
      .withColumn("id_str", col("id_norm").cast("string"))

    // Map de estados (acepta ambos idiomas)
    val isDifference  = col("result_norm_lc").isin("difference", "diferencia", "no_matches")
    val isOnlyInRef   = col("result_norm_lc") === "only_in_ref"
    val isOnlyInNew   = col("result_norm_lc") === "only_in_new"

    val diffIds      = diffNorm.filter(isDifference).select($"id_str".as("id")).distinct()
    val onlyInRefIds = diffNorm.filter(isOnlyInRef).select($"id_str".as("id")).distinct()
    val onlyInNewIds = diffNorm.filter(isOnlyInNew).select($"id_str".as("id")).distinct()

    // ---------------------------
    // NORMALIZACIÓN: DUPLICADOS (dupDf)
    // Acepta español (varied_duplicates) o inglés (duplicates_w_variations)
    // Acepta origen "Referencia"/"Nuevos" o "Reference"/"New"
    // ---------------------------
    val varDupCol =
      if (has(dupDf, "duplicates_w_variations")) "duplicates_w_variations"
      else if (has(dupDf, "varied_duplicates")) "varied_duplicates"
      else "duplicates_w_variations"

    val dupNorm = dupDf
      .withColumn("origin_lc", lower(col("origin")))
      .withColumn("origin_norm",
        when(col("origin_lc").isin("reference", "referencia"), lit("reference"))
          .when(col("origin_lc").isin("new", "nuevos", "nuevo"), lit("new"))
          .otherwise(col("origin_lc"))
      )
      .withColumn("exact_dup_l", coalesce(col("exact_duplicates").cast("long"), lit(0L)))
      .withColumn("varied_dup_l", coalesce(col(varDupCol).cast("long"), lit(0L)))
      .withColumn("id_str", col("id").cast("string"))

    // Exact duplicates = IDs que tienen duplicados 100% iguales y SIN variaciones
    val exactDupIdsRef = dupNorm
      .filter($"origin_norm" === "reference" && $"exact_dup_l" > 0 && $"varied_dup_l" === 0)
      .select($"id_str".as("id")).distinct()

    val exactDupIdsNew = dupNorm
      .filter($"origin_norm" === "new" && $"exact_dup_l" > 0 && $"varied_dup_l" === 0)
      .select($"id_str".as("id")).distinct()

    // Duplicados con variaciones
    val variedDupIdsRef = dupNorm
      .filter($"origin_norm" === "reference" && $"varied_dup_l" > 0)
      .select($"id_str".as("id")).distinct()

    val variedDupIdsNew = dupNorm
      .filter($"origin_norm" === "new" && $"varied_dup_l" > 0)
      .select($"id_str".as("id")).distinct()

    // ---------------------------
    // MATCHES exactos por ID: intersección exacta de filas -> IDs distintos
    // ---------------------------
    val commonCols = refDf.columns.toSet.intersect(newDf.columns.toSet).toSeq
    val matchesDf  = refDf.select(commonCols.map(col): _*)
      .intersect(newDf.select(commonCols.map(col): _*))

    val matchIds = matchesDf.select(col("id").cast("string").as("id")).distinct()
    val exactMatchCount = matchIds.count()

    // ---------------------------
    // Utilidad formato %
    // ---------------------------
    def pct(n: Long): String =
      if (totalRef == 0) "-" else "%.1f%%".formatLocal(Locale.US, n.toDouble / totalRef * 100)

    val deltaTotal = totalNew - totalRef
    val deltaLabel = if (deltaTotal > 0) s"+$deltaTotal" else deltaTotal.toString

    // ---------------------------
    // Construcción del resumen (todas STRING para casar con tu DDL)
    // ---------------------------
    val summaryData = Seq(
      ("Total Records",                 s"$totalRef",                 s"$totalNew",                 pct(totalRef),              deltaLabel, "-"),
      ("1:1 Exact Matches",             s"$exactMatchCount",          s"$exactMatchCount",          pct(exactMatchCount),       "Match",    matchIds.as[String].collect().mkString(",")),
      ("1:1 Differences",               s"${diffIds.count()}",        s"${diffIds.count()}",        pct(diffIds.count()),       "No Match", diffIds.as[String].collect().mkString(",")),
      ("1:0 (Only in Reference)",       s"${onlyInRefIds.count()}",   "-",                          pct(onlyInRefIds.count()),  "Missing",  onlyInRefIds.as[String].collect().mkString(",")),
      ("0:1 (Only in New)",             "-",                          s"${onlyInNewIds.count()}",   "-",                         "Extra",    onlyInNewIds.as[String].collect().mkString(",")),
      ("Exact Duplicates",              s"${exactDupIdsRef.count()}", s"${exactDupIdsNew.count()}", pct(exactDupIdsRef.count()),"Exact",    exactDupIdsRef.as[String].collect().mkString(",")),
      ("Duplicates with Variations (ref)", s"${variedDupIdsRef.count()}", "-",                      pct(variedDupIdsRef.count()), "Variations", variedDupIdsRef.as[String].collect().mkString(",")),
      ("Duplicates with Variations (new)", "-", s"${variedDupIdsNew.count()}",                     "-",                         "Variations", variedDupIdsNew.as[String].collect().mkString(","))
    ).toDF("Metrica", "total_Ref", "total_New", "pct_Ref", "Status", "Examples")
     .withColumn("partition_hour", lit(partitionHour))

    summaryData
  }
}
