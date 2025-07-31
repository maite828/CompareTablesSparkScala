import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Locale

object SummaryGenerator {

  private def has(df: DataFrame, c: String): Boolean = df.columns.contains(c)

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

    // --- Normalize differences schema -> "Results" + id as string ---
    val diffNorm0 =
      if (has(diffDf, "Results")) diffDf
      else if (has(diffDf, "Resultado")) diffDf.withColumnRenamed("Resultado", "Results")
      else diffDf

    val diffIdCol = if (has(diffNorm0, "id")) "id" else compositeKeyCols.head

    val diffNorm = diffNorm0
      .withColumn("id_str", col(diffIdCol).cast("string"))
      .withColumn("results_lc", lower(col("Results")))

    // IDs by result type
    val diffIds      = diffNorm.filter(col("results_lc").isin("difference","diferencia","no_matches"))
                               .select($"id_str".as("id")).distinct()
    val onlyInRefIds = diffNorm.filter(col("results_lc")==="only_in_ref")
                               .select($"id_str".as("id")).distinct()
    val onlyInNewIds = diffNorm.filter(col("results_lc")==="only_in_new")
                               .select($"id_str".as("id")).distinct()

    // --- Normalize duplicates schema -> duplicates_w_variations, occurrences ---
    val dupNorm0a =
      if (has(dupDf, "varied_duplicates"))
        dupDf.withColumnRenamed("varied_duplicates", "duplicates_w_variations")
      else dupDf

    val dupNorm0 =
      if (has(dupNorm0a, "total"))
        dupNorm0a.withColumnRenamed("total", "occurrences")
      else dupNorm0a

    val dupNorm = dupNorm0
      .withColumn("origin_lc", lower(col("origin")))
      .withColumn(
        "origin_norm",
        when(col("origin_lc").isin("reference","referencia","ref"), lit("reference"))
          .when(col("origin_lc").isin("new","nuevos","nuevo"), lit("new"))
          .otherwise(col("origin_lc"))
      )
      .withColumn("id_str", col("id").cast("string"))
      .withColumn("exact_l", coalesce(col("exact_duplicates").cast("long"), lit(0L)))
      .withColumn("var_l",   coalesce(col("duplicates_w_variations").cast("long"), lit(0L)))
      .withColumn("occ_l",   coalesce(col("occurrences").cast("long"), lit(0L)))

    // --- 1:1 Exact Matches (by ID) ---
    val commonCols = refDf.columns.toSet.intersect(newDf.columns.toSet).toSeq
    val matchesDf  = refDf.select(commonCols.map(col): _*)
                          .intersect(newDf.select(commonCols.map(col): _*))

    val matchIds = matchesDf.select(col("id").cast("string").as("id")).distinct()
    val exactMatchCount = matchIds.count()

    // --- Exact Duplicates (by ID, inclusive with Matches) ---
    // Count IDs that have exact duplicates and NO variations on each side.
    val exactDupIdsRef = dupNorm
      .filter($"origin_norm"==="reference" && $"exact_l">0 && $"var_l"===0)
      .select($"id_str".as("id")).distinct()
    val exactDupIdsNew = dupNorm
      .filter($"origin_norm"==="new" && $"exact_l">0 && $"var_l"===0)
      .select($"id_str".as("id")).distinct()

    val exactDupCountRef = exactDupIdsRef.count()
    val exactDupCountNew = exactDupIdsNew.count()

    // --- Duplicates with Variations (by ID) ---
    val variedDupIdsRef = dupNorm.filter($"origin_norm"==="reference" && $"var_l">0)
                                 .select($"id_str".as("id")).distinct()
    val variedDupIdsNew = dupNorm.filter($"origin_norm"==="new" && $"var_l">0)
                                 .select($"id_str".as("id")).distinct()

    val variedDupCountRef = variedDupIdsRef.count()
    val variedDupCountNew = variedDupIdsNew.count()

    // --- % helper ---
    def pct(n: Long): String =
      if (totalRef == 0) "-" else "%.1f%%".formatLocal(Locale.US, n.toDouble / totalRef * 100)

    val deltaTotal = totalNew - totalRef
    val deltaLabel = if (deltaTotal > 0) s"+$deltaTotal" else deltaTotal.toString

    // --- Output (all STRING columns to match DDL) ---
    val summaryData = Seq(
      ("Duplicates with Variations (ref)", s"$variedDupCountRef", "-",                    pct(variedDupCountRef), "Variations", variedDupIdsRef.as[String].collect().mkString(",")),
      ("Duplicates with Variations (new)", "-",                    s"$variedDupCountNew", "-",                    "Variations", variedDupIdsNew.as[String].collect().mkString(",")),
      ("1:0 (Only in Reference)",          s"${onlyInRefIds.count()}", "-",               pct(onlyInRefIds.count()), "Missing",  onlyInRefIds.as[String].collect().mkString(",")),
      ("1:1 Matches variations",           s"${diffIds.count()}",  s"${diffIds.count()}", pct(diffIds.count()),   "No Match",   diffIds.as[String].collect().mkString(",")),
      ("1:1 Exact Matches",                s"$exactMatchCount",    s"$exactMatchCount",   pct(exactMatchCount),   "Match",      matchIds.as[String].collect().mkString(",")),
      ("Exact Duplicates",                 s"$exactDupCountRef",   s"$exactDupCountNew",  pct(exactDupCountRef),  "Exact",      exactDupIdsRef.as[String].collect().mkString(",")),
      ("0:1 (Only in New)",                "-",                    s"${onlyInNewIds.count()}", "-",               "Extra",      onlyInNewIds.as[String].collect().mkString(",")),
      ("Total Records",                    s"$totalRef",           s"$totalNew",          pct(totalRef),         deltaLabel,   "-")
    ).toDF("Metrica", "total_Ref", "total_New", "pct_Ref", "Status", "Examples")
     .withColumn("partition_hour", lit(partitionHour))

    summaryData
  }
}
