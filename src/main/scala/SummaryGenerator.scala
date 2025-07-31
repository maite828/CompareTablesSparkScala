import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import java.util.Locale

object SummaryGenerator {

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

    // IDs with differences between ref and new (per ID)
    val diffIds      = diffDf.filter(col("Results") === "NO_MATCH").select("id").distinct()
    val onlyInRefIds = diffDf.filter(col("Results") === "ONLY_IN_REF").select("id").distinct()
    val onlyInNewIds = diffDf.filter(col("Results") === "ONLY_IN_NEW").select("id").distinct()

    // --------------------------------------------------
    // Normalize duplicates columns (origin/exact/varied/occurrences)
    // Accepts both Spanish/English variants coming from DuplicateDetector/Hive
    // --------------------------------------------------
    val hasVariedCol = dupDf.columns.contains("duplicates_w_variations")
    val hasOccCol    = dupDf.columns.contains("total")

    val dupNorm = dupDf
      .withColumn("origin_lc", lower(col("origin")))
      .withColumn(
        "origin_norm",
        when(col("origin_lc").isin("reference"), lit("reference"))
          .when(col("origin_lc").isin("new"), lit("new"))
          .otherwise(col("origin_lc"))
      )
      .withColumn("id_str", col("id").cast("string"))
      .withColumn("exact_l", coalesce(col("exact_duplicates").cast("long"), lit(0L)))
      .withColumn(
        "var_l",
        coalesce(col("duplicates_w_variations").cast("long"), lit(0L))
      )
      .withColumn(
        "occ_l",
        coalesce(col("occurrences").cast("long"), lit(0L))
      )

    // Exact Duplicates (IDs that have exact duplicates and NO variations)
    val exactDupIdsRef = dupNorm.filter(col("origin_norm") === "reference" && col("exact_l") > 0 && col("var_l") === 0)
      .select(col("id_str").as("id")).distinct()
    val exactDupIdsNew = dupNorm.filter(col("origin_norm") === "new" && col("exact_l") > 0 && col("var_l") === 0)
      .select(col("id_str").as("id")).distinct()

    val exactDupCountRef = exactDupIdsRef.count()
    val exactDupCountNew = exactDupIdsNew.count()

    // Duplicates with Variations (IDs)
    val variedDupIdsRef = dupNorm.filter(col("origin_norm") === "reference" && col("var_l") > 0)
      .select(col("id_str").as("id")).distinct()
    val variedDupIdsNew = dupNorm.filter(col("origin_norm") === "new" && col("var_l") > 0)
      .select(col("id_str").as("id")).distinct()

    // --------------------------------------------------
    // Exact 1:1 Matches (IDs)
    // Intersect full rows then dedupe by id
    // --------------------------------------------------
    val commonCols = refDf.columns.toSet.intersect(newDf.columns.toSet).toSeq
    val matchesDf = refDf.select(commonCols.map(col): _*).intersect(newDf.select(commonCols.map(col): _*))
    val matchIds = matchesDf.select("id").distinct()
    val exactMatchCount = matchIds.count()

    // --------------------------------------------------
    // Helpers
    // --------------------------------------------------
    def pct(n: Long): String = if (totalRef == 0) "-" else "%.1f%%".formatLocal(Locale.US, n.toDouble / totalRef * 100)

    val deltaTotal = totalNew - totalRef
    val deltaLabel = if (deltaTotal > 0) s"+${deltaTotal}" else deltaTotal.toString

    // --------------------------------------------------
    // Build summary rows
    // --------------------------------------------------
    val summaryData = Seq(
      ("Exact Duplicates",                exactDupCountRef.toString, "1"            .takeRight(0) /*placeholder*/, pct(exactDupCountRef), "Exact",      exactDupIdsRef.selectExpr("CAST(id AS STRING)").as[String].collect().mkString(",")),
      ("Duplicates with Variations (ref)", variedDupIdsRef.count().toString, "-",    pct(variedDupIdsRef.count()),   "Variations", variedDupIdsRef.selectExpr("CAST(id AS STRING)").as[String].collect().mkString(",")),
      ("Duplicates with Variations (new)", "-",                         variedDupIdsNew.count().toString, "-",                "Variations", variedDupIdsNew.selectExpr("CAST(id AS STRING)").as[String].collect().mkString(",")),
      ("1:1 Exact Matches",               exactMatchCount.toString,     exactMatchCount.toString,          pct(exactMatchCount), "Match",      matchIds.selectExpr("CAST(id AS STRING)").as[String].collect().mkString(",")),
      ("1:1 Matches variations",          diffIds.count().toString,     diffIds.count().toString,          pct(diffIds.count()), "No Match",  diffIds.selectExpr("CAST(id AS STRING)").as[String].collect().mkString(",")),
      ("1:0 (Only in Reference)",         onlyInRefIds.count().toString, "-",                                pct(onlyInRefIds.count()), "Missing",   onlyInRefIds.selectExpr("CAST(id AS STRING)").as[String].collect().mkString(",")),
      ("0:1 (Only in New)",               "-",                          onlyInNewIds.count().toString,      "-",                "Extra",      onlyInNewIds.selectExpr("CAST(id AS STRING)").as[String].collect().mkString(",")),
      ("Total Records",                   totalRef.toString,             totalNew.toString,                  pct(totalRef),       deltaLabel,   "-")
    ).toDF("Metrica", "total_Ref", "total_New", "pct_Ref", "Status", "Examples")
      .withColumn("partition_hour", lit(partitionHour))

    // --------------------------------------------------
    // Enforce desired order when writing (best effort)
    // --------------------------------------------------
    val ordered = summaryData
      .withColumn(
        "_order",
        when(col("Metrica") === "Exact Duplicates", lit(1))
          .when(col("Metrica") === "Duplicates with Variations (ref)", lit(2))
          .when(col("Metrica") === "Duplicates with Variations (new)", lit(3))
          .when(col("Metrica") === "1:1 Exact Matches", lit(4))
          .when(col("Metrica") === "1:1 Matches variations", lit(5))
          .when(col("Metrica") === "1:0 (Only in Reference)", lit(6))
          .when(col("Metrica") === "0:1 (Only in New)", lit(7))
          .when(col("Metrica") === "Total Records", lit(8))
          .otherwise(lit(999))
      )
      .orderBy(col("_order"))
      .drop("_order")

    ordered
  }
}
