import ComparatorDefaults.SampleIdsForSummary

import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.immutable.ListMap

case class SummaryRow(
                       block: String,
                       metric: String,
                       universe: String,
                       numerator: String,
                       denominator: String,
                       pct: String,
                       samples: String
                     )

// Bundle to reduce parameter count - REMOVED rawDf references
final case class SummaryInputs(
                                spark: SparkSession,
                                refDf: DataFrame,
                                newDf: DataFrame,
                                diffDf: DataFrame,
                                dupDf: DataFrame,
                                compositeKeyCols: Seq[String]
                              )

// DTOs to avoid long parameter lists in private helpers.
final case class IdSets(ref: DataFrame, neu: DataFrame, both: DataFrame)
final case class CoreCounts(
                             totalRowsRef: Long,
                             totalRowsNew: Long,
                             nRefIds: Long,
                             nNewIds: Long,
                             nBothIds: Long,
                             onlyRef: DataFrame,
                             onlyNew: DataFrame
                           )
final case class DupSets(both: DataFrame, onlyRef: DataFrame, onlyNew: DataFrame, any: DataFrame)
final case class DiffSets(exact: DataFrame, variations: DataFrame)
final case class MetricCounts(
                               totalRowsRef: Long,
                               totalRowsNew: Long,
                               nRefIds: Long,
                               nNewIds: Long,
                               nBothIds: Long,
                               qualityOk: Long
                             )
final case class MetricSets(
                             idsExact: DataFrame,
                             idsVariations: DataFrame,
                             idsOnlyR: DataFrame,
                             idsOnlyN: DataFrame,
                             dupIdsBoth: DataFrame,
                             dupIdsOnlyRef: DataFrame,
                             dupIdsOnlyNew: DataFrame
                           )

case class SummaryRow(
                       block: String,
                       metric: String,
                       universe: String,
                       numerator: String,
                       denominator: String,
                       pct: String,
                       samples: String
                     )

// Bundle to reduce parameter count - REMOVED rawDf references
final case class SummaryInputs(
                                spark: SparkSession,
                                refDf: DataFrame,
                                newDf: DataFrame,
                                diffDf: DataFrame,
                                dupDf: DataFrame,
                                compositeKeyCols: Seq[String]
                              )

// DTOs to avoid long parameter lists in private helpers.
final case class IdSets(ref: DataFrame, neu: DataFrame, both: DataFrame)
final case class CoreCounts(
                             totalRowsRef: Long,
                             totalRowsNew: Long,
                             nRefIds: Long,
                             nNewIds: Long,
                             nBothIds: Long,
                             onlyRef: DataFrame,
                             onlyNew: DataFrame
                           )
final case class DupSets(both: DataFrame, onlyRef: DataFrame, onlyNew: DataFrame, any: DataFrame)
final case class DiffSets(exact: DataFrame, variations: DataFrame)
final case class MetricCounts(
                               totalRowsRef: Long,
                               totalRowsNew: Long,
                               nRefIds: Long,
                               nNewIds: Long,
                               nBothIds: Long,
                               qualityOk: Long
                             )
final case class MetricSets(
                             idsExact: DataFrame,
                             idsVariations: DataFrame,
                             idsOnlyR: DataFrame,
                             idsOnlyN: DataFrame,
                             dupIdsBoth: DataFrame,
                             dupIdsOnlyRef: DataFrame,
                             dupIdsOnlyNew: DataFrame
                           )

object SummaryGenerator extends Serializable {

  // ─────────────────────────── Orchestration ───────────────────────────
  def generateSummaryTable(in: SummaryInputs): DataFrame = {
    import in.spark.implicits._

    val cidCol = buildCid(in.compositeKeyCols)
    val ids    = buildIdSets(in.refDf, in.newDf, cidCol)

    // Temporarily cached DFs to be released in finally
    var cacheThese: Seq[DataFrame] = Seq.empty

    try {
      val core = computeCoreCounts(in, ids)
      val dups = computeDupSets(in, cidCol)
      val dif  = computeDiffSets(in, ids)

      // qualityOk = exact matches among IDs present on both sides and NOT duplicated on either side
      // IMPORTANT: Use nBothIds as denominator (not nRefIds) to ensure coherence - only IDs in both sides can have quality
      val qualityOk = dif.exact.except(dups.any).count()

      // Cache sets that are used for (count + sample) to avoid recomputation
      cacheThese = Seq(dif.exact, dif.variations, core.onlyRef, core.onlyNew, dups.both, dups.onlyRef, dups.onlyNew)
      cacheThese.foreach(_.cache())

      val preCounts = ListMap(
        "idsExact" -> dif.exact.count(),
        "idsVariations" -> dif.variations.count(),
        "idsOnlyRef" -> core.onlyRef.count(),
        "idsOnlyNew" -> core.onlyNew.count(),
        "dupBoth" -> dups.both.count(),
        "dupOnlyRef" -> dups.onlyRef.count(),
        "dupOnlyNew" -> dups.onlyNew.count()
      )

      val counts = MetricCounts(totalRowsRef = core.totalRowsRef, totalRowsNew = core.totalRowsNew, nRefIds = core.nRefIds,
        nNewIds = core.nNewIds, nBothIds = core.nBothIds, qualityOk = qualityOk
      )

      val sets = MetricSets(
        dif.exact, dif.variations, core.onlyRef, core.onlyNew,
        dups.both, dups.onlyRef, dups.onlyNew
      )

      val rows = buildRows(counts, sets, preCounts)(in.spark)
      in.spark.createDataset(rows).toDF()
    } finally {
      // Always release temporary caches and IdSets
      cacheThese.foreach(df => df.unpersist(blocking = true))
      ids.both.unpersist(blocking = true)
      ids.ref.unpersist(blocking = true)
      ids.neu.unpersist(blocking = true)
    }
  }

  // ─────────────────────────── Helpers ───────────────────────────

  // Percentage with 4 decimals; "-" when denominator is 0.
  def pctStr(num: Long, den: Long): String =
    if (den == 0) "-" else f"${num.toDouble / den * 100}%.4f%%"

  // Normalize empty strings to NULL
  private val NullStr: Column = expr("cast(null as string)")
  private def nz(c: Column): Column = {
    val s = c.cast("string")
    val t = trim(s)
    when(t === lit(""), NullStr).otherwise(s)
  }

  // Build canonical ID from composite keys (NULL-safe).
  private def buildCid(keys: Seq[String]): Column =
    concat_ws("_", keys.map(k => coalesce(nz(col(k)), lit("NULL"))): _*)

  // Distinct ID sets reused multiple times; cached and safely released by caller.
  private def buildIdSets(refDf: DataFrame, newDf: DataFrame, cid: Column): IdSets = {
    val idsRef = refDf.select(cid.as("cid")).distinct().cache()
    val idsNew = newDf.select(cid.as("cid")).distinct().cache()
    val idsBoth = idsRef.intersect(idsNew).cache()
    IdSets(idsRef, idsNew, idsBoth)
  }

  private def computeCoreCounts(in: SummaryInputs, ids: IdSets): CoreCounts = {
    val totalRowsRef = in.refDf.count()
    val totalRowsNew = in.newDf.count()
    val nRefIds = ids.ref.count()
    val nNewIds = ids.neu.count()
    val nBothIds = ids.both.count()
    val onlyRef = ids.ref.except(ids.neu)
    val onlyNew = ids.neu.except(ids.ref)
    CoreCounts(totalRowsRef, totalRowsNew, nRefIds, nNewIds, nBothIds, onlyRef, onlyNew)
  }

  /**
   * Extract duplicate IDs from the already-computed duplicates table.
   * This ensures 100% coherence with the duplicates table (respects priorityCol, etc.)
   *
   * If dupDf is empty (checkDuplicates=false), returns empty DataFrames.
   */
  private def computeDupSets(in: SummaryInputs, cid: Column): DupSets = {
    import in.spark.implicits._

    // If duplicates detection was disabled, return empty sets
    // Check if DataFrame has columns (emptyDataFrame has no columns)
    if (in.dupDf.columns.isEmpty) {
      val emptyDf = in.spark.emptyDataFrame.select(cid.as("cid"))
      return DupSets(emptyDf, emptyDf, emptyDf, emptyDf)
    }

    // Extract distinct IDs from duplicates table, grouped by origin and category
    val dupIds = in.dupDf
      .select($"origin", $"id".as("cid"), $"category")
      .distinct()

    // Separate by origin
    val dRef = dupIds.filter($"origin" === "ref").select($"cid").distinct()
    val dNew = dupIds.filter($"origin" === "new").select($"cid").distinct()

    // Compute intersections using category column (already computed in duplicates table)
    val both = dupIds.filter($"category" === "both").select($"cid").distinct()
    val oRef = dupIds.filter($"category" === "only_ref").select($"cid").distinct()
    val oNew = dupIds.filter($"category" === "only_new").select($"cid").distinct()
    val any  = dRef.union(dNew).distinct()

    DupSets(both, oRef, oNew, any)
  }

  private def computeDiffSets(in: SummaryInputs, ids: IdSets): DiffSets = {
    import in.spark.implicits._
    val diffAgg = in.diffDf.groupBy($"id")
      .agg(
        max(when(lower($"results") === "no_match", 1).otherwise(0)).as("has_nm"),
        max(when(lower($"results").isin("only_in_ref","only_in_new"), 1).otherwise(0)).as("has_only")
      )
      .withColumn("has_diff", greatest(col("has_nm"), col("has_only")))
      .select(col("id").as("cid"), col("has_diff"))

    val variations = diffAgg.filter(col("has_diff") === 1).select("cid").intersect(ids.both)
    val exact      = ids.both.except(variations)
    DiffSets(exact, variations)
  }

  // Convenience pretty-printer for sample IDs (random sampling for diversity)
  private def idsToStr(df: DataFrame, limit: Int = SampleIdsForSummary)(implicit spark: SparkSession): String = {
    import spark.implicits._
    df.orderBy(rand()).limit(limit).as[String].collect().mkString(",")
  }

  // Build final rows (KPIs, MATCH/NO MATCH/GAP/DUPS blocks)
  private def buildRows(
                         counts: MetricCounts,
                         sets: MetricSets,
                         precomputedCounts: ListMap[String, Long]
                       )(implicit spark: SparkSession): Seq[SummaryRow] = {

    def row(b: String, m: String, u: String, num: Long, den: Long, ex: String) =
      SummaryRow(
        block = b,
        metric = m,
        universe = u,
        numerator = num.toString,
        denominator = if (den > 0) den.toString else "-",
        pct = pctStr(num, den),
        samples = if (ex.nonEmpty) ex else "-"
      )

    val idsExactCount = precomputedCounts("idsExact")
    val idsVariationsCount = precomputedCounts("idsVariations")
    val idsOnlyRefCount = precomputedCounts("idsOnlyRef")
    val idsOnlyNewCount = precomputedCounts("idsOnlyNew")
    val dupBothCount = precomputedCounts("dupBoth")
    val dupOnlyRefCount = precomputedCounts("dupOnlyRef")
    val dupOnlyNewCount = precomputedCounts("dupOnlyNew")

    Seq(
      row("KPIS", "Unique IDs",          "REF",  counts.nRefIds,      0, "" ),
      row("KPIS", "Unique IDs",          "NEW",  counts.nNewIds,      0, "" ),
      row("KPIS", "Total rows REF",      "ROWS", counts.totalRowsRef, 0, "" ),
      row("KPIS", "Total rows NEW",      "ROWS", counts.totalRowsNew, 0, "" ),
      row("KPIS", "Total diff(new-ref)", "ROWS", counts.totalRowsNew - counts.totalRowsRef, counts.totalRowsRef, "" ),
      row("KPIS", "Global quality",      "BOTH", counts.qualityOk,   counts.nBothIds, ""),

      row("EXACT MATCH",   "1:1 (all columns)",           "BOTH", idsExactCount,      counts.nBothIds, idsToStr(sets.idsExact)),
      row("PARTIAL MATCH", "1:1 (match & no_match cols)", "BOTH", idsVariationsCount, counts.nBothIds, idsToStr(sets.idsVariations)),

      row("GAP", "1:0 (only in ref)", "REF",  idsOnlyRefCount, counts.nRefIds, idsToStr(sets.idsOnlyR)),
      row("GAP", "0:1 (only in new)", "NEW",  idsOnlyNewCount, counts.nNewIds, idsToStr(sets.idsOnlyN)),

      row("DUPS", "duplicates (both)",        "BOTH", dupBothCount,    counts.nBothIds, idsToStr(sets.dupIdsBoth)),
      row("DUPS", "duplicates (only in ref)", "REF",  dupOnlyRefCount, counts.nRefIds,  idsToStr(sets.dupIdsOnlyRef)),
      row("DUPS", "duplicates (only in new)", "NEW",  dupOnlyNewCount, counts.nNewIds,  idsToStr(sets.dupIdsOnlyNew))
    )
  }

  // ─────────────────────────── Excel export ───────────────────────────
  def exportToExcel(df: DataFrame, path: String): Unit = {
    df.write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'Sheet1'!A1")
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .mode(SaveMode.Overwrite)
      .save(path)
  }
}