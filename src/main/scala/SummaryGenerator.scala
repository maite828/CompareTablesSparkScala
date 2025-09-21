/**
 * SummaryGenerator is an object that provides utilities to generate a summary table comparing two DataFrames,
 * typically used for data quality and reconciliation between a reference and a new dataset.
 *
 * Main Features:
 * - Calculates key metrics such as unique IDs, total rows, differences in row counts, and global quality.
 * - Identifies exact matches, variations, gaps (IDs only in one dataset), and duplicates across datasets.
 * - Provides examples of IDs for each metric for easier inspection.
 * - Supports exporting the summary table to Excel format.
 *
 * Key Methods:
 * - generateSummaryTable: Builds a summary DataFrame with metrics and sample IDs, given reference, new, diff, and duplicate DataFrames.
 * - exportToExcel: Exports the summary DataFrame to an Excel file using the crealytics spark-excel library.
 *
 * Helper Functions:
 * - pctStr: Formats percentage values with one decimal, returns "-" if denominator is zero.
 * - nz: Normalizes empty strings to NULL for consistent ID construction.
 * - idsToStr: Converts a DataFrame of IDs to a comma-separated string of examples.
 *
 * Usage:
 * 1. Prepare the required DataFrames (refDf, newDf, diffDf, dupDf, etc.) and configuration.
 * 2. Call generateSummaryTable to obtain the summary DataFrame.
 * 3. Optionally, export the summary to Excel using exportToExcel.
 */

import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import CompareConfig._

case class SummaryRow(
  bloque: String,
  metrica: String,
  universo: String,
  numerador: String,
  denominador: String,
  pct: String,
  ejemplos: String
)

object SummaryGenerator {

  /** % con un decimal; "-" cuando el denominador es 0 */
   def pctStr(num: Long, den: Long): String =
    if (den == 0) "-" else f"${num.toDouble / den * 100}%.2f%%"

  /** normaliza strings vacíos a NULL */
   def nz(c: org.apache.spark.sql.Column) =
    when(trim(c.cast("string")) === "", lit(null)).otherwise(c)

  def generateSummaryTable(
      spark: SparkSession,
      refDf:           DataFrame,
      newDf:           DataFrame,
      diffDf:          DataFrame,
      dupDf:           DataFrame,
      compositeKeyCols: Seq[String],
      refDfRaw:        DataFrame,
      newDfRaw:        DataFrame,
      config:          CompareConfig
  ): DataFrame = {

    import spark.implicits._

    // Construcción de ID compuesto
    val cid = concat_ws("_", compositeKeyCols.map(c => coalesce(nz(col(c)), lit("NULL"))): _*)

    val idsRef   = refDf.select(cid.as("cid")).distinct()
    val idsNew   = newDf.select(cid.as("cid")).distinct()
    val idsBoth  = idsRef.intersect(idsNew)
    val idsOnlyR = idsRef.except(idsNew)
    val idsOnlyN = idsNew.except(idsRef)

    val totalRowsRef = refDfRaw.count()
    val totalRowsNew = newDfRaw.count()
    val nRefIds      = idsRef.count()
    val nNewIds      = idsNew.count()
    val nBothIds     = idsBoth.count()

    // Duplicados
    def dupIds(df: DataFrame) =
      df.groupBy(cid.as("cid"))
        .agg(count(lit(1)).as("cnt"))
        .filter($"cnt" > 1)
        .select($"cid")

    val dupIdsRef     = dupIds(refDf)
    val dupIdsNew     = dupIds(newDf)
    val dupIdsBoth    = dupIdsRef.intersect(dupIdsNew)
    val dupIdsOnlyRef = dupIdsRef.except(dupIdsNew)
    val dupIdsOnlyNew = dupIdsNew.except(dupIdsRef)
    val dupIdsAny     = dupIdsRef.union(dupIdsNew).distinct()

    // Exact / Variations en BOTH basados en diffDf
    val diffAgg = diffDf.groupBy($"id")
      .agg(
        max(when(lower($"results") === "no_match", 1).otherwise(0)).as("has_nm"),
        max(when(lower($"results").isin("only_in_ref","only_in_new"), 1).otherwise(0)).as("has_only")
      )
      .withColumn("has_diff", greatest($"has_nm", $"has_only"))
      .select($"id".as("cid"), $"has_diff")

    val idsVariations = diffAgg.filter($"has_diff" === 1).select("cid").intersect(idsBoth)
    val idsExact      = idsBoth.except(idsVariations)

    // Quality global
    val qualityIds = idsExact.except(dupIdsAny)
    val qualityOk  = qualityIds.count()

    // Ejemplos
    def idsToStr(df: DataFrame, limit: Int = 6) =
      df.orderBy("cid").limit(limit).as[String].collect().mkString(",")

    // Constructor de fila
    def row(b: String, m: String, u: String, num: Long, den: Long, ex: String) =
      SummaryRow(
        bloque      = b,
        metrica     = m,
        universo    = u,
        numerador   = num.toString,
        denominador = if (den > 0) den.toString else "-",
        pct         = pctStr(num, den),
        ejemplos    = if (ex.nonEmpty) ex else "-"
      )

    // Métricas
    val rows = Seq(
      row("KPIS", "IDs Uniques", "REF",  nRefIds, 0,   ""          ),
      row("KPIS", "IDs Uniques", "NEW",  nNewIds, 0,""          ),
      row("KPIS", "Total rows REF", "ROWS", totalRowsRef, 0, ""          ),
      row("KPIS", "Total rows NEW", "ROWS", totalRowsNew, 0, ""          ),
      row("KPIS", "Total diff(new-ref)", "ROWS", totalRowsNew - totalRowsRef, totalRowsRef, "" ),
      row("KPIS", "Quality global", "REF", qualityOk, nRefIds, ""),

      row("EXACT_MATCH", "1:1 (all columns)", "BOTH", idsExact.count(), nBothIds, idsToStr(idsExact)),
      row("PARTIAL_MATCH", "1:1 (match and not_match cols)","BOTH", idsVariations.count(), nBothIds, idsToStr(idsVariations)),

      row("GAP", "1:0 (only in ref)", "REF", idsOnlyR.count(), nRefIds, idsToStr(idsOnlyR)),
      row("GAP", "0:1 (only in new)", "NEW", idsOnlyN.count(), nNewIds, idsToStr(idsOnlyN)),

      row("DUPS", "duplicates (both)", "BOTH", dupIdsBoth.count(), nBothIds, idsToStr(dupIdsBoth)),
      row("DUPS", "duplicates (only in ref)", "REF",  dupIdsOnlyRef.count(), nRefIds, idsToStr(dupIdsOnlyRef)),
      row("DUPS", "duplicates (only in new)", "NEW",  dupIdsOnlyNew.count(), nNewIds, idsToStr(dupIdsOnlyNew))
    )

    spark.createDataset(rows).toDF()
  }

  /** Exporta el DataFrame de resumen a Excel */
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
