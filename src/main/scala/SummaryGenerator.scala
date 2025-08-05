import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Fila de la tabla de resumen ejecutivo */
case class SummaryRow(
  bloque:       String,
  metrica:      String,
  universo:     String,
  numerador:    String,
  denominador:  String,
  pct:          String,
  ejemplos:     String
)

object SummaryGenerator {

  /** % con un decimal; “-” cuando el denominador es 0 */
  private def pctStr(num: Long, den: Long): String =
    if (den == 0) "-" else f"${num.toDouble / den * 100}%.1f%%"

  /** normaliza strings vacíos a NULL */
  private def nz(c: org.apache.spark.sql.Column) =
    when(trim(c.cast("string")) === "", lit(null)).otherwise(c)

  // ──────────────────────────────────────────────────────────────
  def generateSummaryTable(
      spark: SparkSession,
      refDf:       DataFrame,
      newDf:       DataFrame,
      diffDf:      DataFrame,
      dupDf:       DataFrame,              // (no se usa pero se mantiene la firma)
      compositeKeyCols: Seq[String],
      refDfRaw:    DataFrame,
      newDfRaw:    DataFrame
  ): DataFrame = {

    import spark.implicits._

    /* ── 1) IDs compuestos ─────────────────────────────────────── */
    val cid = concat_ws("_", compositeKeyCols.map(c => coalesce(nz(col(c)), lit("NULL"))): _*)

    val idsRef   = refDf.select(cid.as("cid")).distinct()
    val idsNew   = newDf.select(cid.as("cid")).distinct()
    val idsBoth  = idsRef.intersect(idsNew)
    val idsOnlyR = idsRef.except(idsNew)
    val idsOnlyN = idsNew.except(idsRef)

    val totalRowsRef = refDfRaw.count()      // filas REF
    val totalRowsNew = newDfRaw.count()      // filas NEW
    val nRefIds      = idsRef.count()        // IDs REF
    val nNewIds      = idsNew.count()        // IDs NEW
    val nBothIds     = idsBoth.count()       // IDs en ambos

    /* ── 2) duplicados (conjuntos disjuntos) ────────────────────── */
    def dupIds(df: DataFrame) =
      df.groupBy(cid.as("cid")).agg(count(lit(1)).as("cnt"))
        .filter($"cnt" > 1).select($"cid")

    val dupIdsRef      = dupIds(refDf)
    val dupIdsNew      = dupIds(newDf)
    val dupIdsBoth     = dupIdsRef.intersect(dupIdsNew)
    val dupIdsOnlyRef  = dupIdsRef.except(dupIdsNew)
    val dupIdsOnlyNew  = dupIdsNew.except(dupIdsRef)
    val dupIdsAny      = dupIdsRef.union(dupIdsNew).distinct()   // duplicados en cualquiera de las dos

    /* ── 3) exact / variations ────────────────────────────────────
       Tomamos como “no exacto” cualquier ID de BOTH con NO_MATCH o ONLY_IN_* en differences.
     */
    val diffAgg = diffDf.groupBy($"id")
      .agg(
        max(when(lower($"results") === "no_match", 1).otherwise(0)).as("has_nm"),
        max(when(lower($"results").isin("only_in_ref","only_in_new"),1).otherwise(0)).as("has_only")
      )
      .withColumn("has_diff", greatest($"has_nm", $"has_only"))
      .select($"id".as("cid"), $"has_diff")

    val idsVariations = diffAgg.filter($"has_diff"===1).select($"cid").intersect(idsBoth)
    val idsExact      = idsBoth.except(idsVariations)

    /* ── 4) helpers ─────────────────────────────────────────────── */
    def idsToStr(df: DataFrame, limit:Int = 6) =
      df.orderBy($"cid").limit(limit).as[String].collect().mkString(",")

    def row(b:String,m:String,u:String,num:Long,den:Long,ex:String) = SummaryRow(
      bloque       = b,
      metrica      = m,
      universo     = u,
      numerador    = num.toString,
      denominador  = if (den>0) den.toString else "-",
      pct          = pctStr(num,den),
      ejemplos     = if (ex.nonEmpty) ex else "-"
    )

    /* ── 5) QUALITY GLOBAL (penaliza duplicados) ───────────────────
       Quality = IDs REF con MATCH EXACTO y que NO están duplicados ni en REF ni en NEW
                 -----------------------------------------------------------------------
                                  IDs REF
     */
    val qualityIds = idsExact.except(dupIdsAny)     // exactos y sin duplicados en ninguna tabla
    val qualityOk  = qualityIds.count()             // numerador
    val qualityDen = nRefIds                        // denominador (IDs REF)

    /* ── 6) filas de resultado ──────────────────────────────────── */
    val rows = Seq(
      // KPIS
      row("KPIS","IDs Uniques","REF",  nRefIds, 0,""),
      row("KPIS","IDs Uniques","NEW",  nNewIds, 0,""),
      row("KPIS","Total REF"   ,"ROWS", totalRowsRef , 0,""),
      row("KPIS","Total NEW"   ,"ROWS", totalRowsNew , 0,""),
      row("KPIS","Total (NEW-REF)","ROWS", totalRowsNew - totalRowsRef, totalRowsRef, ""),
      // Quality global sobre IDs REF, penalizando duplicados en REF o NEW
      row("KPIS","Quality global","REF", qualityOk, qualityDen, ""),

      // COMPARACIÓN CONTRA REF (denominador = IDs REF)
      row("MATCH","1:1 (exact matches)","REF", idsExact.count(), nRefIds, idsToStr(idsExact)),
      row("NO MATCH","1:1 (match not identical)","REF", idsVariations.count(), nRefIds, idsToStr(idsVariations)),
      row("GAP","1:0 (only in reference)","REF", idsOnlyR.count(), nRefIds, idsToStr(idsOnlyR)),
      row("GAP","0:1 (only in new)","REF", idsOnlyN.count(), nRefIds, idsToStr(idsOnlyN)),

      // Duplicados (impacto en REF / NEW)
      row("DUPS","Duplicates (both)","REF", dupIdsBoth.count(), nRefIds, idsToStr(dupIdsBoth)),
      row("DUPS","duplicates (ref)","REF", dupIdsOnlyRef.count(), nRefIds, idsToStr(dupIdsOnlyRef)),
      row("DUPS","Duplicates (new)","NEW", dupIdsOnlyNew.count(), nNewIds, idsToStr(dupIdsOnlyNew))
    )

    spark.createDataset(rows).toDF()
  }
}
