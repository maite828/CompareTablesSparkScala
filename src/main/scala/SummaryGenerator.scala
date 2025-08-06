// src/main/scala/com/example/compare/SummaryGenerator.scala

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

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

  private def pctStr(num: Long, den: Long): String =
    if (den == 0) "-" else f"${num.toDouble/den*100}%.1f%%"

  def generateSummaryTable(
      spark: SparkSession,
      refDf: DataFrame,
      newDf: DataFrame,
      diffDf: DataFrame,
      dupDf: DataFrame,
      compositeKeyCols: Seq[String],
      refRaw: DataFrame,
      newRaw: DataFrame,
      config: CompareConfig
  ): DataFrame = {
    import spark.implicits._

    val cid = concat_ws("_", compositeKeyCols.map(c =>
      coalesce(when(trim(col(c).cast("string"))==="", lit(null)).otherwise(col(c)), lit("NULL"))
    ):_*)

    val idsRef  = refDf.select(cid.as("cid")).distinct()
    val idsNew  = newDf.select(cid.as("cid")).distinct()
    val idsBoth = idsRef.intersect(idsNew)
    val onlyR   = idsRef.except(idsNew)
    val onlyN   = idsNew.except(idsRef)

    val nRef  = idsRef.count()
    val nNew  = idsNew.count()
    val nBoth = idsBoth.count()
    val rowsR = refRaw.count()
    val rowsN = newRaw.count()

    // duplicados
    def dupIds(df: DataFrame) = df.groupBy(cid.as("cid")).count().filter($"count">1).select($"cid")
    val dupRef = dupIds(refDf)
    val dupNew = dupIds(newDf)
    val dupAny = dupRef.union(dupNew).distinct()

    // exact/variations
    val diffAgg = diffDf.groupBy($"id")
      .agg(max(when(lower($"results")==="no_match",1).otherwise(0)).as("has_nm"),
           max(when(lower($"results").isin("only_in_ref","only_in_new"),1).otherwise(0)).as("has_only"))
      .withColumn("has_diff", greatest($"has_nm",$"has_only"))
      .select($"id".as("cid"), $"has_diff")
    val variations = diffAgg.filter($"has_diff"===1).select($"cid").intersect(idsBoth)
    val exact      = idsBoth.except(variations)

    // quality global
    val qual = exact.except(dupAny).count()

    def toStr(df: DataFrame, lim: Int=6) = df.orderBy($"cid").limit(lim).as[String].collect().mkString(",")
    def row(b: String,m: String,u:String,num:Long,den:Long,ex:String) = SummaryRow(
      b,m,u,num.toString, if (den>0) den.toString else "-", pctStr(num,den), if (ex.nonEmpty) ex else "-"
    )

    val rows = Seq(
      row("KPIS","IDs Uniques","REF", nRef, 0, ""),
      row("KPIS","IDs Uniques","NEW", nNew, 0, ""),
      row("KPIS","Total REF","ROWS", rowsR, 0, ""),
      row("KPIS","Total NEW","ROWS", rowsN, 0, ""),
      row("KPIS","Total (NEW-REF)","ROWS", rowsN-rowsR, rowsR, ""),
      row("KPIS","Quality global","REF", qual, nRef, ""),

      row("MATCH","1:1 (exact matches)","BOTH", exact.count(), nBoth, toStr(exact)),
      row("NO MATCH","1:1 (match not identical)","BOTH", variations.count(), nBoth, toStr(variations)),
      row("GAP","1:0 (only in reference)","REF", onlyR.count(), nRef, toStr(onlyR)),
      row("GAP","0:1 (only in new)","NEW", onlyN.count(), nNew, toStr(onlyN)),

      row("DUPS","duplicates (both)","BOTH", dupRef.intersect(dupNew).count(), nBoth, toStr(dupRef.intersect(dupNew))),
      row("DUPS","duplicates (ref)","REF", dupRef.except(dupNew).count(), nRef, toStr(dupRef.except(dupNew))),
      row("DUPS","duplicates (new)","NEW", dupNew.except(dupRef).count(), nNew, toStr(dupNew.except(dupRef)))
    )

    spark.createDataset(rows).toDF()
  }
}
