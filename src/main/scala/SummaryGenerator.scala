import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Locale

/* -------- fila intermedia (nombres en minúsculas) -------------------- */
private case class SummaryRow(
  metrica:   String,
  total_ref: String,
  total_new: String,
  pct_ref:   String,
  status:    String,
  examples:  String,
  table:     String
)

object SummaryGenerator {

  private def signed(n: Long): String = if (n > 0) s"+$n" else n.toString

  def generateSummaryTable(
      spark: SparkSession,
      refDf: DataFrame,
      newDf: DataFrame,
      diffDf: DataFrame,     // usamos diffDf para MATCH/NO_MATCH
      dupDf: DataFrame,
      compositeKeyCols: Seq[String],
      partitionHour: String,
      refDfRaw: DataFrame,
      newDfRaw: DataFrame
  ): DataFrame = {

    import spark.implicits._

    /* 0) Normaliza NULL a "NULL" en todos los DF ---------------------- */
    val idCol   = compositeKeyCols.head
    val idFixed = coalesce(col(idCol).cast("string"), lit("NULL"))

    val refFix   = refDf   .withColumn("id_fix", idFixed)
    val newFix   = newDf   .withColumn("id_fix", idFixed)
    val refRawFx = refDfRaw.withColumn("id_fix", idFixed)
    val newRawFx = newDfRaw.withColumn("id_fix", idFixed)

    /* Totales --------------------------------------------------------- */
    val totalIdsRef  = refFix.select("id_fix").distinct().count()
    val totalRowsRef = refDfRaw.count()
    val totalRowsNew = newDfRaw.count()

    /* 1) Duplicados por lado (conteo por ID en crudo) ----------------- */
    val refDupIds = refRawFx.groupBy($"id_fix").agg(count(lit(1)).as("cnt"))
                            .filter($"cnt" > 1).select("id_fix").distinct()
    val newDupIds = newRawFx.groupBy($"id_fix").agg(count(lit(1)).as("cnt"))
                            .filter($"cnt" > 1).select("id_fix").distinct()

    // EXACT = duplicados en ambos lados; ref-only / new-only según corresponda
    val exactBothDupIds = refDupIds.intersect(newDupIds)
    val refOnlyDupIds   = refDupIds.except(newDupIds)
    val newOnlyDupIds   = newDupIds.except(refDupIds)

    /* 2) MATCH / NO_MATCH desde diffDf (consistente con DiffGenerator) - */
    // Requiere includeEqualsInDiff = true para que tengamos MATCH en diffDf.
    val diffById = diffDf
      .groupBy(col("id"))
      .agg(
        max(when(lower(col("results")) === "no_match", 1).otherwise(0)).as("has_no_match"),
        max(when(lower(col("results")).isin("only_in_ref","only_in_new"), 1).otherwise(0)).as("has_only")
      )

    val exactMatchIds = diffById
      .filter(col("has_no_match") === 0 && col("has_only") === 0)
      .select(col("id").cast("string").as("id_fix"))
      .distinct()

    val mismatchIds = diffById
      .filter(col("has_no_match") === 1)
      .select(col("id").cast("string").as("id_fix"))
      .distinct()

    /* 3) Only-in ------------------------------------------------------ */
    val onlyRefIds = refFix.select("id_fix").except(newFix.select("id_fix"))
    val onlyNewIds = newFix.select("id_fix").except(refFix.select("id_fix"))

    /* helpers ---------------------------------------------------------- */
    def pct(n: Long): String =
      if (totalIdsRef == 0) "-" else "%.1f%%".formatLocal(Locale.US, n.toDouble/totalIdsRef*100)

    def idsToStr(df: DataFrame): String = df.limit(50).as[String].collect().mkString(",")

    /* 4) Construcción de filas ---------------------------------------- */
    val rows = Seq(
      SummaryRow("exact duplicates",
        exactBothDupIds.count().toString,
        exactBothDupIds.count().toString,
        pct(exactBothDupIds.count()),
        "EXACT",
        idsToStr(exactBothDupIds),
        "duplicate_records"),

      SummaryRow("duplicates with variations (ref)",
        refOnlyDupIds.count().toString, "-",
        pct(refOnlyDupIds.count()),
        "VARIATIONS",
        idsToStr(refOnlyDupIds),
        "duplicate_records"),

      SummaryRow("duplicates with variations (new)",
        "-", newOnlyDupIds.count().toString, "-",
        "VARIATIONS",
        idsToStr(newOnlyDupIds),
        "duplicate_records"),

      SummaryRow("1:1 exact matches",
        exactMatchIds.count().toString,
        exactMatchIds.count().toString,
        pct(exactMatchIds.count()),
        "MATCH",
        idsToStr(exactMatchIds),
        "different_records"),

      SummaryRow("1:1 matches variations",
        mismatchIds.count().toString,
        mismatchIds.count().toString,
        pct(mismatchIds.count()),
        "NO_MATCH",
        idsToStr(mismatchIds),
        "different_records"),

      SummaryRow("1:0 (only in reference)",
        onlyRefIds.count().toString, "-",
        pct(onlyRefIds.count()),
        "ONLY_IN_REF",
        idsToStr(onlyRefIds),
        "different_records"),

      SummaryRow("0:1 (only in new)",
        "-", onlyNewIds.count().toString, "-",
        "ONLY_IN_NEW",
        idsToStr(onlyNewIds),
        "different_records"),

      SummaryRow("total records",
        totalRowsRef.toString,
        totalRowsNew.toString,
        "100.0%",
        signed(totalRowsNew - totalRowsRef),
        "-",
        "-")
    )

    /* 5) Orden y salida ------------------------------------------------ */
    val df = spark.createDataset(rows).toDF()
      .withColumn("partition_hour", lit(partitionHour))
      .withColumn("_o",
        when($"metrica"==="exact duplicates",1)
        .when($"metrica"==="duplicates with variations (ref)",2)
        .when($"metrica"==="duplicates with variations (new)",3)
        .when($"metrica"==="1:1 exact matches",4)
        .when($"metrica"==="1:1 matches variations",5)
        .when($"metrica"==="1:0 (only in reference)",6)
        .when($"metrica"==="0:1 (only in new)",7)
        .otherwise(8))

    df.orderBy("_o").drop("_o")
  }
}
