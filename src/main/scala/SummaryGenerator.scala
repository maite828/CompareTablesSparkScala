import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import java.util.Locale

/* ----- fila intermedia que luego convertimos a DataFrame -------------- */
private case class SummaryRow(
  Metrica:   String,
  total_Ref: String,
  total_New: String,
  pct_Ref:   String,
  Status:    String,
  Examples:  String
)

object SummaryGenerator {

  /** Convierte +1 / 0 / -1 a “+1” / “0” / “-1”. */
  private def signed(n: Long): String = if (n > 0) s"+$n" else n.toString

  /** Calcula la tabla resumen completa (ID NULL incluido). */
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

    /* 0) Clave nula → literal "NULL" en todos los DF ------------------- */
    val idCol   = compositeKeyCols.head
    val idFixed = coalesce(col(idCol).cast("string"), lit("NULL"))

    val refFix  = refDf .withColumn("id_fix", idFixed)
    val newFix  = newDf .withColumn("id_fix", idFixed)
    val diffFix = diffDf.withColumn("id_fix", idFixed)
    val dupFix  = dupDf .withColumn("id_fix", coalesce($"id".cast("string"), lit("NULL")))

    /* Totales */
    val totalIdsRef  = refFix.select("id_fix").distinct().count()   // 11
    val totalRowsRef = refDfRaw.count()                            // 12
    val totalRowsNew = newDfRaw.count()                            // 13

    /* 1) Normaliza duplicados ----------------------------------------- */
    val dupNorm = dupFix
      .withColumn("origin_norm",
        when(lower($"origin").isin("reference","ref","referencia"), "reference")
          .otherwise("new"))
      .withColumn("exact_l", $"exact_duplicates".cast("long"))
      .withColumn("var_l",   $"duplicates_w_variations".cast("long"))

    val exactDupIdsRef = dupNorm.filter($"origin_norm"==="reference" && $"exact_l">0 && $"var_l"===0)
                                .select("id_fix").distinct()
    val exactDupIdsNew = dupNorm.filter($"origin_norm"==="new"        && $"exact_l">0 && $"var_l"===0)
                                .select("id_fix").distinct()

    val variedDupIdsRef = dupNorm.filter($"origin_norm"==="reference" && $"var_l">0)
                                 .select("id_fix").distinct()
    val variedDupIdsNew = dupNorm.filter($"origin_norm"==="new"       && $"var_l">0)
                                 .select("id_fix").distinct()

    /* 2) 1:1 exact & variaciones -------------------------------------- */
    val commonCols = refFix.columns.toSet.intersect(newFix.columns.toSet).toSeq
    val exactMatchIds = refFix.select(commonCols.map(col): _*)
                              .intersect(newFix.select(commonCols.map(col): _*))
                              .select("id_fix").distinct()

    val mismatchIds = diffFix.filter($"Results"==="NO_MATCH")
                             .select("id_fix").distinct()

    /* 3) Only-in ------------------------------------------------------- */
    val onlyRefIds = refFix.select("id_fix").except(newFix.select("id_fix"))
    val onlyNewIds = newFix.select("id_fix").except(refFix.select("id_fix"))

    /* 4) utilidades ---------------------------------------------------- */
    def pct(n: Long): String =
      if (totalIdsRef == 0) "-" else "%.1f%%".formatLocal(Locale.US, n.toDouble/totalIdsRef*100)

    def idsToStr(df: DataFrame): String = df.limit(20).as[String].collect().mkString(",")

    /* 5) Construcción en memoria (todo String) ------------------------ */
    val rows = Seq(
      SummaryRow("Exact Duplicates",
        exactDupIdsRef.count().toString,
        exactDupIdsNew.count().toString,
        pct(exactDupIdsRef.count()),
        "Exact",
        idsToStr(exactDupIdsRef.union(exactDupIdsNew).distinct())),

      SummaryRow("Duplicates with Variations (ref)",
        variedDupIdsRef.count().toString, "-",
        pct(variedDupIdsRef.count()),
        "Variations",
        idsToStr(variedDupIdsRef)),

      SummaryRow("Duplicates with Variations (new)",
        "-", variedDupIdsNew.count().toString, "-",
        "Variations",
        idsToStr(variedDupIdsNew)),

      SummaryRow("1:1 Exact Matches",
        exactMatchIds.count().toString,
        exactMatchIds.count().toString,
        pct(exactMatchIds.count()),
        "Match",
        idsToStr(exactMatchIds)),

      SummaryRow("1:1 Matches variations",
        mismatchIds.count().toString,
        mismatchIds.count().toString,
        pct(mismatchIds.count()),
        "No Match",
        idsToStr(mismatchIds)),

      SummaryRow("1:0 (Only in Reference)",
        onlyRefIds.count().toString, "-",
        pct(onlyRefIds.count()),
        "Missing",
        idsToStr(onlyRefIds)),

      SummaryRow("0:1 (Only in New)",
        "-", onlyNewIds.count().toString, "-",
        "Extra",
        idsToStr(onlyNewIds)),

      SummaryRow("Total Records",
        totalRowsRef.toString,
        totalRowsNew.toString,
        "100.0%",
        signed(totalRowsNew - totalRowsRef),
        "-")
    )

    /* 6) Convierte a DF y ordena -------------------------------------- */
    val summaryDf = spark.createDataset(rows).toDF()
      .withColumn("partition_hour", lit(partitionHour))

    summaryDf
      .withColumn("_o",
        when($"Metrica"==="Exact Duplicates",1)
        .when($"Metrica"==="Duplicates with Variations (ref)",2)
        .when($"Metrica"==="Duplicates with Variations (new)",3)
        .when($"Metrica"==="1:1 Exact Matches",4)
        .when($"Metrica"==="1:1 Matches variations",5)
        .when($"Metrica"==="1:0 (Only in Reference)",6)
        .when($"Metrica"==="0:1 (Only in New)",7)
        .otherwise(8))
      .orderBy("_o")
      .drop("_o")
  }
}
