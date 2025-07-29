import org.apache.spark.sql.{DataFrame, SparkSession}
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
    val totalCombined = totalRef + totalNew

    // IDs duplicados
    val exactDupIdsRef = dupDf.filter($"origin" === "Referencia" && $"exact_duplicates" > 0).select("id").distinct()
    val exactDupIdsNew = dupDf.filter($"origin" === "Nuevos" && $"exact_duplicates" > 0).select("id").distinct()

    val variedDupIdsRef = dupDf.filter($"origin" === "Referencia" && $"varied_duplicates" > 0).select("id").distinct()
    val variedDupIdsNew = dupDf.filter($"origin" === "Nuevos" && $"varied_duplicates" > 0).select("id").distinct()

    // IDs con diferencias
    val diffIds = diffDf.filter($"Resultado" === "DIFERENCIA").select("id").distinct()

    // IDs únicos
    val onlyInRefIds = diffDf.filter($"Resultado" === "ONLY_IN_REF").select("id").distinct()
    val onlyInNewIds = diffDf.filter($"Resultado" === "ONLY_IN_NEW").select("id").distinct()

    // Para calcular exactos, quitamos los que aparecen como diferencias o duplicados
    val conflictingIds = diffIds.union(onlyInRefIds).union(onlyInNewIds)
      .union(exactDupIdsRef).union(exactDupIdsNew)
      .union(variedDupIdsRef).union(variedDupIdsNew)
      .distinct()

    val allRefIds = refDf.select("id").distinct()
    val exactMatchIds = allRefIds.except(conflictingIds)
    val exactMatchCount = exactMatchIds.count()

    def pct(n: Long): String =
      if (totalRef == 0) "-" else "%.1f%%".formatLocal(Locale.US, n.toDouble / totalRef * 100)

    val totalPct = if (totalCombined == 0) "-" else "%.1f%%".formatLocal(Locale.US, totalRef.toDouble / totalCombined * 100)
    val deltaTotal = totalNew - totalRef
    val deltaLabel = if (deltaTotal > 0) s"+$deltaTotal" else deltaTotal.toString

    val summaryData = Seq(
      ("Total registros", totalRef.toString, totalNew.toString, totalPct, deltaLabel, "-"),
      ("Registros 1:1 exactos (Match)", exactMatchCount.toString, exactMatchCount.toString, pct(exactMatchCount), "Match", exactMatchIds.selectExpr("CAST(id AS STRING)").as[String].collect().mkString(",")),
      ("Registros 1:1 con diferencias", diffIds.count().toString, diffIds.count().toString, pct(diffIds.count()), "No Match", diffIds.selectExpr("CAST(id AS STRING)").as[String].collect().mkString(",")),
      ("Registros 1:0", onlyInRefIds.count().toString, "-", pct(onlyInRefIds.count()), "Falta", onlyInRefIds.selectExpr("CAST(id AS STRING)").as[String].collect().mkString(",")),
      ("Registros 0:1", "-", onlyInNewIds.count().toString, "-", "Sobra", onlyInNewIds.selectExpr("CAST(id AS STRING)").as[String].collect().mkString(",")),
      ("Duplicados exactos", exactDupIdsRef.count().toString, exactDupIdsNew.count().toString, pct(exactDupIdsRef.count()), "Exactos", exactDupIdsRef.selectExpr("CAST(id AS STRING)").as[String].collect().mkString(",")),
      ("Duplicados con variaciones (ref)", variedDupIdsRef.count().toString, "-", pct(variedDupIdsRef.count()), "Diferencias", variedDupIdsRef.selectExpr("CAST(id AS STRING)").as[String].collect().mkString(",")),
      ("Duplicados con variaciones (new)", "-", variedDupIdsNew.count().toString, "-", "Diferencias", variedDupIdsNew.selectExpr("CAST(id AS STRING)").as[String].collect().mkString(","))
    ).toDF("Metrica", "count_Ref", "count_New", "pct_Ref", "Status", "Ejemplos")
     .withColumn("partition_hour", lit(partitionHour))

    summaryData
  }
}
