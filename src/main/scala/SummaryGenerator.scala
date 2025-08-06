import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.OutputStream
import org.apache.spark.sql.SaveMode


case class SummaryRow(
  bloque: String,
  metrica: String,
  universo: String,
  numerador: String,
  denominador: String,
  pct: String,
  ejemplos: String
)


  /**
   * Construye el DataFrame de resumen ejecutivo.
   *
   * @param spark          SparkSession
   * @param refDf          DataFrame de referencia (ya agregado)
   * @param newDf          DataFrame nueva versión (ya agregado)
   * @param diffDf         Tabla de diferencias
   * @param dupDf          Tabla de duplicados
   * @param compositeKeyCols Columnas de clave compuesta
   * @param refDfRaw       DataFrame original REF (antes de agregación)
   * @param newDfRaw       DataFrame original NEW (antes de agregación)
   * @param config         CompareConfig con políticas
   */
object SummaryGenerator {

  /** % con un decimal; “-” cuando el denominador es 0 */
  private def pctStr(num: Long, den: Long): String =
    if (den == 0) "-" else f"${num.toDouble / den * 100}%.1f%%"

  /** normaliza strings vacíos a NULL */
  private def nz(c: org.apache.spark.sql.Column) =
    when(trim(c.cast("string")) === "", lit(null)).otherwise(c)

  def generateSummaryTable(
      spark: SparkSession,
      refDf:       DataFrame,
      newDf:       DataFrame,
      diffDf:      DataFrame,
      dupDf:       DataFrame,
      compositeKeyCols: Seq[String],
      refDfRaw:    DataFrame,
      newDfRaw:    DataFrame,
      config: CompareConfig
  ): DataFrame = {

    import spark.implicits._

    // 1) Construcción de ID compuesto a partir de las key columns
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

    // 2) Detección de duplicados por lado
    def dupIds(df: DataFrame) =
      df.groupBy(cid.as("cid"))
        .agg(count(lit(1)).as("cnt"))
        .filter($"cnt" > 1)
        .select($"cid")

    val dupIdsRef      = dupIds(refDf)
    val dupIdsNew      = dupIds(newDf)
    val dupIdsBoth     = dupIdsRef.intersect(dupIdsNew)
    val dupIdsOnlyRef  = dupIdsRef.except(dupIdsNew)
    val dupIdsOnlyNew  = dupIdsNew.except(dupIdsRef)
    val dupIdsAny      = dupIdsRef.union(dupIdsNew).distinct()

    // 3) Exact / Variations en BOTH basados en diffDf
    val diffAgg = diffDf.groupBy($"id")
      .agg(
        max(when(lower($"results") === "no_match", 1).otherwise(0)).as("has_nm"),
        max(when(lower($"results").isin("only_in_ref","only_in_new"),1).otherwise(0)).as("has_only")
      )
      .withColumn("has_diff", greatest($"has_nm", $"has_only"))
      .select($"id".as("cid"), $"has_diff")

    val idsVariations = diffAgg.filter($"has_diff" === 1).select($"cid").intersect(idsBoth)
    val idsExact      = idsBoth.except(idsVariations)

    // 4) Quality global: exact matches sin duplicados en REF o NEW
    val qualityIds = idsExact.except(dupIdsAny)
    val qualityOk  = qualityIds.count()

    // 5) Helper para ejemplos
    def idsToStr(df: DataFrame, limit: Int = 6) =
      df.orderBy($"cid").limit(limit).as[String].collect().mkString(",")

    // 6) Constructor de fila
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

    // 7) Secuencia de métricas (sin Overcoverage)
    val rows = Seq(
      // KPIS básicos
      row("KPIS", "IDs Uniques",           "REF",  nRefIds,     0, ""           ),
      row("KPIS", "IDs Uniques",           "NEW",  nNewIds,     0, ""           ),
      row("KPIS", "Total REF",             "ROWS", totalRowsRef, 0, ""           ),
      row("KPIS", "Total NEW",             "ROWS", totalRowsNew, 0, ""           ),
      row("KPIS", "Total (NEW-REF)",       "ROWS", totalRowsNew - totalRowsRef, totalRowsRef, "" ),
      row("KPIS", "Quality global",        "REF",  qualityOk,   nRefIds,     "" ),

      // Comparación 1:1 en intersección
      row("MATCH",    "1:1 (exact matches)",      "BOTH", idsExact.count(),      nBothIds,     idsToStr(idsExact)     ),
      row("NO MATCH", "1:1 (match not identical)", "BOTH", idsVariations.count(), nBothIds,     idsToStr(idsVariations)),

      // GAP
      row("GAP", "1:0 (only in reference)", "REF",  idsOnlyR.count(), nRefIds, idsToStr(idsOnlyR)),
      row("GAP", "0:1 (only in new)",       "NEW",  idsOnlyN.count(), nNewIds, idsToStr(idsOnlyN)),

      // Duplicados
      row("DUPS", "duplicates (both)", "BOTH", dupIdsBoth.count(),    nBothIds,    idsToStr(dupIdsBoth)   ),
      row("DUPS", "duplicates (ref)",  "REF",  dupIdsOnlyRef.count(), nRefIds,     idsToStr(dupIdsOnlyRef) ),
      row("DUPS", "duplicates (new)",  "NEW",  dupIdsOnlyNew.count(), nNewIds,     idsToStr(dupIdsOnlyNew) )
    )

    spark.createDataset(rows).toDF()
  }

  /** Exporta el DataFrame de resumen a un archivo Excel en cualquier filesystem compatible con Hadoop (p.ej. S3) */
 /** Exportar DataFrame a Excel en S3 u HDFS usando spark-excel */
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


