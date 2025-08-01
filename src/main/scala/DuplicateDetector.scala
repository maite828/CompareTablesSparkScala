import org.apache.spark.sql.{DataFrame, SparkSession, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Fila de salida de la tabla de duplicados */
case class DuplicateOut(
  origin: String,                 // "Ref" | "New"
  id: String,                     // valor de la primera clave (o "NULL")
  exact_duplicates: String,       // total - countDistinct(hash)
  duplicates_w_variations: String,// max(countDistinct(hash) - 1, 0)
  occurrences: String,            // total del grupo
  variations: String,             // campo: [v1,v2]|...
  partition_hour: String
)

object DuplicateDetector {

  /** Detecta duplicados exactos y con variaciones en refDf y newDf */
  def detectDuplicatesTable(
      spark: SparkSession,
      refDf: DataFrame,
      newDf: DataFrame,
      compositeKeyCols: Seq[String],
      partitionHour: String
  ): DataFrame = {

    import spark.implicits._

    /* 0) Unir ambos DF y etiquetar origen -------------------------------- */
    val withSrc = refDf.withColumn("_src", lit("ref"))
      .unionByName(newDf.withColumn("_src", lit("new")))

    val nonKeyCols = withSrc.columns
      .filterNot(c => c == "_src" || compositeKeyCols.contains(c))

    /* 1) Hash de la fila (sin _src) para duplicados exactos --------------- */
    val rowHash = sha2(
      concat_ws("§", withSrc.columns
        .filterNot(_ == "_src")
        .map(c => coalesce(col(c).cast("string"), lit("__NULL__"))): _*),
      256)

    val hashed = withSrc.withColumn("_row_hash", rowHash)

    /* 2) Agregación por (_src + compositeKey) ----------------------------- */
    val aggExprs =
      Seq(
        count(lit(1)).as("occurrences"),
        (count(lit(1)) - countDistinct("_row_hash")).as("exact_dup"),
        (greatest(lit(0), countDistinct("_row_hash") - lit(1))).as("var_dup")
      ) ++ nonKeyCols.map { c =>
        // guardamos *todas* las variaciones, null incluido como "__NULL__"
        collect_set(coalesce(col(c).cast("string"), lit("__NULL__"))).as(s"${c}_set")
      }

    val grouped = hashed
      .groupBy($"_src" +: compositeKeyCols.map(col): _*)
      .agg(aggExprs.head, aggExprs.tail: _*)
      .filter($"occurrences" > 1)          // sólo donde hay duplicados

    /* 3) Mapear al case-class -------------------------------------------- */
    val ds = grouped.map { r =>
      val origin = if (r.getAs[String]("_src") == "ref") "Ref" else "New"

      val idVal = Option(r.getAs[Any](compositeKeyCols.head)).orNull
      val idStr = if (idVal == null) "NULL" else idVal.toString

      // Variaciones por campo (más de 1 valor distinto ≠ __NULL__)
      val variationParts: Seq[String] = nonKeyCols.flatMap { c =>
        val vs: Seq[String] = Option(r.getAs[Seq[String]](s"${c}_set"))
          .getOrElse(Seq.empty[String])
          .filterNot(_ == "__NULL__")
          .distinct
        if (vs.size > 1) Some(s"$c: [${vs.mkString(",")}]") else None
      }.toSeq

      val variationsStr = if (variationParts.isEmpty) "-" 
                          else variationParts.mkString(" | ")

      DuplicateOut(
        origin               = origin,
        id                   = idStr,
        exact_duplicates     = r.getAs[Long]("exact_dup").toString,
        duplicates_w_variations = r.getAs[Long]("var_dup").toString,
        occurrences          = r.getAs[Long]("occurrences").toString,
        variations           = variationsStr,
        partition_hour       = partitionHour
      )
    }(Encoders.product[DuplicateOut])

    ds.toDF()
  }
}
