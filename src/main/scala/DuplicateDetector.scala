import org.apache.spark.sql.{DataFrame, SparkSession, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Fila de salida de la tabla customer_duplicates (todo en minúsculas) */
case class DuplicateOut(
  origin: String,                 // "ref" | "new"
  id: String,                     // primer campo clave (o "NULL")
  exact_duplicates: String,       // total - countDistinct(hash)
  duplicates_w_variations: String,// max(countDistinct(hash) - 1, 0)
  occurrences: String,            // total del grupo
  variations: String             // campo: [v1,v2] | ...
)

object DuplicateDetector {

  /** Detecta duplicados exactos y con variaciones */
  def detectDuplicatesTable(
      spark: SparkSession,
      refDf: DataFrame,
      newDf: DataFrame,
      compositeKeyCols: Seq[String]
  ): DataFrame = {

    import spark.implicits._

    /* 0) Unir DF y etiquetar origen ----------------------------------- */
    val withSrc = refDf.withColumn("_src", lit("ref"))
      .unionByName(newDf.withColumn("_src", lit("new")))

    val nonKeyCols = withSrc.columns
      .filterNot(c => c == "_src" || compositeKeyCols.contains(c))

    /* 1) Hash de la fila (sin _src) ------------------------------------ */
    val rowHash = sha2(
      concat_ws("§",
        withSrc.columns
          .filterNot(_ == "_src")
          .map(c => coalesce(col(c).cast("string"), lit("__NULL__"))): _*),
      256)

    val hashed = withSrc.withColumn("_row_hash", rowHash)

    /* 2) Agregación por (_src + clave) --------------------------------- */
    val aggExprs =
      Seq(
        count(lit(1)).as("occurrences"),
        (count(lit(1)) - countDistinct("_row_hash")).as("exact_dup"),
        (greatest(lit(0), countDistinct("_row_hash") - lit(1))).as("var_dup")
      ) ++ nonKeyCols.map { c =>
        collect_set(coalesce(col(c).cast("string"), lit("__NULL__"))).as(s"${c}_set")
      }

    val grouped = hashed
      .groupBy($"_src" +: compositeKeyCols.map(col): _*)
      .agg(aggExprs.head, aggExprs.tail: _*)
      .filter($"occurrences" > 1)            // sólo grupos duplicados

    /* 3) Mapear al case-class y devolver DF ---------------------------- */
    grouped.map { r =>
      val origin = r.getAs[String]("_src")          // "ref" | "new"

      val idVal  = Option(r.getAs[Any](compositeKeyCols.head)).orNull
      val idStr  = if (idVal == null) "NULL" else idVal.toString

      val variationParts = nonKeyCols.flatMap { c =>
        val vs = Option(r.getAs[Seq[String]](s"${c}_set"))
          .getOrElse(Seq.empty)
          .filterNot(_ == "__NULL__")
          .distinct
        if (vs.size > 1) Some(s"$c: [${vs.mkString(",")}]") else None
      }

      DuplicateOut(
        origin                   = origin,
        id                       = idStr,
        exact_duplicates         = r.getAs[Long]("exact_dup").toString,
        duplicates_w_variations  = r.getAs[Long]("var_dup").toString,
        occurrences              = r.getAs[Long]("occurrences").toString,
        variations               = variationParts.mkString(" | ").ifEmpty("-")
      )
    }(Encoders.product[DuplicateOut]).toDF()
  }

  /** Extension: devuelve "-" si la cadena está vacía */
  private implicit class StrOps(val s: String) extends AnyVal {
    def ifEmpty(repl: String): String = if (s.isEmpty) repl else s
  }
}
