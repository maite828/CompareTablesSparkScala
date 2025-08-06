// src/main/scala/DuplicateDetector.scala
import org.apache.spark.sql.{DataFrame, SparkSession, Encoders}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import CompareConfig._

/**
 * Fila de salida de la tabla customer_duplicates
 */
case class DuplicateOut(
  origin: String,                 // "ref" | "new"
  id: String,                     // compuesto o "NULL"
  exact_duplicates: String,       // total - countDistinct(hash)
  duplicates_w_variations: String,// max(countDistinct(hash) - 1, 0)
  occurrences: String,            // total del grupo
  variations: String              // campo: [v1,v2] | ...
)

object DuplicateDetector {

  /**
   * Detecta duplicados exactos y con variaciones, con política de prioridad si se define.
   */
  def detectDuplicatesTable(
      spark: SparkSession,
      refDf: DataFrame,
      newDf: DataFrame,
      compositeKeyCols: Seq[String],
      config: CompareConfig
  ): DataFrame = {
    import spark.implicits._

    // 1) Unir y etiquetar origen
    val withSrc = refDf.withColumn("_src", lit("ref"))
      .unionByName(newDf.withColumn("_src", lit("new")))

    // 2) Si priorityCol presente, quedarnos con fila ranking = 1
    val base = config.priorityCol match {
      case Some(prio) if withSrc.columns.contains(prio) =>
        val w = Window.partitionBy(("_src" +: compositeKeyCols).map(col): _*)
                   .orderBy(col(prio).desc_nulls_last)
        withSrc.withColumn("_rn", row_number().over(w))
               .filter(col("_rn") === 1)
               .drop("_rn")
      case _ => withSrc
    }

    // 3) Columnas no clave
    val nonKeyCols = base.columns.filterNot(c => c == "_src" || compositeKeyCols.contains(c))

    // 4) Hash de fila sin origen
    val hashCol = sha2(
      concat_ws("§", base.columns.filter(_ != "_src").map(c => coalesce(col(c).cast("string"), lit("__NULL__"))): _*),
      256
    )
    val hashed = base.withColumn("_row_hash", hashCol)

    // 5) Agregación por origen + claves
    val aggExprs = Seq(
      count(lit(1)).as("occurrences"),
      (count(lit(1)) - countDistinct("_row_hash")).as("exact_dup"),
      greatest(lit(0), countDistinct("_row_hash") - lit(1)).as("var_dup")
    ) ++ nonKeyCols.map(c => collect_set(coalesce(col(c).cast("string"), lit("__NULL__"))).as(s"${c}_set"))

    val grouped = hashed
      .groupBy((col("_src") +: compositeKeyCols.map(col)): _*)
      .agg(aggExprs.head, aggExprs.tail: _*)
      .filter(col("occurrences") > 1)

    // 6) Mapear a DuplicateOut
    grouped.map { r =>
      val origin      = r.getAs[String]("_src")
      val compositeId = compositeKeyCols.map(k => Option(r.getAs[Any](k)).map(_.toString).getOrElse("NULL")).mkString("_")
      val variationParts = nonKeyCols.flatMap { c =>
        val vs = Option(r.getAs[Seq[String]](s"${c}_set")).getOrElse(Seq.empty)
                 .filterNot(_ == "__NULL__").distinct
        if (vs.size > 1) Some(s"$c: [${vs.mkString(",")}]") else None
      }
      DuplicateOut(
        origin                   = origin,
        id                       = compositeId,
        exact_duplicates         = r.getAs[Long]("exact_dup").toString,
        duplicates_w_variations  = r.getAs[Long]("var_dup").toString,
        occurrences              = r.getAs[Long]("occurrences").toString,
        variations               = variationParts.mkString(" | ").ifEmpty("-")
      )
    }(Encoders.product[DuplicateOut]).toDF()
  }

  // Extensión simple para cadenas vacías
  private implicit class StrOps(val s: String) extends AnyVal {
    def ifEmpty(repl: String): String = if (s.isEmpty) repl else s
  }
}
