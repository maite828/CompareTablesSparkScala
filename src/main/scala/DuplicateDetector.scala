// src/main/scala/DuplicateDetector.scala
// (sin package)

import org.apache.spark.sql.{DataFrame, SparkSession, Encoders, Column, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import CompareConfig._

/** Fila de salida de la tabla de duplicados */
case class DuplicateOut(
  origin: String,            // "ref" | "new"
  id: String,                // clave compuesta concatenada (o "NULL")
  exact_duplicates: String,  // total - countDistinct(hash)
  dups_w_variations: String, // max(countDistinct(hash) - 1, 0)
  occurrences: String,       // total del grupo
  variations: String         // "col: [v1,v2] | ..."
)

object DuplicateDetector {

  /** Detecta duplicados exactos y con variaciones por origen y clave compuesta. */
  def detectDuplicatesTable(
      spark: SparkSession,
      refDf: DataFrame,
      newDf: DataFrame,
      compositeKeyCols: Seq[String],
      config: CompareConfig
  ): DataFrame = {
    import spark.implicits._

    // 0) Normalizar claves vacías a NULL para agrupar correctamente
    val normKey: Column => Column = c => when(trim(c.cast(StringType)) === "", lit(null)).otherwise(c)
    val nRef = compositeKeyCols.foldLeft(refDf)((df, k) => df.withColumn(k, normKey(col(k))))
    val nNew = compositeKeyCols.foldLeft(newDf)((df, k) => df.withColumn(k, normKey(col(k))))

    // 1) Unir y etiquetar origen
    val withSrc = nRef.withColumn("_src", lit("ref"))
      .unionByName(nNew.withColumn("_src", lit("new")))

    // 2) Si hay priorityCol, quedarnos con fila ranking = 1 por (_src + claves)
    val base = {
      val maybeCol = config.priorityCol.flatMap { c =>
        Option(c).map(_.trim).filter(_.nonEmpty).filter(withSrc.columns.contains)
      }
      maybeCol match {
        case Some(prio) =>
          val w = Window
            .partitionBy(("_src" +: compositeKeyCols).map(col): _*)
            .orderBy(col(prio).desc_nulls_last)
          withSrc.withColumn("_rn", row_number().over(w))
                 .filter(col("_rn") === 1)
                 .drop("_rn")
        case None => withSrc
      }
    }

    // 3) Columnas no clave (excluye _src)
    val nonKeyCols = base.columns.filterNot(c => c == "_src" || compositeKeyCols.contains(c))

    // 4) Hash estable por fila (sin _src), ordenando columnas para evitar variaciones
    val colsForHash = base.columns.filter(_ != "_src").sorted
    val hashCol = sha2(
      concat_ws("§", colsForHash.map { c =>
        coalesce(col(c).cast(StringType), lit("__NULL__"))
      }: _*),
      256
    )
    val hashed = base.withColumn("_row_hash", hashCol)

    // 5) Agregación por (_src + claves)
    val aggExprs = Seq(
      count(lit(1)).as("occurrences"),
      (count(lit(1)) - countDistinct(col("_row_hash"))).as("exact_dup"),
      greatest(lit(0), countDistinct(col("_row_hash")) - lit(1)).as("var_dup")
    ) ++ nonKeyCols.map { c =>
      collect_set(coalesce(col(c).cast(StringType), lit("__NULL__"))).as(s"${c}_set")
    }

    val grouped = hashed
      .groupBy(("_src" +: compositeKeyCols).map(col): _*)
      .agg(aggExprs.head, aggExprs.tail: _*)
      .filter(col("occurrences") > 1)

    // 6) Selección de columnas base + sets de variaciones
    val baseCols: Seq[Column] = Seq(
      col("_src").as("origin"),
      concat_ws("_", compositeKeyCols.map(k => coalesce(col(k).cast(StringType), lit("NULL"))): _*).as("id"),
      col("exact_dup"),
      col("var_dup"),
      col("occurrences")
    )
    val variationCols: Seq[Column] = nonKeyCols.map(c => col(s"${c}_set"))

    // 7) Map a DuplicateOut con construcción de texto de variaciones
    grouped
      .select((baseCols ++ variationCols): _*)
      .map { row =>
        val origin = row.getAs[String]("origin")
        val id     = row.getAs[String]("id")
        val exact  = row.getAs[Long]("exact_dup").toString
        val varV   = row.getAs[Long]("var_dup").toString
        val occ    = row.getAs[Long]("occurrences").toString

        val vars = nonKeyCols.flatMap { c =>
          val setColName = s"${c}_set"
          val seqVals = Option(row.getAs[Seq[String]](setColName)).getOrElse(Seq.empty)
            .filterNot(_ == "__NULL__").distinct
          if (seqVals.size > 1) Some(s"$c: [${seqVals.mkString(",")}]") else None
        }.mkString(" | ")

        val variationsText = if (vars.isEmpty) "-" else vars
        DuplicateOut(origin, id, exact, varV, occ, variationsText)
      }(Encoders.product[DuplicateOut])
      .toDF()
  }
}
