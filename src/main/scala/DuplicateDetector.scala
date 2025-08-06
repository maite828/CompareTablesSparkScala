// src/main/scala/com/example/compare/DuplicateDetector.scala

import org.apache.spark.sql.{DataFrame, SparkSession, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class DuplicateOut(
  origin: String,
  id: String,
  exact_duplicates: String,
  duplicates_w_variations: String,
  occurrences: String,
  variations: String
)

object DuplicateDetector {

  def detectDuplicatesTable(
      spark: SparkSession,
      refDf: DataFrame,
      newDf: DataFrame,
      compositeKeyCols: Seq[String],
      config: CompareConfig
  ): DataFrame = {
    import spark.implicits._

    // unir y taguear
    val withSrc = refDf.withColumn("_src", lit("ref"))
      .unionByName(newDf.withColumn("_src", lit("new")))

    val nonKeys = withSrc.columns.filterNot(c => c=="_src" || compositeKeyCols.contains(c))

    // hash de fila
    val rowHash = sha2(
      concat_ws("§", withSrc.columns.filterNot(_=="_src").map(c => coalesce(col(c).cast("string"), lit("__NULL__"))): _*),
      256)

    val hashed = withSrc.withColumn("_row_hash", rowHash)

    // agg
    val aggExprs = Seq(
      count(lit(1)).as("occurrences"),
      (count(lit(1)) - countDistinct("_row_hash")).as("exact_dup"),
      greatest(lit(0), countDistinct("_row_hash") - lit(1)).as("var_dup")
    ) ++ nonKeys.map(c => collect_set(coalesce(col(c).cast("string"), lit("__NULL__"))).as(s"${c}_set"))

    val grouped = hashed.groupBy(($"_src" +: compositeKeyCols.map(col)):_*)
      .agg(aggExprs.head, aggExprs.tail:_*)
      .filter($"occurrences" > 1)

    grouped.map { r =>
      val origin = r.getAs[String]("_src")
      val cid = compositeKeyCols.map(k => Option(r.getAs[Any](k)).map(_.toString).getOrElse("NULL")).mkString("_")
      val vars = nonKeys.flatMap { c =>
        val vs = Option(r.getAs[Seq[String]](s"${c}_set")).getOrElse(Seq.empty)
          .filterNot(_=="__NULL__").distinct
        if (vs.size>1) Some(s"$c: [${vs.mkString(",")}]") else None
      }
      DuplicateOut(
        origin,
        cid,
        r.getAs[Long]("exact_dup").toString,
        r.getAs[Long]("var_dup").toString,
        r.getAs[Long]("occurrences").toString,
        if (vars.isEmpty) "-" else vars.mkString(" | ")
      )
    }(Encoders.product[DuplicateOut]).toDF()
  }
}
