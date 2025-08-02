import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DiffGenerator {

  /** Devuelve la primera cadena no vacía o "-" si es null / vacío */
  private def formatString(c: Column): Column =
    when(trim(c) === "" || c.isNull, lit("-")).otherwise(c.cast("string"))

  /** Genera la tabla customer_differences (todo en minúsculas) */
  def generateDifferencesTable(
      spark: SparkSession,
      refDf: DataFrame,
      newDf: DataFrame,
      compositeKeyCols: Seq[String],
      compareColsIn: Seq[String],
      partitionHour: String,
      includeEquals: Boolean
  ): DataFrame = {

    import spark.implicits._

    /* 0) Sin filtrar claves nulas ------------------------------------- */
    val refClean = refDf
    val newClean = newDf

    /* 1) Columnas a comparar (sin clave) ------------------------------ */
    val compareCols = compareColsIn.filterNot(compositeKeyCols.contains)

    /* 2) Agregación determinista por clave ---------------------------- */
    def aggPerKey(df: DataFrame): DataFrame = {
      val aggs = compareCols.map { c =>
        df.schema(c).dataType match {
          case _: NumericType => max(col(c)).as(c)
          case _: BooleanType => max(col(c)).as(c)
          case _              => first(col(c), ignoreNulls = true).as(c)
        }
      }
      df.groupBy(compositeKeyCols.map(col): _*)
        .agg(aggs.head, aggs.tail: _*)
        .withColumn("_present", lit(1))
    }

    val refAgg = aggPerKey(refClean).alias("ref")
    val newAgg = aggPerKey(newClean).alias("new")

    /* 3) Full-outer join NULL-safe (<=>) ------------------------------ */
    val joinCond = compositeKeyCols
      .map(c => col(s"ref.$c") <=> col(s"new.$c"))
      .reduce(_ && _)

    val joined = refAgg.join(newAgg, joinCond, "fullouter")
      .withColumn("exists_ref", col("ref._present").isNotNull)
      .withColumn("exists_new", col("new._present").isNotNull)

    /* 4) Struct resultado por columna --------------------------------- */
    val diffsPerCol = compareCols.map { c =>
      val refCol = col(s"ref.$c")
      val newCol = col(s"new.$c")

      val result = when(!$"exists_new", lit("ONLY_IN_REF"))
        .when(!$"exists_ref",         lit("ONLY_IN_NEW"))
        .when(refCol <=> newCol,      lit("MATCH"))
        .otherwise                   (lit("NO_MATCH"))

      val idExpr = {
        val base =
          if (compositeKeyCols.length == 1)
            coalesce(col(s"ref.${compositeKeyCols.head}"),
                     col(s"new.${compositeKeyCols.head}"))
          else {
            val refKey = concat_ws("|", compositeKeyCols.map(k => col(s"ref.$k").cast("string")): _*)
            val newKey = concat_ws("|", compositeKeyCols.map(k => col(s"new.$k").cast("string")): _*)
            coalesce(refKey, newKey)
          }
        coalesce(base.cast("string"), lit("NULL"))
      }

      struct(
        idExpr.as("id"),
        lit(c).as("column"),           // ← minúsculas
        formatString(refCol).as("value_ref"),
        formatString(newCol).as("value_new"),
        result.as("results")           // ← minúsculas
      )
    }

    /* 5) Explode + partition_hour ------------------------------------- */
    val exploded = joined
      .select(array(diffsPerCol: _*).as("diffs"))
      .withColumn("d", explode($"diffs"))
      .select("d.*")
      .withColumn("partition_hour", lit(partitionHour))

    /* 6) Incluir / excluir MATCH -------------------------------------- */
    if (includeEquals) exploded
    else               exploded.filter($"results" =!= "MATCH")
  }
}
