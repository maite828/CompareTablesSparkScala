// src/main/scala/DiffGenerator.scala

import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import AggType._   // para MaxAgg, MinAgg, FirstNonNullAgg
import CompareConfig._  // para el case class

object DiffGenerator {

  private def formatString(c: Column): Column = {
    val s = c.cast("string")
    when(s.isNull || trim(s) === "", lit("-")).otherwise(s)
  }

  /** Representación canónica determinista para TODAS las comparaciones. */
  private def canonicalize(c: Column, dt: DataType): Column = dt match {
    case _: NumericType   => c
    case _: BooleanType   => c
    case _: DateType      => c
    case _: TimestampType => c
    case _: StringType    => when(c.isNull, lit(null)).otherwise(trim(c.cast("string")))
    case mt: MapType      =>
      val sorted = array_sort(map_entries(c))
      to_json(map_from_entries(sorted))
    case _: ArrayType  => to_json(c)
    case _: StructType => to_json(c)
    case BinaryType    => when(c.isNull, lit(null)).otherwise(encode(c.cast("binary"), "base64"))
    case _             => to_json(c)
  }

  private def isConstantColumn(df: DataFrame, colName: String): Boolean =
    df.select(col(colName)).distinct().limit(2).count() <= 1

  def generateDifferencesTable(
      spark: SparkSession,
      refDf: DataFrame,
      newDf: DataFrame,
      compositeKeyCols: Seq[String],
      compareColsIn: Seq[String],
      includeEquals: Boolean,
      config: CompareConfig
  ): DataFrame = {
    import spark.implicits._

    // 1) Normalizar claves vacías a NULL
    val norm: Column => Column =
      c => when(trim(c.cast("string")) === "", lit(null)).otherwise(c)
    val nRef = compositeKeyCols.foldLeft(refDf)((df, k) => df.withColumn(k, norm(col(k))))
    val nNew = compositeKeyCols.foldLeft(newDf)((df, k) => df.withColumn(k, norm(col(k))))

    // 2) Filtrar columnas constantes
    val baseCols   = compareColsIn.filterNot(compositeKeyCols.contains).distinct
    val commonCols = nRef.columns.toSet.intersect(nNew.columns.toSet)
    val consts     = commonCols.filter { c =>
      baseCols.contains(c) &&
      isConstantColumn(nRef, c) &&
      isConstantColumn(nNew, c)
    }
    if (consts.nonEmpty)
      println(s"[INFO] Excluyendo columnas constantes: ${consts.mkString(",")}")
    val compareCols = baseCols.filterNot(consts.contains)

    // 3) Pre-orden determinista si hay priorityCol
    def preOrdered(df: DataFrame): DataFrame = config.priorityCol match {
      case Some(prio) if df.columns.contains(prio) =>
        val w = Window.partitionBy(compositeKeyCols.map(col): _*)
                      .orderBy(col(prio).desc_nulls_last)
        df.withColumn("_rn", row_number().over(w))
          .filter($"_rn" === 1)
          .drop("_rn")
      case _ =>
        df
    }
    val refBase = preOrdered(nRef)
    val newBase = preOrdered(nNew)

    // 4) Construir expresiones de agregación usando aggOverrides
    val aggs: Seq[Column] = compareCols.map { c =>
      val dt    = refBase.schema(c).dataType
      val canon = canonicalize(col(c), dt)
      config.aggOverrides.get(c) match {
        case Some(MaxAgg)          => max(canon.cast(dt)).as(c)
        case Some(MinAgg)          => min(canon.cast(dt)).as(c)
        case Some(FirstNonNullAgg) => first(col(c), ignoreNulls = true).as(c)
        case None =>
          dt match {
            case _: NumericType | _: BooleanType | _: DateType | _: TimestampType =>
              max(canon.cast(dt)).as(c)
            case _ =>
              max(canon).as(c)
          }
      }
    }

    val refAgg = refBase
      .groupBy(compositeKeyCols.map(col): _*)
      .agg(aggs.head, aggs.tail: _*)
      .withColumn("_present", lit(1))

    val newAgg = newBase
      .groupBy(compositeKeyCols.map(col): _*)
      .agg(aggs.head, aggs.tail: _*)
      .withColumn("_present", lit(1))

    // 5) Join fullouter con política nullKeyMatches
    val joinCond = compositeKeyCols.map { k =>
      val l = col(s"ref.$k"); val r = col(s"new.$k")
      if (config.nullKeyMatches) l <=> r else (l.isNotNull && r.isNotNull && l === r)
    }.reduce(_ && _)

    val joined = refAgg.alias("ref")
      .join(newAgg.alias("new"), joinCond, "fullouter")
      .withColumn("exists_ref", col("ref._present").isNotNull)
      .withColumn("exists_new", col("new._present").isNotNull)

    // 6) Explode diferencias
    val diffs = compareCols.map(c => buildDiffStruct(compositeKeyCols, c))
    val exploded = joined
      .select(array(diffs: _*).as("diffs"))
      .withColumn("diff", explode($"diffs"))
      .select("diff.*")

    if (includeEquals) exploded else exploded.filter($"results" =!= "MATCH")
  }

  private def buildDiffStruct(keyCols: Seq[String], colName: String): Column = {
    val refCol = col(s"ref.$colName")
    val newCol = col(s"new.$colName")

    val result =
      when(!col("exists_new"), lit("ONLY_IN_REF"))
        .when(!col("exists_ref"), lit("ONLY_IN_NEW"))
        .when(refCol <=> newCol, lit("MATCH"))
        .otherwise(lit("NO_MATCH"))

    val cid = concat_ws("_", keyCols.map { k =>
      val v = coalesce(col(s"ref.$k"), col(s"new.$k"))
      when(v.isNull, lit("NULL")).otherwise(v.cast("string"))
    }: _*)

    struct(
      cid.as("id"),
      lit(colName).as("column"),
      formatString(refCol).as("value_ref"),
      formatString(newCol).as("value_new"),
      result.as("results")
    )
  }
}
