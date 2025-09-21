// src/main/scala/DiffGenerator.scala
// (sin package)

import java.util.Locale
import org.apache.spark.sql.{DataFrame, SparkSession, Column, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import CompareConfig._

object DiffGenerator {

  /** Formatea valores a string respetando la escala de DecimalType. */
  private def formatValue(c: Column, dt: DataType): Column = dt match {
    case _: DecimalType =>
      val s = c.cast(StringType)
      when(s.isNull || trim(s) === "", lit("-")).otherwise(s)
    case _ =>
      val s = c.cast(StringType)
      when(s.isNull || trim(s) === "", lit("-")).otherwise(s)
  }

  /** Representación canónica determinista para comparaciones. */
  private def canonicalize(c: Column, dt: DataType): Column = dt match {
    case _: NumericType   => c
    case _: BooleanType   => c
    case _: DateType      => c
    case _: TimestampType => c
    case _: StringType    => when(c.isNull, lit(null).cast(StringType)).otherwise(c.cast(StringType))
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

  /** Construye la struct para EXACT_MATCH con formateo fijo. */
  private def buildExactMatchStruct(keyCols: Seq[String]): Column = {
    val cid = concat_ws("_", keyCols.map { k =>
      val v = coalesce(col(s"ref.$k"), col(s"new.$k"))
      when(v.isNull, lit("NULL")).otherwise(v.cast(StringType))
    }: _*)
    struct(
      cid.as("id"),
      lit("ALL_COLUMNS").as("column"),
      lit("ALL_MATCH").as("value_ref"),
      lit("ALL_MATCH").as("value_new"),
      lit("EXACT_MATCH").as("results")
    )
  }

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
    val norm: Column => Column = c => when(trim(c.cast(StringType)) === "", lit(null)).otherwise(c)
    val nRef = compositeKeyCols.foldLeft(refDf)((df, k) => df.withColumn(k, norm(col(k))))
    val nNew = compositeKeyCols.foldLeft(newDf)((df, k) => df.withColumn(k, norm(col(k))))

    // 2) Filtrar columnas constantes (comunes a ambos)
    val baseCols   = compareColsIn.filterNot(compositeKeyCols.contains).distinct
    val commonCols = nRef.columns.toSet.intersect(nNew.columns.toSet)
    val consts     = commonCols.filter { c =>
      baseCols.contains(c) && isConstantColumn(nRef, c) && isConstantColumn(nNew, c)
    }
    if (consts.nonEmpty) println(s"[INFO] Excluyendo columnas constantes: ${consts.mkString(",")}")
    val compareCols = baseCols.filterNot(consts.contains)

    // 3) Pre-orden determinista si hay priorityCol
    def preOrdered(df: DataFrame): DataFrame = config.priorityCol match {
      case Some(colName: String) if df.columns.contains(colName) =>
        val w = Window.partitionBy(compositeKeyCols.map(col): _*)
                      .orderBy(col(colName).desc_nulls_last)
        df.withColumn("_rn", row_number().over(w)).filter($"_rn" === 1).drop("_rn")
      case _ => df
    }
    val refBase = preOrdered(nRef)
    val newBase = preOrdered(nNew)

    // 4) Agregación con overrides (por nombre en minúsculas)
    val aggs: Seq[Column] = compareCols.map { c =>
      val dt    = refBase.schema(c).dataType
      val canon = canonicalize(col(c), dt)
      config.aggOverrides
        .getOrElse(c, "")
        .toLowerCase(Locale.ROOT) match {
          case "max"             => max(canon.cast(dt)).as(c)
          case "min"             => min(canon.cast(dt)).as(c)
          case "first_non_null" | "firstnonnull" | "first" =>
            first(col(c), ignoreNulls = true).as(c)
          case _ =>
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

    // 5) Tipos post-agregación
    val dtMap: Map[String, DataType] =
      refAgg.schema.filter(f => compareCols.contains(f.name)).map(f => f.name -> f.dataType).toMap

    // 6) Full outer join con política de nulos
    val joinCond = compositeKeyCols.map { k =>
      val l = col(s"ref.$k"); val r = col(s"new.$k")
      if (config.nullKeyMatches) l <=> r else (l.isNotNull && r.isNotNull && l === r)
    }.reduce(_ && _)

    val joined = refAgg.alias("ref")
      .join(newAgg.alias("new"), joinCond, "fullouter")
      .withColumn("exists_ref", col("ref._present").isNotNull)
      .withColumn("exists_new", col("new._present").isNotNull)

    // Calcular si el registro es exact match
    val exactMatchCondition = compareCols.map { c =>
      col(s"ref.$c") <=> col(s"new.$c")
    }.reduce(_ && _) && col("exists_ref") && col("exists_new")
    val joinedWithExact = joined.withColumn("is_exact_match", exactMatchCondition)

    // 7) Explode diferencias
    val diffs = when(col("is_exact_match"), array(buildExactMatchStruct(compositeKeyCols)))
      .otherwise(array(compareCols.map(c => buildDiffStruct(compositeKeyCols, c, dtMap(c))): _*))
    val exploded = joinedWithExact
      .select(diffs.as("diffs"))
      .withColumn("diff", explode($"diffs"))
      .select("diff.*")

    if (includeEquals) exploded else exploded.filter(!$"results".isin("MATCH", "EXACT_MATCH"))
  }

  /** Construye la struct de diff con formateo fiel por tipo. */
  private def buildDiffStruct(
      keyCols: Seq[String],
      colName: String,
      dt: DataType
  ): Column = {
    val refCol = col(s"ref.$colName")
    val newCol = col(s"new.$colName")

    val result = when(!col("exists_new"), lit("ONLY_IN_NEW"))
      .when(!col("exists_ref"), lit("ONLY_IN_REF"))
      .when(refCol <=> newCol, lit("MATCH"))
      .otherwise(lit("NO_MATCH"))

    val cid = concat_ws("_", keyCols.map { k =>
      val v = coalesce(col(s"ref.$k"), col(s"new.$k"))
      when(v.isNull, lit("NULL")).otherwise(v.cast(StringType))
    }: _*)

    struct(
      cid.as("id"),
      lit(colName).as("column"),
      formatValue(refCol, dt).as("value_ref"),
      formatValue(newCol, dt).as("value_new"),
      result.as("results")
    )
  }
}