import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DiffGenerator {

  private val NULL_KEY_MATCHES: Boolean = true

  private def formatString(c: Column): Column = {
    val s = c.cast("string")
    when(s.isNull || trim(s) === "", lit("-")).otherwise(s)
  }

  /** Representación canónica determinista para TODAS las comparaciones. */
  private def canonicalize(c: Column, dt: DataType): Column = dt match {
    // Numéricos y boolean: usa valor real (max numérico será numérico), luego pásalo a string para comparar
    case _: NumericType   => c
    case _: BooleanType   => c
    case _: DateType      => c
    case _: TimestampType => c

    // String: recorta espacios (pero respeta mayúsculas/minúsculas)
    case _: StringType    => when(c.isNull, lit(null)).otherwise(trim(c.cast("string")))

    // Map: ordena entradas por clave antes de serializar a JSON (determinista)
    case mt: MapType =>
      val entriesSorted = array_sort(map_entries(c))
      to_json(map_from_entries(entriesSorted))

    // Array / Struct: JSON es estable (el orden del array forma parte del valor)
    case _: ArrayType  => to_json(c)
    case _: StructType => to_json(c)

    // Binary: serializa a base64 para obtener cadena estable
    case BinaryType     => when(c.isNull, lit(null)).otherwise(encode(c.cast("binary"), "base64"))

    // Fallback: JSON
    case _              => to_json(c)
  }

  private def isConstantColumn(df: DataFrame, colName: String): Boolean =
    df.select(col(colName)).distinct().limit(2).count() <= 1

  def generateDifferencesTable(
      spark: SparkSession,
      refDf: DataFrame,
      newDf: DataFrame,
      compositeKeyCols: Seq[String],
      compareColsIn: Seq[String],
      includeEquals: Boolean
  ): DataFrame = {

    import spark.implicits._

    val normalizeEmptyToNull: Column => Column =
      c => when(trim(c.cast("string")) === "", lit(null)).otherwise(c)

    val normalizedRef = compositeKeyCols.foldLeft(refDf)((df, k) => df.withColumn(k, normalizeEmptyToNull(col(k))))
    val normalizedNew = compositeKeyCols.foldLeft(newDf)((df, k) => df.withColumn(k, normalizeEmptyToNull(col(k))))

    val baseCols = compareColsIn.filterNot(compositeKeyCols.contains).distinct
    val commonCols = normalizedRef.columns.toSet.intersect(normalizedNew.columns.toSet)
    val constantLike = commonCols.filter(c => baseCols.contains(c) && isConstantColumn(normalizedRef, c) && isConstantColumn(normalizedNew, c))
    if (constantLike.nonEmpty) println(s"[INFO] Excluyendo columnas constantes: ${constantLike.mkString(", ")}")
    val compareCols = baseCols.filterNot(constantLike.contains)

    val refAgg = aggregateByCompositeKey(normalizedRef, compositeKeyCols, compareCols).alias("ref")
    val newAgg = aggregateByCompositeKey(normalizedNew, compositeKeyCols, compareCols).alias("new")

    val joinCond = compositeKeyCols
      .map { k =>
        val l = col(s"ref.$k"); val r = col(s"new.$k")
        if (NULL_KEY_MATCHES) (l <=> r) else (l.isNotNull && r.isNotNull && l === r)
      }.reduce(_ && _)

    val joined = refAgg.join(newAgg, joinCond, "fullouter")
      .withColumn("exists_ref", col("ref._present").isNotNull)
      .withColumn("exists_new", col("new._present").isNotNull)

    val diffs = compareCols.map(c => buildDiffStruct(compositeKeyCols, c))

    val result = joined
      .select(array(diffs: _*).as("diffs"))
      .withColumn("diff", explode($"diffs"))
      .select("diff.*")

    if (includeEquals) result else result.filter($"results" =!= "MATCH")
  }

  /** Agregación determinista por clave: convierte a forma canónica y aplica max. */
  private def aggregateByCompositeKey(
      df: DataFrame,
      keyCols: Seq[String],
      compareCols: Seq[String]
  ): DataFrame = {
    val aggs: Seq[Column] = compareCols.map { c =>
    val dt = df.schema(c).dataType
    val canon = canonicalize(col(c), dt)

    dt match {
      case _: NumericType | _: BooleanType | _: DateType | _: TimestampType =>
        // max numérico/temporal ⇒ determinista y semántico
        max(canon.cast(df.schema(c).dataType)).as(c)
      case _ =>
        // resto: max lexicográfico sobre la representación canónica
        max(canon).as(c)
    }
  }

    df.groupBy(keyCols.map(col): _*)
      .agg(aggs.head, aggs.tail: _*)
      .withColumn("_present", lit(1))
  }

  private def buildDiffStruct(keyCols: Seq[String], colName: String): Column = {
    val refCol = col(s"ref.$colName")
    val newCol = col(s"new.$colName")

    val result =
      when(!col("exists_new"), lit("ONLY_IN_REF"))
        .when(!col("exists_ref"), lit("ONLY_IN_NEW"))
        .when(refCol <=> newCol, lit("MATCH"))
        .otherwise(lit("NO_MATCH"))

    val compositeId = concat_ws(
      "_",
      keyCols.map { k =>
        val v = coalesce(col(s"ref.$k"), col(s"new.$k"))
        when(v.isNull, lit("NULL")).otherwise(v.cast("string"))
      }: _*
    )

    struct(
      compositeId.as("id"),
      lit(colName).as("column"),
      formatString(refCol).as("value_ref"),
      formatString(newCol).as("value_new"),
      result.as("results")
    )
  }
}
