
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DiffGenerator {

  /** Genera tabla de diferencias agnóstica a nombres de columnas, con configuraciones avanzadas. */
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
    val NULL_KEY_MATCHES = config.nullKeyMatches

    // Normalizar empty->null en claves
    def nzKey(c: Column) = when(trim(c.cast("string")) === "", lit(null)).otherwise(c)
    val nRef = compositeKeyCols.foldLeft(refDf)((df, k) => df.withColumn(k, nzKey(col(k))))
    val nNew = compositeKeyCols.foldLeft(newDf)((df, k) => df.withColumn(k, nzKey(col(k))))

    // Excluir columnas constantes
    val baseCols = compareColsIn.filterNot(compositeKeyCols.contains).distinct
    def isConst(df: DataFrame, c: String) = df.select(col(c)).distinct().limit(2).count() <= 1
    val consts = nRef.columns.toSet.intersect(nNew.columns.toSet).filter(c => baseCols.contains(c) && isConst(nRef,c) && isConst(nNew,c))
    val compareCols = baseCols.filterNot(consts.contains)

    // Representación canónica
    def canon(c: Column, dt: DataType): Column = dt match {
      case _: NumericType|_:BooleanType|_:DateType|_:TimestampType => c
      case _: StringType    => when(c.isNull, lit(null)).otherwise(trim(c.cast("string")))
      case mt: MapType      => to_json(map_from_entries(array_sort(map_entries(c))))
      case _: ArrayType|_:StructType => to_json(c)
      case BinaryType       => when(c.isNull, lit(null)).otherwise(encode(c.cast("binary"),"base64"))
      case _                => to_json(c)
    }

    // Lógica de agregado con overrides
    def aggFor(colName: String): Column = {
      val dt = nRef.schema(colName).dataType
      config.aggOverrides.get(colName) match {
        case Some(AggType.MaxAgg)          => max(col(colName)).as(colName)
        case Some(AggType.MinAgg)          => min(col(colName)).as(colName)
        case Some(AggType.FirstNonNullAgg) => first(col(colName), ignoreNulls=true).as(colName)
        case None =>
          dt match {
            case _: NumericType|_:BooleanType|_:DateType|_:TimestampType => max(col(colName)).as(colName)
            case _ => max(canon(col(colName), dt)).as(colName)
          }
      }
    }

    // Agregaciones deterministas
    val refAggExprs = compareCols.map(aggFor)
    val newAggExprs = compareCols.map(aggFor)

    val refAgg = nRef.groupBy(compositeKeyCols.map(col): _*)
      .agg(refAggExprs.head, refAggExprs.tail: _*)
      .withColumn("_present", lit(1))

    val newAgg = nNew.groupBy(compositeKeyCols.map(col): _*)
      .agg(newAggExprs.head, newAggExprs.tail: _*)
      .withColumn("_present", lit(1))

    // Join fullouter con política de NULL en clave
    val joinCond = compositeKeyCols.map { k =>
      val l = col(s"ref.$k"); val r = col(s"new.$k")
      if (NULL_KEY_MATCHES) l <=> r else l.isNotNull && r.isNotNull && l === r
    }.reduce(_ && _)

    val joined = refAgg.alias("ref").join(newAgg.alias("new"), joinCond, "fullouter")
      .withColumn("exists_ref", col("ref._present").isNotNull)
      .withColumn("exists_new", col("new._present").isNotNull)

    // Formateo de salida
    def fmt(c: Column): Column = {
      val s = c.cast("string")
      when(s.isNull || trim(s) === "", lit("-")).otherwise(s)
    }

    val diffs = compareCols.map { c =>
      val refC = col(s"ref.$c"); val newC = col(s"new.$c")
      val res = when(!col("exists_new"), lit("ONLY_IN_REF"))
        .when(!col("exists_ref"), lit("ONLY_IN_NEW"))
        .when(refC <=> newC, lit("MATCH"))
        .otherwise(lit("NO_MATCH"))

      val idCol = concat_ws("_", compositeKeyCols.map { k =>
        val v = coalesce(col(s"ref.$k"), col(s"new.$k"))
        when(v.isNull, lit("NULL")).otherwise(v.cast("string"))
      }: _*)

      struct(
        idCol.as("id"),
        lit(c).as("column"),
        fmt(refC).as("value_ref"),
        fmt(newC).as("value_new"),
        res.as("results")
      )
    }

    val exploded = joined.select(array(diffs:_*).as("diffs"))
      .withColumn("diff", explode(col("diffs")))
      .select("diff.*")

    if (includeEquals) exploded else exploded.filter(col("results") =!= "MATCH")
  }
}
