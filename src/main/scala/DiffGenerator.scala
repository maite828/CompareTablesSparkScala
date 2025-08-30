// src/main/scala/com/santander/cib/adhc/internal_aml_tools/app/table_comparator/DiffGenerator.scala

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util.Locale

object DiffGenerator extends Logging {

  // ─────────────────────────── logger + println ───────────────────────────
  private def info(msg: String): Unit = { logger.info(msg); println(msg) }
  private def warn(msg: String): Unit = { logger.warn(msg); println(msg) }

  // Esquema del resultado por si hay que devolver DF vacío (sin columnas comparables)
  private val diffSchema: StructType = StructType(Seq(
    StructField("id",        StringType, true),
    StructField("column",    StringType, true),
    StructField("value_ref", StringType, true),
    StructField("value_new", StringType, true),
    StructField("results",   StringType, true)
  ))

  /** Formatea valores a cadena, respetando la escala de DecimalType tal cual */
  private def formatValue(c: Column, dt: DataType): Column = dt match {
    case _: DecimalType =>
      val s = c.cast(StringType); when(s.isNull || trim(s) === "", lit("-")).otherwise(s)
    case _ =>
      val s = c.cast(StringType); when(s.isNull || trim(s) === "", lit("-")).otherwise(s)
  }

  /** Representación canónica determinista para comparaciones */
  private def canonicalize(c: Column, dt: DataType): Column = dt match {
    case _: NumericType   => c
    case _: BooleanType   => c
    case _: DateType      => c
    case _: TimestampType => c
    case _: StringType    => when(c.isNull, lit(null)).otherwise(c.cast(StringType))
    case _: MapType       =>
      val sorted = array_sort(map_entries(c)); to_json(map_from_entries(sorted))
    case _: ArrayType     => to_json(c)
    case _: StructType    => to_json(c)
    case BinaryType       => when(c.isNull, lit(null)).otherwise(encode(c.cast("binary"), "base64"))
    case _                => to_json(c)
  }

  /** ¿Es constante (0 ó 1 valores distintos) este DF para la columna? */
  private def isConstantColumn(df: DataFrame, colName: String): Boolean =
    df.select(col(colName)).distinct().limit(2).count() <= 1

  /** Valor constante (en String) si la col es constante y el DF tiene filas; si no, None */
  private def constantValue(df: DataFrame, colName: String): Option[String] = {
    val c = df.select(col(colName)).where(col(colName).isNotNull).distinct().limit(2).collect()
    if (c.length == 1) Option(c(0).get(0)).map(_.toString) else None
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

    val hasRef = nRef.limit(1).count() > 0
    val hasNew = nNew.limit(1).count() > 0
    info(s"[DEBUG] DiffGenerator: hasRef=$hasRef, hasNew=$hasNew")

    // 2) Filtrar columnas de trabajo
    val baseCols   = compareColsIn.filterNot(compositeKeyCols.contains).distinct
    val commonCols = nRef.columns.toSet.intersect(nNew.columns.toSet)

    // 2.a) Excluir constantes SOLO si ambos lados tienen filas y el valor constante es el mismo en ambos
    val compareCols: Seq[String] =
      if (!hasRef || !hasNew) {
        if (!hasRef || !hasNew)
          warn(s"[WARN] Un lado no tiene filas; NO se aplicará filtro de columnas constantes.")
        baseCols.filter(commonCols.contains)
      } else {
        val equalConstantCols = baseCols.filter { c =>
          if (!commonCols.contains(c)) false
          else if (!isConstantColumn(nRef, c) || !isConstantColumn(nNew, c)) false
          else {
            val vRef = constantValue(nRef, c)
            val vNew = constantValue(nNew, c)
            vRef.isDefined && vNew.isDefined && vRef.get == vNew.get
          }
        }
        if (equalConstantCols.nonEmpty)
          info(s"[INFO] Excluyendo columnas constantes con el MISMO valor en ambos lados: ${equalConstantCols.mkString(",")}")
        baseCols.filterNot(equalConstantCols.toSet)
      }

    // Guardarraíl: si no hay columnas comparables, devolvemos DF vacío con esquema
    if (compareCols.isEmpty) {
      warn("[WARN] No hay columnas comparables tras filtros (claves/particiones/constantes/ignorar). Se devuelve DF vacío.")
      return spark.createDataFrame(spark.sparkContext.emptyRDD[Row], diffSchema)
    }

    // 3) Pre-orden determinista si hay priorityCol
    def preOrdered(df: DataFrame): DataFrame = config.priorityCol match {
      case Some(prio) if df.columns.contains(prio) =>
        val w = Window.partitionBy(compositeKeyCols.map(col): _*).orderBy(col(prio).desc_nulls_last)
        df.withColumn("_rn", row_number().over(w)).filter($"_rn" === 1).drop("_rn")
      case _ => df
    }
    val refBase = preOrdered(nRef)
    val newBase = preOrdered(nNew)

    // 4) Agregación con overrides (strings)
    val aggs: Seq[Column] = compareCols.map { c =>
      val dt    = refBase.schema(c).dataType
      val canon = canonicalize(col(c), dt)
      config.aggOverrides.get(c).map(_.toLowerCase(Locale.ROOT)).getOrElse("") match {
        case "max"                           => max(canon.cast(dt)).as(c)
        case "min"                           => min(canon.cast(dt)).as(c)
        case "first_non_null" | "first" |
             "firstnonnull" | "first-non-null" => first(col(c), ignoreNulls = true).as(c)
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

    // 7) Explode diferencias
    val diffs = compareCols.map(c => buildDiffStruct(compositeKeyCols, c, dtMap(c)))
    val exploded = joined
      .select(array(diffs: _*).as("diffs"))
      .withColumn("diff", explode(col("diffs")))
      .select("diff.*")

    if (includeEquals) exploded else exploded.filter(col("results") =!= "MATCH")
  }

  /** Struct de diff para una columna (id, column, value_ref, value_new, results) */
  private def buildDiffStruct(
                               keyCols: Seq[String],
                               colName: String,
                               dt: DataType
                             ): Column = {
    val refCol = col(s"ref.$colName")
    val newCol = col(s"new.$colName")

    val result = when(!col("exists_ref"), lit("ONLY_IN_NEW"))
      .when(!col("exists_new"), lit("ONLY_IN_REF"))
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
