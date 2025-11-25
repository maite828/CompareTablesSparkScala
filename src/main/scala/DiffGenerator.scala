import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import java.util.Locale

object DiffGenerator extends Logging {

  // ─────────────────────────── log + println ───────────────────────────
  private def info(msg: String): Unit = { log.info(msg); println(msg) }
  private def warn(msg: String): Unit = { log.warn(msg); println(msg) }

  // Result schema in case we need to return empty DF (no comparable columns)
  private val diffSchema: StructType = StructType(Seq(
    StructField("id", StringType, true),
    StructField("column", StringType, true),
    StructField("value_ref", StringType, true),
    StructField("value_new", StringType, true),
    StructField("results", StringType, true)
  ))

  // Formats any value as String; keeps Decimal scale; renders null/empty as "-"
  private def formatValue(c: Column, dt: DataType): Column = dt match {
    case _: DecimalType =>
      val s = c.cast(StringType); when(s.isNull || trim(s) === "", lit("-")).otherwise(s)
    case _ =>
      val s = c.cast(StringType); when(s.isNull || trim(s) === "", lit("-")).otherwise(s)
  }

  // Canonicalizes complex types (map/array/struct/binary) to deterministic JSON/BASE64 for comparison
  private def canonicalize(c: Column, dt: DataType): Column = dt match {
    case _: NumericType   => c
    case _: BooleanType   => c
    case _: DateType      => c
    case _: TimestampType => c
    case _: StringType    => when(c.isNull, lit(null)).otherwise(c.cast(StringType))
    case _: MapType       =>
      // sort map entries to make JSON deterministic
      val sorted = array_sort(map_entries(c)); to_json(map_from_entries(sorted))
    case _: ArrayType     => to_json(c)
    case _: StructType    => to_json(c)
    case BinaryType       => when(c.isNull, lit(null)).otherwise(encode(c.cast("binary"), "base64"))
    case _                => to_json(c)
  }

  private def isConstantColumn(df: DataFrame, colName: String): Boolean =
    df.select(col(colName)).distinct().limit(2).count() <= 1

  // Constructs the struct for EXACT_MATCH with fixed formatting.
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

  // ─────────────────────────── Public API ───────────────────────────
  /**
   * End-to-end diff generation while preserving functional behavior.
   * Steps:
   * - Normalize keys (empty -> NULL) and detect presence.
   * - Decide comparable columns (drop equal constants on both sides).
   * - Optional priority reduction (take first by priority per key).
   * - Aggregate per column, full-outer join by keys (null policy respected).
   * - Explode wide row into (id,column,value_ref,value_new,result) rows.
   * - Optionally filter out MATCH rows.
   */
  def generateDifferencesTable(
                                spark: SparkSession,
                                refDf: DataFrame,
                                newDf: DataFrame,
                                compositeKeyCols: Seq[String],
                                compareColsIn: Seq[String],
                                includeEquals: Boolean,
                                config: CompareConfig
                              ): DataFrame = {

    // 1) Normalize key empties to NULL (NULL-safe joins later)
    val nRef = normalizeKeysToNull(refDf, compositeKeyCols)
    val nNew = normalizeKeysToNull(newDf, compositeKeyCols)
    val hasRef = hasAny(nRef); val hasNew = hasAny(nNew)
    info(s"[DEBUG] DiffGenerator: hasRef=$hasRef, hasNew=$hasNew")

    // 2) Compute comparable columns (exclude keys; keep only commons; drop equal-constants)
    val baseCols   = compareColsIn.filterNot(compositeKeyCols.contains).distinct
    val commonCols = nRef.columns.toSet.intersect(nNew.columns.toSet)
    val compareCols = selectCompareColumnsFast(nRef, nNew, baseCols, commonCols, hasRef, hasNew)

    if (compareCols.isEmpty) {
      emptyDiff(spark)
    } else {
      // 3) Apply priority policy (if configured) to collapse duplicates per key
      val (refBase, newBase) = prepareBases(nRef, nNew, compositeKeyCols, config)

      // 4) Aggregate both sides per key and per comparable column
      val (refAgg, newAgg, dtMap) = aggregateSides(refBase, newBase, compositeKeyCols, compareCols, config)

      // 5) FULL OUTER join on keys (null-equality policy applied), explode and format
      val joined = joinSides(refAgg, newAgg, compositeKeyCols, config.nullKeyMatches)

      // Calculate if the record is an exact match (all columns match and exists in both)
      val exactMatchCondition = compareCols.map { c =>
        col(s"ref.$c") <=> col(s"new.$c")
      }.reduce(_ && _) && col("exists_ref") && col("exists_new")
      val joinedWithExact = joined.withColumn("is_exact_match", exactMatchCondition)

      // 6) Explode differences with EXACT_MATCH support
      val exploded = explodeDiffsWithExact(joinedWithExact, compareCols, compositeKeyCols, dtMap)

      // 7) Filter equals if requested (now excluding both MATCH and EXACT_MATCH)
      if (includeEquals) exploded else exploded.filter(!col("results").isin("MATCH", "EXACT_MATCH"))
    }
  }

  // ─────────────────────────── Orchestration helpers ───────────────────────────

  /** Return an empty diff DataFrame with the standard schema. */
  private def emptyDiff(spark: SparkSession): DataFrame = {
    warn("[WARN] No comparable columns after filters (keys/partitions/constants/ignore). Returning empty DF.")
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], diffSchema)
  }

  /** Replace empty-string keys with NULL to enable null-safe equality later. */
  private def normalizeKeysToNull(df: DataFrame, keys: Seq[String]): DataFrame = {
    val norm: Column => Column = c => when(trim(c.cast(StringType)) === "", lit(null)).otherwise(c)
    keys.foldLeft(df)((acc, k) => acc.withColumn(k, norm(col(k))))
  }

  /** Fast presence check: does DF have any row? */
  private def hasAny(df: DataFrame): Boolean = df.limit(1).count() > 0

  /** Apply optional priority policy: take first row by priority within each key partition. */
  private def preOrderByPriority(df: DataFrame, keys: Seq[String], config: CompareConfig): DataFrame = {
    if (config.priorityCols.nonEmpty) {
      val existingPriorityCols = config.priorityCols.filter(df.columns.contains)
      
      if (existingPriorityCols.nonEmpty) {
        // Order by priorityCols in order (first has highest precedence)
        val orderCols = existingPriorityCols.map(col(_).desc_nulls_last)
        
        val w = Window.partitionBy(keys.map(col): _*).orderBy(orderCols: _*)
        df.withColumn("_rn", row_number().over(w)).filter(col("_rn") === 1).drop("_rn")
      } else {
        df
      }
    } else {
      df
    }
  }

  /** Prepare both sides (priority reduction if needed). */
  private def prepareBases(
                            nRef: DataFrame,
                            nNew: DataFrame,
                            keys: Seq[String],
                            config: CompareConfig
                          ): (DataFrame, DataFrame) = {
    val refBase = preOrderByPriority(nRef, keys, config)
    val newBase = preOrderByPriority(nNew, keys, config)
    (refBase, newBase)
  }

  /** Build per-column aggregations honoring overrides and compute both sides. */
  private def aggregateSides(
                              refBase: DataFrame,
                              newBase: DataFrame,
                              keys: Seq[String],
                              compareCols: Seq[String],
                              config: CompareConfig
                            ): (DataFrame, DataFrame, Map[String, DataType]) = {
    val aggs   = buildAggExpressions(refBase, compareCols, config)
    val refAgg = aggregateSide(refBase, keys, aggs)
    val newAgg = aggregateSide(newBase, keys, aggs)
    val dtMap  = typeMapFrom(refAgg, compareCols)
    (refAgg, newAgg, dtMap)
  }

  // ---------- Fast constant/equal pruning (single pass per side) ----------

  /**
   * Compute, for a set of columns, exact countDistinct and a representative non-null value.
   * Returns Map(col -> (countDistinct, firstNonNullAsString)).
   * This is an exact computation (countDistinct) in a single aggregation job per DataFrame,
   * much cheaper than N separate jobs.
   */
  private def constantStats(df: DataFrame, candidateCols: Seq[String]): Map[String, (Long, Option[String])] = {
    if (candidateCols.isEmpty) {
      Map.empty
    } else {
      val aggExprs: Seq[Column] = candidateCols.flatMap { c =>
        Seq(
          countDistinct(col(c)).as(s"cd__${c}"),
          first(col(c), ignoreNulls = true).cast(StringType).as(s"fv__${c}")
        )
      }
      val row = df.agg(aggExprs.head, aggExprs.tail: _*).collect()(0)
      candidateCols.map { c =>
        val cd = row.getAs[Long](s"cd__${c}")
        val fv = Option(row.getAs[String](s"fv__${c}"))
        c -> (cd -> fv)
      }.toMap
    }
  }

  /**
   * Detect columns that are constant and equal on both sides so we can skip them.
   * Semantics preserved:
   *  - If both sides are constant but value is NULL (no non-null representatives), we DO NOT prune.
   *  - Only prune when both are constant (<=1 distinct) AND both have a non-null representative AND they are equal.
   */
  private def equalConstantColumnsFast(nRef: DataFrame, nNew: DataFrame, candidateCols: Seq[String], commonCols: Set[String]): Seq[String] = {
    val base = candidateCols.filter(commonCols.contains)
    if (base.isEmpty) {
      Seq.empty
    } else {
      val refStats = constantStats(nRef, base)
      val newStats = constantStats(nNew, base)
      base.filter { c =>
        val (cdR, vR) = refStats.getOrElse(c, (2L, None))
        val (cdN, vN) = newStats.getOrElse(c, (2L, None))
        cdR <= 1L && cdN <= 1L && vR.isDefined && vN.isDefined && vR.get == vN.get
      }
    }
  }

  /** Choose final set of columns to compare (commons minus equal-constants). */
  private def selectCompareColumnsFast(
                                        nRef: DataFrame,
                                        nNew: DataFrame,
                                        baseCols: Seq[String],
                                        commonCols: Set[String],
                                        hasRef: Boolean,
                                        hasNew: Boolean
                                      ): Seq[String] = {
    val base = baseCols.filter(commonCols.contains)
    if (!hasRef || !hasNew) {
      warn("[WARN] One side has no rows; constant column filter will NOT be applied.")
      base
    } else {
      val equalConsts = equalConstantColumnsFast(nRef, nNew, base, commonCols)
      if (equalConsts.nonEmpty) info(s"[INFO] Excluding constant columns with SAME value on both sides: ${equalConsts.mkString(",")}")
      base.filterNot(equalConsts.toSet)
    }
  }

  // ─────────────────────────── Aggregations & join ───────────────────────────

  // Builds one aggregation expression per comparable column honoring overrides
  private def buildAggExpressions(refBase: DataFrame, compareCols: Seq[String], config: CompareConfig): Seq[Column] = {
    compareCols.map { c =>
      val dt    = refBase.schema(c).dataType
      val canon = canonicalize(col(c), dt)
      config.aggOverrides.get(c).map(_.toLowerCase(Locale.ROOT)).getOrElse("") match {
        case "max" => max(canon.cast(dt)).as(c)
        case "min" => min(canon.cast(dt)).as(c)
        case "first_non_null" | "first" | "firstnonnull" | "first-non-null" =>
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
  }

  // Groups by composite keys and computes aggregates; flags presence for join semantics
  private def aggregateSide(base: DataFrame, keys: Seq[String], aggs: Seq[Column]): DataFrame =
    base.groupBy(keys.map(col): _*).agg(aggs.head, aggs.tail: _*).withColumn("_present", lit(1))

  // Builds the FULL OUTER join condition honoring null-key policy
  private def joinSides(refAgg: DataFrame, newAgg: DataFrame, keys: Seq[String], nullKeyMatches: Boolean): DataFrame = {
    val joinCond = keys.map { k =>
      val l = col(s"ref.$k"); val r = col(s"new.$k")
      if (nullKeyMatches) l <=> r else (l.isNotNull && r.isNotNull && l === r)
    }.reduce(_ && _)

    refAgg.alias("ref")
      .join(newAgg.alias("new"), joinCond, "fullouter")
      .withColumn("exists_ref", col("ref._present").isNotNull)
      .withColumn("exists_new", col("new._present").isNotNull)
  }

  private def typeMapFrom(df: DataFrame, compareCols: Seq[String]): Map[String, DataType] =
    df.schema.filter(f => compareCols.contains(f.name)).map(f => f.name -> f.dataType).toMap

  // NEW: Produces one row per (key, column) with values and result label, with EXACT_MATCH support
  private def explodeDiffsWithExact(joined: DataFrame, compareCols: Seq[String], keyCols: Seq[String], dtMap: Map[String, DataType]): DataFrame = {
    val diffs = when(col("is_exact_match"), array(buildExactMatchStruct(keyCols)))
      .otherwise(array(compareCols.map(c => buildDiffStruct(keyCols, c, dtMap(c))): _*))

    joined
      .select(diffs.as("diffs"))
      .withColumn("diff", explode(col("diffs")))
      .select("diff.*")
  }

  // Diff struct for a column (id, column, value_ref, value_new, results)
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