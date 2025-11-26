
import ComparatorDefaults.{HashNullToken, HashSeparator, MinOccurrencesToBeDuplicate, Sha256Bits}
import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

case class DuplicateOut(
                         origin: String,                // "ref" | "new"
                         id: String,                    // composite key or "NULL"
                         category: String,              // "both" | "only_ref" | "only_new"
                         exact_duplicates: String,      // total - countDistinct(hash)
                         dupes_w_variations: String,    // max(countDistinct(hash) - 1, 0)
                         occurrences: String,           // group total
                         variations: String             // "field: [v1,v2] | ..."
                       )

object DuplicateDetector {

  // Constants for hashing/NULL-safe
  private val NullToken = HashNullToken

  /**
   * Detect duplicates (exact and with variations) on each side independently.
   * - If config.priorityCol is defined and present, within each (_src + keys) partition we keep
   *   the row with highest priority (desc_nulls_last) to avoid counting intra-partition noise.
   * - Row hash is built null-safely across all columns (except `_src`), using a stable separator.
   * - Variation lists are deterministically ordered (array_sort) for stable outputs.
   */
  def detectDuplicatesTable(
                             spark: SparkSession,
                             refDf: DataFrame,
                             newDf: DataFrame,
                             compositeKeyCols: Seq[String],
                             config: CompareConfig
                           ): DataFrame = {
    import spark.implicits._

    // Union both sides labeling the origin; schema must match (unionByName).
    val withSrc = unionWithOrigin(refDf, newDf)

    // Apply optional priority policy (keep top-1 per (_src + keys)).
    val base = applyPriorityIf(config, withSrc, compositeKeyCols)

    // Determine non-key columns once.
    val nonKeys = nonKeyColumns(base, compositeKeyCols)

    // Compute a null-safe row hash excluding `_src`.
    val hashed = withRowHash(base)

    // Aggregate per (origin + keys) to compute counts and collect variation sets.
    val grouped = aggregateByGroup(hashed, compositeKeyCols, nonKeys)

    // Select columns for output (including canonical ID).
    val selected = selectForOutput(grouped, compositeKeyCols, nonKeys)

    // Compute duplicate categories (both/only_ref/only_new) to match summary logic
    val categories = computeDuplicateCategories(selected, spark)

    // Join with categories and map to final DTO
    val withCategory = selected.join(categories, Seq("id", "origin"), "left")

    // Map to the final DTO, formatting variation text (excluding priorityCol if configured).
    withCategory.map(rowToDuplicateOut(_, nonKeys, config))(Encoders.product[DuplicateOut]).toDF()
  }

  // ─────────────────────────── Helpers ───────────────────────────

  // Add `_src` column distinguishing ref/new sides, then union by name.
  private def unionWithOrigin(refDf: DataFrame, newDf: DataFrame): DataFrame =
    refDf.withColumn("_src", lit("ref"))
      .unionByName(newDf.withColumn("_src", lit("new")))

  // If `priorityCols` exists, keep top-1 per group of identical rows (excluding priority columns).
  // This means: only eliminate duplicates when ALL columns are the same except priorityCols.
  // Duplicates with variations in other columns are preserved.
  // Multiple columns are ordered by precedence (first column has highest priority).
  private def applyPriorityIf(config: CompareConfig, df: DataFrame, keys: Seq[String]): DataFrame = {
    if (config.priorityCols.nonEmpty) {
      // Filter only columns that exist in the DataFrame
      val existingPriorityCols = config.priorityCols.filter(df.columns.contains)

      if (existingPriorityCols.nonEmpty) {
        // Partition by ALL columns except priorityCols (and _rn which we'll add)
        val partitionCols = df.columns.filterNot(config.priorityCols.contains)

        // Order by priorityCols in order (first has highest precedence)
        val orderCols = existingPriorityCols.map(col(_).desc_nulls_last)

        val w = Window.partitionBy(partitionCols.map(col): _*).orderBy(orderCols: _*)
        df.withColumn("_rn", row_number().over(w)).filter(col("_rn") === 1).drop("_rn")
      } else {
        df
      }
    } else {
      df
    }
  }

  // Non-key columns (excludes `_src` and composite keys).
  private def nonKeyColumns(df: DataFrame, keys: Seq[String]): Seq[String] =
    df.columns.filterNot(c => c == "_src" || keys.contains(c))

  /**
   * Add null-safe `_row_hash` built from every column except `_src`.
   * We cast every field to string and replace NULLs with a fixed token to make the hash stable.
   */
  private def withRowHash(df: DataFrame): DataFrame = {
    val colsForHash = df.columns.filter(_ != "_src").map { c =>
      // Keep empty strings as-is (changing it would alter semantics); only NULL becomes NullToken.
      coalesce(col(c).cast(StringType), lit(NullToken))
    }
    val hashCol = sha2(concat_ws(HashSeparator, colsForHash: _*), Sha256Bits)
    df.withColumn("_row_hash", hashCol)
  }

  /**
   * Group by origin + keys. Compute:
   *  - occurrences (rows in group)
   *  - exact_dup = total - countDistinct(_row_hash)
   *  - var_dup   = max(countDistinct(_row_hash) - 1, 0)
   *  - For each non-key column, collect_set of values (null-safe via previous cast) and sort arrays
   *    to make output deterministic.
   */
  private def aggregateByGroup(hashed: DataFrame, keys: Seq[String], nonKeys: Seq[String]): DataFrame = {
    // Base metrics
    val baseAggs: Seq[Column] = Seq(
      count(lit(1)).as("occurrences"),
      (count(lit(1)) - countDistinct("_row_hash")).as("exact_dup"),
      greatest(lit(0), countDistinct("_row_hash") - lit(1)).as("var_dup")
    )

    // Deterministic variation sets per non-key column
    val variationAggs: Seq[Column] =
      nonKeys.map { c =>
        array_sort(collect_set(coalesce(col(c).cast(StringType), lit(NullToken)))).as(s"${c}_set")
      }

    hashed
      .groupBy((col("_src") +: keys.map(col)): _*)
      .agg((baseAggs ++ variationAggs).head, (baseAggs ++ variationAggs).tail: _*)
      .filter(col("occurrences") >= MinOccurrencesToBeDuplicate)
  }

  // Select final columns and build a NULL-safe composite ID for the key.
  private def selectForOutput(grouped: DataFrame, keys: Seq[String], nonKeys: Seq[String]): DataFrame = {
    val idCol =
      concat_ws("_", keys.map(k => coalesce(col(k).cast(StringType), lit("NULL"))): _*).as("id")

    val baseCols: Seq[Column] = Seq(
      col("_src").as("origin"),
      idCol,
      col("exact_dup"),
      col("var_dup"),
      col("occurrences")
    )

    val variationCols: Seq[Column] = nonKeys.map(c => col(s"${c}_set"))
    grouped.select((baseCols ++ variationCols): _*)
  }

  /**
   * Compute duplicate categories (both/only_ref/only_new) matching summary logic.
   * Input is the 'selected' DataFrame after selectForOutput (has 'id' and 'origin' columns).
   * Returns DataFrame with columns: id, origin, category
   */
  private def computeDuplicateCategories(selected: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Get distinct (id, origin) pairs
    val idsBySrc = selected.select($"id", $"origin").distinct()

    // Identify which IDs appear in ref and/or new
    val refIds = idsBySrc.filter($"origin" === "ref").select($"id").distinct()
    val newIds = idsBySrc.filter($"origin" === "new").select($"id").distinct()

    val bothIds = refIds.intersect(newIds).withColumn("category", lit("both"))
    val onlyRefIds = refIds.except(newIds).withColumn("category", lit("only_ref"))
    val onlyNewIds = newIds.except(refIds).withColumn("category", lit("only_new"))

    // Union all categories
    val allCategories = bothIds.union(onlyRefIds).union(onlyNewIds)

    // Cross join with origin values to create entries for both ref and new rows
    val srcValues = Seq("ref", "new").toDF("origin")
    allCategories.crossJoin(srcValues)
  }

  // Build output DTO from a grouped row; format variations as "col: [v1,v2] | ..." (skip NullToken).
  // Exclude priorityCol from variations if configured (it's used to resolve duplicates, not a variation to report).
  private def rowToDuplicateOut(row: org.apache.spark.sql.Row, nonKeys: Seq[String], config: CompareConfig): DuplicateOut = {
    val origin = row.getAs[String]("origin")
    val id = row.getAs[String]("id")
    val category = row.getAs[String]("category")
    val exact = row.getAs[Long]("exact_dup").toString
    val vdup = row.getAs[Long]("var_dup").toString
    val occ = row.getAs[Long]("occurrences").toString

    // Exclude ALL priorityCols from variations
    val nonKeysForVariations = nonKeys.filterNot(config.priorityCols.contains)

    val variationsText = buildVariationsText(row, nonKeysForVariations)
    DuplicateOut(origin, id, category, exact, vdup, occ, variationsText)
  }

  // Render deterministic variations text, ignoring the special NullToken.
  private def buildVariationsText(row: org.apache.spark.sql.Row, nonKeys: Seq[String]): String = {
    val parts = nonKeys.flatMap { c =>
      val arr = Option(row.getAs[Seq[String]](s"${c}_set")).getOrElse(Seq.empty)
      val cleaned = arr.filterNot(_ == NullToken).distinct
      if (cleaned.size > 1) Some(s"$c: [${cleaned.mkString(",")}]") else None
    }
    if (parts.isEmpty) "-" else parts.mkString(" | ")
  }
}
