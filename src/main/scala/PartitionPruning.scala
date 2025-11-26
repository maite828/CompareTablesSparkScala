import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Partition pruning and spec parsing/resolution.
 * FULLY GENERIC: No hardcoded assumptions about column names.
 * Supports wildcard resolution for any partition column via metastore.
 */
object PartitionPruning {

  private final case class Values(values: Seq[String]) {
    def nonEmpty: Boolean = values.nonEmpty
    def isEmpty: Boolean = values.isEmpty
    def isWildcard: Boolean = values.isEmpty || values.exists(v => v == "*" || v.isEmpty)
  }

  def loadWithPartition(
                         spark: SparkSession,
                         tableName: String,
                         partitionSpec: Option[String]
                       ): DataFrame = {
    val baseDf = spark.table(tableName)
    val spec   = partitionSpec.map(_.trim).getOrElse("")

    if (spec.isEmpty) {
      println(s"[DEBUG] loadWithPartition: $tableName without spec -> no filter | hasData=${!baseDf.isEmpty}")
      baseDf
    } else {
      // Parse the spec into a map of partition_column -> values
      val specMap = parsePartitionSpecToMap(spec)

      // Resolve wildcards: if any column has wildcard (*), resolve available values from metastore
      val resolvedMap = resolveWildcards(spark, tableName, specMap)

      // Build filter predicates from resolved map
      val filter = buildFilterFromMap(resolvedMap)

      val filtered = baseDf.where(filter)
      println(s"[DEBUG] loadWithPartition: $tableName with filter: $filter | hasData=${!filtered.isEmpty}")
      filtered
    }
  }

  /**
   * Resolves wildcard (*) values for any partition column by querying metastore.
   * Only resolves wildcards that are explicitly marked with *.
   * If a column is not in the spec, it's ignored (no filter applied).
   */
  private def resolveWildcards(
                                spark: SparkSession,
                                tableName: String,
                                specMap: Map[String, Values]
                              ): Map[String, Values] = {

    // Find columns with wildcards
    val wildcardCols = specMap.filter { case (_, vals) => vals.isWildcard }.keys.toSeq

    if (wildcardCols.isEmpty) {
      // No wildcards to resolve, return as-is
      specMap
    } else {
      // Resolve wildcards by querying SHOW PARTITIONS
      println(s"[DEBUG] Resolving wildcards for columns: ${wildcardCols.mkString(", ")}")

      val resolvedValues = resolveWildcardColumns(spark, tableName, wildcardCols, specMap)

      // Merge resolved values back into specMap
      specMap.map { case (col, vals) =>
        if (vals.isWildcard && resolvedValues.contains(col)) {
          col -> Values(resolvedValues(col))
        } else {
          col -> vals
        }
      }
    }
  }

  /**
   * Generic wildcard resolution: queries SHOW PARTITIONS and extracts available values
   * for the specified wildcard columns, optionally filtered by other non-wildcard columns.
   */
  private def resolveWildcardColumns(
                                      spark: SparkSession,
                                      tableName: String,
                                      wildcardCols: Seq[String],
                                      specMap: Map[String, Values]
                                    ): Map[String, Seq[String]] = {
    try {
      // Get all partitions from metastore
      val allPartitions: Array[String] = spark.sql(s"SHOW PARTITIONS $tableName")
        .collect()
        .map(_.getString(0))

      // Parse each partition string into a map: "col1=val1/col2=val2" -> Map(col1->val1, col2->val2)
      val parsedPartitions = allPartitions.map(parsePartitionString)

      // Filter partitions by non-wildcard constraints
      val filteredPartitions = parsedPartitions.filter { partMap =>
        specMap.forall { case (col, vals) =>
          if (vals.isWildcard) {
            // Wildcard column: no filter
            true
          } else {
            // Non-wildcard: partition must match one of the specified values
            partMap.get(col).exists(vals.values.contains)
          }
        }
      }

      // Extract distinct values for each wildcard column
      val resolved = wildcardCols.map { col =>
        val distinctVals = filteredPartitions.flatMap(_.get(col)).distinct.sorted
        println(s"[DEBUG] Resolved wildcard column '$col': ${distinctVals.length} values -> ${distinctVals.take(10).mkString(", ")}${if (distinctVals.length > 10) "..." else ""}")
        col -> distinctVals.toSeq
      }.toMap

      resolved

    } catch {
      case ex: Throwable =>
        println(s"[WARN] Failed to resolve wildcards via SHOW PARTITIONS: ${ex.getMessage}")
        // Return empty sequences for wildcard columns (will result in no filter)
        wildcardCols.map(_ -> Seq.empty[String]).toMap
    }
  }

  /**
   * Parses a partition string "col1=val1/col2=val2/col3=val3" into Map(col1->val1, col2->val2, col3->val3)
   */
  private def parsePartitionString(partStr: String): Map[String, String] = {
    partStr.split("/").flatMap { kv =>
      kv.split("=", 2) match {
        case Array(k, v) => Some(k.trim -> v.trim)
        case _ => None
      }
    }.toMap
  }

  /**
   * Builds a Spark SQL filter Column from the resolved partition map.
   * Only includes columns that have non-empty values.
   */
  private def buildFilterFromMap(specMap: Map[String, Values]): Column = {
    val filters = specMap.toSeq.flatMap { case (colName, vals) =>
      if (vals.isEmpty || vals.isWildcard) {
        // No filter for this column
        None
      } else {
        // Build: col IN (val1, val2, ...)
        Some(col(colName).isin(vals.values: _*))
      }
    }

    if (filters.isEmpty) {
      lit(true) // No filter
    } else {
      filters.reduce(_ && _) // Combine with AND
    }
  }

  /**
   * Parses a partitionSpec string into a Map of column -> Values.
   * Supports formats:
   *   - col=value
   *   - col=*
   *   - col=[val1,val2,val3]
   *   - col=(val1|val2|val3)
   *   - col=IN[val1,val2]
   *   - col=IN(val1,val2)
   */
  private def parsePartitionSpecToMap(spec: String): Map[String, Values] = {
    val toks = spec.split("/").iterator.map(_.trim).filter(_.nonEmpty).toSeq
    def unq(s: String): String = s.stripPrefix("\"").stripPrefix("'").stripSuffix("\"").stripSuffix("'").trim
    def splitList(s: String, or: Boolean): Seq[String] = {
      val sep = if (or) "\\|" else "[,|]"
      s.split(sep).toSeq.map(unq).filter(_.nonEmpty)
    }
    val InList1 = "(?i)^in\\s*\\[(.*)\\]$".r
    val InList2 = "(?i)^in\\s*\\((.*)\\)$".r
    val EqList1 = "^\\[(.*)\\]$".r
    val EqList2 = "^\\((.*)\\)$".r
    toks.flatMap { kv =>
      kv.split("=", 2) match {
        case Array(k, rhsRaw) =>
          val k1  = unq(k)
          val rhs = rhsRaw.trim
          val vals: Seq[String] =
            if (rhs == "*" || rhs.isEmpty) { Nil }
            else {
              rhs match {
                case InList1(inner) => splitList(inner, or = false)
                case InList2(inner) => splitList(inner, or = false)
                case EqList1(inner) => splitList(inner, or = false)
                case EqList2(inner) => splitList(inner, or = true)
                case other => Seq(unq(other))
              }
            }
          Some(k1 -> Values(vals))
        case _ => None
      }
    }.toMap
  }
}