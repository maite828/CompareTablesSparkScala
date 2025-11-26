
import ComparatorDefaults.{MaxOrderMismatchesToLog, MaxTypeDiffsToLog}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer
import scala.math.min

object SchemaChecker {

  // How many missing columns to print at most (per side) to avoid log floods.
  private val MaxMissingToLog: Int = MaxOrderMismatchesToLog

  final case class ColumnDiff(
                               name: String,
                               refType: String,
                               newType: String,
                               issue: String, // "type-mismatch" | "nullable-mismatch" | "metadata-mismatch"
                               refNullable: Option[Boolean] = None,
                               newNullable: Option[Boolean] = None
                             )

  final case class SchemaReport(
                                 refColCount: Int,
                                 newColCount: Int,
                                 missingInNew: Seq[String],
                                 missingInRef: Seq[String],
                                 typeDiffs: Seq[ColumnDiff], // limited to MaxTypeDiffsToLog for logging
                                 typeDiffsTotal: Int, // total number of type/nullability/metadata diffs
                                 orderMatches: Boolean,
                                 orderMismatches: Seq[String], // limited to MaxOrderMismatchesToLog for logging
                                 orderMismatchesTotal: Int // total number of order mismatches
                               )

  // ─────────────────────────── Public API ───────────────────────────

  // Basic comparison (backward compatible).
  def compare(ref: DataFrame, neu: DataFrame): (Seq[String], Seq[String], Seq[ColumnDiff]) = {
    val rmap = ref.schema.fields.map(f => f.name -> f).toMap
    val nmap = neu.schema.fields.map(f => f.name -> f).toMap

    val missingInNew = (rmap.keySet -- nmap.keySet).toSeq.sorted
    val missingInRef = (nmap.keySet -- rmap.keySet).toSeq.sorted

    val commons = (rmap.keySet intersect nmap.keySet).toSeq
    val typeDiffsBuf = ArrayBuffer[ColumnDiff]()

    commons.foreach { c =>
      val rf = rmap(c);
      val nf = nmap(c)
      if (rf.dataType.simpleString != nf.dataType.simpleString) {
        typeDiffsBuf += ColumnDiff(c, rf.dataType.simpleString, nf.dataType.simpleString, "type-mismatch", Some(rf.nullable), Some(nf.nullable))
      }
      if (rf.nullable != nf.nullable) {
        typeDiffsBuf += ColumnDiff(c, rf.dataType.simpleString, nf.dataType.simpleString, "nullable-mismatch", Some(rf.nullable), Some(nf.nullable))
      }
      if (rf.metadata != nf.metadata) {
        typeDiffsBuf += ColumnDiff(c, rf.dataType.simpleString, nf.dataType.simpleString, "metadata-mismatch", Some(rf.nullable), Some(nf.nullable))
      }
    }

    (missingInNew, missingInRef, typeDiffsBuf.toSeq.sortBy(_.name))
  }

  // Exhaustive comparison: names, types, nullability, metadata and ORDER.
  def analyze(ref: DataFrame, neu: DataFrame): SchemaReport = {
    val refFields = ref.schema.fields
    val newFields = neu.schema.fields

    val refNames = refFields.map(_.name)
    val newNames = newFields.map(_.name)

    val (missingInNew, missingInRef) = missingColumns(refNames, newNames)

    // Limited logging builders (collect up to limit, count all)
    val (typeDiffsLimited, typeDiffsTotal) =
      buildTypeDiffsLimited(refFields.map(f => f.name -> f).toMap, newFields.map(f => f.name -> f).toMap, MaxTypeDiffsToLog)

    val orderMatches = refNames.sameElements(newNames)
    val (orderLimited, orderTotal) =
      if (orderMatches) (Seq.empty[String], 0) else orderDiffsLimited(refNames, newNames, MaxOrderMismatchesToLog)

    SchemaReport(
      refColCount = refNames.length,
      newColCount = newNames.length,
      missingInNew = missingInNew,
      missingInRef = missingInRef,
      typeDiffs = typeDiffsLimited,
      typeDiffsTotal = typeDiffsTotal,
      orderMatches = orderMatches,
      orderMismatches = orderLimited,
      orderMismatchesTotal = orderTotal
    )
  }

  // Formatted log of the report, info/warn
  def log(report: SchemaReport, info: String => Unit, warn: String => Unit): Unit = {
    logHeader(report, info)
    logOrder(report, info, warn)
    logMissing(report, warn, info)
    logTypeDiffs(report, warn, info)
    logSummary(report, info, warn)
  }

  // ─────────────────────────── Helpers ───────────────────────────

  private def missingColumns(refNames: Array[String], newNames: Array[String]): (Seq[String], Seq[String]) = {
    val rset = refNames.toSet
    val nset = newNames.toSet
    val missingInNew = (rset -- nset).toSeq.sorted
    val missingInRef = (nset -- rset).toSeq.sorted
    (missingInNew, missingInRef)
  }

  private def buildTypeDiffsLimited(
                                     refMap: Map[String, org.apache.spark.sql.types.StructField],
                                     newMap: Map[String, org.apache.spark.sql.types.StructField],
                                     limit: Int
                                   ): (Seq[ColumnDiff], Int) = {
    val commons = (refMap.keySet intersect newMap.keySet).toSeq.sorted
    val shown = ArrayBuffer[ColumnDiff]()
    var total = 0

    commons.foreach { c =>
      val rf = refMap(c);
      val nf = newMap(c)

      if (rf.dataType.simpleString != nf.dataType.simpleString) {
        total += 1
        if (shown.size < limit) {
          shown += ColumnDiff(c, rf.dataType.simpleString, nf.dataType.simpleString, "type-mismatch", Some(rf.nullable), Some(nf.nullable))
        }
      }
      if (rf.nullable != nf.nullable) {
        total += 1
        if (shown.size < limit) {
          shown += ColumnDiff(c, rf.dataType.simpleString, nf.dataType.simpleString, "nullable-mismatch", Some(rf.nullable), Some(nf.nullable))
        }
      }
      if (rf.metadata != nf.metadata) {
        total += 1
        if (shown.size < limit) {
          shown += ColumnDiff(c, rf.dataType.simpleString, nf.dataType.simpleString, "metadata-mismatch", Some(rf.nullable), Some(nf.nullable))
        }
      }
    }
    (shown.toSeq, total)
  }

  private def orderDiffsLimited(refNames: Array[String], newNames: Array[String], limit: Int): (Seq[String], Int) = {
    val msgs = ArrayBuffer[String]()
    var total = 0
    val upto = min(refNames.length, newNames.length)

    var i = 0
    while (i < upto) {
      val rn = refNames(i);
      val nn = newNames(i)
      if (rn != nn) {
        total += 1
        if (msgs.size < limit) msgs += s"idx=$i: REF='$rn' vs NEW='$nn'"
      }
      i += 1
    }
    if (refNames.length > newNames.length) {
      var j = newNames.length
      while (j < refNames.length) {
        total += 1
        if (msgs.size < limit) msgs += s"idx=$j: REF extra '${refNames(j)}'"
        j += 1
      }
    } else if (newNames.length > refNames.length) {
      var j = refNames.length
      while (j < newNames.length) {
        total += 1
        if (msgs.size < limit) msgs += s"idx=$j: NEW extra '${newNames(j)}'"
        j += 1
      }
    }
    (msgs.toSeq, total)
  }

  private def logHeader(r: SchemaReport, info: String => Unit): Unit = {
    info(s"[SCHEMA] REF cols=${r.refColCount} | NEW cols=${r.newColCount}")
    info(s"[SCHEMA] ✓ Both tables have ${r.refColCount} total columns (including partitions)")
  }

  private def logOrder(r: SchemaReport, info: String => Unit, warn: String => Unit): Unit = {
    if (r.orderMatches) {
      info("[SCHEMA] ✓ Column order: OK (identical sequences).")
    } else {
      warn("[SCHEMA] ⚠ Column order: MISMATCH (non-critical - Spark compares by name, not position)")
      if (r.orderMismatches.nonEmpty) {
        val lines = r.orderMismatches.mkString("\n  - ")
        warn(s"[SCHEMA] First order differences (max $MaxOrderMismatchesToLog of ${r.orderMismatchesTotal}):\n  - $lines")
        if (r.orderMismatchesTotal > r.orderMismatches.size) {
          warn(s"[SCHEMA] ... (${r.orderMismatchesTotal - r.orderMismatches.size} additional order differences)")
        }
      } else if (r.orderMismatchesTotal > 0) {
        warn(s"[SCHEMA] Order differences: ${r.orderMismatchesTotal} (omitted from logs)")
      }
      info("[SCHEMA] → Impact: NONE - Column order does not affect comparison logic")
    }
  }

  private def logMissing(r: SchemaReport, warn: String => Unit, info: String => Unit): Unit = {
    def logSide(tag: String, xs: Seq[String]): Unit = {
      if (xs.nonEmpty) {
        val head = xs.take(MaxMissingToLog)
        val isPartitionCol = head.exists(c => c.contains("partition") || c.contains("process_") || c == "geo" || c == "data_date_part")
        val prefix = if (isPartitionCol) "✓" else "⚠"
        warn(s"[SCHEMA] $prefix Columns only in $tag (${xs.size}): ${head.mkString(",")}${if (xs.size > head.size) s", ... (+${xs.size - head.size})" else ""}")
        if (isPartitionCol) {
          info(s"[SCHEMA] → Impact: Expected (partition columns) - These will NOT be compared")
        } else {
          info(s"[SCHEMA] → Impact: These columns will appear as ONLY_IN_$tag in differences table")
        }
      }
    }

    logSide("REF", r.missingInNew)
    logSide("NEW", r.missingInRef)
  }

  private def logTypeDiffs(r: SchemaReport, warn: String => Unit, info: String => Unit): Unit = {
    if (r.typeDiffsTotal > 0) {
      val onlyMetadata = r.typeDiffs.forall(_.issue == "metadata-mismatch")
      val prefix = if (onlyMetadata) "⚠" else "❌"
      warn(s"[SCHEMA] $prefix Type/nullability/metadata differences (showing up to $MaxTypeDiffsToLog of ${r.typeDiffsTotal}):")
      if (r.typeDiffs.nonEmpty) {
        val toShow = r.typeDiffs.map { d =>
          val nul = (d.refNullable, d.newNullable) match {
            case (Some(rn), Some(nn)) => s" | nullable $rn vs $nn"
            case _ => ""
          }
          s"  - ${d.name}: ${d.issue} | type ${d.refType} vs ${d.newType}$nul"
        }.mkString("\n")
        warn(toShow)
      }
      if (r.typeDiffsTotal > r.typeDiffs.size) {
        warn(s"[SCHEMA] ... (${r.typeDiffsTotal - r.typeDiffs.size} additional type differences)")
      }

      // Explain impact
      if (onlyMetadata) {
        info("[SCHEMA] → Impact: MINIMAL - Only metadata differs (Hive comments, etc.), values will compare correctly")
      } else {
        val hasTypeMismatch = r.typeDiffs.exists(_.issue == "type-mismatch")
        val hasNullableMismatch = r.typeDiffs.exists(_.issue == "nullable-mismatch")
        if (hasTypeMismatch) {
          warn("[SCHEMA] → Impact: CRITICAL - Type mismatches may cause comparison errors or incorrect results")
        }
        if (hasNullableMismatch) {
          info("[SCHEMA] → Impact: MODERATE - Nullability differences may affect null handling in joins")
        }
      }
    }
  }

  private def logSummary(r: SchemaReport, info: String => Unit, warn: String => Unit): Unit = {
    val identical =
      r.orderMatches &&
        r.missingInNew.isEmpty &&
        r.missingInRef.isEmpty &&
        r.typeDiffsTotal == 0

    if (identical) {
      info("[SCHEMA] ✓ REF and NEW are IDENTICAL (names + order + types + nullability + metadata).")
      info("[SCHEMA] → Ready to compare: All columns will be compared")
    } else {
      // Calculate comparable columns
      val commonCols = r.refColCount - r.missingInNew.size
      val onlyMetadata = r.typeDiffsTotal > 0 && r.typeDiffs.forall(_.issue == "metadata-mismatch")
      val critical = r.typeDiffs.exists(d => d.issue == "type-mismatch" || d.issue == "nullable-mismatch")

      if (critical) {
        warn("[SCHEMA] ❌ CRITICAL schema differences detected - Review type/nullability mismatches above")
      } else if (onlyMetadata || r.missingInNew.nonEmpty || r.missingInRef.nonEmpty || !r.orderMatches) {
        warn("[SCHEMA] ⚠ Schema differences detected (see details above) - Non-critical, comparison will proceed")
      }

      info(s"[SCHEMA] → Ready to compare: $commonCols common columns (excluding partition columns and mismatches)")
    }
  }
}