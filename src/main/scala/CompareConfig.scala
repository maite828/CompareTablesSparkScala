
import org.apache.spark.sql.SparkSession
import java.time.LocalDate
import java.io.File

// Supported aggregation types for overrides
sealed trait AggType
object AggType {
  case object MaxAgg          extends AggType
  case object MinAgg          extends AggType
  case object FirstNonNullAgg extends AggType
}

// Configuration parameters for the entire comparison
final case class CompareConfig(
                                spark: SparkSession,
                                refTable: String,
                                newTable: String,
                                partitionSpec: Option[String],                 // Some("""geo="ES"/data_date_part="2025-08-14"""")
                                compositeKeyCols: Seq[String],                 // composite keys
                                ignoreCols: Seq[String],                       // columns to ignore
                                initiativeName: String,                        // label
                                tablePrefix: String,                           // "default.result_"
                                outputBucket: String = new File("spark-warehouse/results").toURI.toString.stripSuffix("/"), // "s3a://my-bucket/results/"
                                checkDuplicates: Boolean = false,
                                includeEqualsInDiff: Boolean = false,
                                autoCreateTables: Boolean = true,
                                exportExcelPath: Option[String] = None,        // Some("s3a://my-bucket/results/summary.xlsx")

                                // Fields for Diff/Duplicates
                                priorityCols: Seq[String] = Seq.empty,     // columns to prioritize rows (ordered by precedence, desc_nulls_last)
                                aggOverrides: Map[String, String] = Map.empty, // Map("amount" -> "max")
                                nullKeyMatches: Boolean = true,                // null-safe equality in keys

                                // Date to write in output tables (default today in ISO)
                                outputDateISO: String = LocalDate.now.toString, // yyyy-MM-dd

                                // NEW: per-side partition spec overrides (highest precedence over partitionSpec)
                                refPartitionSpecOverride: Option[String] = None,
                                newPartitionSpecOverride: Option[String] = None,

                                // NEW: optional filters per side (applied right after reading)
                                refFilter: Option[String] = None,
                                newFilter: Option[String] = None,

                                // NEW: Column mapping (refName -> newName) to rename columns in NEW table before comparison
                                columnMapping: Map[String, String] = Map.empty
                              )
