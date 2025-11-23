import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/** Diffs & dups builders (pure computations; side effects handled by controller). */
object ComparisonBuilders {

  def computeDifferences(
                          spark: SparkSession,
                          refDf: DataFrame,
                          newDf: DataFrame,
                          compositeKeyCols: Seq[String],
                          colsToCompare: Seq[String],
                          includeEqualsInDiff: Boolean,
                          config: CompareConfig
                        ): DataFrame = {
    val df = DiffGenerator.generateDifferencesTable(
      spark, refDf, newDf, compositeKeyCols, colsToCompare, includeEqualsInDiff, config
    )
    // PERF OPTIMIZATION: Use SER for 30-50% less memory usage (serialized + compressed)
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def computeDuplicates(
                         spark: SparkSession,
                         refDf: DataFrame,
                         newDf: DataFrame,
                         compositeKeyCols: Seq[String],
                         config: CompareConfig
                       ): DataFrame =
    DuplicateDetector.detectDuplicatesTable(spark, refDf, newDf, compositeKeyCols, config)
}
