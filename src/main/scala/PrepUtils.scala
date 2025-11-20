import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
/** Prep helpers: target partitions, select/repartition, compare columns, file logging. */
object PrepUtils {
  def pickTargetPartitions(spark: SparkSession): Int = {
    val base = spark.sparkContext.defaultParallelism
    math.max(128, math.min(512, base * 4)) // high fan-out; AQE will coalesce
  }
  def selectAndRepartition(df: DataFrame, neededCols: Seq[String], keyCols: Seq[String], nParts: Int): DataFrame =
    df.select(neededCols.map(col): _*).repartition(nParts, keyCols.map(col): _*)
  def computeColsToCompare(
                            rawRef: DataFrame,
                            partitionSpec: Option[String],
                            ignoreCols: Seq[String],
                            compositeKeyCols: Seq[String]
                          ): Seq[String] = {
    val partitionKeys =
      partitionSpec.map(_.split("/").map(_.split("=", 2)(0).trim).toSet).getOrElse(Set.empty[String])
    rawRef.columns.toSeq
      .filterNot(ignoreCols.contains)
      .filterNot(partitionKeys.contains)
      .filterNot(compositeKeyCols.contains)
  }
  def logFilteredInputFiles(refDf: DataFrame, newDf: DataFrame, info: String => Unit): Unit = {
    try {
      val refFiles = refDf.inputFiles.length
      val newFiles = newDf.inputFiles.length
      info(s"[PARTITIONS] ✓ Filtered input files: REF=$refFiles files, NEW=$newFiles files")
      info(s"[PARTITIONS] → Partition filters applied successfully")
    } catch { case _: Throwable => () }
  }
}