import org.apache.spark.sql.SparkSession

import java.util.Properties

trait ISubProcessSelectorApp {
  def getApp(args: Array[String], app: String)(implicit spark: SparkSession, properties: Properties): Unit = {
    // Default empty implementation — optional to override
  }
  def getApp(args: Array[String], appName: String, subProcess: String)(implicit spark: SparkSession, properties: Properties): Unit = {
    // Default empty implementation — optional to override
  }
}
