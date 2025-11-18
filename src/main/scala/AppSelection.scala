import org.apache.spark.sql.SparkSession

import java.util.Properties

class AppSelection extends ISubProcessSelectorApp {

  override def getApp(args: Array[String], app: String)(implicit spark: SparkSession, properties: Properties): Unit = {
    app match {
      case "TableComparatorApp" => TableComparatorApp.execute(args)
      case _ => throw new Exception(s"Unknown process: $app")
    }
  }
}