
package com.santander.cib.adhc.internal_aml_tools

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.util.Properties

object Main2 extends Logging {

  /** KV-mode si hay >=1 args y al menos uno contiene '='. */
  private def isKvMode(args: Array[String]): Boolean = {
    args != null && args.nonEmpty && args.exists(a => a != null && a.contains("="))
  }

  def main(args: Array[String]): Unit = {
    // ---- Traza de argumentos (log + println) ----
    logger.info("Arguments received:")
    println("Arguments received:")
    if (args != null) {
      args.zipWithIndex.foreach { case (arg, idx) =>
        logger.info(s"  Arg[$idx]: $arg")
        println(s"  Arg[$idx]: $arg")
      }
    } else {
      logger.info("  <null args>")
      println("  <null args>")
    }

    if (isKvMode(args)) {
      // -------------------- KV MODE (único soportado) --------------------
      logger.info("Detected KV mode (key=value arguments).")
      println("Detected KV mode (key=value arguments).")

      val spark = SparkSession.getActiveSession.getOrElse(
        SparkSession.builder().appName("AML-Internal-Tools").getOrCreate()
      )
      val properties = new Properties()

      // Pasamos los args tal cual a la app del comparador
      new AppSelection().getApp(args, "TableComparatorApp")(spark, properties)

      logger.info("Process completed successfully (KV mode).")
      println("Process completed successfully (KV mode).")

    } else {
      // -------------------- LEGACY MODE (sin cambios) --------------------
      logger.info("KV mode not detected. Falling back to legacy mode.")
      println("KV mode not detected. Falling back to legacy mode.")

      if (args == null || args.length < 3) {
        val msg =
          "Usage (legacy): <propertiesFilePath> <processType> <processName>\n" +
            "Or (KV-mode): key=value arguments, e.g.\n" +
            "  refTable=db.table newTable=db.table initiativeName=swift tablePrefix=default.resutados_ " +
            "  outputBucket=s3a://bucket/path executionDate=yyyy-MM-dd partitionSpec=geo=*/data_date_part=YYYY-MM-dd/ " +
            "  compositeKeyCols=id ignoreCols=last_update checkDuplicates=true includeEqualsInDiff=false"
        logger.error(msg)
        println(msg)
        sys.exit(1)
      }

      val (spark, properties) = ImplicitContext.initialize(args(0))
      new AppSelection().getApp(args, args(1))(spark, properties)

      logger.info("Process completed successfully (legacy mode).")
      println("Process completed successfully (legacy mode).")
    }
  }
}
