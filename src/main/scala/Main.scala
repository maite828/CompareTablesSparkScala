import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.util.Properties

object Main extends Logging {

  /** KV-mode si hay >=1 args y al menos uno contiene '='. */
  private def isKvMode(args: Array[String]): Boolean = {
    args != null && args.nonEmpty && args.exists(a => a != null && a.contains("="))
  }

  def main(args: Array[String]): Unit = {
    // ---- Traza de argumentos (log + println) ----
    log.info("Arguments received:")
    println("Arguments received:")
    if (args != null) {
      args.zipWithIndex.foreach { case (arg, idx) =>
        log.info(s"  Arg[$idx]: $arg")
        println(s"  Arg[$idx]: $arg")
      }
    } else {
      log.info("  <null args>")
      println("  <null args>")
    }

    if (isKvMode(args)) {
      // -------------------- KV MODE (único soportado) --------------------
      log.info("Detected KV mode (key=value arguments).")
      println("Detected KV mode (key=value arguments).")

      val spark = SparkSession.getActiveSession.getOrElse(
        SparkSession.builder().appName("AML-Internal-Tools").getOrCreate()
      )
      val properties = new Properties()

      // Pasamos los args tal cual a la app del comparador
      new AppSelection().getApp(args, "TableComparatorApp")(spark, properties)

      // Traza final de las tablas generadas (útil en logs Airflow)
      val kv = parseKvArgs(args)
      val prefix     = kv.getOrElse("tablePrefix", "")
      val initiative = kv.getOrElse("initiativeName", "")
      val datePart   = kv.getOrElse("executionDate", LocalDate.now.toString)
      if (prefix.nonEmpty && initiative.nonEmpty) {
        showComparisonResults(spark, prefix, initiative, datePart)
      } else {
        log.info("[INFO] No tablePrefix/initiativeName provided; skipping table dumps.")
      }

      log.info("Process completed successfully (KV mode).")
      println("Process completed successfully (KV mode).")

    } else {
      // -------------------- LEGACY MODE (sin cambios) --------------------
      log.info("KV mode not detected. Falling back to legacy mode.")
      println("KV mode not detected. Falling back to legacy mode.")

      if (args == null || args.length < 3) {
        val msg =
          "Usage (legacy): <propertiesFilePath> <processType> <processName>\n" +
            "Or (KV-mode): key=value arguments, e.g.\n" +
            "  refTable=db.table newTable=db.table initiativeName=swift tablePrefix=default.resutados_ " +
            "  outputBucket=s3a://bucket/path executionDate=yyyy-MM-dd partitionSpec=geo=*/data_date_part=YYYY-MM-dd/ " +
            "  compositeKeyCols=id ignoreCols=last_update checkDuplicates=true includeEqualsInDiff=false"
        log.error(msg)
        println(msg)
        sys.exit(1)
      }

      val (spark, properties) = ImplicitContext.initialize(args(0))
      new AppSelection().getApp(args, args(1))(spark, properties)

      log.info("Process completed successfully (legacy mode).")
      println("Process completed successfully (legacy mode).")
    }
  }

  /** Construye un map key=value ignorando args mal formados. */
  private def parseKvArgs(args: Array[String]): Map[String, String] =
    Option(args).map(_.toSeq).getOrElse(Seq.empty)
      .flatMap { arg =>
        Option(arg).flatMap { a =>
          a.split("=", 2) match {
            case Array(k, v) if k.nonEmpty => Some(k -> v)
            case _ => None
          }
        }
      }.toMap

  /** Muestra resultados filtrados por initiative y fecha (data_date_part). */
  private def showComparisonResults(
      spark: SparkSession,
      prefix: String,
      initiative: String,
      datePart: String
  ): Unit = {
    def q(table: String) =
      s"""
         |SELECT *
         |FROM $table
         |WHERE initiative = '$initiative'
         |  AND data_date_part = '$datePart'
         |""".stripMargin

    log.info("\n-- Available Hive tables --")
    spark.sql("SHOW TABLES").show(false)

    log.info(s"\n-- Differences (${prefix}differences) --")
    spark.sql(q(prefix + "differences")).show(100, false)

    log.info(s"\n-- Summary (${prefix}summary) --")
    spark.sql(q(prefix + "summary")).show(100, false)

    log.info(s"\n-- Duplicates (${prefix}duplicates) --")
    spark.sql(q(prefix + "duplicates")).show(100, false)
  }
}
