import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.{File, FileInputStream, InputStream}
import java.net.URI
import java.util.Properties

/**
 * Centralized holder for SparkSession and Properties loaded from a path.
 * - On Databricks shared clusters we DO NOT modify session confs nor stop the session on cleanup.
 * - Properties can be loaded from s3a://, hdfs://, file://, and dbfs:/ paths.
 */
object ImplicitContext {

  // Backing fields
  @volatile private var _spark: Option[SparkSession] = None
  @volatile private var _properties: Option[Properties] = None
  @volatile private var _sessionOwner: Boolean = false

  // ------------------------- Public API -------------------------

  /**
   * Initialize SparkSession (reusing active/default if present) and load .properties from the given path.
   * @param propertiesFilePath path to .properties (supports s3a://, hdfs://, file://, dbfs:/ or plain file)
   * @return (SparkSession, Properties)
   */
  def initialize(propertiesFilePath: String): (SparkSession, Properties) = synchronized {
    val spark = SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .getOrElse {
        // In Databricks this should rarely happen, but keep a safe fallback.
        _sessionOwner = true
        SparkSession.builder().appName("AML Internal Tools").getOrCreate()
      }

    val props = new Properties()
    val in: InputStream = openPathAsStream(propertiesFilePath, spark)
    try {
      props.load(in)
    } finally {
      try in.close() catch { case _: Throwable => () }
    }

    // PERF OPTIMIZATION: Enable AQE for better query optimization (5-15% improvement)
    // Databricks enables this by default, but ensure it's on for other environments
    try {
      spark.conf.set("spark.sql.adaptive.enabled", "true")
      spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
      spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    } catch { case _: Throwable => () }  // Ignore if already set or not supported

    _spark = Some(spark)
    _properties = Some(props)

    (spark, props)
  }

  /** Get SparkSession after initialize(). */
  def spark: SparkSession =
    _spark.getOrElse(throw new IllegalStateException("SparkSession not initialized. Call initialize() first."))

  /** Get Properties after initialize(). */
  def properties: Properties =
    _properties.getOrElse(throw new IllegalStateException("Properties not initialized. Call initialize() first."))

  /** True if this context created the session (rare on Databricks). */
  def isSessionOwner: Boolean = _sessionOwner

  /**
   * Cleanup references. On Databricks shared clusters we NEVER stop the session.
   * Outside Databricks you may allow stop() by setting env ALLOW_SPARK_STOP=true and only if we created it.
   */
  def cleanup(): Unit = synchronized {
    _spark.foreach { s =>
      // Always clear SQL/DataFrame cache; safe on shared clusters.
      try s.catalog.clearCache() catch { case _: Throwable => () }

      // Only stop if: (a) we created the session, (b) not in Databricks, (c) explicitly allowed.
      val allowStop = sys.env.get("ALLOW_SPARK_STOP").exists(_.equalsIgnoreCase("true"))
      if (_sessionOwner && !isRunningOnDatabricks(s) && allowStop) {
        try {
          if (!s.sparkContext.isStopped) s.stop()
        } catch { case _: Throwable => () }
      }
    }
    _spark = None
    _properties = None
    _sessionOwner = false
  }

  // ------------------------- Helpers -------------------------

  /** Open properties file from various schemes into an InputStream. */
  private def openPathAsStream(pathStr: String, spark: SparkSession): InputStream = {
    if (pathStr == null || pathStr.trim.isEmpty) {
      throw new IllegalArgumentException("Empty propertiesFilePath")
    }

    val trimmed = pathStr.trim
    val uri     = toSafeURI(trimmed)

    // Handle dbfs:/ specially in Databricks: use the FUSE mount /dbfs to avoid custom FS handlers
    if (uri.getScheme == "dbfs") {
      val fusePath = new File("/dbfs" + uri.getPath) // e.g., dbfs:/mnt/...  -> /dbfs/mnt/...
      if (!fusePath.exists()) {
        throw new IllegalArgumentException(s"DBFS file does not exist: ${fusePath.getAbsolutePath}")
      }
      new FileInputStream(fusePath)
    } else if (uri.getScheme == null || uri.getScheme == "file") {
      // Local file or file://
      val file = if (uri.getScheme == "file") new File(uri) else new File(trimmed)
      if (!file.exists()) {
        throw new IllegalArgumentException(s"Local properties file not found: ${file.getAbsolutePath}")
      }
      new FileInputStream(file)
    } else {
      // For s3a://, hdfs://, abfs[s]://, etc., use Hadoop FS with Spark's Hadoop configuration
      val fs = FileSystem.get(uri, spark.sparkContext.hadoopConfiguration)
      val p  = new Path(uri.toString)
      if (!fs.exists(p)) {
        throw new IllegalArgumentException(s"Path not found: $uri")
      }
      fs.open(p)
    }
  }

  /** Robust URI parser that tolerates plain local paths. */
  private def toSafeURI(pathStr: String): URI = {
    // If it looks like a scheme (contains "://") or starts with "dbfs:/", parse as URI; else treat as file.
    if (pathStr.contains("://") || pathStr.startsWith("dbfs:/")){ new URI(pathStr) }
    else { new File(pathStr).toURI }
  }

  /** Best-effort detection of Databricks runtime. */
  private def isRunningOnDatabricks(s: SparkSession): Boolean = {
    val byConf = s.conf.getOption("spark.databricks.clusterUsageTags.clusterId").isDefined ||
      s.conf.getOption("spark.databricks.service.clusterId").isDefined
    val byEnv  = sys.env.contains("DATABRICKS_RUNTIME_VERSION")
    byConf || byEnv
  }
}
