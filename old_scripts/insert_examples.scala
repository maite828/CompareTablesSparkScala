import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

val spark = SparkSession.builder()
  .appName("Insert into Hive-like Tables")
  .enableHiveSupport()
  .getOrCreate()

val partitionHour = java.time.LocalDateTime.now().withMinute(0).withSecond(0).withNano(0).toString.replace("T", "_")

// 1. Datos para customer_differences
val diffData = Seq(
  ("1", "amount", "100.50", "100.49", "DIFERENCIA"),
  ("2", "status", "pending", "expired", "DIFERENCIA"),
  ("3", "country", "MX", null, "ONLY_IN_REF"),
  ("6", "country", null, "DE", "ONLY_IN_NEW")
)

val diffDf = spark.createDataFrame(diffData).toDF("id", "Columna", "Valor_ref", "Valor_new", "Resultado")
  .withColumn("partition_hour", lit(partitionHour))

diffDf.write.mode("append").insertInto("customer_differences")

// 2. Datos para customer_summary
val summaryData = Seq(
  ("COINCIDE", 48L, "68.57")
)

val summaryDf = spark.createDataFrame(summaryData).toDF("Resultado", "count", "% Ref")
  .withColumn("partition_hour", lit(partitionHour))

summaryDf.write.mode("append").insertInto("customer_summary")

// 3. Datos para customer_duplicates
val dupSchema = StructType(Seq(
  StructField("origin", StringType),
  StructField("id", StringType),
  StructField("exact_duplicates", BooleanType),
  StructField("total", IntegerType),
  StructField("variations", ArrayType(
    StructType(Seq(
      StructField("row", StructType(Seq(
        StructField("country", StringType),
        StructField("amount", DoubleType),
        StructField("status", StringType)
      )))
    ))
  ))
))

val dupData = Seq(
  Row("ref", "5", false, 2, Seq(
    Row(Row("FR", 300.0, "active")),
    Row(Row("FR", 300.5, "active"))
  )),
  Row("new", "6", false, 3, Seq(
    Row(Row("DE", 400.0, "new")),
    Row(Row("DE", 400.0, "new")),
    Row(Row("DE", 400.1, "new"))
  ))
)

val dupDf = spark.createDataFrame(
  spark.sparkContext.parallelize(dupData),
  dupSchema
).withColumn("partition_hour", lit(partitionHour))

dupDf.write.mode("append").insertInto("customer_duplicates")

println(s"✅ Datos de prueba insertados con partición: $partitionHour")

